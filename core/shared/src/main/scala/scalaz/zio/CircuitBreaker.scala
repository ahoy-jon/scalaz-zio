package scalaz.zio

import scalaz.zio.CircuitBreaker.CircuitStatus.{ Closed, HalfOpen, Open }
import scalaz.zio.CircuitBreaker._

import scala.concurrent.duration.Duration

final class CircuitBreaker[+R] private (state: Ref[CircuitStatus], circuitConfiguration: CircuitConfiguration[R]) {

  final def apply[E >: R, A](io: IO[E, A]): IO[E, A] =
    for {
      now          <- scalaz.zio.system.currentTimeMillis
      _            <- tick
      currentState <- state.get
      x <- currentState match {
            case Open(_) => IO.fail(circuitConfiguration.rejected)
            case _ =>
              io.redeem(err => {
                state.update(_.fail(now, circuitConfiguration)) *> IO.fail(err)
              }, value => {
                state.update(_.succ()) *> IO.point(value)
              })
          }
    } yield x

  final def status: IO[Nothing, CircuitStatus] = state.get

  final def tick: IO[Nothing, Unit] =
    for {
      now <- scalaz.zio.system.currentTimeMillis
      _   <- state.update(_.checkStatus(now, circuitConfiguration))
    } yield {}

  final def forceOpen: IO[Nothing, Unit] =
    for {
      now <- scalaz.zio.system.currentTimeMillis
      _   <- state.set(Open(circuitConfiguration.durationOpen.toMillis + now))

    } yield {}
}

object CircuitBreaker {

  case class CircuitConfiguration[+E](maxFailures: Int, durationOpen: Duration, durationH: Duration, rejected: E)

  sealed trait CircuitStatus {
    self =>

    final def checkStatus(now: Long, conf: CircuitConfiguration[_]): CircuitStatus =
      self match {
        case Open(forceOpenUntil) if forceOpenUntil < now => HalfOpen(conf.durationH.toMillis + now)
        case _                                            => self
      }

    final def succ(): CircuitStatus =
      self match {
        case Open(_) => self
        case _       => Closed(0)
      }

    final def fail(now: Long, conf: CircuitConfiguration[_]): CircuitStatus =
      self match {
        //TODO move to checkStatus
        case Closed(failures) if failures < conf.maxFailures => Closed(failures + 1)
        case Closed(_)                                       => Open(conf.durationOpen.toMillis + now)
        //BUG ! ?
        case HalfOpen(d) => Open(d + now)
        case _           => self
      }

    final def success[E](now: Long, conf: CircuitConfiguration[E]): CircuitStatus =
      self match {
        case Open(forceOpenUntil) if forceOpenUntil < now => HalfOpen(conf.durationH.toMillis + now)
        case _                                            => Closed(0)
      }
  }

  object CircuitStatus {

    final case class Closed(failures: Int) extends CircuitStatus

    final case class Open(forceOpenUntil: Long) extends CircuitStatus

    final case class HalfOpen(retryUntil: Long) extends CircuitStatus

  }

  final def apply[E](maxFailures: Int,
                     durationO: Duration,
                     durationH: Duration,
                     rejected: E): IO[Nothing, CircuitBreaker[E]] = {
    val configuration = CircuitConfiguration[E](maxFailures, durationO, durationH, rejected)
    Ref[CircuitStatus](CircuitStatus.Closed(0))
      .map(status => new CircuitBreaker(status, configuration))
  }
}
