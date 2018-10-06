package scalaz.zio

import scalaz.zio.CircuitBreaker.CircuitStatus.{ Closed, HalfOpen, Open }
import scalaz.zio.CircuitBreaker._

import scala.concurrent.duration.Duration

final class CircuitBreaker[+R] private (state: Ref[CircuitStatus],
                                        circuitConfiguration: CircuitConfiguration,
                                        rejectedReplacement: RejectedReplacement[R] //,
                                        //statusChangeCallbacks: StatusChangeCallbacks,
                                        //rejectionCallbacks: RejectionCallbacks
) {

  import CircuitBreaker._

  def apply[E >: R, A](io: IO[E, A]): IO[E, A] =
    for {
      now          <- scalaz.zio.system.currentTimeMillis
      _            <- tick
      currentState <- state.get
      x <- currentState match {
            case Open(openUtil) => IO.fail(rejectedReplacement.rejectedWhenOpen(openUtil - now))
            case _ =>
              peekRedem(io)(
                _ =>  updateStateWithIO(state)(_.fail(now,circuitConfiguration)),
                _ =>  updateStateWithIO(state)(_.succ(circuitConfiguration)))

          }
    } yield x

  def status: IO[Nothing, CircuitStatus] = state.get

  def tick: IO[Nothing, Unit] =
    for {
      now   <- scalaz.zio.system.currentTimeMillis
      unit  <- state.update(_.checkStatus(now)).void
    } yield unit

  def forceOpen: IO[Nothing, Unit] =
    for {
      now          <- scalaz.zio.system.currentTimeMillis
      initialState <- circuitConfiguration.openPolicy.initial
      decision     <- circuitConfiguration.openPolicy.update({}, initialState)
      unit         <- state.set(Open(decision.delay.toMillis + now))
    } yield unit
}

object CircuitBreaker {

  protected def peekRedem[E,A, E2 >: E](io:IO[E,A])(err:E => IO[E2,Unit], succ: A => IO[E2,Unit]):IO[E2,A] = {
    io.redeem(e => err(e) *> IO.fail(e), a => succ(a) *> IO.now(a))
  }

  protected def updateStateWithIO[X](ref: Ref[X])(f: X => IO[Nothing, X]): IO[Nothing, Unit] =
    for {
      x    <- ref.get
      y    <- f(x)
      unit <- ref.set(y)
    } yield unit


  trait RejectedReplacement[+R] {
    def rejectedWhenOpen(circuitWillTryToResetIn: Long): R

    def rejectedWhenHalfOpen: R
  }

  case class RejectionCallbacks(whenOpen: IO[Any, Unit], whenHalfOpen: IO[Any, Unit])

  object RejectionCallbacks {
    val nocallbacks = RejectionCallbacks(IO.unit, IO.unit)
  }

  case class StatusChangeCallbacks(onClosed: IO[Any, Unit], onHalfOpen: IO[Any, Unit], onOpen: IO[Any, Unit])

  object StatusChangeCallbacks {

    val nocallbacks = StatusChangeCallbacks(IO.unit, IO.unit, IO.unit)

  }

  case class CircuitConfiguration(keepClosed: Schedule[Any, Any], openPolicy: Schedule[Any, Any], durationH: Duration)

  sealed trait CircuitStatus {

    self =>

    final def checkStatus(
      now: Long
      //, conf: CircuitConfiguration
    ): CircuitStatus =
      self match {
        case Open(forceOpenUntil) if forceOpenUntil < now => HalfOpen
        case _                                            => self
      }

    final def succ(conf: CircuitConfiguration): IO[Nothing, CircuitStatus] =
      self match {
        case _: Open => IO.now(self)
        case _       => conf.keepClosed.initial.map(x => Closed(x))
      }

    private final def open(now: Long, conf: CircuitConfiguration): IO[Nothing, Open] =
      for {
        initOpen <- conf.openPolicy.initial
        decision <- conf.openPolicy.update({}, initOpen)
      } yield {
        Open(decision.delay.toMillis + now)
      }

    final def fail(
      now: Long,
      conf: CircuitConfiguration
    ): IO[Nothing, CircuitStatus] =
      self match {
        //TODO move to checkStatus
        case Closed(state) =>
          conf.keepClosed
            .update({}, state.asInstanceOf[conf.keepClosed.State])
            .flatMap(decision => {
              if (decision.cont)
                IO.now(Closed(decision.state))
              else {
                open(now, conf)
              }
            })

        case HalfOpen => open(now, conf)
        case _        => IO.now(self)
      }

    final def success(
      now: Long,
      conf: CircuitConfiguration
    ): IO[Nothing, CircuitStatus] =
      self match {
        case Open(forceOpenUntil) if forceOpenUntil < now => IO.now(HalfOpen)
        case _                                            => conf.keepClosed.initial.map(state => Closed(state))
      }
  }

  object CircuitStatus {

    final case class Closed(state: Any) extends CircuitStatus

    final case class Open(forceOpenUntil: Long) extends CircuitStatus

    final case object HalfOpen extends CircuitStatus

  }

  final def apply[E](maxFailures: Int,
                     durationO: Duration,
                     durationH: Duration,
                     rejected: E): IO[Nothing, CircuitBreaker[E]] = {

    val configuration = CircuitConfiguration(Schedule.recurs(maxFailures + 1), Schedule.spaced(durationO), durationH)

    for {
      initialState <- configuration.keepClosed.initial
      status       <- Ref[CircuitStatus](CircuitStatus.Closed(initialState))
    } yield {
      new CircuitBreaker(
        status,
        configuration,
        new RejectedReplacement[E] {
          override def rejectedWhenOpen(circuitWillTryToResetIn: Long): E = rejected

          override def rejectedWhenHalfOpen: E = rejected
        } //,
        //StatusChangeCallbacks.nocallbacks,
        //RejectionCallbacks.nocallbacks
      )
    }
  }
}
