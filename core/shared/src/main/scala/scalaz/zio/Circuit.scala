package scalaz.zio

import scalaz.zio.Circuit.CircuitStatus.{ Closed, HalfOpen, Open }
import scalaz.zio.Circuit._

import scala.concurrent.duration.Duration

final class Circuit[+R](state: Ref[CircuitStatus],
                        maxFailures: Int,
                        durationO: Duration,
                        durationH: Duration,
                        rejected: R) {

  def process[E >: R, A](io: IO[E, A]): IO[E, A] = {
    val now: Long = System.currentTimeMillis()

    io.attempt.flatMap {
      case Right(_) =>
        IO.flatten(state.modify {
          case Open(d) =>
            val newState =
              if (d < now) HalfOpen(durationH.toMillis + now)
              else Open(d)
            IO.fail(rejected) -> newState
          case _ => io -> Closed(0)
        })
      case Left(err) =>
        IO.flatten(state.modify {
          case Closed(failureCount) =>
            val newState =
              if (failureCount < maxFailures)
                Closed(failureCount + 1)
              else Open(durationO.toMillis + now)
            IO.fail(err) -> newState

          case Open(d) =>
            val newState =
              if (d < now) HalfOpen(durationH.toMillis + now)
              else Open(d)
            IO.fail(rejected) -> newState
          case HalfOpen(d) => IO.fail(rejected) -> Open(d + now)
        })
    }
  }

  def getCurrentState: IO[Nothing, CircuitStatus] = state.get
}

object Circuit {

  sealed trait CircuitStatus
  object CircuitStatus {
    case class Closed(maxFailure: Int)  extends CircuitStatus
    case class Open(duration: Long)     extends CircuitStatus
    case class HalfOpen(duration: Long) extends CircuitStatus
  }

  def apply[E](maxFailures: Int, durationO: Duration, durationH: Duration, rejected: E): IO[Nothing, Circuit[E]] =
    Ref[CircuitStatus](CircuitStatus.Closed(0))
      .map(status => new Circuit(status, maxFailures, durationO, durationH, rejected))
}
