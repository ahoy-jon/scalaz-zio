package scalaz.zio

import scalaz.zio.Circuit._
import scalaz.zio.Schedule.Decision

import scala.concurrent.duration.{ Duration, FiniteDuration }

final class Circuit[R, S](rejected: R, policy: Ref[Schedule[Unit, CircuitState]], max: Int, reset: FiniteDuration) {
  def close(f: Int): Schedule[Unit, CircuitState] = { //TODO: improve it using reconsider
    val now = System.currentTimeMillis()
    Schedule[CircuitState, Unit, CircuitState](IO.now(CircuitState(now, Closed(f))),
                                               (_, s) => IO.now(Decision.done(Duration.Zero, s, s)))
  }
  lazy val open: Schedule[Unit, CircuitState] = { //TODO
    val now = System.currentTimeMillis()
    Schedule[CircuitState, Unit, CircuitState](
      IO.now(CircuitState(now, Open)),
      (_, s) => IO.now(Decision.cont(reset, s, CircuitState(now + reset.toMillis, Open)))
    )
  }

  lazy val halfOpen: Schedule[Unit, CircuitState] = { //TODO
    val now = System.currentTimeMillis()
    Schedule[CircuitState, Unit, CircuitState](
      IO.now(CircuitState(now, HalfOpen)),
      (_, s) => IO.now(Decision.cont(reset, s, CircuitState(reset.toMillis, HalfOpen)))
    )
  }

  def markFailures[E, A](io: IO[E, A]): IO[E, A] =
    io.redeem(
      err => {

        policy.get.flatMap { schedule =>
          schedule.initial.flatMap {
            case CircuitState(_, Closed(f)) =>
              policy.update(_ => if (f < max) close(f + 1) else open)
            case _ => IO.unit
          }
        } *> IO.fail(err)
      },
      succ => policy.update(_ => close(0)) *> IO.now(succ)
    )

  def process[E, A](io: IO[E, A]): IO[Any, A] =
    policy.get.flatMap { schedule =>
      schedule.initial.flatMap { state =>
        state match { //compiler will complain if we call update with the matched value :/ -_-
          case CircuitState(_, Closed(_)) => markFailures(io)
          case CircuitState(_, Open) =>
            val now = System.currentTimeMillis()
            schedule.update((), state).flatMap { d =>
              if (d.finish().currentMS < now)
                policy.update(_ => halfOpen) *> io.redeem(err => policy.update(_ => open) *> IO.fail(err),
                                                          succ => policy.update(_ => close(0)) *> IO.now(succ))
              else IO.fail(rejected)
            }
          case CircuitState(_, HalfOpen) => IO.fail(rejected)
        }
      }

    }

}

object Circuit {
  sealed trait State
  final case class Closed(nbErr: Int) extends State
  final case object Open              extends State
  final case object HalfOpen          extends State

  case class CircuitState(currentMS: Long, state: State)

  /**
   * Create a new circuit breaker with the given default error for rejected tasks,
   * the maximum number of failure and the given policy to use it in open state
   *
   */
  def apply[R, A](rejected: R, max: Int, reset: FiniteDuration): IO[Nothing, Circuit[R, A]] = {
    val init: Schedule[Unit, CircuitState] =
      Schedule[CircuitState, Unit, CircuitState](
        IO.now(CircuitState(System.currentTimeMillis(), Closed(0))),
        (_, s) => IO.now(Decision.done(Duration.Zero, s, s))
      )
    Ref[Schedule[Unit, CircuitState]](init).map(state => new Circuit[R, A](rejected, state, max, reset))
  }

}
