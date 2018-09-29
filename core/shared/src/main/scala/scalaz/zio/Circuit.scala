package scalaz.zio

import scalaz.zio.Circuit._

final class Circuit[R, S](rejected: R, policy: Schedule[Any, S], state: Ref[Circuit.State], max: Int) {

  def process[E, A](io: IO[E, A]): IO[Any, A] =
    currentState.flatMap {
      case Closed(_) => markFailures(io)
      case s @ Open(_, _) =>
        s.isExpired.flatMap {
          case true =>
            state.update(_ => HalfOpen) *> io.redeem(
              err => state.update(_ => Open(err, policy)) *> IO.fail(err),
              succ => state.set(Closed(0)) *> IO.now(succ)
            )
          case false => IO.fail(rejected)
        }
      case _ => IO.fail(rejected)
    }

  private def markFailures[E, A](io: IO[E, A]): IO[E, A] =
    io.redeem(
      err => {
        state.update {
          case Closed(f) =>
            if (f < max)
              Closed(f + 1)
            else Open(err, policy)
          case state => state
        } *> IO.fail(err)
      },
      succ => state.update(_ => Closed(0)) *> IO.now(succ)
    )

  def currentState: IO[Nothing, Circuit.State] = state.get

}

object Circuit {
  sealed trait State
  final case class Closed(nbErr: Int) extends State
  final case class Open[E, S](error: E, policy: Schedule[E, S]) extends State {
    val now: Long = System.currentTimeMillis()
    def isExpired: IO[Nothing, Boolean] = {
      val now: Long = System.currentTimeMillis()
      policy.initial.flatMap(initial => policy.update(error, initial).map(d => this.now + d.delay.toMillis < now))
    }

  }
  final case object HalfOpen extends State

  /**
    * Create a new circuit breaker with the given default error for rejected tasks, and the given policy.
    */
  def apply[E, R, A](rejected: R,
                               max: Int,
                               policy: Schedule[Any, A]): IO[Nothing, Circuit[R, A]] =
    Ref[State](Closed(0)).map(state => new Circuit[R, A](rejected, policy, state, max))

}

