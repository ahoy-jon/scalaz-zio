package scalaz.zio

import scalaz.zio.CircuitBreaker.CircuitStatus.{Closed, HalfOpen, Open}
import scalaz.zio.CircuitBreaker._

import scala.concurrent.duration.Duration

final class CircuitBreaker[+R] private(
                                                     state: Ref[CircuitStatus[Any]],
                                                     circuitConfiguration: CircuitConfiguration[Any],
                                                     rejectedReplacement: RejectedReplacement[R]//,
                                                     //statusChangeCallbacks: StatusChangeCallbacks,
                                                     //rejectionCallbacks: RejectionCallbacks
                                                   ) {

  def apply[E >: R, A](io: IO[E, A]): IO[E, A] =
    for {
      now <- scalaz.zio.system.currentTimeMillis
      _ <- tick
      currentState <- state.get
      x <- currentState match {
        case Open(openUtil) => IO.fail(rejectedReplacement.rejectedWhenOpen(openUtil - now))
        case _ =>
          io.redeem(err => {
            (for {
              currentState <- state.get
              newState <- currentState.fail(now, circuitConfiguration)
              _ <- {
                state.set(newState)
              }
            } yield {}) *> IO.fail(err)

          }, value => {
            (for {
              currentState <- state.get
              newState <- currentState.succ(configuration = circuitConfiguration)
              _ <- state.set(newState)
            } yield {}) *> IO.point(value)
          })
      }
    } yield x

  def status: IO[Nothing, CircuitStatus[Any]] = state.get

  def tick: IO[Nothing, Unit] =
    for {
      now <- scalaz.zio.system.currentTimeMillis
      _ <- state.update(_.checkStatus(now, circuitConfiguration))
    } yield {}

  def forceOpen: IO[Nothing, Unit] =
    for {
      now <- scalaz.zio.system.currentTimeMillis
      _ <- state.set(Open(circuitConfiguration.durationOpen.toMillis + now))

    } yield {}
}

object CircuitBreaker {

  type ScheduleAux[S] = Schedule[Any, Any] {
    type State = S
  }

  trait RejectedReplacement[+R] {
    def rejectedWhenOpen(circuitWillTryToResetIn: Long): R

    def rejectedWhenHalfOpen: R
  }

  case class RejectionCallbacks(whenOpen: IO[Any, Unit],
                                whenHalfOpen: IO[Any, Unit])

  object RejectionCallbacks {
    val nocallbacks = RejectionCallbacks(IO.unit, IO.unit)
  }

  case class StatusChangeCallbacks(onClosed: IO[Any, Unit],
                                   onHalfOpen: IO[Any, Unit],
                                   onOpen: IO[Any, Unit])


  object StatusChangeCallbacks {

    val nocallbacks = StatusChangeCallbacks(IO.unit, IO.unit, IO.unit)


  }

  case class CircuitConfiguration[+X](keepClosed: Schedule[Any,Any], durationOpen: Duration, durationH: Duration)

  sealed trait CircuitStatus[+X] {

    self =>
    type ClosedState = Any


    final def checkStatus[NClosedState >: ClosedState](now: Long, conf: CircuitConfiguration[NClosedState]): CircuitStatus[NClosedState] =
      self match {
        case Open(forceOpenUntil) if forceOpenUntil < now => HalfOpen(conf.durationH.toMillis + now)
        case _ => self
      }

    final def succ[NClosedState >: ClosedState](configuration: CircuitConfiguration[NClosedState]): IO[Nothing, CircuitStatus[NClosedState]] =
      self match {
        case Open(_) => IO.now(self)
        case _ =>
          val value: IO[Nothing, Closed] = configuration.keepClosed.initial.map(x => Closed(x))
          value
      }

    final def fail[NClosedState >: ClosedState](now: Long, conf: CircuitConfiguration[NClosedState]): IO[Nothing, CircuitStatus[NClosedState]] =
      self match {
        //TODO move to checkStatus
        case Closed(state) =>
          conf.keepClosed.update({}, state.asInstanceOf[conf.keepClosed.State]).map(decision => {
            if (decision.cont)
              Closed(decision.state)
            else {
              Open(conf.durationOpen.toMillis + now)
            }
          })

        //BUG ! ?
        case HalfOpen(d) => IO.now(Open(d + now))
        case _ => IO.now(self)
      }


    final def success[NClosedState >: ClosedState](now: Long, conf: CircuitConfiguration[NClosedState]): IO[Nothing, CircuitStatus[NClosedState]] =
      self match {
        case Open(forceOpenUntil) if forceOpenUntil < now => IO.now(HalfOpen(conf.durationH.toMillis + now))
        case _ => conf.keepClosed.initial.map(state => Closed(state))
      }
  }

  object CircuitStatus {

    final case class Closed(state: Any) extends CircuitStatus[Nothing]

    final case class Open(forceOpenUntil: Long) extends CircuitStatus[Nothing]

    final case class HalfOpen(retryUntil: Long) extends CircuitStatus[Nothing]

  }

  final def apply[E](maxFailures: Int,
                     durationO: Duration,
                     durationH: Duration,
                     rejected: E): IO[Nothing, CircuitBreaker[E]] = {

    val configuration = CircuitConfiguration(Schedule.recurs(maxFailures), durationO, durationH)

    for {
      initialState <- configuration.keepClosed.initial
      status <- Ref[CircuitStatus[Any]](CircuitStatus.Closed(initialState))
    } yield {
      new CircuitBreaker(status, configuration, new RejectedReplacement[E] {
        override def rejectedWhenOpen(circuitWillTryToResetIn: Long): E = rejected

        override def rejectedWhenHalfOpen: E = rejected
      }//,
        //StatusChangeCallbacks.nocallbacks,
        //RejectionCallbacks.nocallbacks

      )
    }
  }
}
