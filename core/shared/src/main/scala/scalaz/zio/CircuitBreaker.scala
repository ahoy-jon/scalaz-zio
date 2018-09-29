package scalaz.zio

import CircuitBreaker.{Closed, Open, Status}

final class CircuitBreaker[+BreakingFailure](ref:        Ref[CircuitBreakerStatus],
                                             initStatus: CircuitBreakerStatus,
                                             whenOpen:   IO[BreakingFailure, Nothing]) {
  def protect[E >: BreakingFailure, A](io: IO[E, A]): IO[E, A] = {
    for {
      status <- this.status
      x <- {
        if (status == Closed) {
          io.redeem(e => ref.update(_.incFail) *> IO.fail(e), b => ref.set(initStatus) *> IO.point(b))
        } else whenOpen
      }
    } yield {
      x

    }
  }

  def status: IO[Nothing, Status] = ref.get.map(_.status)

  def nbRemainingFailure: IO[Nothing, Long] = ref.get.map(_.nbRemainingFailure)
}

case class CircuitBreakerStatus(nbRemainingFailure: Long, status: Status = Closed) {

  def incFail: CircuitBreakerStatus = {
    if (nbRemainingFailure > 1) {
      copy(nbRemainingFailure = nbRemainingFailure - 1)
    } else {
      copy(nbRemainingFailure = nbRemainingFailure - 1, status = Open)
    }
  }

}

object CircuitBreaker {

  sealed trait Status

  object Closed extends Status

  object Open extends Status

  def apply[BreakingFailure](nbConsecutiveFailure: Long,
                             whenOpen:             IO[BreakingFailure, Nothing]): IO[Nothing, CircuitBreaker[BreakingFailure]] = {
    val initStatus: CircuitBreakerStatus = CircuitBreakerStatus(nbConsecutiveFailure)
    Ref(initStatus).map(ref => {
      new CircuitBreaker[BreakingFailure](ref, initStatus, whenOpen)
    })
  }
}