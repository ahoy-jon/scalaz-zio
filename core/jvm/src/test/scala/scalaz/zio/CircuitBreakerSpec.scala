package scalaz.zio

import scalaz.zio.CircuitBreaker.Open

class CircuitBreakerSpec extends AbstractRTSSpec {
  def is = "CircuitBreakerSpec".title ^ s2"""
     protect must work correctly $e1
     circuit breaker using schedule $e2
    """

  sealed trait State
  object State {
    case object Failed      extends State
    case object Success     extends State
    case object CircuitOpen extends State
  }

  import State._

  def e1 =
    unsafeRun(for {
      circuit <- CircuitBreaker(2, IO.fail(CircuitOpen))
      v       <- circuit.protect(IO.fail(Failed)).attempt
      nbR1    <- circuit.nbRemainingFailure
      w       <- circuit.protect(IO.point(Success)).attempt
      nbR2    <- circuit.nbRemainingFailure
      _       <- circuit.protect(IO.fail(Failed)).attempt
      nbR3    <- circuit.nbRemainingFailure
      _       <- circuit.protect(IO.fail(Failed)).attempt
      nbR4    <- circuit.nbRemainingFailure
      status  <- circuit.status
      x       <- circuit.protect(IO.point(Success)).attempt
      nbR5    <- circuit.nbRemainingFailure
      y       <- circuit.protect(IO.fail(Failed)).attempt
    } yield {
      (v must beLeft[State](Failed))
        .and(nbR1 must_=== 1)
        .and(w must beRight[State](Success))
        .and(nbR2 must_=== 2)
        .and(nbR3 must_=== 1)
        .and(nbR4 must_=== 0)
        .and(status must_=== Open)
        .and(nbR5 must_=== 0)
        .and(x must beLeft[State](CircuitOpen))
        .and(y must beLeft[State](CircuitOpen))
    })

  import scala.concurrent.duration._
  def e2 =
    {
      val policy =  Schedule.spaced(1.second)
      unsafeRun(for {
        circuit <- Circuit[State, String, Int]("Rejected", 1, policy)
        v       <- circuit.process(IO.fail(Failed)).attempt
        nbR1 <- circuit.currentState
        w       <- circuit.process(IO.point(Success)).attempt
        nbR2    <- circuit.currentState
        _       <- circuit.process(IO.fail(Failed)).attempt
        nbR3    <- circuit.currentState
        _       <- circuit.process(IO.fail(Failed)).attempt
        nbR4    <- circuit.currentState
        x       <- circuit.process(IO.fail(Failed)).attempt
        nbR5    <- circuit.currentState
        y       <- circuit.process(IO.point(Success)).attempt
        nbR6    <- circuit.currentState
        z <- IO.sleep(2.seconds) *> circuit.process(IO.point(Success)).attempt
        nbR7 <- circuit.currentState
      } yield {
        (v must beLeft[Any](Failed))
          .and(nbR1 must_=== Circuit.Closed(1))
          .and(w must beRight[State](Success))
          .and(nbR2 must_=== Circuit.Closed(0))
          .and(nbR3 must_=== Circuit.Closed(1))
          .and(nbR4 must_=== Circuit.Open(Failed, policy))
          .and(x must beLeft[Any]("Rejected"))
          .and(nbR5 must_=== Circuit.Open(Failed, policy))
          .and(y must beLeft[Any]("Rejected"))
          .and(nbR6 must_=== nbR5)
          .and(z must beRight[State](Success))
          .and(nbR7 must_=== Circuit.Closed(0))
      })
    }

}
