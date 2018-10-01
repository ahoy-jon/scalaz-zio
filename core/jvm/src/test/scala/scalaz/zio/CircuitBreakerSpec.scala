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
    unsafeRun(for {
      circuit <- Circuit[State](1, 1.second, Duration.Zero, CircuitOpen)
      v       <- circuit.process(IO.fail(Failed)).attempt
      nbR1    <- circuit.getCurrentState
      w       <- circuit.process(IO.point(Success)).attempt
      nbR2    <- circuit.getCurrentState
      _       <- circuit.process(IO.fail(Failed)).attempt
      nbR3    <- circuit.getCurrentState
      _       <- circuit.process(IO.fail(Failed)).attempt
      nbR4    <- circuit.getCurrentState
      x       <- circuit.process(IO.fail(Failed)).attempt
      nbR5    <- circuit.getCurrentState
      y       <- circuit.process(IO.point(Success)).attempt
      nbR6    <- circuit.getCurrentState
      z       <- IO.sleep(2.seconds) *> circuit.process(IO.point(Success)).attempt
      nbR7    <- circuit.getCurrentState
      success <- circuit.process(IO.point(Success)).attempt
      nbC     <- circuit.getCurrentState
    } yield {
      (v must beLeft[Any](Failed))
        .and(nbR1 must_=== Circuit.CircuitStatus.Closed(1))
        .and(w must beRight[State](Success))
        .and(nbR2 must_=== Circuit.CircuitStatus.Closed(0))
        .and(nbR3 must_=== Circuit.CircuitStatus.Closed(1))
        .and(nbR4 must beAnInstanceOf[Circuit.CircuitStatus.Open])
        .and(x must beLeft[Any](CircuitOpen))
        .and(nbR5 must beAnInstanceOf[Circuit.CircuitStatus.Open])
        .and(y must beLeft[Any](CircuitOpen))
        .and(nbR6 must_=== nbR5)
        .and(z must beLeft[State](CircuitOpen))
        .and(nbR7 must beAnInstanceOf[Circuit.CircuitStatus.HalfOpen])
        .and(success must beRight[State](Success))
        .and(nbC must_=== Circuit.CircuitStatus.Closed(0))
    })

}
