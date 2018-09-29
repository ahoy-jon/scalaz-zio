package scalaz.zio

import scalaz.zio.CircuitBreaker.Open

class CircuitBreakerSpec extends AbstractRTSSpec {
  def is = "CircuitBreakerSpec".title ^ s2"""
     protect must work correctly $e
    """

  sealed trait State
  object State {
    case object Failed      extends State
    case object Success     extends State
    case object CircuitOpen extends State
  }

  import State._

  def e =
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
}
