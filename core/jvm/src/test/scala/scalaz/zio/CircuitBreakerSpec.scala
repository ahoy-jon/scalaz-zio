package scalaz.zio

import scalaz.zio.CircuitBreaker.CircuitStatus._

import scala.concurrent.duration.{ Duration, _ }

class CircuitBreakerSpec extends AbstractRTSSpec {
  def is =
    "CircuitBreakerSpec".title ^
      s2"""
     circuit breaker for a given configuration
        call bunch of failed IOs must increment failures in Closed status then succeed $e1
        call bunch of failed IOs exceeding maxFailures must turn the status to Open $e2
        call bunch of failed IOs must turn the status to HalfOpen after the duration of Open $e3
        call bunch of IOs $e4
        call $e
        check race $e6
    """

  sealed trait State

  object State {

    case object Failed extends State

    case object Success extends State

    case object CircuitOpen extends State

  }

  import State._

  def e1 = unsafeRun(
    for {
      circuit <- CircuitBreaker[String](3, 1.second, Duration.Zero, "Rejected")
      status0 <- circuit.status
      io1     <- circuit(IO.fail("Error-1")).attempt
      status1 <- circuit.status
      io2     <- circuit(IO.fail("Error-2")).attempt
      status2 <- circuit.status
      io3     <- circuit(IO.fail("Error-3")).attempt
      status3 <- circuit.status
    } yield
      (status0 must_=== Closed(0))
        .and(status1 must_=== Closed(1))
        .and(status2 must_=== Closed(2))
        .and(status3 must_=== Closed(3))
        .and(io1 must beLeft("Error-1"))
        .and(io2 must beLeft("Error-2"))
        .and(io3 must beLeft("Error-3"))
  )

  def e2 = unsafeRun(
    for {
      circuit <- CircuitBreaker[String](2, 1.second, Duration.Zero, "Rejected")
      status0 <- circuit.status
      io1     <- circuit(IO.fail("Error-1")).attempt
      status1 <- circuit.status
      io2     <- circuit(IO.fail("Error-2")).attempt
      status2 <- circuit.status
      io3     <- circuit(IO.fail("Error-3")).attempt
      status3 <- circuit.status
      io4     <- circuit(IO.fail("Error-4")).attempt
      status4 <- circuit.status
    } yield
      (status0 must_=== Closed(0))
        .and(status1 must_=== Closed(1))
        .and(status2 must_=== Closed(2))
        .and(status3 must beAnInstanceOf[Open])
        .and(io1 must beLeft("Error-1"))
        .and(io2 must beLeft("Error-2"))
        .and(io3 must beLeft("Error-3"))
        .and(io4 must beLeft("Rejected"))
  )

  def e3 = unsafeRun(
    for {
      circuit <- CircuitBreaker[String](1, 300.millis, 200.millis, "Rejected")
      status  <- circuit.status
      _       <- circuit.forceOpen
      status0 <- circuit.status
      _       <- IO.sleep(300.millis)
      _       <- circuit.tick
      status1 <- circuit.status

    } yield {
      (status must beAnInstanceOf[Closed])
        .and(status0 must beAnInstanceOf[Open])
        .and(status1 must beAnInstanceOf[HalfOpen])
    }
  )

  def e = unsafeRun {
    for {
      circuit <- CircuitBreaker[Int](1, 1.second, Duration.Zero, -1)
      _       <- circuit.forceOpen
      v       <- circuit(IO.never).attempt
    } yield {
      v must beLeft(-1)
    }
  }

  def e6 = unsafeRun(
    IO.point(1).race(IO.point(2)).map(x => x must beEqualTo(1))
  )

  def e4 =
    unsafeRun(for {
      circuit <- CircuitBreaker[State](maxFailures = 1,
                                       durationO = 300.millis,
                                       durationH = Duration.Zero,
                                       rejected = CircuitOpen)
      v    <- circuit(IO.fail(Failed)).attempt
      nbR1 <- circuit.status
      w    <- circuit(IO.point(Success)).attempt
      nbR2 <- circuit.status
      _    <- circuit(IO.fail(Failed)).attempt
      nbR3 <- circuit.status
      _    <- circuit(IO.fail(Failed)).attempt
      nbR4 <- circuit.status
      x    <- circuit(IO.fail(Failed)).attempt
      nbR5 <- circuit.status
      _    <- IO.sleep(300.millis)
      y    <- circuit(IO.point(Success)).attempt
      nbR6 <- circuit.status
    } yield {
      (v must beLeft[Any](Failed))
        .and(nbR1 must_=== Closed(1))
        .and(w must beRight[State](Success))
        .and(nbR2 must_=== Closed(0))
        .and(nbR3 must_=== Closed(1))
        .and(nbR4 must beAnInstanceOf[Open])
        .and(x must beLeft[Any](CircuitOpen))
        .and(nbR5 must beAnInstanceOf[Open])
        .and(y must beRight[State](Success))
        .and(nbR6 must_=== Closed(0))
    })

}
