package scalaz.zio

import scalaz.zio.CircuitBreaker.CircuitStatus._
import scalaz.zio.CircuitBreaker.{ RejectionCallbacks, StatusChangeCallbacks }

import scala.concurrent.duration._

class CircuitBreakerSpec extends AbstractRTSSpec {
  def is =
    "CircuitBreakerSpec".title ^
      s2"""
     circuit breaker must follow monix tests
        should work for successful async tasks $m0
        should work for successful immediate tasks $m1
        should be stack safe for successful async tasks (flatMap) $m2
        complete workflow with failures and exponential backoff $m4

     circuit breaker for a given configuration
        call bunch of failed IOs must increment failures in Closed status then succeed $e1
        call bunch of failed IOs exceeding maxFailures must turn the status to Open $e2
        call bunch of failed IOs must turn the status to HalfOpen after the duration of Open $e3
        call bunch of IOs $e4
        call $e
    """

  sealed trait Error

  object Error {

    case object Rejected extends Error

    case class Exception(text: String) extends Error

  }

  //"should work for successful async tasks"
  def m0 = unsafeRun(
    for {
      circuit <- CircuitBreaker(5, 1.minute, Error.Rejected)
      count   <- Ref(0)
      fibers  <- IO.forkAll(for (i <- 0 until 1000) yield circuit(count.update(_ + 1).void))
      _       <- fibers.join
      ct      <- count.get
    } yield {
      ct must_=== 1000
    }
  )

  //"should work for successful immediate tasks"
  def m1 = unsafeRun(
    for {
      circuit <- CircuitBreaker(5, 1.minute, Error.Rejected)
      count   <- Ref(0)
      _       <- IO.sequence(for (i <- 0 until 1000) yield circuit(count.update(_ + 1).void))
      ct      <- count.get
    } yield {
      ct must_=== 1000
    }
  )

  //"should be stack safe for successful async tasks (flatMap)"
  def m2 = unsafeRun(
    CircuitBreaker(5, 1.minute, Error.Rejected).flatMap(circuit => {

      def loop(n: Int, acc: Int): IO[Error, Fiber[Error, Int]] =
        if (n > 0)
          for {
            fiber <- circuit(IO.point(acc + 1)).fork
            x     <- fiber.join
            res   <- loop(n - 1, x)
          } yield res
        else
          IO.now(acc).fork

      for {
        fiber <- loop(100000, 0)
        res   <- fiber.join
      } yield {
        res must_=== 100000
      }
    })
  )

  //"should be stack safe for successful async tasks (defer)"
  //https://github.com/monix/monix/blob/master/monix-eval/shared/src/test/scala/monix/eval/TaskCircuitBreakerSuite.scala#L111-L127
  /*def m3 = unsafeRun({
    for {
      circuit <-  CircuitBreaker(5, 1.minute, Error.Rejected)

    } yield {
      ???
    }
  })*/

  /*
  test("complete workflow with failures and exponential backoff") { implicit s =>

  }
   */

  def m4 =
    unsafeRun({

      for {
        openedCount   <- Ref(0)
        closedCount   <- Ref(0)
        halfOpenCount <- Ref(0)
        rejectedCount <- Ref(0)
        //exponentialBackoffFactor = 2,  resetTimeout = 1.minute, maxResetTimeout = 10.minutes
        circuit <- CircuitBreaker[Error](
                    maxFailures = 5,
                    durationO = 10.minutes,
                    rejected = Error.Rejected,
                    statusChangeCallbacks = StatusChangeCallbacks(onClosed = closedCount.update(_ + 1).void,
                                                                  onHalfOpen = halfOpenCount.update(_ + 1).void,
                                                                  onOpen = openedCount.update(_ + 1).void),
                    rejectionCallbacks = RejectionCallbacks(whenOpen = rejectedCount.update(_ + 1).void,
                                                            whenHalfOpen = rejectedCount.update(_ + 1).void)
                  )
        dummy         = Error.Exception("dummy")
        taskInError   = circuit(IO.fail(dummy)).attempt
        taskInSuccess = circuit(IO.now(1))

        s1 <- for {
               attempt1 <- taskInError
               attempt2 <- taskInError
               state    <- circuit.status
             } yield {
               (attempt1 must left(dummy))
                 .and(attempt2 must left(dummy))
                 .and(
                   //WARNING : IMPLEMENTATION DETAIL
                   state must_== Closed(2)
                 )
             }

        s2 <- for {
               succ  <- taskInSuccess
               state <- circuit.status
             } yield {
               (succ must_== 1).and(
                 state must_== Closed(0)
               )
             }

        s3 <- for {
               attempt1 <- taskInError
               attempt2 <- taskInError
               attempt3 <- taskInError
               attempt4 <- taskInError
               state    <- circuit.status
             } yield {
               (attempt1 must left(dummy))
                 .and(attempt2 must left(dummy))
                 .and(attempt3 must left(dummy))
                 .and(attempt4 must left(dummy))
                 .and(
                   state must_== Closed(4)
                 )
             }

        s4 <- for {
               attempt1 <- taskInError
               state    <- circuit.status
             } yield {
               (attempt1 must left(dummy)).and(
                 state must beAnInstanceOf[Open]
               )
             }
        //TODO clock based test

      } yield s1 and s2 and s3 and s4

      /*



  // Getting rejections from now on, testing reset timeout
  var resetTimeout = 60.seconds
  for (i <- 0 until 30) {
    val now = s.clockMonotonic(MILLISECONDS)
    val nextTimeout = {
      val value = resetTimeout * 2
      if (value > 10.minutes) 10.minutes else value
    }

    intercept[ExecutionRejectedException](taskInError.runAsync.value.get.get)
    s.tick(resetTimeout - 1.second)
    intercept[ExecutionRejectedException](taskInError.runAsync.value.get.get)

    // After 1 minute we should attempt a reset
    s.tick(1.second)
    assertEquals(circuitBreaker.state, TaskCircuitBreaker.Open(now, resetTimeout))

    // Starting the HalfOpen state
    val delayedTask = circuitBreaker.protect(Task.raiseError(dummy).delayExecution(1.second))
    val delayedResult = delayedTask.runAsync

    assertEquals(circuitBreaker.state,
      TaskCircuitBreaker.HalfOpen(resetTimeout = resetTimeout))

    // Rejecting all other tasks
    intercept[ExecutionRejectedException](taskInError.runAsync.value.get.get)
    intercept[ExecutionRejectedException](taskInError.runAsync.value.get.get)

    // Should migrate back into Open
    s.tick(1.second)
    assertEquals(delayedResult.value, Some(Failure(dummy)))
    assertEquals(circuitBreaker.state, TaskCircuitBreaker.Open(
      startedAt = s.clockMonotonic(MILLISECONDS),
      resetTimeout = nextTimeout
    ))

    intercept[ExecutionRejectedException](taskInError.runAsync.value.get.get)

    // Calculate next reset timeout
    resetTimeout = nextTimeout
  }

  // Going back into Closed
  s.tick(resetTimeout)

  val delayedTask = circuitBreaker.protect(Task.evalAsync(1).delayExecution(1.second))
  val delayedResult = delayedTask.runAsync

  assertEquals(circuitBreaker.state, TaskCircuitBreaker.HalfOpen(resetTimeout = resetTimeout))
  intercept[ExecutionRejectedException](taskInError.runAsync.value.get.get)

  s.tick(1.second)
  assertEquals(delayedResult.value, Some(Success(1)))
  assertEquals(circuitBreaker.state, TaskCircuitBreaker.Closed(0))

  assertEquals(rejectedCount, 5 * 30 + 1)
  assertEquals(openedCount, 30 + 1)
  assertEquals(halfOpenCount, 30 + 1)
  assertEquals(closedCount, 1)
     */
    })

  sealed trait State

  object State {

    case object Failed extends State

    case object Success extends State

    case object CircuitOpen extends State

  }

  import State._

  def e1 = unsafeRun(
    for {
      circuit <- CircuitBreaker[String](4, 1.second, "Rejected")
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
      circuit <- CircuitBreaker[String](3, 1.second, "Rejected")
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
      circuit <- CircuitBreaker[String](1, 300.millis, "Rejected")
      status  <- circuit.status
      _       <- circuit.forceOpen
      status0 <- circuit.status
      _       <- IO.sleep(300.millis)
      _       <- circuit.tick
      status1 <- circuit.status

    } yield {
      (status must beAnInstanceOf[Closed])
        .and(status0 must beAnInstanceOf[Open])
        .and(status1 must beAnInstanceOf[HalfOpen.type])
    }
  )

  def e = unsafeRun {
    for {
      circuit <- CircuitBreaker[Int](1, 1.second, -1)
      _       <- circuit.forceOpen
      v       <- circuit(IO.never).attempt
    } yield {
      v must beLeft(-1)
    }
  }

  def e4 =
    unsafeRun(for {
      circuit <- CircuitBreaker[State](maxFailures = 2, durationO = 300.millis, rejected = CircuitOpen)
      v       <- circuit(IO.fail(Failed)).attempt
      nbR1    <- circuit.status
      w       <- circuit(IO.point(Success)).attempt
      nbR2    <- circuit.status
      _       <- circuit(IO.fail(Failed)).attempt
      nbR3    <- circuit.status
      _       <- circuit(IO.fail(Failed)).attempt
      nbR4    <- circuit.status
      x       <- circuit(IO.fail(Failed)).attempt
      nbR5    <- circuit.status
      _       <- IO.sleep(300.millis)
      y       <- circuit(IO.point(Success)).attempt
      nbR6    <- circuit.status
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
