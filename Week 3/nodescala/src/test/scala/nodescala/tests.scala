package nodescala



import scala.language.postfixOps
import scala.util.{Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.async
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala._
import java.util.concurrent.TimeoutException
import scala.concurrent.TimeoutException

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be created") {
    val v = 517
    assert(Await.result(Future.always(v), 0 nanos) == v)
  }

  test("A Future should never be created") {
    intercept[TimeoutException] {
      Await.result(Future.never[Int], 1 second)
    }
  }

  test("All Futures should be completed") {
    val range = (1 to 10).toList
    val fs = Future.all(range.map(Future(_)))
    assert(Await.result(fs, 1 second) == range)
  }

  test("Sequence of futures should fail if one future failed") {
    intercept[Error] {
      val fs = Future.all((1 to 10).+:(throw new Error()).map(Future(_)).toList)
      Await.result(fs, 1 second)
    }
  }

  test("Future delay") {
    Await.result(Future.delay(1 second), 2 seconds)
    intercept[TimeoutException] {
      Await.result(Future.delay(2 seconds), 1 second)
    }
  }

  test("Future now") {
    assert(Future.always(1).now == 1)
    intercept[UnsupportedOperationException] {
      Future.failed(new UnsupportedOperationException).now
    }
    intercept[NoSuchElementException] {
      (for(d <- Future.delay(1 minute); f <- Future.always(1)) yield f).now
    }

  }

  test("Future continueWith") {
    val f = Future {throw new Error} continueWith { f => true  }
    assert(Await.result(f, 1 second))

    intercept[UnsupportedOperationException] {
      Await.result(Future.always(true) continueWith { f => throw new UnsupportedOperationException }, 1 second)
    }
  }

  test("Future continue") {
    val f = Future {throw new Error} continue { f => true  }
    assert(Await.result(f, 1 second))

    intercept[UnsupportedOperationException] {
      Await.result(Future.always(true) continue { f => throw new UnsupportedOperationException }, 1 second)
    }

    assert(Await.result(Future.always(1) continue {
      case Success(i) => i * 2
      case Failure(t) => 0
    }, 1 second) == 2)

  }

  test("CancellationTokenSource should allow stopping the computation") {
    val cts = CancellationTokenSource()
    val ct = cts.cancellationToken
    val p = Promise[String]()

    async {
      while (ct.nonCancelled) {
        // do work
      }

      p.success("done")
    }

    cts.unsubscribe()
    assert(Await.result(p.future, 1 second) == "done")
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }

  test("Listener should serve the next request as a future") {
    val dummy = new DummyListener(8191, "/test")
    val subscription = dummy.start()

    def test(req: Request) {
      val f = dummy.nextRequest()
      dummy.emit(req)
      val (reqReturned, xchg) = Await.result(f, 1 second)

      assert(reqReturned == req)
    }

    test(immutable.Map("StrangeHeader" -> List("StrangeValue1")))
    test(immutable.Map("StrangeHeader" -> List("StrangeValue2")))

    subscription.unsubscribe()
  }

  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




