package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.Some

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.{dispatcher, system}

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  var seqCounter = 0L

  var lastSeq = -1L

  val persistence = context.actorOf(persistenceProps)

  var waitingPersistence = Map.empty[Long, (ActorRef, Cancellable)]

  override def preStart(): Unit =
    arbiter ! Join

  override def postStop(): Unit =
    waitingPersistence foreach {
      case (_, (_, cancellable)) =>
        cancellable.cancel()
    }

  def receive = {
    case JoinedPrimary   ⇒ context.become(leader)
    case JoinedSecondary ⇒ context.become(replica)
  }

  def schedulePersist(key:String, valueOption:Option[String], id:Long):Cancellable =
    system.scheduler.schedule(0.millis, 100.millis) {
      persistence ! Persist(key, valueOption, id)
    }

  def persist(key:String, valueOption:Option[String], id:Long):Unit = {
    valueOption match {
      case Some(value) =>
        kv += (key -> value)
      case None =>
        kv -= key
    }
    waitingPersistence += (id -> (sender, schedulePersist(key, valueOption, id)))
  }

  def persisted(key:String, id:Long, callback:ActorRef => Unit):Unit = {
    waitingPersistence.get(id).map{ case (sender, cancellable) =>
      waitingPersistence -= id
      cancellable.cancel()
      callback(sender)
    }
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      persist(key, Some(value), id)
    case Remove(key, id) =>
      persist(key, None, id)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      persisted(key, id, _ ! OperationAck(id))
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Snapshot(key, valueOption, seq) =>
      val expected = lastSeq + 1
      if(seq < expected) {
        sender ! SnapshotAck(key, seq)
      }else if(seq == expected) {
        persist(key, valueOption, seq)
        lastSeq = seq
      }
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      persisted(key, id, _ ! SnapshotAck(key, id))
  }

}
