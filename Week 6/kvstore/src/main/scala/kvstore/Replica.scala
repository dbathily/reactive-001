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
  context.watch(persistence)

  var waitingPersistence = Map.empty[Long, (ActorRef, Persist, Cancellable)]
  var waitingReplicates = Map.empty[Long, (ActorRef, Replicate, Set[(ActorRef, Cancellable)], Cancellable)]
  var waitingInitialReplicates = Map.empty[Long, (Replicate, Set[(ActorRef, Cancellable)], Cancellable)]

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 3) {
    case _: PersistenceException => Restart
  }

  override def preStart(): Unit =
    arbiter ! Join

  override def postStop(): Unit = {
    waitingPersistence foreach {
      case (_, (_, _, cancellable)) =>
        cancellable.cancel()
    }
    waitingReplicates foreach {
      case (_, (_, _, waiting, timeout)) =>
        timeout.cancel()
        waiting foreach {
          case (_, cancellable) =>
            cancellable.cancel()
        }
    }
  }

  def receive = {
    case JoinedPrimary   ⇒ context.become(leader)
    case JoinedSecondary ⇒ context.become(replica)
  }

  def schedule(call: => Unit): Cancellable =
    system.scheduler.schedule(0.millis, 100.millis) {
      call
    }

  def timeout(onTimeout: => Unit): Cancellable =
    system.scheduler.scheduleOnce(1.second)(onTimeout)

  def schedulePersist(call: => Unit)(onTimeout: => Unit): Cancellable = {
    val retry = schedule(call)
    val t = timeout {
      retry.cancel()
      onTimeout
    }

    new Cancellable {
      def isCancelled: Boolean = retry.isCancelled && t.isCancelled
      def cancel(): Boolean = retry.cancel() && t.cancel()
    }
  }
    

  def persist(client:ActorRef, key:String, valueOption:Option[String], id:Long)
             (onTimeout: Option[ActorRef => Unit] = None): Unit = {

    valueOption match {
      case Some(value) =>
        kv += (key -> value)
      case None =>
        kv -= key
    }

    val p = Persist(key, valueOption, id)

    waitingPersistence += (id -> (client, p, schedulePersist { persistence ! p } {
      waitingPersistence -= id
      onTimeout.map(_(client))
    }))

  }

  def persisted(key:String, id:Long, onPersisted:(ActorRef, Persist) => Unit): Unit = {
    waitingPersistence.get(id).map { case (sender, p, cancellable) =>
      cancellable.cancel()
      waitingPersistence -= id
      onPersisted(sender, p)
    }
  }

  def replicate(client:ActorRef, p:Persist): Unit = { secondaries.size > 0 match {
    case true =>
      val r = Replicate(p.key, p.valueOption, p.id)
      waitingReplicates += (p.id -> (client, r, (secondaries map {
        case (replicate, replicator) =>
          (replicator, schedule {
            replicator ! r
          })
      }).toSet, timeout(cancelReplication(client, p.id))))
    case false =>
      client ! OperationAck(p.id)

  }}

  def cancelReplication(client:ActorRef, id:Long): Unit = {
    waitingReplicates.get(id).map {case (_, _, waiting, _) =>
      waiting foreach { case (_, cancellable) =>
        cancellable.cancel()
      }
    }
    waitingReplicates -= id
    client ! OperationFailed(id)
  }

  def replicateToNewlyAdded(replicators:Set[ActorRef]): Unit = {
    kv.zipWithIndex map { case ((key, value), i) =>
      val r = Replicate(key, Some(value), i.toLong)
      waitingInitialReplicates += (i.toLong -> (r, replicators map { replicator =>
        (replicator, schedule(replicator ! r))
      }, timeout(cancelInitialReplication(i))))
    }
  }

  def cancelInitialReplication(id:Long): Unit = {
    waitingInitialReplicates.get(id).map { case (_, waiting, _) =>
      waiting foreach { case (_, cancellable) =>
        cancellable.cancel()
      }
    }
    waitingInitialReplicates -= id
  }

  def removeWaitingReplicate(replicator:ActorRef, client:ActorRef, r:Replicate,
                             waiting:Set[(ActorRef, Cancellable)], timeout:Cancellable): Unit = {
    waiting.find(_._1 == replicator).map { e =>
      e._2.cancel()
      val updated = waiting - e
      if(updated.size > 0)
        waitingReplicates = waitingReplicates.updated(r.id, (client, r, updated, timeout))
      else {
        timeout.cancel()
        waitingReplicates -= r.id
        client ! OperationAck(r.id)
      }
    }
  }

  def removeInitialWaitingReplicate(replicator:ActorRef, r:Replicate,
                                    waiting:Set[(ActorRef, Cancellable)], timeout:Cancellable): Unit = {
    waiting.find(_._1 == replicator).map { e =>
      e._2.cancel()
      val updated = waiting - e
      if(updated.size > 0)
        waitingInitialReplicates = waitingInitialReplicates.updated(r.id, (r, updated, timeout))
      else {
        timeout.cancel()
        waitingInitialReplicates -= r.id
      }
    }
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) =>
      persist(sender, key, Some(value), id)(Some(_ ! OperationFailed(id)))
    case Remove(key, id) =>
      persist(sender, key, None, id)(Some(_ ! OperationFailed(id)))
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      persisted(key, id, replicate)
    case Replicated(key, id) =>
      waitingReplicates.get(id).map { case (client, r, waiting, timeout) =>
        removeWaitingReplicate(sender, client, r, waiting, timeout)
      }
      waitingInitialReplicates.get(id).map { case (r, waiting, timeout) =>
         removeInitialWaitingReplicate(sender, r, waiting, timeout)
      }
    case Replicas(replicas) =>

      val (removed, alreadyAdded) = secondaries.partition {
        case (replica, replicator) => !replicas.contains(replica)
      }

      removed foreach {
        case (replica, replicator) =>
          context.stop(replicator)
          waitingReplicates foreach {
            case (id, (client, r, waiting, timeout)) =>
              removeWaitingReplicate(replicator, client, r, waiting, timeout)
          }
          waitingInitialReplicates foreach {
            case (id, (r, waiting, timeout)) =>
              removeInitialWaitingReplicate(replicator, r, waiting, timeout)
          }
          secondaries -= replica
      }

      val newReplicas = replicas filter {
        replica => replica != self && !secondaries.contains(replica)
      } map {
        replica =>
          val replicator = context.actorOf(Replicator.props(replica))
          secondaries += replica -> replicator
          replicator
      }

      replicateToNewlyAdded(newReplicas)

  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Snapshot(key, valueOption, seq) =>
      val expected = lastSeq + 1
      if(seq < expected) {
        sender ! SnapshotAck(key, seq)
      }else if(seq == expected) {
        persist(sender, key, valueOption, seq)()
        lastSeq = seq
      }
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Persisted(key, id) =>
      persisted(key, id, (client, p) => client ! SnapshotAck(key, id))
  }

}
