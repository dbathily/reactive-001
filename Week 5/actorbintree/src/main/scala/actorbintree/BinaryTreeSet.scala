/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case o:Operation => root ! o
    case GC => {
      val newRoot = createRoot
      context become garbageCollecting(newRoot)
      root ! CopyTo(newRoot)
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case o:Operation => pendingQueue = pendingQueue enqueue o
    case CopyFinished => {
      context.stop(root)
      root = newRoot
      while( !pendingQueue.isEmpty ) {
        pendingQueue = pendingQueue.dequeue match {
          case (o, q) =>
            root ! o
            q
        }
      }
      context unbecome()
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, e) => insert(requester, id, e)
    case Contains(requester, id, e) => contains(requester, id, e)
    case Remove(requester, id, e) => remove(requester, id, e)
    case CopyTo(node) => {
      val expected = subtrees.values.toSet
      context become copying(expected, false)
      if(!removed)
        node ! Insert(self, elem, elem)
      else
        self ! OperationFinished(elem)
      expected.foreach(_ ! CopyTo(node))
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(elem) =>
      context become copying(expected, true)
      if(expected.isEmpty)
        self ! CopyFinished

    case CopyFinished =>
      val rest = expected - sender
      if(rest.isEmpty && insertConfirmed) {
        context.parent ! CopyFinished
      }else{
        context become copying(rest, insertConfirmed)
      }

  }

  private def positionFor(e: Int) = {
    if(e < elem) Left else Right
  }

  private def insert(requester: ActorRef, id: Int, e: Int): Unit = {
    if(e == elem) {
      if(removed)
        removed = false
      requester ! OperationFinished(id)
    } else
      insert(positionFor(e), requester, id, e)
  }

  private def insert(position: Position, requester: ActorRef, id: Int, e:Int): Unit = {
    subtrees.get(position).map(_ ! Insert(requester, id, e)).getOrElse {
      subtrees += (position -> context.actorOf(props(e, false)))
      requester ! OperationFinished(id)
    }
  }

  private def contains(requester: ActorRef, id: Int, e: Int): Unit = {
     if(e == elem)
       requester ! ContainsResult(id, !removed)
    else
       subtrees.get(positionFor(e)).map(_ ! Contains(requester, id, e)).getOrElse {
         requester ! ContainsResult(id, false)
       }
  }

  private def remove(requester: ActorRef, id: Int, e: Int): Unit = {
    if(e == elem) {
      removed = true
      requester ! OperationFinished(id)
    } else
      subtrees.get(positionFor(e)).map(_ ! Remove(requester, id, e)).getOrElse {
        requester ! OperationFinished(id)
      }
  }
}
