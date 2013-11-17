package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: A =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: A, b: A) =>
    findMin(insert(a, insert(b, empty))) == Math.min(a,b)
  }

  property("empty") = forAll { a: A =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }

  property("sorts") = forAll { h: H =>
    isOrdered(seq(h))
  }

  property("min3") = forAll { (h1: H, h2: H) =>
    findMin(meld(h1, h2)) == Math.min(findMin(h1), findMin(h2))
  }
  
  property("meld") = forAll { (h1: H, h2: H) =>
    seq(meld(h1, h2)) == (seq(h1) ++ seq(h2)).sorted
  }

  import Ordering.Implicits._

  def seq(rest: H): Seq[A] = {
    if(isEmpty(rest))
      Seq()
    else
      Seq(findMin(rest)) ++ seq(deleteMin(rest))
  }
  
  def isOrdered[O: Ordering](seq:Seq[O]):Boolean = seq match {
    case Nil => true
    case x :: Nil => true
    case x :: xs => (x <= xs.head) && isOrdered(xs.tail)
  }

  lazy val genHeap: Gen[H] = for {
    k <- arbitrary[A]
    m <- oneOf(value(empty), genHeap)
    v <- insert(k, m)
  } yield v

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
