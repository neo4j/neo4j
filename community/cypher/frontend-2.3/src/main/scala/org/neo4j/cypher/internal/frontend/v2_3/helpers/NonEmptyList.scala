/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v2_3.helpers

import org.neo4j.cypher.internal.frontend.v2_3.InternalException

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

object NonEmptyList {

  def from[T](input: Iterable[T]): NonEmptyList[T] =
    from(input.iterator)

  def from[T](input: Iterator[T]): NonEmptyList[T] =
    input.asNonEmptyListOption.getOrElse(
      throw new RuntimeException("Attempt to construct empty non-empty list ")
    )

  def apply[T](first: T, tail: T*): NonEmptyList[T] =
    loop(Last(first), tail.iterator).reverse

  def newBuilder[T]: mutable.Builder[T, Option[NonEmptyList[T]]] = new mutable.Builder[T, Option[NonEmptyList[T]]] {
    private var vecBuilder = Vector.newBuilder[T]

    override def +=(elem: T): this.type = {
      vecBuilder += elem
      this
    }

    override def result(): Option[NonEmptyList[T]] = {
      vecBuilder.result().toNonEmptyListOption
    }

    override def clear(): Unit = {
      vecBuilder.clear()
    }
  }

  implicit def canBuildFrom[T] = new CanBuildFrom[Any, T, Option[NonEmptyList[T]]] {
    def apply(from: Any) = newBuilder[T]
    def apply() = newBuilder[T]
  }

  implicit class IterableConverter[T](iterable: Iterable[T]) {
    def toReverseNonEmptyListOption: Option[NonEmptyList[T]] =
      iterable.iterator.asReverseNonEmptyListOption

    def toNonEmptyListOption: Option[NonEmptyList[T]] =
      iterable.iterator.asNonEmptyListOption

    def toNonEmptyList: NonEmptyList[T] =
      toNonEmptyListOption.getOrElse(throw new InternalException("Attempt to construct empty non-empty list "))
  }

  implicit class VectorConverter[T](vector: Vector[T]) {
    def toReverseNonEmptyListOption: Option[NonEmptyList[T]] =
      vector.iterator.asReverseNonEmptyListOption

    def toNonEmptyListOption: Option[NonEmptyList[T]] =
      vector.reverseIterator.asReverseNonEmptyListOption
  }

  implicit class IteratorConverter[T](iterator: Iterator[T]) {
    def asReverseNonEmptyListOption: Option[NonEmptyList[T]] =
      if (iterator.isEmpty) None else Some(loop(Last(iterator.next()), iterator))

    def asNonEmptyListOption: Option[NonEmptyList[T]]  =
      asReverseNonEmptyListOption.map(_.reverse)
  }

  @tailrec
  private def loop[X](acc: NonEmptyList[X], iterator: Iterator[X]): NonEmptyList[X] =
    if (iterator.hasNext) loop(Fby(iterator.next(), acc), iterator) else acc
}

// NonEmptyLists are linked lists of at least a single or multiple elements
//
// The interface follows scala collection but is not identical with it, most
// notably filter and partition have different signatures.
//
// NonEmptyLists also do not implement Traversable or Iterable directly but
// must be converted using to{Seq|Set|List|Iterable} explicitly due to
// the differing signatures.
//
sealed trait NonEmptyList[+T] {

  self =>

  import NonEmptyList._

  def head: T

  def tailOption: Option[NonEmptyList[T]]

  def hasTail: Boolean
  def isLast: Boolean

  def +:[X >: T](elem: X): NonEmptyList[X] =
    Fby(elem, self)

  final def :+[X >: T](elem: X): NonEmptyList[X] =
    (elem +: self.reverse).reverse

  final def ++:[X >: T](iterable: Iterable[X]): NonEmptyList[X] =
    self.++:(iterable.iterator)

  final def ++:[X >: T](iterator: Iterator[X]): NonEmptyList[X] = iterator.asNonEmptyListOption match {
    case Some(prefix) => prefix.reverse.mapAndPrependReversedTo[X, X](identity, self)
    case None => self
  }

  def ++[X >: T](other: NonEmptyList[X]): NonEmptyList[X] =
    reverse.mapAndPrependReversedTo[X, X](identity, other)

  @tailrec
  final def foreach(f: T => Unit): Unit = self match {
    case Last(elem) => f(elem)
    case Fby(elem, tail) => f(elem); tail.foreach(f)
  }

  final def filter[X >: T](f: X => Boolean): Option[NonEmptyList[T]] =
    foldLeft[Option[NonEmptyList[T]]](None) {
      case (None, elem) => if (f(elem)) Some(Last(elem)) else None
      case (acc@Some(nel), elem) => if (f(elem)) Some(Fby(elem, nel)) else acc
    }.map(_.reverse)

  final def forall[X >: T](predicate: (X) => Boolean): Boolean =
    !exists(x => !predicate(x))

  @tailrec
  final def exists[X >: T](predicate: (X) => Boolean): Boolean = self match {
    case Last(elem) => predicate(elem)
    case Fby(elem, _) if predicate(elem) => true
    case Fby(_, tail) => tail.exists(predicate)
  }

  final def map[S](f: T => S): NonEmptyList[S] = self match {
    case Fby(elem, tail) => tail.mapAndPrependReversedTo[T, S](f, Last(f(elem))).reverse
    case Last(elem) => Last(f(elem))
  }

  final def collect[S](pf: PartialFunction[T, S]): Option[NonEmptyList[S]] =
    foldLeft(newBuilder[S]) { (builder, elem) =>
      if (pf.isDefinedAt(elem)) builder += pf(elem) else builder
    }.result()

  @tailrec
  final def mapAndPrependReversedTo[X >: T, Y](f: X => Y, acc: NonEmptyList[Y]): NonEmptyList[Y] = self match {
    case Fby(elem, tail) => tail.mapAndPrependReversedTo(f, Fby(f(elem), acc))
    case Last(elem) => Fby(f(elem), acc)
  }

  final def flatMap[S](f: T => NonEmptyList[S]): NonEmptyList[S] = self match {
    case Last(elem) => f(elem)
    case _ => reverseFlatMap(f).reverse
  }

  final def reverseFlatMap[S](f: T => NonEmptyList[S]): NonEmptyList[S] = self match {
    case Fby(elem, tail) => tail.reverseFlatMapLoop(f(elem).reverse, f)
    case Last(elem) => f(elem).reverse
  }

  final def foldLeft[A](acc0: A)(f: (A, T) => A): A =
    foldLeftLoop(acc0, f)

  final def reduceLeft[X >: T](f: (X, X) => X): X = self match {
    case Fby(head, tail) => tail.reduceLeftLoop(head, f)
    case Last(value) => value
  }

  // Partition each element into one of two lists using f
  //
  // It holds that one of the two partitions must not be empty.
  // This is encoded in the result type, i.e. this function
  // returns
  //
  // - either a non empty list of As, and an option of a non empty list of Bs
  // - or an option of a non empty list of As, and a non empty list of Bs
  //
  final def partition[A, B](f: T => Either[A, B])
  : Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])] =
    self match {
      case Fby(elem, tail) => tail.partitionLoop(f, asPartitions(f(elem)))
      case Last(elem) => asPartitions(f(elem))
    }

  final def groupBy[X >: T, K](f: X => K): Map[K, NonEmptyList[X]] =
    foldLeft(Map.empty[K, NonEmptyList[X]]) {
      (m, value) =>
        val key = f(value)
        val nel = m.get(key).map(cur => Fby(value, cur)).getOrElse(Last(value))
        m.updated(key, nel)
    }.mapValues(_.reverse)

  final def reverse: NonEmptyList[T] = self match {
    case Fby(elem, tail) => tail.mapAndPrependReversedTo[T, T](identity, Last(elem))
    case _ => self
  }

  final def min[X >: T](implicit ordering: Ordering[X]): X =
    reduceLeft { (left, right) => if (ordering.compare(left, right) <= 0) left else right }

  final def max[X >: T](implicit ordering: Ordering[X]): X =
    min(ordering.reverse)

  def toIterable: Iterable[T] = new Iterable[T] {
    def iterator = new Iterator[T] {
      private var remaining: Option[NonEmptyList[T]] = Some(self)

      override def hasNext: Boolean = remaining.nonEmpty

      override def next(): T = remaining match {
        case Some(nel) =>
          remaining = nel.tailOption
          nel.head
        case None =>
          throw new NoSuchElementException("next on empty iterator")
      }
    }
  }

  final def toSet[X >: T]: Set[X] = foldLeft(Set.empty[X])(_ + _)
  final def toSeq: Seq[T] = foldLeft(Seq.empty[T])(_ :+ _)
  final def toList: List[T] = foldLeft(List.empty[T])(_ :+ _)

  @tailrec
  private def reverseFlatMapLoop[S](acc: NonEmptyList[S], f: T => NonEmptyList[S]): NonEmptyList[S] = self match {
    case Fby(elem, tail) => tail.reverseFlatMapLoop(f(elem).mapAndPrependReversedTo[S, S](identity, acc), f)
    case Last(elem) => f(elem).mapAndPrependReversedTo[S, S](identity, acc)
  }

  @tailrec
  private def foldLeftLoop[A, X >: T](acc0: A, f: (A, X) => A): A = self match {
    case Last(head) => f(acc0, head)
    case Fby(head, tail) => tail.foldLeftLoop(f(acc0, head), f)
  }

  @tailrec
  private def reduceLeftLoop[X >: T](acc: X, f: (X, X) => X): X = self match {
    case Fby(elem, tail) => tail.reduceLeftLoop(f(acc, elem), f)
    case Last(elem) => f(acc, elem)
  }

  private def asPartitions[A, B](item: Either[A, B])
  : Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])] =
    item match {
      case Left(l) => Left((NonEmptyList(l), None))
      case Right(r) => Right((None, NonEmptyList(r)))
    }

  @tailrec
  private def partitionLoop[A, B](f: T => Either[A, B],
                                  acc: Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])])
  : Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])] =
    self match {
      case Fby(elem, tail) => tail.partitionLoop(f, appendToPartitions(f(elem), acc))
      case Last(elem) => reversePartitions(appendToPartitions(f(elem), acc))
    }

  private def appendToPartitions[A, B](value: Either[A, B],
                                       acc: Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])])
  : Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])] =
    (value, acc) match {
      case (Left(elem), Left((lefts, optRights))) => Left((Fby(elem, lefts), optRights))
      case (Left(elem), Right((optLefts, rights))) => Right((prependToOptionalNonEmptyList(elem, optLefts), rights))
      case (Right(elem), Left((lefts, optRights))) => Left((lefts, prependToOptionalNonEmptyList(elem, optRights)))
      case (Right(elem), Right((optLefts, rights))) => Right((optLefts, Fby(elem, rights)))
    }

  private def reversePartitions[A, B](acc: Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])])
  : Either[(NonEmptyList[A], Option[NonEmptyList[B]]), (Option[NonEmptyList[A]], NonEmptyList[B])] =
    acc match {
      case Left((lefts, optRights)) => Left((lefts.reverse, optRights.map(_.reverse)))
      case Right((optLefts, rights)) => Right((optLefts.map(_.reverse), rights.reverse))
    }

  private def prependToOptionalNonEmptyList[X](elem: X, optNel: Option[NonEmptyList[X]]): Option[NonEmptyList[X]] =
    optNel.map { nel => Fby(elem, nel) } orElse Some(Last(elem))
}

final case class Fby[+T](head: T, tail: NonEmptyList[T]) extends NonEmptyList[T] {
  override def tailOption: Option[NonEmptyList[T]] = Some(tail)
  override def hasTail: Boolean = true
  override def isLast: Boolean = false
  override def toString = s"${head.toString}, ${tail.toString}"
}

final case class Last[+T](head: T) extends NonEmptyList[T] {
  override def tailOption: Option[NonEmptyList[T]] = None
  override def hasTail: Boolean = false
  override def isLast: Boolean = true
  override def toString = s"${head.toString}"
}
