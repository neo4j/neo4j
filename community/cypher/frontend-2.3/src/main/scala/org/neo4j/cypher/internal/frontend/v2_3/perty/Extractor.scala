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
package org.neo4j.cypher.internal.frontend.v2_3.perty

import org.neo4j.cypher.internal.frontend.v2_3.perty.helpers.TypeTagSupport

import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.runtime.universe._

/**
 * Type of an "unapply-like" converter from I to O
 *
 * (type-safe total version of a partial function with static argument type tags)
 **/
sealed abstract class Extractor[-I : TypeTag, +O : TypeTag] {
  self =>

  // Run extractor, trying to extract a value of type O
  def apply[X <: I : TypeTag](x: X): Option[O]

  // Return extractor that runs other after running self if self does not extract a value
  def orElse[H <: I : TypeTag, P >: O : TypeTag](other: Extractor[H, P]): ExtractorSeq[H, P] = {
    val builder = ExtractorSeq.newBuilder[H, P]
    builder += this
    builder += other
    builder.result()
  }

  // Return extractor that runs other on any value extracted by self
  def andThen[P >: O : TypeTag, V : TypeTag](other: Extractor[P, V]): Extractor[I, V] =
    new SimpleExtractor[I, V] {
      def apply[X <: I : TypeTag](x: X): Option[V] = self(x).flatMap(other(_))
    }

  // Return extractor with different input type that runs self for any value <: I && J
  def lift[J : TypeTag]: Extractor[J, O] = Extractor.fromFilteredInput[J, I] andThen self

  def asSeq[P >: O : TypeTag]: ExtractorSeq[I, P]
}

object Extractor {
  def empty[O : TypeTag] = new SimpleExtractor[Any, O] {
    def apply[X <: Any : TypeTag](x: X) = None
  }

  def fromIdentity[T : TypeTag] = fromInput[T, T]

  def fromInput[I  <: O : TypeTag, O : TypeTag] = new SimpleExtractor[I, O] {
    def apply[X <: I : TypeTag](x: X) = Some(x)
  }

  def fromFilteredInput[I : TypeTag, O : TypeTag]  = new SimpleExtractor[I, O] {
    def apply[X <: I : TypeTag](x: X): Option[O] = {
      val typeX: Type = TypeTagSupport.mostSpecificRuntimeTypeTag(x, typeTag[X]).tpe
      val typeO: Type = typeOf[O]
      if (typeX <:< typeO) Some(x.asInstanceOf[O]) else None
    }
  }

  // Extractor from partial function
  implicit def pick[I : TypeTag, O : TypeTag](pf: PartialFunction[I, O]): SimpleExtractor[I, O] =
    extract(pf.lift)

  // Extractor from total function
  implicit def extract[I : TypeTag, O : TypeTag](f: I => Option[O]): SimpleExtractor[I, O] = {
    new SimpleExtractor[I, O] {
      def apply[X <: I : TypeTag](x: X) = f(x)
    }
  }
}

abstract class SimpleExtractor[-I : TypeTag, +O : TypeTag] extends Extractor[I, O] {
  self: SimpleExtractor[I, O] =>

  def asSeq[P >: O : TypeTag]: ExtractorSeq[I, P] = ExtractorSeq[I, P](Seq(self))
}

final case class ExtractorSeq[-I : TypeTag, +O : TypeTag](extractors: Seq[SimpleExtractor[I, O]])
  extends Extractor[I, O]  {

  self =>

  // Run extractor, trying to extract a value of type O
  def apply[X <: I : TypeTag](x: X): Option[O] = {
    @tailrec
    def findFirstSolution(remaining: Seq[Extractor[I, O]]): Option[O] =
      if (remaining.isEmpty)
        None
      else {
        val solution = remaining.head(x)
        if (solution.isEmpty) findFirstSolution(remaining.tail) else solution
      }

    findFirstSolution(extractors)
  }

  def asSeq[P >: O : TypeTag]: ExtractorSeq[I, P] =
    if (typeOf[O] =:= typeOf[P])
      self.asInstanceOf[ExtractorSeq[I, P]]
    else
      map(identity)

  override def andThen[P >: O : TypeTag, V : TypeTag](other: Extractor[P, V]): Extractor[I, V] =
    map( _ andThen other )

  override def lift[J: TypeTag] =
    map( _.lift[J] )

  def map[X : TypeTag, Y : TypeTag](f: Extractor[I, O] => Extractor[X, Y]) = {
    val builder = ExtractorSeq.newBuilder[X, Y]
    extractors.foreach { x => builder += f(x) }
    builder.result()
  }
}

object ExtractorSeq {
  def newBuilder[I : TypeTag, O : TypeTag] = new mutable.Builder[Extractor[I, O], ExtractorSeq[I, O]] {
    val builder: mutable.Builder[SimpleExtractor[I, O], Seq[SimpleExtractor[I, O]]] =
      Seq.newBuilder[SimpleExtractor[I, O]]

    def +=(elem: Extractor[I, O]): this.type = {
      elem match {
        case simple: SimpleExtractor[I, O] => builder += simple
        case multi: ExtractorSeq[I, O]     => builder ++= multi.extractors
      }
      this
    }

    def clear(): Unit = {
      builder.clear()
    }

    def result(): ExtractorSeq[I, O] = {
      ExtractorSeq(builder.result())
    }
  }
}
