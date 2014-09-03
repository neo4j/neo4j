/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.perty.bling

import scala.annotation.tailrec
import scala.language.higherKinds

import scala.reflect.runtime.universe.TypeTag

import Drill._

sealed abstract class FunSeqExtractor[-I : TypeTag, O : TypeTag]
  extends LayeredDrillExtractor[I, O] {

  override type Self[-U, V] = FunSeqExtractor[U, V]

  def drills: Seq[Drill[I, O]]

  def filterDrill(f: Drill[I, O] => Boolean): Self[I, O] =
    transform(_.filter(f))

  def mapDrill[U : TypeTag, V : TypeTag](f: Drill[I, O] => Drill[U, V]): Self[U, V] =
    transform(_.map(f))

  def flatMapDrill[U : TypeTag, V : TypeTag](f: Drill[I, O] => Seq[Drill[U, V]]): Self[U, V] =
    transform(_.flatMap(f))

  def transform[A0 : TypeTag, O1 : TypeTag](f: Seq[Drill[I, O]] => Seq[Drill[A0, O1]]): Self[A0, O1] =
    FunSeqExtractorFactory.fromSeq[A0, O1](f(drills))

  override def ++[A1 <: I : TypeTag](other: Self[A1, O]): Self[A1, O] =
    FunSeqExtractorFactory.fromSeq[A1, O](drills ++ other.drills)
}

object FunSeqExtractor {
  final class Empty[-I: TypeTag, O: TypeTag] extends FunSeqExtractor[I, O] {
    override def drills: Seq[Drill[I, O]] = Seq.empty
    override val fixPoint: Extractor[I, O] = Extractor.empty[O]
    override val drill: Drill[I, O] = (inner: Extractor[Any, O]) => fixPoint
  }

  abstract class Single[-I: TypeTag, O: TypeTag] extends FunSeqExtractor[I, O] {
    final override def drills: Seq[Drill[I, O]] = Seq(drill)
    final override val fixPoint: Extractor[I, O] = drill.fixPoint
  }

  abstract class Multi[-I: TypeTag, O: TypeTag] extends FunSeqExtractor[I, O] {
    final override val drill: Drill[I, O] =
      (layer: Extractor[Any, O]) =>
        new SimpleExtractor[I, O] {
          def apply[X <: I : TypeTag](x: X): Option[O] = {
            @tailrec
            def findResult(remaining: Seq[Drill[I, O]]): Option[O] = remaining match {
              case Seq(hd, tl @ _*) =>
                val result = hd(layer)(x)
                if (result.isEmpty) findResult(tl) else result
              case _ =>
                None
            }

            findResult(drills)
          }
        }
    final override val fixPoint: Extractor[I, O] = drill.fixPoint
  }
}

