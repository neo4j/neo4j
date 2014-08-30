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

import scala.reflect.ClassTag

abstract class FunDiggerFactory extends DiggerFactory {
  override type DiggerImpl[-A, M, O] <: AbstractFunDigger[A, M, O]

  def diggerImplClassTag[A, M, O]: ClassTag[DiggerImpl[A, M, O]]

  abstract class AbstractFunDigger[-A : ClassTag, M, O] extends AbstractDigger[A, M, O] {
    self =>

    def drills: Seq[Drill[A, M, O]]

    def apply(layer: Extractor[M, O]): Extractor[A, O] =
      (arg: A) => drill(arg)(layer)

    case object fixPoint extends Extractor[M, O] {
      def apply(member: M) = member match {
        case arg: A => drill(arg)(fixPoint)
        case _      => None
      }
    }

    def lift[A0 : ClassTag] = mapInput[A0] { case v: A => v }

    def mapInput[A0 : ClassTag](f: PartialFunction[A0, A]): DiggerImpl[A0, M, O] =
      fromDrillSeq[A0, M, O](drills.map {
        (aDrill) =>
          (arg: A0) => (layer: Extractor[M, O]) =>  f.lift.apply(arg).flatMap[O](aDrill(_)(layer))
      })

    def mapMembers[M1](f: M => M1): DiggerImpl[A, M1, O] =
      fromDrillSeq[A, M1, O](drills.map {
        (aDrill) => (arg: A) => (layer: Extractor[M1, O]) => aDrill(arg)(x => layer(f(x)))
      })

    def mapOutput[O1 <: O](f: O => O1): DiggerImpl[A, M, O1] =
      fromDrillSeq[A, M, O1](drills.map {
        (aDrill) => (arg: A) => (layer: Extractor[M, O1]) => aDrill(arg)(layer).map(f)
      })

    override def orElse[A1 <: A : ClassTag](other: Digger[A1, M, O]): DiggerImpl[A1, M, O] = {
      implicit val tag: ClassTag[DiggerImpl[A1, M, O]] = diggerImplClassTag[A1, M, O]
      other match {
        case tag(funDigger) => self.++[A1](funDigger)
        case otherDigger    => fromDrillSeq[A1, M, O](drills :+ other.drill)
      }
    }

    override def ++[A1 <: A : ClassTag](other: DiggerImpl[A1, M, O]): DiggerImpl[A1, M, O] =
      fromDrillSeq(drills ++ other.drills)
  }
}
