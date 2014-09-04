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

object FunDigger extends FunDiggerFactory {
  override type DiggerImpl[-A, M, O] = FunDigger.AbstractFunDigger[A, M, O]

  def diggerImplClassTag[A, M, O] = implicitly[ClassTag[DiggerImpl[A, M, O]]]

  protected def newEmpty[A: ClassTag, M, O] =
    new FunDigger.AbstractFunDigger[A, M, O] {
      override def drills: Seq[Drill[A, M, O]] = Seq.empty
      override val drill: Drill[A, M, O] = (arg: A) => (layer: Extractor[M, O]) => None
    }

  protected def newFromSingle[A: ClassTag, M, O](newDrill: Drill[A, M, O]) =
    new SingleFunDigger[A, M, O] {
      override val drill: Drill[A, M, O] = newDrill
    }

  protected def newFromMany[A: ClassTag, M, O](newDrills: Seq[Drill[A, M, O]]) =
    new MultiFunDigger[A, M, O] {
      override val drills: Seq[Drill[A, M, O]] = newDrills
    }
}

abstract class SingleFunDigger[-A : ClassTag, M, O] extends FunDigger.AbstractFunDigger[A, M, O] {
  override val drills: Seq[Drill[A, M, O]] = Seq(drill)
}

abstract class MultiFunDigger[-A : ClassTag, M, O] extends FunDigger.AbstractFunDigger[A, M, O] {
  override val drill: Drill[A, M, O] =
    (arg: A) => (layer: Extractor[M, O]) =>
      drills.view.flatMap(sel => sel(arg)(layer)).headOption
}
