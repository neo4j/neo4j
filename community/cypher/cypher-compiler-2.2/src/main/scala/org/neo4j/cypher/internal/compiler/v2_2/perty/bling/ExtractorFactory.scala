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

import scala.language.higherKinds

import scala.reflect.runtime.universe.TypeTag

trait ExtractorFactory {
  type Impl[-I, O] <: Extractor[I, O]

  final def apply[I : TypeTag, O : TypeTag](newDrills: Drill[I, O]*): Impl[I, O] = fromSeq[I, O](newDrills)
  final def empty[I : TypeTag, O : TypeTag]: Impl[I, O] = newEmpty[I, O]
  final def fromSingle[I : TypeTag, O : TypeTag](drill: Drill[I, O]): Impl[I, O] = newFromSingle[I, O](drill)

  def fromSeq[I : TypeTag, O : TypeTag](drills: Seq[Drill[I, O]]): Impl[I, O] =
    if (drills.isEmpty)
      newEmpty[I, O]
    else
      if (drills.tails.isEmpty) newFromSingle[I, O](drills.head) else newFromSeq[I, O](drills)

  protected def newEmpty[I : TypeTag, O : TypeTag]: Impl[I, O]
  protected def newFromSingle[I : TypeTag, O : TypeTag](drill: Drill[I, O]): Impl[I, O]
  protected def newFromSeq[I : TypeTag, O : TypeTag](drills: Seq[Drill[I, O]]): Impl[I, O]
}


