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

import scala.reflect.runtime.universe.TypeTag

object FunSeqExtractorFactory extends ExtractorFactory {
  override type Impl[-I, O] = FunSeqExtractor[I, O]

  protected def newEmpty[I : TypeTag, O : TypeTag] = new FunSeqExtractor.Empty[I, O]

  protected def newFromSingle[I: TypeTag, O : TypeTag](newDrill: Drill[I, O]) =
    new FunSeqExtractor.Single[I, O] {
      override def drill: Drill[I, O] = newDrill
    }

  protected def newFromSeq[I: TypeTag, O : TypeTag](newDrills: Seq[Drill[I, O]]) =
    new FunSeqExtractor.Multi[I, O] {
      override def drills: Seq[Drill[I, O]] = newDrills
    }
}





