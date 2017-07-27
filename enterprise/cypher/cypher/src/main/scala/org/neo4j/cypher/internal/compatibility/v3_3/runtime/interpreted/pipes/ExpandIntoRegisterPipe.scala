/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection

/**
  * Expand when both end-points are known, find all relationships of the given
  * type in the given direction between the two end-points.
  *
  * This is done by checking both nodes and starts from any non-dense node of the two.
  * If both nodes are dense, we find the degree of each and expand from the smaller of the two
  *
  * This pipe also caches relationship information between nodes for the duration of the query
  */
case class ExpandIntoRegisterPipe(source: Pipe,
                                  fromOffset: Int,
                                  relOffset: Int,
                                  toOffset: Int,
                                  dir: SemanticDirection,
                                  lazyTypes: LazyTypes,
                                  pipelineInformation: PipelineInformation)
                                 (val id: Id = new Id)
  extends PipeWithSource(source) with PrimitiveCachingExpandInto {
  self =>
  private final val CACHE_SIZE = 100000

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //cache of known connected nodes
    val relCache = new PrimitiveRelationshipsCache(CACHE_SIZE)

    input.flatMap {
      inputRow =>
        val fromNode = inputRow.getLongAt(fromOffset)
        val toNode = inputRow.getLongAt(toOffset)
        val relationships: PrimitiveLongIterator = relCache.get(fromNode, toNode, dir)
          .getOrElse(findRelationships(state.query, fromNode, toNode, relCache, dir, lazyTypes.types(state.query)))

        PrimitiveLongHelper.map(relationships, (relId: Long) => {
          val outputRow = ExecutionContext(pipelineInformation.numberOfLongs)
          outputRow.copyFrom(inputRow)
          outputRow.setLongAt(relOffset, relId)
          outputRow
        })
    }
  }
}
