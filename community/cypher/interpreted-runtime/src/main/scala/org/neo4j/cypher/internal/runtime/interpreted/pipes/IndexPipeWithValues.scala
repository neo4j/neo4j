/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.IndexedNodeWithProperties
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext

/**
  * Provides a helper method for index pipes that get nodes together with actual property values.
  */
trait IndexPipeWithValues extends Pipe {

  // Name of the node variable
  val ident: String
  // all indices where the index can provide values
  val propertyIndicesWithValues: Array[Int]
  // the names of the properties where we will get values
  val propertyNamesWithValues: Array[String]

  /**
    * Create an Iterator of ExecutionContexts given an Iterator of tuples of nodes and property values,
    * by copying the node and all values into the given baseContext.
    */
  def createResultsFromTupleIterator(baseContext: ExecutionContext, tupleIterator: Iterator[IndexedNodeWithProperties]): Iterator[ExecutionContext] = {
    tupleIterator.map {
      case IndexedNodeWithProperties(node, values) =>
        val valueEntries = (0 until values.length).map(i => propertyNamesWithValues(i) -> values(i) )
        val newEntries = (ident -> node) +: valueEntries
        executionContextFactory.copyWith(baseContext, newEntries)
    }
  }
}
