/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType
import org.neo4j.values.AnyValues
import org.scalatest.mock.MockitoSugar

import scala.collection.Map

class FakePipe(val data: Iterator[Map[String, Any]], newVariables: (String, CypherType)*) extends Pipe with MockitoSugar {

  def this(data: Traversable[Map[String, Any]], variables: (String, CypherType)*) = this(data.toIterator, variables:_*)

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    data.map(m => ExecutionContext(collection.mutable.Map(m.mapValues(AnyValues.of).toSeq: _*)))

  var id = new Id
}
