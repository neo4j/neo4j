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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.{CypherTypeException, ExecutionContext}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

class ArgumentPipeTest extends CypherFunSuite {
  implicit val pipesMonitor = mock[PipeMonitor]

  test("should verify that node symbols are nodes in the initial context") {
    val pipe = ArgumentPipe(SymbolTable(Map("a" -> CTNode)))()
    val state = newQueryState("a" -> 1)

    evaluating {
      pipe.internalCreateResults(state)
    } should produce[CypherTypeException]
  }

  test("should not verify that nulls are nodes in the initial context") {
    val pipe = ArgumentPipe(SymbolTable(Map("a" -> CTNode)))()
    val state = newQueryState("a" -> null)

    pipe.internalCreateResults(state)
  }

  test("should verify that relationship symbols are relationships in the initial context") {
    val pipe = ArgumentPipe(SymbolTable(Map("a" -> CTRelationship)))()
    val state = newQueryState("a" -> 1)

    evaluating {
      pipe.internalCreateResults(state)
    } should produce[CypherTypeException]
  }

  test("should not verify that nulls are relationships in the initial context") {
    val pipe = ArgumentPipe(SymbolTable(Map("a" -> CTRelationship)))()
    val state = newQueryState("a" -> null)

    pipe.internalCreateResults(state)
  }

  def newQueryState(entries: (String, Any)*): QueryState =
    QueryStateHelper.empty.copy(initialContext = Some(ExecutionContext.from(entries: _*)))
}
