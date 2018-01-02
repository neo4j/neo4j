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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.commands.{AllIdentifiers, CreateNodeStartItem, Query}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.CreateNode
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class QueryTest extends CypherFunSuite {

  test("shouldCompactCreateStatements") {
    val end = Query.
      start(CreateNodeStartItem(CreateNode("b", Map(), Seq.empty))).
      returns()

    val start = Query.
      start(CreateNodeStartItem(CreateNode("a", Map(), Seq.empty))).
      tail(end).
      returns(AllIdentifiers())

    val compacted = start.compact

    val expected = Query.
      start(
      CreateNodeStartItem(CreateNode("a", Map(), Seq.empty)),
      CreateNodeStartItem(CreateNode("b", Map(), Seq.empty))).
      returns()

    expected should equal(compacted)
  }

  test("shouldCompactManyCreateStatementsWithoutBlowingUp") {
    val allHeads = (1 to 10000).map(x => Query.updates(CreateNode(s"a$x", Map.empty, Seq.empty)).returns())
    val query = allHeads.reduceLeft[Query] {
      case (acc, update) => update.copy( tail = Some(acc) )
    }

    // does not throw
    query.compact
  }
}
