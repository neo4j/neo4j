/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v1_9.commands.{AllIdentifiers, CreateNodeStartItem, Query}
import org.neo4j.cypher.internal.compiler.v1_9.mutation.CreateNode
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v1_9.parser.CypherParser

class QueryTest extends Assertions {
  @Test
  def shouldCompactCreateStatements() {
    val end = Query.
      start(CreateNodeStartItem(CreateNode("b", Map()))).
      returns()

    val start = Query.
      start(CreateNodeStartItem(CreateNode("a", Map()))).
      tail(end).
      returns(AllIdentifiers())

    val compacted = start.compact

    val expected = Query.
      start(
      CreateNodeStartItem(CreateNode("a", Map())),
      CreateNodeStartItem(CreateNode("b", Map()))).
      returns()

    assert(expected === compacted)
  }

  @Test
  def integrationTest() {
    val parser = CypherParser()
    val q = parser.parse("create (a1) create (a2) create (a3) create (a4) create (a5) create (a6) create (a7)")
    assert(q.tail.nonEmpty, "wasn't compacted enough")
    val compacted = q.compact

    assert(compacted.tail.isEmpty, "wasn't compacted enough")
    assert(compacted.start.size === 7, "lost create commands")
  }

  @Test
  def integrationTest2() {
    val parser = CypherParser()
    val q = parser.parse("create (a1) create (a2) create (a3) with a1 create (a4) return a1, a4")
    val compacted = q.compact
    var lastQ = compacted

    while (lastQ.tail.nonEmpty)
      lastQ = lastQ.tail.get

    assert(lastQ.returns.columns === List("a1", "a4"), "Lost the tail while compacting")
  }
}
