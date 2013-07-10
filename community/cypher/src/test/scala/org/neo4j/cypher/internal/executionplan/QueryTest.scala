/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan

import org.junit.Test
import org.neo4j.cypher.internal.commands.{AllIdentifiers, CreateNodeStartItem, Query}
import org.neo4j.cypher.internal.mutation.CreateNode
import org.scalatest.Assertions
import org.neo4j.cypher.CypherParser

class QueryTest extends Assertions {
  @Test
  def shouldCompactCreateStatements() {
    val end = Query.
      start(CreateNodeStartItem(CreateNode("b", Map(), Seq.empty, bare = true))).
      returns()

    val start = Query.
      start(CreateNodeStartItem(CreateNode("a", Map(), Seq.empty, bare = true))).
      tail(end).
      returns(AllIdentifiers())

    val compacted = start.compact

    val expected = Query.
      start(
      CreateNodeStartItem(CreateNode("a", Map(), Seq.empty, bare = true)),
      CreateNodeStartItem(CreateNode("b", Map(), Seq.empty, bare = true))).
      returns()

    assert(expected === compacted)
  }

  @Test
  def integrationTest() {
    val parser = new CypherParser()
    val q:Query = parser.parse("create (a1) create (a2) create (a3) create (a4) create (a5) create (a6) create (a7)").asInstanceOf[Query]
    assert(q.tail.nonEmpty, "wasn't compacted enough")
    val compacted = q.compact

    assert(compacted.tail.isEmpty, "wasn't compacted enough")
    assert(compacted.start.size === 7, "lost create commands")
  }

  @Test
  def integrationTest2() {
    val parser = new CypherParser()
    val q = parser.parse("create (a1) create (a2) create (a3) with a1 create (a4) return a1, a4").asInstanceOf[Query]
    val compacted = q.compact
    var lastQ = compacted

    while (lastQ.tail.nonEmpty)
      lastQ = lastQ.tail.get

    assert(lastQ.returns.columns === List("a1", "a4"), "Lost the tail while compacting")
  }
}