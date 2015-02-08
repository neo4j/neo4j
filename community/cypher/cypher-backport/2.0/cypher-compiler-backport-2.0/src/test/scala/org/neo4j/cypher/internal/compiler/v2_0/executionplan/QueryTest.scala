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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.commands.{AllIdentifiers, CreateNodeStartItem, Query}
import org.neo4j.cypher.internal.compiler.v2_0.mutation.CreateNode
import org.scalatest.Assertions

class QueryTest extends Assertions {
  @Test
  def shouldCompactCreateStatements() {
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

    assert(expected === compacted)
  }
}
