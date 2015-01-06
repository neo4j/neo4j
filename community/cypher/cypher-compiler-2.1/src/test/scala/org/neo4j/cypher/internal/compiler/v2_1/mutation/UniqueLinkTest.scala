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
package org.neo4j.cypher.internal.compiler.v2_1.mutation

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects._
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.graphdb.Direction

class UniqueLinkTest extends CypherFunSuite {
  test("given both end nodes, only claims to write relationships, not nodes") {
    val link = UniqueLink("a", "b", "r", "X", Direction.OUTGOING)
    val symbols = new SymbolTable(Map("a" -> CTNode, "b" -> CTNode))
    link.effects(symbols) should equal(READS_RELATIONSHIPS | WRITES_RELATIONSHIPS)
  }

  test("given one end, creates nodes and relationships") {
    val link = UniqueLink("a", "b", "r", "X", Direction.OUTGOING)
    val symbols = new SymbolTable(Map("a" -> CTNode))
    link.effects(symbols) should equal(ALL)
  }
}
