/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.util.v3_4.test_helpers.WindowsStringSafe
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class DumpToStringAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  implicit val windowsSafe = WindowsStringSafe

  test("format node") {
    createNode(Map("prop1" -> "A", "prop2" -> 2))

    executeWith(Configs.All + Configs.Morsel, ("match (n) return n")).dumpToString() should
      equal(
        """+----------------------------+
          || n                          |
          |+----------------------------+
          || Node[0]{prop1:"A",prop2:2} |
          |+----------------------------+
          |1 row
          |""".stripMargin)
  }

  test("format relationship") {
    relate(createNode(), createNode(), "T", Map("prop1" -> "A", "prop2" -> 2))

    executeWith(Configs.All + Configs.Morsel, "match ()-[r]->() return r").dumpToString() should equal(
      """+--------------------------+
        || r                        |
        |+--------------------------+
        || :T[0]{prop1:"A",prop2:2} |
        |+--------------------------+
        |1 row
        |""".stripMargin)
  }

  test("format collection of maps") {
    executeWith(Configs.All + Configs.Morsel,  """RETURN [{ inner: 'Map1' }, { inner: 'Map2' }]""").dumpToString() should
      equal(
        """+----------------------------------------+
          || [{ inner: 'Map1' }, { inner: 'Map2' }] |
          |+----------------------------------------+
          || [{inner -> "Map1"},{inner -> "Map2"}]  |
          |+----------------------------------------+
          |1 row
          |""".stripMargin)
  }
}
