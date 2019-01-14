/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.util.v3_4.test_helpers.WindowsStringSafe
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class DumpToStringAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  implicit val windowsSafe = WindowsStringSafe

  test("format node") {
    createNode(Map("prop1" -> "A", "prop2" -> 2))

    executeWith(Configs.All, ("match (n) return n")).dumpToString() should
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

    executeWith(Configs.All, "match ()-[r]->() return r").dumpToString() should equal(
      """+--------------------------+
        || r                        |
        |+--------------------------+
        || :T[0]{prop1:"A",prop2:2} |
        |+--------------------------+
        |1 row
        |""".stripMargin)
  }

  test("format collection of maps") {
    executeWith(Configs.All,  """RETURN [{ inner: 'Map1' }, { inner: 'Map2' }]""").dumpToString() should
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
