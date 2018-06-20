/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher._

class UnionAcceptanceTest extends ExecutionEngineFunSuite with LernaeanTestSupport {

  test("Should work when doing union with same return varibles") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "B")

    val query =
    """
      |MATCH (N:A)
      |RETURN
      |N.a as A,
      |N.b as B
      |UNION
      |MATCH (M:B) RETURN
      |M.b as A,
      |M.a as B
    """.stripMargin

    val result = testWith(Configs.All - Configs.Compiled, query)
    val expected = List(Map("A" -> "a", "B" -> "b"), Map("A" -> "b", "B" -> "a"))

    result.toList should equal(expected)
  }

  test("Should work when doing union with permutated return varibles") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "b", "b" -> "a"), "B")

    val query =
    """
      |MATCH (N:A)
      |RETURN
      |N.a as B,
      |N.b as A
      |UNION
      |MATCH (M:B) RETURN
      |M.b as A,
      |M.a as B
    """.stripMargin


    val result = testWith(Configs.All - Configs.Compiled - Configs.BackwardsCompatibility + Scenarios.Compatibility3_1Cost + Configs.AllRulePlanners, query)
    val expected = List(Map("A" -> "b", "B" -> "a"), Map("A" -> "a", "B" -> "b"))

    result.toList should equal(expected)
  }

}
