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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class ListExpressionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  private val combinedCallconfiguration = Configs.CommunityInterpreted - Configs.AllRulePlanners - Configs.Version2_3

  // EXTRACT

  test("extract eagerly") {
    val a = createNode("name" -> "original")

    val QUERY = """MATCH (a)
                  |WITH collect(a) AS nodes
                  |WITH nodes, extract(x in nodes | x.name) as oldNames
                  |UNWIND nodes AS n
                  |SET n.name = "newName"
                  |RETURN n.name, oldNames""".stripMargin

    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, QUERY).toList

    result should equal(List(Map("n.name" -> "newName", "oldNames" -> List("original"))))
  }

  // FILTER

  test("filter eagerly") {
    val a = createNode("name" -> "original")

    val QUERY = """MATCH (a)
                  |WITH collect(a) AS nodes
                  |WITH nodes, filter(x in nodes WHERE x.name = "original") as noopFiltered
                  |UNWIND nodes AS n
                  |SET n.name = "newName"
                  |RETURN n.name, length(noopFiltered)""".stripMargin

    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, QUERY).toList

    result should equal(List(Map("n.name" -> "newName", "length(noopFiltered)" -> 1)))
  }

  test("length on filter") {
    val q = "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN length(filter(x in collect(r) WHERE x <> null)) as cn"

    executeWith(Configs.Interpreted, q)
      .toList should equal(List(Map("cn" -> 0)))
  }
}
