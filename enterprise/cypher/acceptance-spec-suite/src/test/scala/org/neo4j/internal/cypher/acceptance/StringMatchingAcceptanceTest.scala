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

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class StringMatchingAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  val expectedToSucceed = Configs.Interpreted - Configs.Version2_3

  var aNode: Node = null
  var bNode: Node = null
  var cNode: Node = null
  var dNode: Node = null
  var eNode: Node = null
  var fNode: Node = null

  override def initTest() {
    super.initTest()
    aNode = createLabeledNode(Map("name" -> "ABCDEF"), "LABEL")
    bNode = createLabeledNode(Map("name" -> "AB"), "LABEL")
    cNode = createLabeledNode(Map("name" -> "abcdef"), "LABEL")
    dNode = createLabeledNode(Map("name" -> "ab"), "LABEL")
    eNode = createLabeledNode(Map("name" -> ""), "LABEL")
    fNode = createLabeledNode("LABEL")
  }

  test("should return null when END WITH is used on non-strings"){
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name ENDS WITH 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should return null when CONTAINS is used on non-strings"){
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name CONTAINS 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should return null when CONTAINS is used on non-strings that contains integers") {
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name CONTAINS '1'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be(List())
  }

  test("should return null when STARTS WITH is used on non-strings"){
    val result = executeWith(expectedToSucceed,
      """
        | CREATE ({name: 1})
        | WITH *
        | MATCH (a)
        | WHERE a.name STARTS WITH 'foo'
        | RETURN a.name""".stripMargin)
    result.columnAs("a.name").toList should be (List())
  }

  test("should distinguish between one and multiple spaces in strings") {
    graph.execute("CREATE (:Label{prop:'1 2'})")
    graph.execute("CREATE (:Label{prop:'1  2'})")

    val result = innerExecuteDeprecated("MATCH (n:Label) RETURN length(n.prop) as l", Map.empty)
    result.toSet should equal(Set(Map("l" -> 3), Map("l" -> 4)))
  }
}
