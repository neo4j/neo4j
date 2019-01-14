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
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class MiscAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // This test verifies a bugfix in slotted runtime
  test("should be able to compare integers") {
    val query = """
      UNWIND range(0, 1) AS i
      UNWIND range(0, 1) AS j
      WITH i, j
      WHERE i <> j
      RETURN i, j"""

    val result = executeWith(Configs.Interpreted, query)
    result.toList should equal(List(Map("j" -> 1, "i" -> 0), Map("j" -> 0, "i" -> 1)))
  }

  test("order by after projection") {
    val query =
      """
        |UNWIND [ 1,2 ] as x
        |UNWIND [ 3,4 ] as y
        |RETURN x AS y, y as y3
        |ORDER BY y
      """.stripMargin

    val result = executeWith(Configs.All, query, expectedDifferentResults = Configs.OldAndRule)
    result.toList should equal(List(Map("y" -> 1, "y3" -> 3), Map("y" -> 1, "y3" -> 4), Map("y" -> 2, "y3" -> 3), Map("y" -> 2, "y3" -> 4)))
  }

  test("should unwind nodes") {
    val n = createNode("prop" -> 42)

    val query = "UNWIND $nodes AS n WITH n WHERE n.prop = 42 RETURN n"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("nodes" -> List(n)))

    result.toList should equal(List(Map("n" -> n)))
  }

  test("should unwind nodes from literal list") {
    val n = createNode("prop" -> 42)

    val query = "UNWIND [$node] AS n WITH n WHERE n.prop = 42 RETURN n"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("node" -> n))

    result.toList should equal(List(Map("n" -> n)))
  }

  test("should unwind relationships") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "prop" -> 42)

    val query = "UNWIND $relationships AS r WITH r WHERE r.prop = 42 RETURN r"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("relationships" -> List(r)))

    result.toList should equal(List(Map("r" -> r)))
  }

  test("should unwind relationships from literal list") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "prop" -> 42)

    val query = "UNWIND [$relationship] AS r WITH r WHERE r.prop = 42 RETURN r"
    val result = executeWith(Configs.All - Configs.Version2_3, query, params = Map("relationship" -> r))

    result.toList should equal(List(Map("r" -> r)))
  }

  test("should be able to use long values for LIMIT in interpreted runtime") {
    val a = createNode()
    val b = createNode()

    val limit: Long = Int.MaxValue + 1l
    // If we would use Ints for storing the limit, then we would end up with "limit 0"
    // thus, if we actually return the two nodes, then it proves that we used a long
    val query = "MATCH (n) RETURN n LIMIT " + limit
    val worksCorrectlyInConfig = Configs.Version3_4 + Configs.Version3_3 - Configs.AllRulePlanners
    // the query will work in all configs, but only have the correct result in those specified configs
    val result = executeWith(Configs.All, query, Configs.All - worksCorrectlyInConfig)
    result.toList should equal(List(Map("n" -> a), Map("n" -> b)))
  }

  test("should not explode on complex filter() projection in write query") {

    val query = """UNWIND [{children : [
                  |            {_type : "browseNodeId", _text : "20" },
                  |            {_type : "childNodes", _text : "21" }
                  |        ]},
                  |       {children : [
                  |            {_type : "browseNodeId", _text : "30" },
                  |            {_type : "childNodes", _text : "31" }
                  |        ]}] AS row
                  |
                  |WITH   head(filter( child IN row.children WHERE child._type = "browseNodeId" ))._text as nodeId,
                  |       head(filter( child IN row.children WHERE child._type = "childNodes" )) as childElement
                  |
                  |MERGE  (parent:Category { id: toInt(nodeId) })
                  |
                  |RETURN *""".stripMargin

    val result = graph.execute(query)
    result.resultAsString() // should not explode
  }
}
