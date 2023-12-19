/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs
import org.neo4j.kernel.impl.proc.Procedures

class ProceduresAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  private val expectSucceed = Configs.Interpreted - Configs.AllRulePlanners - Configs.Version2_3

  test("should return result") {
    registerTestProcedures()

    val result = executeWith(expectSucceed,
      "CALL org.neo4j.stream123() YIELD count, name RETURN count, name ORDER BY count")

    result.toList should equal(List(
      Map("count" -> 1, "name" -> "count1" ),
      Map("count" -> 2, "name" -> "count2" ),
      Map("count" -> 3, "name" -> "count3" )
    ))
  }

  test("should call cypher from procedure") {
    registerTestProcedures()

    graph.execute("UNWIND [1,2,3] AS i CREATE (a:Cat)")

    val result = executeWith(expectSucceed,
      "CALL org.neo4j.aNodeWithLabel( 'Cat' ) YIELD node RETURN node")

    result.size should equal(1)
  }

  test("should recursively call cypher and procedure") {
    registerTestProcedures()

    graph.execute("UNWIND [1,2,3] AS i CREATE (a:Cat)")

    val result = executeWith(expectSucceed,
      "CALL org.neo4j.recurseN( 3 ) YIELD node RETURN node")

    result.size should equal(1)
  }

  test("should call Core API") {
    registerTestProcedures()

    graph.execute("UNWIND [1,2,3] AS i CREATE (a:Cat)")
    graph.execute("UNWIND [1,2] AS i CREATE (a:Mouse)")

    val result = executeWith(expectSucceed,
      "CALL org.neo4j.findNodesWithLabel( 'Cat' ) YIELD node RETURN node")

    result.size should equal(3)
  }

  test("should call expand using Core API") {
    registerTestProcedures()

    graph.execute("CREATE (c:Cat) WITH c UNWIND [1,2,3] AS i CREATE (c)-[:HUNTS]->(m:Mouse)")

    val result = executeWith(expectSucceed,
      "MATCH (c:Cat) CALL org.neo4j.expandNode( id( c ) ) YIELD node AS n RETURN n")

    result.size should equal(3)
  }

  test("should create node with loop using Core API") {
    registerTestProcedures()

    executeWith(expectSucceed, "CALL org.neo4j.createNodeWithLoop( 'Node', 'Rel' ) YIELD node RETURN count(node)")

    val result = innerExecuteDeprecated("MATCH (n)-->(n) RETURN n")
    result.size should equal(1)
  }

  test("should find shortest path using Graph Algos Djikstra") {
    registerTestProcedures()

    graph.execute(
      """
        |CREATE (s:Start)
        |CREATE (e:End)
        |CREATE (n1)
        |CREATE (n2)
        |CREATE (n3)
        |CREATE (n4)
        |CREATE (n5)
        |CREATE (s)-[:Rel {weight:5}]->(n1)
        |CREATE (s)-[:Rel {weight:7}]->(n2)
        |CREATE (s)-[:Rel {weight:1}]->(n3)
        |CREATE (n1)-[:Rel {weight:2}]->(n2)
        |CREATE (n1)-[:Rel {weight:6}]->(n4)
        |CREATE (n3)-[:Rel {weight:1}]->(n4)
        |CREATE (n4)-[:Rel {weight:1}]->(n5)
        |CREATE (n5)-[:Rel {weight:1}]->(e)
        |CREATE (n2)-[:Rel {weight:2}]->(e)
        |""".stripMargin)

    val result = executeWith(expectSucceed,
      "MATCH (s:Start),(e:End) CALL org.neo4j.graphAlgosDjikstra( s, e, 'Rel', 'weight' ) YIELD node RETURN node")

    result.size should equal(5) // s -> n3 -> n4 -> n5 -> e
  }

  test("should use traversal API") {
    registerTestProcedures()

    // Given
    graph.execute(TestGraph.movies)
    graph.execute("MATCH (c:Person) WHERE c.name in ['Clint Eastwood', 'Gene Hackman'] SET c:Western")

    // When
    val result = executeWith(expectSucceed,
      """MATCH (k:Person {name:'Keanu Reeves'})
                 |CALL org.neo4j.movieTraversal(k) YIELD path RETURN last(nodes(path)).name AS name""".stripMargin)

    // Then
    result.toList should equal(List(Map("name" -> "Clint Eastwood")))
  }

  test("should use correct temporal types") {
    registerTestProcedures()

    val result =innerExecuteDeprecated(
      "CALL org.neo4j.time(localtime.statement())")

    result.toList should be(empty) // and not crash
  }

  test("should call procedure with query parameters overriding default values") {
    registerTestProcedures()

    graph.execute("UNWIND [1,2,3] AS i CREATE (a:Cat)")

    val result = executeWith(Configs.Procs,
      "CALL org.neo4j.aNodeWithLabel", params = Map("label" -> "Cat"))

    result.size should equal(1)
  }

  private def registerTestProcedures(): Unit = {
    graph.getDependencyResolver.resolveDependency(classOf[Procedures]).registerProcedure(classOf[TestProcedure])
  }
}
