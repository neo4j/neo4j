/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExhaustiveLimitPlanningIntegrationTest
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("should plan exhaustive limit for single update followed by LIMIT 0") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .build()
    val query =
      s"""
         |CREATE (m:M)
         |RETURN m LIMIT 0
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .exhaustiveLimit(0)
      .create(createNode("m", "M"))
      .argument()
      .build()
  }

  test("should plan exhaustive limit for reads followed by multiple updates followed by LIMIT") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CREATE (m:M)
         |RETURN m LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .exhaustiveLimit(3)
      .create(createNode("m", "M"))
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should plan exhaustive limit for reads followed by write procedure call followed by LIMIT") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .addProcedure(
        procedureSignature("my.writeProc")
          .withAccessMode(ProcedureReadWriteAccess)
          .build()
      )
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CALL my.writeProc()
         |RETURN n LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("n")
      .exhaustiveLimit(3)
      .procedureCall("my.writeProc()")
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should not plan exhaustive limit if LIMIT precedes update") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |WITH n LIMIT 3
         |CREATE (m:M)
         |RETURN m
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .create(createNode("m", "M"))
      .limit(3)
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should not plan exhaustive limit when already eagerized") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:M)-[]->()", 20)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CREATE (m:M)
         |WITH m ORDER BY m
         |MATCH (m)-[r]->(o)
         |RETURN m LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .limit(3)
      .expand("(m)-[r]->(o)")
      .sort("m ASC")
      .eager()
      .create(createNode("m", "M"))
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should plan exhaustive limit for create in subquery") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:N)-[]->()", 20)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CALL {
         |  CREATE (m:M)
         |  RETURN m
         |}
         |RETURN m LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .exhaustiveLimit(3)
      .apply()
      .|.create(createNode("m", "M"))
      .|.argument()
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should plan exhaustive limit for write procedure call in subquery") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("N", 10)
      .addProcedure(
        procedureSignature("my.writeProc")
          .withAccessMode(ProcedureReadWriteAccess)
          .build()
      )
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CALL {
         |  CALL my.writeProc()
         |}
         |RETURN n LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("n")
      .exhaustiveLimit(3)
      .subqueryForeach()
      .|.procedureCall("my.writeProc()")
      .|.argument()
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should plan exhaustive limit for reads followed by multiple updates followed by SKIP + LIMIT") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CREATE (m:M)
         |RETURN m SKIP 10 LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .skip(10)
      .exhaustiveLimit(add(literalInt(3), literalInt(10)))
      .create(createNode("m", "M"))
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should not plan exhaustive limit if SKIP + LIMIT precedes update") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |WITH n SKIP 10 LIMIT 3
         |CREATE (m:M)
         |RETURN m
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .create(createNode("m", "M"))
      .skip(10)
      .limit(add(literalInt(3), literalInt(10)))
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should not plan exhaustive limit when already eagerized before SKIP + LIMIT") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:M)-[]->()", 20)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CREATE (m:M)
         |WITH m ORDER BY m
         |MATCH (m)-[r]->(o)
         |RETURN m SKIP 10 LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .skip(10)
      .limit(add(literalInt(3), literalInt(10)))
      .expand("(m)-[r]->(o)")
      .sort("m ASC")
      .eager()
      .create(createNode("m", "M"))
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("should plan exhaustive limit for create in subquery before SKIP + LIMIT") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:N)-[]->()", 20)
      .setLabelCardinality("N", 10)
      .build()

    val query =
      s"""
         |MATCH (n:N)
         |CALL {
         |  CREATE (m:M)
         |  RETURN m
         |}
         |RETURN m SKIP 10 LIMIT 3
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("m")
      .skip(10)
      .exhaustiveLimit(add(literalInt(3), literalInt(10)))
      .apply()
      .|.create(createNode("m", "M"))
      .|.argument()
      .nodeByLabelScan("n", "N")
      .build()
  }

  test("Should plan exhaustive limit when updating plan on RHS of apply") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(10)
      .build()

    val query =
      s"""
         |MATCH (n)
         |CALL {
         |  CREATE (a)
         |  RETURN count(a) AS ca
         |}
         |RETURN n LIMIT 0
         |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("n")
      .exhaustiveLimit(0)
      .apply()
      .|.aggregation(Seq(), Seq("count(a) AS ca"))
      .|.create(createNode("a"))
      .|.argument()
      .allNodeScan("n")
      .build()
  }

  test("Should plan exhaustive limit with LIMIT 0, even if there is an eager plan in between") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(10)
      .build()

    val query =
      """CREATE (a) 
        |WITH a 
        |ORDER BY a 
        |WITH a AS b 
        |RETURN b 
        |LIMIT 0""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("b")
      .exhaustiveLimit(0)
      .projection("a AS b")
      .sort("a ASC")
      .create(createNode("a"))
      .argument()
      .build()
  }

  test("Should not plan exhaustive limit with LIMIT 1, if there is an eager plan in between") {
    // given
    val config = plannerBuilder()
      .setAllNodesCardinality(10)
      .build()

    val query =
      """CREATE (a) 
        |WITH a 
        |ORDER BY a 
        |WITH a AS b 
        |RETURN b 
        |LIMIT 1
        |""".stripMargin

    // when
    val plan = config.plan(query)

    // then
    plan shouldEqual config.planBuilder()
      .produceResults("b")
      .limit(1)
      .projection("a AS b")
      .sort("a ASC")
      .create(createNode("a"))
      .argument()
      .build()
  }
}
