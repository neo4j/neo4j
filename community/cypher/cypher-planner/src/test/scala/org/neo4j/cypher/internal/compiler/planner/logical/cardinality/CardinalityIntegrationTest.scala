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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

import java.lang.Boolean.TRUE

import scala.math.sqrt

abstract class CardinalityIntegrationTest extends CypherFunSuite with CardinalityIntegrationTestSupport {

  val allNodes = 733.0
  val personCount = 324.0
  val relCount = 50.0
  val rel2Count = 78.0

  def getIndexType: IndexType

  test("should agree between QPP with 2 relationships and its expansion") {
    val config = plannerBuilder()
      .setAllNodesCardinality(200)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 20)
      .setRelationshipCardinality("()-[]->()", 70)
      .setRelationshipCardinality("()-[:R]->()", 40)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[:R]->()", 35)
      .setRelationshipCardinality("()-[:R]->(:B)", 25)
      .setRelationshipCardinality("()-[:S]->()", 48)
      .setRelationshipCardinality("(:B)-[:S]->()", 12)
      .setRelationshipCardinality("(:B)-[:R]->()", 15)
      .build()

    val expectedCardinality = 35 * 40 * 40 * 15 / math.pow(200, 3) * math.pow(.99, 6)

    queryShouldHaveCardinality(
      config,
      "MATCH (a:A)(()-[r:R]->()<-[s:R]-()){2}(b:B)",
      expectedCardinality
    )

    queryShouldHaveCardinality(
      config,
      "MATCH (a:A)-[r1:R]->()<-[s1:R]-()-[r2:R]->()<-[s2:R]-(b:B)",
      expectedCardinality
    )
  }

  test("query containing a WITH and LIMIT on low/fractional cardinality") {
    val i = .1
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()",
      Math.min(i, 10.0) * relCount / i
    )
  }

  test("query containing a WITH and LIMIT on high cardinality") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT 10 MATCH (a)-[:REL]->()",
      Math.min(i, 10.0) * relCount / i
    )
  }

  test("query containing a WITH and LIMIT on parameterized cardinality") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT $limit MATCH (a)-[:REL]->()",
      Math.min(i, DEFAULT_LIMIT_CARDINALITY) * relCount / i
    )
  }

  test("query containing a WITH and aggregation vol. 2") {
    val patternNodeCrossProduct = allNodes * allNodes
    val labelSelectivity = personCount / allNodes
    val maxRelCount = patternNodeCrossProduct * labelSelectivity
    val relSelectivity = rel2Count / maxRelCount
    val firstQG = patternNodeCrossProduct * labelSelectivity * relSelectivity
    val aggregation = Math.sqrt(firstQG)

    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personCount)
      .setAllRelationshipsCardinality(relCount + rel2Count)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("(:Person)-[:REL]->()", relCount)
      .setRelationshipCardinality("()-[:REL2]->()", rel2Count)
      .setRelationshipCardinality("(:Person)-[:REL2]->()", rel2Count)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person)-[:REL2]->(b) WITH a, count(*) as c MATCH (a)-[:REL]->()",
      aggregation * relCount / personCount
    )
  }

  test("query containing both SKIP and LIMIT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (n:Person) WITH n SKIP 5 LIMIT 10", Math.min(i, 10.0))
  }

  test("query containing LIMIT by expression") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, "MATCH (n:Person) WITH n LIMIT toInteger(1+1)", Math.min(i, 2.0))
  }

  test("query containing both SKIP and LIMIT with large skip, so skip + limit exceeds total row count boundary") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      s"MATCH (n:Person) WITH n SKIP ${(personCount - 5).toInt} LIMIT 10",
      Math.min(i, 5.0)
    )
  }

  test("query containing SKIP by expression") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(config, s"MATCH (n:Person) WITH n SKIP toInteger($personCount - 2)", Math.min(i, 2.0))
  }

  test("should reduce cardinality for a WHERE after a WITH") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT 10 WHERE a.age = 20",
      Math.min(i, 10.0) * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality using index stats for a WHERE after a WITH") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .addNodeIndex("Person", Seq("age"), 0.3, 0.2, indexType = getIndexType)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a:Person) WITH a, 1 AS x WHERE a.age = 20", i * 0.3 * 0.2)
  }

  test("should reduce cardinality for a WHERE after a WITH, unknown LIMIT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a LIMIT $limit WHERE a.age = 20",
      Math.min(i, DEFAULT_LIMIT_CARDINALITY) * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with ORDER BY") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH a ORDER BY a.name WHERE a.age = 20",
      i * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with DISTINCT") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH DISTINCT a WHERE a.age = 20",
      i * DEFAULT_DISTINCT_SELECTIVITY * DEFAULT_PROPERTY_SELECTIVITY * DEFAULT_EQUALITY_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION without grouping") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH count(a) AS count WHERE count > 20",
      DEFAULT_RANGE_SELECTIVITY
    )
  }

  test("should reduce cardinality for a WHERE after a WITH, with AGGREGATION with grouping") {
    val i = personCount
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", i)
      .build()
    queryShouldHaveCardinality(
      config,
      "MATCH (a:Person) WITH count(a) AS count, a.name AS name WHERE count > 20",
      Math.sqrt(i) * DEFAULT_RANGE_SELECTIVITY
    )
  }

  private val signature = ProcedureSignature(
    QualifiedName(Seq("my", "proc"), "foo"),
    IndexedSeq(FieldSignature("int", CTInteger)),
    Some(IndexedSeq(FieldSignature("x", CTNode))),
    None,
    ProcedureReadOnlyAccess,
    id = 0
  )

  test("standalone procedure call should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "CALL my.proc.foo(42) YIELD x", DEFAULT_MULTIPLIER)
  }

  test("procedure call with no input should not have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Foo", 0)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) CALL my.proc.foo(42) YIELD x", 1)
  }

  test("procedure call with large input should multiply cardinality") {
    val inputSize = 1000000
    val config = plannerBuilder()
      .setAllNodesCardinality(inputSize)
      .setLabelCardinality("Foo", inputSize)
      .addProcedure(signature)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) CALL my.proc.foo(42) YIELD x", DEFAULT_MULTIPLIER * inputSize)
  }

  test("standalone LOAD CSV should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "LOAD CSV FROM 'foo' AS csv", DEFAULT_MULTIPLIER)
  }

  test("LOAD CSV with no input should not have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Foo", 0)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) LOAD CSV FROM 'foo' AS csv", 1)
  }

  test("LOAD CSV with large input should multiply cardinality") {
    val inputSize = 1000000
    val config = plannerBuilder()
      .setAllNodesCardinality(inputSize)
      .setLabelCardinality("Foo", inputSize)
      .build()
    queryShouldHaveCardinality(config, "MATCH (:Foo) LOAD CSV FROM 'foo' AS csv", DEFAULT_MULTIPLIER * inputSize)
  }

  test("UNWIND with no information should have default cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND $foo AS i", DEFAULT_MULTIPLIER)
  }

  test("UNWIND with empty list literal should have 0 cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND [] AS i", 0.0)
  }

  test("UNWIND with non-empty list literal should have list size cardinality") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND [1, 2, 3, 4, 5] AS i", 5)
  }

  test("UNWIND with single element range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, 0) AS i", 1)
  }

  test("UNWIND with empty range 1") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, -1) AS i", 0.0)
  }

  test("UNWIND with empty range 2") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(10, 0, 1) AS i", 0.0)
  }

  test("UNWIND with empty range 3") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(0, 10, -1) AS i", 0.0)
  }

  test("UNWIND with non-empty range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 10) AS i", 10)
  }

  test("UNWIND with non-empty DESC range") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(10, 1, -1) AS i", 10)
  }

  test("UNWIND with non-empty range with aligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 9, 2) AS i", 5)
  }

  test("UNWIND with non-empty DESC range with aligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(9, 1, -2) AS i", 5)
  }

  test("UNWIND with non-empty range with unaligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(1, 9, 3) AS i", 3)
  }

  test("UNWIND with non-empty DESC range with unaligned step") {
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodes)
      .build()
    queryShouldHaveCardinality(config, "UNWIND range(9, 1, -3) AS i", 3)
  }

  test("empty graph") {
    val config = plannerBuilder()
      .setAllNodesCardinality(0)
      .build()
    queryShouldHaveCardinality(config, "MATCH (a) WHERE a.prop = 10", 0)
  }

  test("honours bound arguments") {
    val relCount = 1000.0
    val fooCount = 100.0
    val barCount = 400.0
    val inboundCardinality = 13
    val nodeCount = fooCount + barCount
    val config = plannerBuilder()
      .setAllNodesCardinality(nodeCount)
      .setLabelCardinality("FOO", fooCount)
      .setLabelCardinality("BAR", barCount)
      .setAllRelationshipsCardinality(relCount)
      .setRelationshipCardinality("()-[:TYPE]->()", relCount)
      .setRelationshipCardinality("(:FOO)-[:TYPE]->()", relCount)
      .setRelationshipCardinality("()-[:TYPE]->(:BAR)", relCount)
      .setRelationshipCardinality("(:FOO)-[:TYPE]->(:BAR)", relCount)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (a:FOO) WITH a LIMIT 1 UNWIND range(1, $inboundCardinality) AS i MATCH (a:FOO)-[:TYPE]->(b:BAR)",
      relCount / nodeCount * inboundCardinality
    )
  }

  test("input cardinality <1.0 => input cardinality * scan cardinality") {
    val inboundCardinality = 1
    val whereSelectivity = 0.5
    val nodes = 500
    val config = plannerBuilder()
      .setAllNodesCardinality(nodes)
      .setLabelCardinality("Foo", inboundCardinality)
      .addNodeIndex("Foo", Seq("bar"), 1.0, whereSelectivity, indexType = getIndexType)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (f:Foo) WHERE f.bar = 1 WITH f, 1 AS horizon MATCH (a)",
      inboundCardinality * whereSelectivity * nodes
    )
  }

  test("input cardinality >1.0 => input cardinality * scan cardinality") {
    val inboundCardinality = 10
    val whereSelectivity = 1.0
    val nodes = 500
    val config = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("Foo", inboundCardinality)
      .addNodeIndex("Foo", Seq("bar"), 1.0, whereSelectivity, indexType = getIndexType)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (f:Foo) WHERE f.bar = 1 WITH f, 1 AS horizon MATCH (a)",
      inboundCardinality * whereSelectivity * nodes
    )
  }

  test("should use relationship index for cardinality estimation with inlined type predicate") {
    val inboundCardinality = 10
    val whereSelectivity = 0.1
    val config = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("A", inboundCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", inboundCardinality)
      .setRelationshipCardinality("()-[:R]->()", inboundCardinality)
      .setAllRelationshipsCardinality(10)
      .addRelationshipIndex("R", Seq("prop"), whereSelectivity, whereSelectivity, indexType = getIndexType)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (a:A)-[r:R]->() WHERE r.prop IS NOT NULL",
      inboundCardinality * whereSelectivity
    )
  }

  test("should use relationship index for cardinality estimation with non-inlined type predicate") {
    val inboundCardinality = 10
    val whereSelectivity = 0.1
    val config = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("A", inboundCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", inboundCardinality)
      .setRelationshipCardinality("()-[:R]->()", inboundCardinality)
      .setAllRelationshipsCardinality(10)
      .addRelationshipIndex("R", Seq("prop"), whereSelectivity, whereSelectivity, indexType = getIndexType)
      .build()

    queryShouldHaveCardinality(
      config,
      s"MATCH (a:A)-[r]->() WHERE r:R AND r.prop IS NOT NULL",
      inboundCardinality * whereSelectivity
    )
  }

  test("should only use predicates marked as solved for cardinality estimation of node index seek") {
    val labelCardinality = 50
    val existsSelectivity = 0.5
    val uniqueSelectivity = 0.1
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", labelCardinality)
      .addNodeIndex("Person", Seq("prop1", "prop2"), existsSelectivity, uniqueSelectivity, indexType = getIndexType)
      .build()

    val query = "MATCH (n:Person) WHERE n.prop1 > 0 AND n.prop2 = 0"

    val planState = config.planState(query + " RETURN n")
    val plan = planState.logicalPlan
    val cardinalities = planState.planningAttributes.effectiveCardinalities
    val nodeIndexSeekCardinality = plan.flatten.collectFirst { case lp: NodeIndexSeek => cardinalities.get(lp.id) }.get

    // The range selectivity defaults to equality selectivity if there are few unique values.
    nodeIndexSeekCardinality.amount shouldEqual (labelCardinality * existsSelectivity * sqrt(uniqueSelectivity))

    queryShouldHaveCardinality(config, query, labelCardinality * existsSelectivity * uniqueSelectivity)
  }

  test("node by id seek should have cardinality 1") {
    val allNodesCardinality = 1000
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodesCardinality)
      .build()

    val query = "MATCH (n) WHERE id(n) = 5"
    queryShouldHaveCardinality(config, query, 1)
  }

  test("directed relationship by id seek should have cardinality 1") {
    val allNodesCardinality = 1000
    val allRelationshipsCardinality = 20
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodesCardinality)
      .setAllRelationshipsCardinality(allRelationshipsCardinality)
      .build()

    val query = "MATCH ()-[r]->() WHERE id(r) = 5"
    queryShouldHaveCardinality(config, query, 1)
  }

  test("undirected relationship by id seek should have cardinality 2") {
    val allNodesCardinality = 1000
    val allRelationshipsCardinality = 20
    val config = plannerBuilder()
      .setAllNodesCardinality(allNodesCardinality)
      .setAllRelationshipsCardinality(allRelationshipsCardinality)
      .build()

    val query = "MATCH ()-[r]-() WHERE id(r) = 5"
    queryShouldHaveCardinality(config, query, 2)
  }

  test("relationship type scan on bound start node should correctly calculate cardinality") {
    val aCardinality = 500
    val rCardinality = 10
    val arCardinality = 7

    val config = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", arCardinality)
      .build()

    val query =
      """
        |MATCH (a:A)
        |WITH a, 1 AS foo
        |MATCH (a)-[r:R]->() USING SCAN r:R
        |""".stripMargin

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipTypeScan(LogicalVariable("r"), _, _, _, _, _) => true
      },
      aCardinality * rCardinality
    )
    // The whole query checks that r's start node is a
    queryShouldHaveCardinality(config, query, arCardinality)
  }

  test("relationship type scan with equal start and end node should correctly calculate cardinality") {
    val aCardinality = 500
    val rCardinality = 100
    val araCardinality = 7

    val config = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->(:A)", araCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", araCardinality * 5)
      .build()

    val query = "MATCH (a:A)-[r:R]->(a)"

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipTypeScan(LogicalVariable("r"), _, _, _, _, _) => true
      },
      rCardinality
    )
  }

  test("relationship by id seek on bound start node should correctly calculate cardinality") {
    val aCardinality = 5
    val rCardinality = 1000
    val arCardinality = 700

    val config = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", arCardinality)
      .build()

    val query =
      """
        |MATCH (a:A)
        |WITH a, 1 AS foo
        |MATCH (a)-[r:R]->() WHERE id(r) = 5
        |""".stripMargin

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipByIdSeek(LogicalVariable("r"), _, _, _, _) => true
      },
      aCardinality
    )
    // The whole query checks that r's start node is a
    queryShouldHaveCardinality(config, query, arCardinality * 1.0 / rCardinality)
  }

  test("relationship by id seek with equal start and end node should correctly calculate cardinality") {
    val aCardinality = 5
    val rCardinality = 1000
    val arCardinality = 700

    val config = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("A", aCardinality)
      .setAllRelationshipsCardinality(rCardinality)
      .setRelationshipCardinality("()-[:R]->()", rCardinality)
      .setRelationshipCardinality("(:A)-[:R]->()", arCardinality)
      .build()

    val query = "MATCH (a)-[r:R]->(a) WHERE id(r) = 5"

    // The leaf plan does not yet check that r's start node is a,
    // so we want cardinality estimation to take that into account.
    planShouldHaveCardinality(
      config,
      query,
      {
        case DirectedRelationshipByIdSeek(LogicalVariable("r"), _, _, _, _) => true
      },
      1
    )
  }

  test("text index predicate with an empty string argument") {
    val aNodeCount = 500
    val textIndexSelectivity = 0.1

    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aNodeCount)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = textIndexSelectivity,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .build()

    for (op <- Seq("STARTS WITH", "ENDS WITH", "CONTAINS")) withClue(op) {
      val q = s"MATCH (a:A) WHERE a.prop $op '' "
      queryShouldHaveCardinality(cfg, q, aNodeCount * textIndexSelectivity)
    }
  }

  test("should use distance seekable predicate for cardinality estimation") {
    val labelCardinality = 50
    val propSelectivity = 0.5
    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", labelCardinality)
      .addNodeIndex("Person", Seq("prop"), propSelectivity, 1, indexType = IndexType.POINT)
      .build()

    val query = "MATCH (n:Person) WHERE point.distance(n.prop, point({x: 1.1, y: 5.4})) < 0.5"

    val planState = config.planState(query + " RETURN n")
    val plan = planState.logicalPlan
    val cardinalities = planState.planningAttributes.effectiveCardinalities
    val nodeIndexSeekCardinality = plan.flatten.collectFirst { case lp: NodeIndexSeek => cardinalities.get(lp.id) }.get

    nodeIndexSeekCardinality.amount shouldEqual (labelCardinality * propSelectivity * DEFAULT_RANGE_SEEK_FACTOR)

    queryShouldHaveCardinality(config, query, labelCardinality * propSelectivity * DEFAULT_RANGE_SEEK_FACTOR)
  }

  test("Infer label of intermediate node with outgoing relationships") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(GraphDatabaseInternalSettings.label_inference, TRUE)
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)<-[friendship:KNOWS]-(friend)-[membership:HAS_MEMBER]->(forum)",
      knowsRelationships * hasMemberRelationships / personNodes // since we can infer that friend:Person
    )
  }

  test("Infer label of intermediate node with one incoming relationships") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(GraphDatabaseInternalSettings.label_inference, TRUE)
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)-[friendship:KNOWS]->(friend)<-[membership:HAS_MEMBER]-(forum)",
      knowsRelationships * hasMemberRelationships / personNodes // since we can infer that friend:Person
    )
  }

  test("Infer label of intermediate node with one incoming and one outgoing relationships") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(GraphDatabaseInternalSettings.label_inference, TRUE)
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)-[friendship:KNOWS]->(friend)-[membership:HAS_MEMBER]->(forum)",
      knowsRelationships * hasMemberRelationships / personNodes // since we can infer that friend:Person
    )
  }

  test("Infer most selective label of intermediate node") {
    val allNodes: Double = 300
    val personNodes: Double = 20
    val forumNodes: Double = 15
    val knowsRelationships: Double = 30
    val hasMemberRelationships: Double = 25

    val config = plannerBuilder()
      .withSetting(GraphDatabaseInternalSettings.label_inference, TRUE)
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(allNodes)
      .setLabelCardinality("Person", personNodes)
      .setLabelCardinality("Forum", forumNodes)
      .setRelationshipCardinality("()-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Person)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("(:Forum)-[:KNOWS]->()", knowsRelationships)
      .setRelationshipCardinality("()-[:KNOWS]->(:Person)", knowsRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Forum)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("(:Person)-[:HAS_MEMBER]->()", hasMemberRelationships)
      .setRelationshipCardinality("()-[:HAS_MEMBER]->(:Person)", hasMemberRelationships)
      .build()

    queryShouldHaveCardinality(
      config,
      "MATCH (person)<-[friendship:KNOWS]-(friend)-[membership:HAS_MEMBER]->(forum)",
      knowsRelationships * hasMemberRelationships / forumNodes // since we can infer that friend:Forum
    )
  }
}

class RangeCardinalityIntegrationTest extends CardinalityIntegrationTest {
  override def getIndexType = IndexType.RANGE
}
