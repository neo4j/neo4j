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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class ExpandPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("Should build plans containing expand for single relationship pattern") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 50)
      .build()

    val plan = cfg.plan("MATCH (a)-[r]->(b) RETURN r").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .allRelationshipsScan("(a)-[r]->(b)")
      .build()
  }

  test("should take shortcut for plans returning no results without getting lost in IDP loop") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .withSetting(GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold, Int.box(16))
      .build()

    val query =
      """
        |MATCH
        |  (a)-[r*]->(b)-[q*]->(c),
        |  (x)-[r*]->(y)-[q*]->(z),
        |  (d)-[r*]->(e)-[q*]->(f)
        |RETURN *
        |""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c", "d", "e", "f", "q", "r", "x", "y", "z")
        .projection(
          "NULL AS e",
          "NULL AS x",
          "NULL AS y",
          "NULL AS f",
          "NULL AS a",
          "NULL AS c",
          "NULL AS r",
          "NULL AS q",
          "NULL AS b",
          "NULL AS z",
          "NULL AS d"
        )
        .limit(0)
        .argument()
        .build()
    )
  }

  test("should take shortcut for plans returning no results because of duplicate relationships") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val query =
      """
        |MATCH
        |  (a)-[r]->(b)-[r]->(c)
        |RETURN *
        |""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c", "r")
        .projection("NULL AS a", "NULL AS b", "NULL AS c", "NULL AS r")
        .limit(0)
        .argument()
        .build()
    )
  }

  test("Should build plans containing expand for two unrelated relationship patterns") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 2000)
      .setLabelCardinality("C", 3000)
      .setLabelCardinality("D", 4000)
      .setRelationshipCardinality("(:A)-[]-(:B)", 100)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .setRelationshipCardinality("()-[]->(:B)", 100)
      .setRelationshipCardinality("(:C)-[]->(:D)", 100)
      .setRelationshipCardinality("(:C)-[]->()", 100)
      .setRelationshipCardinality("()-[]->(:D)", 100)
      .setRelationshipCardinality("()-[]->()", 100000)
      .build()

    val plan = cfg.plan("MATCH (a:A)-[r1]->(b:B), (c:C)-[r2]->(d:D) RETURN r1, r2").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("not r1 = r2")
      .cartesianProduct()
      .|.filter("d:D")
      .|.expandAll("(c)-[r2]->(d)")
      .|.nodeByLabelScan("c", "C")
      .filter("b:B")
      .expandAll("(a)-[r1]->(b)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should build plans containing expand for self-referencing relationship patterns") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 5000)
      .build()

    val plan = cfg.plan("MATCH (a)-[r]->(a) RETURN r").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .expandInto("(a)-[r]->(a)")
      .allNodeScan("a")
      .build()
  }

  test("Should build plans containing expand for looping relationship patterns") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("()-[]->()", 2000)
      .setRelationshipCardinality("(:A)-[]->()", 20)
      .build()

    val plan = cfg.plan("MATCH (a:A)-[r1]->(b)<-[r2]-(a) RETURN r1, r2").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("not r2 = r1")
      .expandInto("(a)-[r1]->(b)")
      .expandAll("(a)-[r2]->(b)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:X]-()", 500)
      .build()

    val plan = cfg.plan("MATCH (start)-[rel:X]-(a) WHERE a.name = 'Andres' RETURN a").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .expandAll("(a)-[rel:X]-(start)")
      .filter("a.name = 'Andres'")
      .allNodeScan("a")
      .build()
  }

  test("Should build plans expanding from the more expensive side if that is requested by using a hint") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(2000)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("Person", 1000)
      .setRelationshipCardinality("(:A)-[]->(:Person)", 10)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("()-[]->(:Person)", 500)
      .setRelationshipCardinality("()-[]->()", 5000)
      .addNodeIndex("Person", Seq("name"), existsSelectivity = 1.0, uniqueSelectivity = 0.1)
      .build()

    val plan = cfg.plan(
      "MATCH (a:A)-[r]->(b) USING INDEX b:Person(name) WHERE b:Person AND b.name = 'Andres' return r"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("a:A")
      .expandAll("(b)<-[r]-(a)")
      .nodeIndexOperator("b:Person(name = 'Andres')", indexType = IndexType.RANGE)
      .build()
  }

  test("should plan typed expand with not-inlined type predicate") {
    val query =
      """MATCH (a)-[r]->(b)
        |WHERE r:REL
        |RETURN a, b, r""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 10)
      .removeRelationshipLookupIndex() // We want to test Expand
      .build()

    val plan = cfg
      .plan(query)
      .stripProduceResults

    val expectedPlan = cfg.subPlanBuilder()
      .expand("(a)-[r:REL]->(b)")
      .allNodeScan("a")
      .build()

    plan shouldEqual expectedPlan
  }

  test("should consider dependency to target node when extracting predicates for var length expand") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .build()

    val plan = planner.plan("MATCH (a:A)-[r* {aProp: a.prop, bProp: b.prop}]->(b) RETURN a, b")

    plan shouldBe planner.planBuilder()
      .produceResults("a", "b")
      // this filter should go on top as we do not know the node b before finishing the expand.
      .filter("all(anon_1 IN r WHERE anon_1.bProp = b.prop)")
      .expand("(a)-[r*1..]->(b)", relationshipPredicates = Seq(Predicate("anon_0", "anon_0.aProp = a.prop")))
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should consider dependency to target node when extracting inner predicates for var length expand") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("(:A)-[]->()", 100)
      .build()

    val plan = planner.plan("MATCH p = ((a:A)-[r*1..3]->(b)) WHERE all(x IN nodes(p) WHERE x = a OR x = b) RETURN p")

    val path = PathExpression(NodePathStep(
      v"a",
      MultiRelationshipPathStep(v"r", OUTGOING, Some(v"b"), NilPathStep()(pos))(pos)
    )(pos))(pos)

    plan shouldEqual planner.planBuilder()
      .produceResults("p")
      .projection(Map("p" -> path))
      .filterExpression(
        allInList(
          v"x",
          nodes(path),
          ors(
            equals(v"x", v"a"),
            equals(v"x", v"b")
          )
        )
      )
      .expand("(a)-[r*1..3]->(b)")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("should be able to solve two variable length relationships in one pattern") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("()-[]->(:A)", 100)
      .build()

    val plan = planner.plan("MATCH (a:A)<-[r*]-(b)-[r2*]-(c) RETURN a,r")

    plan shouldEqual planner.planBuilder()
      .produceResults("a", "r")
      .filter("NONE(anon_0 IN r2 WHERE anon_0 IN r)")
      .expand("(b)-[r2*]-(c)")
      .expand("(a)<-[r*]-(b)", projectedDir = INCOMING)
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should be able to plan var-length expand with always-false predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val plan = planner.plan("MATCH (a)-[r*]->(b) WHERE false RETURN r").stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .projection("NULL AS a", "NULL AS b", "NULL AS r")
        .limit(0)
        .argument()
        .build()
    )
  }

  test(
    "should plan limit 0 + projection for query with multiple references to same relationship followed by projections"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query = """MATCH (a)-[r]->()<-[r]-(b) WITH a, r, b, 1 AS dummy RETURN *, 2 as dummy2;"""

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "dummy", "r", "dummy2")
        .projection("2 AS dummy2")
        .projection("1 AS dummy")
        .projection("NULL AS a", "NULL AS anon_0", "NULL AS b", "NULL AS r")
        .limit(0)
        .argument()
        .build()
    )
  }

  test("should be able to plan query with multiple references to same relationship followed by aggregation") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query = """MATCH (a)-[r]->()<-[r]-(b) WITH a, r, b, 1 AS dummy RETURN count(*) AS count;"""

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("count")
        .aggregation(Seq(), Seq("count(*) AS count"))
        .projection("1 AS dummy")
        .projection("NULL AS a", "NULL AS anon_0", "NULL AS b", "NULL AS r")
        .limit(0)
        .argument()
        .build()
    )
  }

  test(
    "should plan limit 0 + projection for query with multiple references to same relationship on one side of union"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """
        |  MATCH (a)-[r]->()<-[r]-(b) RETURN a, r, b, 1 AS dummy
        |UNION
        |  MATCH (a)-[r]->(b) RETURN a, r, b, 1 AS dummy;""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "r", "b", "dummy")
        .distinct("b AS b", "dummy AS dummy", "a AS a", "r AS r")
        // we could improve and recognise that the LHS will never return a row and therefore simplify to the RHS
        .union()
        .|.projection("a AS a", "r AS r", "b AS b", "dummy AS dummy")
        .|.projection("1 AS dummy")
        .|.allRelationshipsScan("(a)-[r]->(b)")
        .projection("a AS a", "r AS r", "b AS b", "dummy AS dummy")
        .projection("1 AS dummy")
        .projection("NULL AS a", "NULL AS anon_0", "NULL AS b", "NULL AS r")
        .limit(0)
        .argument()
        .build()
    )
  }

  test("should plan limit 0 + projection for query with multiple references to same relationship in subquery") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (c)-->(d)
        |CALL {
        |  MATCH (a)-[r]->()<-[r]-(b)
        |  WITH a, r, b, 1 AS dummy
        |  RETURN *, 2 as dummy2
        |}
        |RETURN *""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "c", "d", "dummy", "dummy2", "r")
        // we could improve on this to recognise that the RHS will be empty and therefore the cartesian product will be empty as well
        .cartesianProduct()
        .|.projection("2 AS dummy2")
        .|.projection("1 AS dummy")
        .|.projection("NULL AS a", "NULL AS anon_1", "NULL AS b", "NULL AS r")
        .|.limit(0)
        .|.argument()
        .allRelationshipsScan("(c)-[anon_0]->(d)")
        .build()
    )
  }

  test("should plan limit 0 + projection for query with multiple references to same relationship in complex EXISTS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-->(b)
        |WHERE EXISTS {
        |  MATCH (a)-[r]->()<-[r]-(b)
        |  RETURN a, b, r
        |}
        |RETURN *""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        // we could improve and simplify the RHS to WHERE false and then convert this to limit 0 again
        .semiApply()
        .|.projection("NULL AS anon_1", "NULL AS r")
        .|.limit(0)
        .|.argument("a", "b")
        .allRelationshipsScan("(a)-[anon_0]->(b)")
        .build()
    )
  }

  test("should plan limit 0 + projection for query with multiple references to same relationship in EXISTS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-->(b)
        |WHERE EXISTS {
        |  MATCH (a)-[r]->()<-[r]-(b)
        |  WITH a, r, b, 1 AS dummy
        |  RETURN a, b, r, dummy, 2 as dummy2
        |}
        |RETURN *""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        // we could improve and simplify the RHS to WHERE false and then convert this to limit 0 again
        .semiApply()
        .|.projection("2 AS dummy2")
        .|.projection("1 AS dummy")
        .|.projection("NULL AS anon_1", "NULL AS r")
        .|.limit(0)
        .|.argument("a", "b")
        .allRelationshipsScan("(a)-[anon_0]->(b)")
        .build()
    )
  }

  test("should plan limit 0 + projection for query with multiple references to same relationship in simple EXISTS") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-->(b)
        |WHERE EXISTS {
        |  MATCH (a)-[r]->()<-[r]-(b)
        |}
        |RETURN *""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        // we could improve and simplify the RHS to WHERE false and then convert this to limit 0 again
        .semiApply()
        .|.projection("NULL AS anon_1", "NULL AS r")
        .|.limit(0)
        .|.argument("a", "b")
        .allRelationshipsScan("(a)-[anon_0]->(b)")
        .build()
    )
  }

  test("should be able to plan query with multiple references to same relationship in complex COUNT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-->(b)
        |RETURN COUNT {
        |  MATCH (a)-[r]->()<-[r]-(b)
        |  RETURN a, b, r
        |} AS count""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("count")
        // we could improve and simplify the RHS to `WITH 0 AS count`
        .apply()
        .|.aggregation(Seq(), Seq("count(*) AS count"))
        .|.projection("NULL AS anon_1", "NULL AS r")
        .|.limit(0)
        .|.argument("a", "b")
        .allRelationshipsScan("(a)-[anon_0]->(b)")
        .build()
    )
  }

  test("should be able to plan query with multiple references to same relationship in COUNT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-->(b)
        |RETURN COUNT {
        |  MATCH (a)-[r]->()<-[r]-(b)
        |  WITH a, r, b, 1 AS dummy
        |  RETURN a, b, r, dummy, 2 as dummy2
        |} AS count""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("count")
        // we could improve and simplify the RHS to `WITH 0 AS count`
        .apply()
        .|.aggregation(Seq(), Seq("count(*) AS count"))
        .|.projection("1 AS dummy")
        .|.projection("NULL AS anon_1", "NULL AS r")
        .|.limit(0)
        .|.argument("a", "b")
        .allRelationshipsScan("(a)-[anon_0]->(b)")
        .build()
    )
  }

  test("should be able to plan query with multiple references to same relationship in simple COUNT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-->(b)
        |RETURN COUNT {
        |  MATCH (a)-[r]->()<-[r]-(b)
        |} AS count""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("count")
        // we could improve and simplify the RHS to `WITH 0 AS count`
        .apply()
        .|.aggregation(Seq(), Seq("count(*) AS count"))
        .|.projection("NULL AS anon_1", "NULL AS r")
        .|.limit(0)
        .|.argument("a", "b")
        .allRelationshipsScan("(a)-[anon_0]->(b)")
        .build()
    )
  }

  test("should plan limit 0 + projection for query with multiple references to same relationship in 2nd query part") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-->(b)
        |WITH *, 1 as dummy
        |MATCH (a)-[r]->()<-[r]-(b)
        |RETURN *""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "dummy", "r")
        .projection("NULL AS anon_1", "NULL AS r")
        .limit(0)
        .projection("1 AS dummy")
        .allRelationshipsScan("(a)-[anon_0]->(b)")
        .build()
    )
  }

  test("should plan limit 0 + projection + sort if sorted") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query =
      """MATCH (a)-[r]->()<-[r]-(b)
        |RETURN * ORDER BY a""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "r")
        .sort("a ASC")
        .projection("NULL AS a", "NULL AS anon_0", "NULL AS b", "NULL AS r")
        .limit(0)
        .argument()
        .build()
    )
  }

  test(
    "should plan limit 0 + projection for query with multiple references to same relationship with path assignment"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[]->()", 100)
      .build()

    val query = """MATCH p = (a)-[r]->()<-[r]-(b) RETURN *;"""

    val path = PathExpression(
      NodePathStep(
        v"a",
        SingleRelationshipPathStep(
          v"r",
          OUTGOING,
          Some(v"anon_0"),
          SingleRelationshipPathStep(v"r", INCOMING, Some(v"b"), NilPathStep()(pos))(pos)
        )(pos)
      )(pos)
    )(pos)

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b", "p", "r")
        .projection(Map("p" -> path))
        .projection("NULL AS a", "NULL AS anon_0", "NULL AS b", "NULL AS r")
        .limit(0)
        .argument()
        .build()
    )
  }

  test("should plan ExpandInto for MERGE with ON CREATE with update one property") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:TYPE]->()", 10000)
      .build()
    val query =
      """MATCH (a {name:'A'}), (b {name:'B'})
        |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = 'foo'""".stripMargin

    planner.plan(query) should containPlanMatching {
      case Expand(_, _, _, _, _, _, ExpandInto) =>
    }
  }

  test("should plan ExpandInto for MERGE with ON CREATE with deleting one property") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:TYPE]->()", 10000)
      .build()
    val query =
      """MATCH (a {name:'A'}), (b {name:'B'})
        |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = null""".stripMargin

    planner.plan(query) should containPlanMatching {
      case Expand(_, _, _, _, _, _, ExpandInto) =>
    }
  }

  test("should plan ExpandInto for MERGE with ON CREATE with update all properties from node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:TYPE]->()", 10000)
      .build()
    val query = "MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r = a"

    planner.plan(query) should containPlanMatching {
      case Expand(_, _, _, _, _, _, ExpandInto) =>
    }
  }

  test("should plan ExpandInto for MERGE with ON MATCH with update all properties from node") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:TYPE]->()", 10000)
      .build()
    val query = "MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON MATCH SET r = a"

    planner.plan(query) should containPlanMatching {
      case Expand(_, _, _, _, _, _, ExpandInto) =>
    }
  }

  test("should plan ExpandInto for MERGE with ON CREATE with update properties from literal map") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:TYPE]->()", 10000)
      .build()
    val query =
      """MATCH (a {name:'A'}), (b {name:'B'})
        |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r += {foo: 'bar', bar: 'baz'}""".stripMargin

    planner.plan(query) should containPlanMatching {
      case Expand(_, _, _, _, _, _, ExpandInto) =>
    }
  }

  test("should plan ExpandInto for MERGE with ON MATCH with update properties from literal map") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:TYPE]->()", 10000)
      .build()
    val query =
      """MATCH (a {name:'A'}), (b {name:'B'})
        |MERGE (a)-[r:TYPE]->(b) ON MATCH SET r += {foo: 'baz', bar: 'baz'}""".stripMargin

    planner.plan(query) should containPlanMatching {
      case Expand(_, _, _, _, _, _, ExpandInto) =>
    }
  }

  test("should prefer relationship type scan over AllNodeScan + Expand, also wit interesting order") {
    val query =
      """
        |MATCH (x)-[:HAS_ATTRIBUTE]->(y)
        |WITH x, y.prop / 2 as division
        |RETURN *
        |ORDER BY division DESC
        |""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(415)
      .setAllRelationshipsCardinality(1434)
      .setRelationshipCardinality("()-[:HAS_ATTRIBUTE]->()", 1099)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("division", "x")
        .sort("division DESC")
        .projection("y.prop / 2 AS division")
        .relationshipTypeScan("(x)-[anon_0:HAS_ATTRIBUTE]->(y)")
        .build()
    )
  }

  test("should prefer all relationship scan over AllNodeScan + Expand, also wit interesting order") {
    val query =
      """
        |MATCH (x)-->(y)
        |WITH x, y.prop / 2 as division
        |RETURN *
        |ORDER BY division DESC
        |""".stripMargin

    val planner = plannerBuilder()
      .setAllNodesCardinality(800)
      .setAllRelationshipsCardinality(1434)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("division", "x")
        .sort("division DESC")
        .projection("y.prop / 2 AS division")
        .allRelationshipsScan("(x)-[anon_0]->(y)")
        .build()
    )
  }

  test(
    "should NOT plan Expand(Into) and CartesianProduct of two composite index seeks with high uniqueSelectivity (i.e. many duplicate entries)"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 800)
      .setLabelCardinality("B", 600)
      .addNodeIndex("A", Seq("x", "y", "z"), existsSelectivity = 0.5, uniqueSelectivity = 0.8)
      .addNodeIndex("A", Seq("x", "y"), existsSelectivity = 0.8, uniqueSelectivity = 0.9)
      .addNodeIndex("B", Seq("x", "y", "z"), existsSelectivity = 0.4, uniqueSelectivity = 0.7)
      .addNodeIndex("B", Seq("x", "y"), existsSelectivity = 0.9, uniqueSelectivity = 0.8)
      .setRelationshipCardinality("()-[]->()", 2000)
      .setRelationshipCardinality("(:A)-[]->(:B)", 2000)
      .setRelationshipCardinality("()-[]->(:B)", 2000)
      .setRelationshipCardinality("(:A)-[]->()", 2000)
      .defaultRelationshipCardinalityTo0(false)
      .build()

    val q =
      """
        |MATCH (a:A {x:1, y: 2, z: 3})-[r]->(b:B {x: 4, y: 5, z: 6})
        |RETURN a, b
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filterExpressionOrString("a:A", andsReorderable("a.x = 1", "a.y = 2", "a.z = 3"))
      .expandAll("(b)<-[r]-(a)")
      .nodeIndexOperator("b:B(x = 4, y = 5, z = 6)", supportPartitionedScan = false)
      .build()
  }
}
