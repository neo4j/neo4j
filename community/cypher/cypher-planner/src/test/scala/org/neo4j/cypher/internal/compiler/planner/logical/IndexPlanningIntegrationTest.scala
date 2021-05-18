/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class IndexPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with LogicalPlanningIntegrationTestSupport {

  override val pushdownPropertyReads: Boolean = false

  test("should not plan index usage if predicate depends on variable from same QueryGraph") {

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan =
        new given {
          indexOn("Label", "prop")
        } getLogicalPlanFor s"MATCH (a)-->(b:Label) WHERE b.prop $op a.prop RETURN a"

      plan._2 should beLike {
        case Selection(_,
              Expand(
                _:NodeIndexScan | _:NodeByLabelScan,
                _, _, _, _, _, _
              )
             ) => ()
      }
    }
  }

  test("should plan index usage if predicate depends on simple variable from horizon") {

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan =
        new given {
          indexOn("Label", "prop")
        } getLogicalPlanFor s"WITH 'foo' AS foo MATCH (a)-->(b:Label) WHERE b.prop $op foo RETURN a"

      plan._2 should beLike {
        case Expand(
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek | _:NodeIndexContainsScan | _:NodeIndexEndsWithScan, _
              ), _, _, _, _, _, _) => ()
      }
    }
  }

  test("should plan index usage if predicate depends on property of variable from horizon") {

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan =
        new given {
          indexOn("Label", "prop")
        } getLogicalPlanFor s"WITH {prop: 'foo'} AS foo MATCH (a)-->(b:Label) WHERE b.prop $op foo.prop RETURN a"

      plan._2 should beLike {
        case Expand(
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek | _:NodeIndexContainsScan | _:NodeIndexEndsWithScan, _
              ), _, _, _, _, _, _) => ()
      }
    }
  }

  test("should not plan index usage if distance predicate depends on variable from same QueryGraph") {

    val plan =
      new given {
        indexOn("Place", "location")
      } getLogicalPlanFor
        s"""MATCH (p:Place)-->(x:Preference)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
           |RETURN p.location as point
        """.stripMargin

    plan._2 should beLike {
      case Projection(
            Selection(_,
              Expand(
                _:NodeByLabelScan, _, _, _, _, _, _
              )
            ), _) => ()
    }
  }

  test("should plan index usage if distance predicate depends on variable from the horizon") {

    val plan =
      new given {
        indexOn("Place", "location")
      } getLogicalPlanFor
        s"""WITH 10 AS maxDistance
           |MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance
           |RETURN p.location as point
        """.stripMargin

    plan._2 should beLike {
      case Projection(
            Selection(_,
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek, _
              )
            ), _) => ()
    }
  }

  test("should plan index usage if distance predicate depends on property read of variable from the horizon") {

    val plan =
      new given {
        indexOn("Place", "location")
      } getLogicalPlanFor
        s"""WITH {maxDistance: 10} AS x
           |MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
           |RETURN p.location as point
        """.stripMargin

    plan._2 should beLike {
      case Projection(
            Selection(_,
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek, _
              )
            ), _) => ()
    }
  }

  test("should allow one join and one index hint on the same variable") {
    val (_, plan, _, _) =
      new given {
        indexOn("S", "p")
        indexOn("T", "p") // This index is enforced by hint
        indexOn("T", "foo") // This index would normally be preferred
      } getLogicalPlanFor(
        s"""MATCH (s:S {p: 10})<-[r]-(t:T {foo: 2})
           |USING JOIN ON t
           |USING INDEX t:T(p)
           |WHERE 0 <= t.p <= 10
           |RETURN s, r, t
        """.stripMargin, stripProduceResults = false)

    plan should equal(
      new LogicalPlanBuilder()
        .produceResults("s", "r", "t")
        .nodeHashJoin("t")
        .|.expandAll("(s)<-[r]-(t)")
        .|.nodeIndexOperator("s:S(p = 10)")
        .filter("t.foo = 2")
        .nodeIndexOperator("t:T(0 <= p <= 10)")
        .build()
    )
  }

  test("should allow one join and one scan hint on the same variable") {
    val (_, plan, _, _) =
      new given {
        indexOn("S", "p")
        indexOn("T", "p")
        indexOn("T", "foo") // This index would normally be preferred
      } getLogicalPlanFor(
        s"""MATCH (s:S {p: 10})<-[r]-(t:T {foo: 2})
           |USING JOIN ON t
           |USING SCAN t:T
           |RETURN s, r, t
        """.stripMargin, stripProduceResults = false)

    plan should equal(
      new LogicalPlanBuilder()
        .produceResults("s", "r", "t")
        .nodeHashJoin("t")
        .|.expandAll("(s)<-[r]-(t)")
        .|.nodeIndexOperator("s:S(p = 10)")
        .filter("t.foo = 2")
        .nodeByLabelScan("t", "T")
        .build()
    )
  }

  test("should or-leaf-plan in reasonable time") {
    import scala.concurrent.ExecutionContext.Implicits.global

    val futurePlan =
      Future(
        new given {
          uniqueIndexOn("Coleslaw", "name")
        } getLogicalPlanFor
          """
            |MATCH (n:Coleslaw) USING INDEX n:Coleslaw(name)
            |WHERE (n.age < 10 AND ( n.name IN $p0 OR
            |        n.name IN $p1 OR
            |        n.name IN $p2 OR
            |        n.name IN $p3 OR
            |        n.name IN $p4 OR
            |        n.name IN $p5 OR
            |        n.name IN $p6 OR
            |        n.name IN $p7 OR
            |        n.name IN $p8 OR
            |        n.name IN $p9 OR
            |        n.name IN $p10 OR
            |        n.name IN $p11 OR
            |        n.name IN $p12 OR
            |        n.name IN $p13 OR
            |        n.name IN $p14 OR
            |        n.name IN $p15 OR
            |        n.name IN $p16 OR
            |        n.name IN $p17 OR
            |        n.name IN $p18 OR
            |        n.name IN $p19 OR
            |        n.name IN $p20 OR
            |        n.name IN $p21 OR
            |        n.name IN $p22 OR
            |        n.name IN $p23 OR
            |        n.name IN $p24 OR
            |        n.name IN $p25) AND n.legal)
            |RETURN n.name as name
        """.stripMargin)(global)

    Await.result(futurePlan, 1.minutes)
  }

  test("should not plan index scan if predicate variable is an argument") {
    val plan =
      new given {
        indexOn("Label", "prop")
      } getLogicalPlanFor
        """
           |MATCH (a: Label {prop: $param})
           |MATCH (b)
           |WHERE (a:Label {prop: $param})-[]-(b)
           |RETURN a
           |""".stripMargin

    plan._2 should beLike {
      case SemiApply(
             CartesianProduct(_: NodeIndexSeek, _: AllNodesScan, _),
             Expand(
               Selection(_, _: Argument), _, _, _, _, _, _)) => ()
    }
  }

  test("should prefer label scan to node index scan from existence constraint with same cardinality") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) RETURN n")

    plan shouldEqual planner.planBuilder()
                            .produceResults("n")
                            .nodeByLabelScan("n", "Label")
                            .build()
  }

  test("should prefer label scan to node index scan from existence constraint with same cardinality, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN n")

    plan shouldEqual planner.planBuilder()
                            .produceResults("n")
                            .filter("n.x = 1")
                            .nodeByLabelScan("n", "Label")
                            .build()
  }

  test("should prefer type scan to relationship index scan from existence constraint with same cardinality, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .enablePlanningRelationshipIndexes()
      .enableRelationshipByTypeLookup()
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) RETURN r")

    plan shouldEqual planner.planBuilder()
                            .produceResults("r")
                            .relationshipTypeScan("(a)-[r:REL]->(b)")
                            .build()
  }

  test("should prefer node index scan from existence constraint to label scan with same cardinality, if indexed property is used") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) RETURN n.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("n.prop AS p")
                            .nodeIndexOperator("n:Label(prop)")
                            .build()
  }

  test("should prefer relationship index scan from existence constraint to type scan with same cardinality, if indexed property is used") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .enablePlanningRelationshipIndexes()
      .enableRelationshipByTypeLookup()
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) RETURN r.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("r.prop AS p")
                            .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)")
                            .build()
  }

  test("should prefer node index scan from existence constraint to label scan with same cardinality, if indexed property is used, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN n.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("n.prop AS p")
                            .filter("n.x = 1")
                            .nodeIndexOperator("n:Label(prop)")
                            .build()
  }

  test("should prefer relationship index scan from existence constraint to type scan with same cardinality, if indexed property is used, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .enablePlanningRelationshipIndexes()
      .enableRelationshipByTypeLookup()
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.x = 1 RETURN r.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("r.prop AS p")
                            .filter("r.x = 1")
                            .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)")
                            .build()
  }

  test("should prefer node index scan from aggregation to node index scan from existence constraint with same cardinality") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(n.counted) AS c"))
                            .nodeIndexOperator("n:Label(counted)")
                            .build()
  }

  test("should prefer relationship index scan from aggregation to relationship index scan from existence constraint with same cardinality") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipIndex("REL", Seq("counted"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .enablePlanningRelationshipIndexes()
      .enableRelationshipByTypeLookup()
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) RETURN count(r.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(r.counted) AS c"))
                            .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)")
                            .build()
  }

  test("should prefer node index scan from aggregation to node index scan from existence constraint with same cardinality, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(n.counted) AS c"))
                            .filter("n.x = 1")
                            .nodeIndexOperator("n:Label(counted)")
                            .build()
  }

  test("should prefer relationship index scan from aggregation to relationship index scan from existence constraint with same cardinality, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipIndex("REL", Seq("counted"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .enablePlanningRelationshipIndexes()
      .enableRelationshipByTypeLookup()
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.x = 1 RETURN count(r.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(r.counted) AS c"))
                            .filter("r.x = 1")
                            .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)")
                            .build()
  }

  test("should prefer node index scan for aggregated property, even if other property is referenced") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.prop <> 1 AND n.x = 1 RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(n.counted) AS c"))
                            .filter("not n.prop = 1", "n.x = 1")
                            .nodeIndexOperator("n:Label(counted)")
                            .build()
  }

  test("should prefer relationship index scan for aggregated property, even if other property is referenced") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipIndex("REL", Seq("counted"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .enablePlanningRelationshipIndexes()
      .enableRelationshipByTypeLookup()
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop <> 1 AND r.x = 1 RETURN count(r.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(r.counted) AS c"))
                            .filter("not r.prop = 1", "r.x = 1")
                            .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)")
                            .build()
  }

}
