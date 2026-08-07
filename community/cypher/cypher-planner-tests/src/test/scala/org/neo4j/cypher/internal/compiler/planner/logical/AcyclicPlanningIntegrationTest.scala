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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.AcyclicParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Acyclic
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.From
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.To
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.UpperBound.Unlimited

class AcyclicPlanningIntegrationTest extends CypherPlannerTestSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport with CypherVersionTestSupport {

  private val pb = plannerBuilder()
    .setAllNodesCardinality(1000)
    .setAllRelationshipsCardinality(300)
    .setLabelCardinality("S", 3)
    .setLabelCardinality("T", 100)
    .setRelationshipCardinality("()-[:R]->()", 250)
    .setRelationshipCardinality("(:S)-[:R]->()", 3)
    .setRelationshipCardinality("()-[:R]->(:S)", 3)
    .setRelationshipCardinality("(:T)-[:R]->()", 250)
    .setRelationshipCardinality("()-[:R]->(:T)", 250)
    .setRelationshipCardinality("(:T)-[:R]->(:T)", 250)

  test("Unsatisfiable query") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC (n1:S)-[r1]->(n1)
        |RETURN n1
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1")
      .projection("NULL AS n1", "NULL AS r1")
      .limit(0)
      .argument()
      .build()
  }

  test("Fixed-length relationships") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC (n1:S)-[r1]->(n2)<-[r2]-(n3)
        |RETURN n1, n3
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n3")
      .filter("NOT n1 = n3", "NOT n2 = n3")
      .expandAll("(n2)<-[]-(n3)")
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[]->(n2)")
      .nodeByLabelScan("n1", "S")
      .build()
  }
  private val intMaxValuePlus1: Long = Int.MaxValue.toLong + 1L

  test(
    "Single QPP - upperbound (Int.MaxValue+1) is too large to rewrite Repeat to VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->{0,$intMaxValuePlus1}(n2)
         |RETURN n1, n2
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n2")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(intMaxValuePlus1),
        start = "n1",
        end = "n2",
        innerStart = "anon_0",
        innerEnd = "anon_1",
        groupNodes = Set(),
        innerNodes = Set("anon_0", "anon_1"),
        previouslyBoundNodes = Set("n1"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r1"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("anon_1"), "NOT anon_0 = anon_1")
      .|.expandAll("(anon_0)-[r1]->(anon_1)")
      .|.argument("anon_0")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test("Single QPP - plan as VarExpand") {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1:S)-[r1]->{0,4}(n2)
        |RETURN n1, n2
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n2")
      .expand(
        "(n1)-[*0..4]->(n2)",
        relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)")),
        pathMode = Acyclic
      )
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test("Multiple ACYCLIC path pattern - Single QPPs - plan as VarExpands") {
    val planner = pb.build()
    val query =
      """
        |MATCH
        |   ACYCLIC (n1:S)-[r1]->{0,4}(n2),
        |   ACYCLIC (n2)-[r2]->{3,10}(n3)
        |RETURN n1, n3
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n3")
      .filter("none(anon_2 IN r2 WHERE anon_2 IN r1)") // Relationship uniqueness hold for the whole graph pattern
      .expand(
        "(n2)-[r2*3..10]->(n3)",
        relationshipPredicates = Seq(Predicate("anon_1", "NOT startNode(anon_1) = endNode(anon_1)")),
        pathMode = Acyclic
      )
      .expand(
        "(n1)-[r1*0..4]->(n2)",
        relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)")),
        pathMode = Acyclic
      )
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test("Multiple ACYCLIC path pattern - Single relationship and single QPP - plan as VarExpand") {
    val planner = pb.build()
    val query =
      """
        |MATCH
        |   ACYCLIC (n1:S)-[r1]->(n2),
        |   ACYCLIC (n2)-[r2]->{3,10}(n3)
        |RETURN n1, n3
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n3")
      .filter("NOT r1 IN r2") // Relationship uniqueness hold for the whole graph pattern
      .expand(
        "(n2)-[r2*3..10]->(n3)",
        relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)")),
        pathMode = Acyclic
      )
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[r1]->(n2)")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test("Multiple ACYCLIC path pattern - Single QPPs - plan as VarExpand, Repeat") {
    val planner = pb.build()
    val query =
      s"""
         |MATCH
         |   ACYCLIC (n1:S)-[r1]->{0,4}(n2),
         |   ACYCLIC (n2)-[r2]->{3,${intMaxValuePlus1}}(n3)
         |RETURN n1, n3
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n3")
      .repeatAcyclic(AcyclicParameters(
        min = 3,
        max = Limited(intMaxValuePlus1),
        start = "n2",
        end = "n3",
        innerStart = "anon_0",
        innerEnd = "anon_1",
        groupNodes = Set(),
        innerNodes = Set("anon_0", "anon_1"),
        previouslyBoundNodes = Set("n2"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set("r1"),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("anon_1"), "NOT anon_0 = anon_1", isRepeatTrailUnique("r2"))
      .|.expandAll("(anon_0)-[r2]->(anon_1)")
      .|.argument("anon_0")
      .expandExpr(
        "(n1)-[r1*0..4]->(n2)",
        relationshipPredicates = Seq(
          Predicate("anon_2", "NOT startNode(anon_2) = endNode(anon_2)").asVariablePredicate
        ),
        pathMode = Acyclic
      )
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test("Multiple ACYCLIC path pattern - Single QPPs - plan as Repeat, VarExpand") {
    val planner = pb.build()
    val query =
      s"""
         |MATCH
         |   ACYCLIC (n1:S)-[r1]->{0,${intMaxValuePlus1}}(n2),
         |   ACYCLIC (n2)-[r2]->{3,10}(n3)
         |RETURN n1, n3
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n3")
      .filter("none(anon_3 IN r2 WHERE anon_3 IN r1)")
      .expandExpr(
        "(n2)-[r2*3..10]->(n3)",
        relationshipPredicates = Seq(
          Predicate("anon_2", "NOT startNode(anon_2) = endNode(anon_2)").asVariablePredicate
        ),
        pathMode = Acyclic
      )
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(intMaxValuePlus1),
        start = "n1",
        end = "n2",
        innerStart = "anon_0",
        innerEnd = "anon_1",
        groupNodes = Set(),
        innerNodes = Set("anon_0", "anon_1"),
        previouslyBoundNodes = Set("n1"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(("r1", "r1")),
        innerRelationships = Set("r1"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("anon_1"), "NOT anon_0 = anon_1", isRepeatTrailUnique("r1"))
      .|.expandAll("(anon_0)-[r1]->(anon_1)")
      .|.argument("anon_0")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test("Multiple ACYCLIC path pattern - Single QPPs - plan as Repeat, Repeat") {
    val planner = pb.build()
    val query =
      s"""
         |MATCH
         |   ACYCLIC (n1:S)-[r1]->{0,${intMaxValuePlus1}}(n2),
         |   ACYCLIC (n2)-[r2]->{3,${intMaxValuePlus1}}(n3)
         |RETURN n1, n3
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n3")
      .repeatAcyclic(AcyclicParameters(
        min = 3,
        max = Limited(intMaxValuePlus1),
        start = "n2",
        end = "n3",
        innerStart = "anon_2",
        innerEnd = "anon_3",
        groupNodes = Set(),
        innerNodes = Set("anon_2", "anon_3"),
        previouslyBoundNodes = Set("n2"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set("r1"),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("anon_3"), "NOT anon_2 = anon_3", isRepeatTrailUnique("r2"))
      .|.expandAll("(anon_2)-[r2]->(anon_3)")
      .|.argument("anon_2")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(intMaxValuePlus1),
        start = "n1",
        end = "n2",
        innerStart = "anon_0",
        innerEnd = "anon_1",
        groupNodes = Set(),
        innerNodes = Set("anon_0", "anon_1"),
        previouslyBoundNodes = Set("n1"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(("r1", "r1")),
        innerRelationships = Set("r1"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("anon_1"), "NOT anon_0 = anon_1", isRepeatTrailUnique("r1"))
      .|.expandAll("(anon_0)-[r1]->(anon_1)")
      .|.argument("anon_0")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test(
    "Fixed-length relationship and QPP - process left-to-right - Repeat"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)((n3)-[r2]->(n4)){0,${intMaxValuePlus1}}(n5)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5") // Should already be solved by a combination of 'NOT n1 = n2' (in case of 0-iterations of the QPP) and 'IsRepeatAcyclic(n4) with n1 in alreadyBoundNodes' (in case of at least one iteration of the QPP)
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(intMaxValuePlus1),
        start = "n2",
        end = "n5",
        innerStart = "n3",
        innerEnd = "n4",
        groupNodes = Set(),
        innerNodes = Set("n3", "n4"),
        previouslyBoundNodes =
          Set("n1", "n2"), // n1 as previously bound node prevents the rewrite to VarExpand. When we start rewriting these cases, then we need to include node uniqueness checks between n1 and the nodes in the QPP '(n2)((n3)-[r2]->(n4)){0,3}(n5)'.
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n4"), "NOT n3 = n4")
      .|.expandAll("(n3)-[r2]->(n4)")
      .|.argument("n3")
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[]->(n2)")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test(
    "Fixed-length relationship and outgoing QPP - process left-to-right - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)((n3)-[r2]->(n4)){0,3}(n5)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5")
      .expand(
        "(n2)-[*0..3]->(n5)",
        relationshipPredicates = Seq(
          Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)"),
          Predicate(
            "anon_0",
            "NOT n1 = endNode(anon_0)"
          )
        ),
        pathMode = Acyclic
      )
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[]->(n2)")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test(
    "Fixed-length relationship and incoming QPP - process left-to-right - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)((n3)<-[r2]-(n4)){0,3}(n5)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5")
      .expand(
        "(n2)<-[*0..3]-(n5)",
        projectedDir = INCOMING,
        relationshipPredicates = Seq(
          Predicate("anon_0", "NOT endNode(anon_0) = startNode(anon_0)"),
          Predicate("anon_0", "NOT n1 = startNode(anon_0)")
        ),
        pathMode = Acyclic
      )
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[]->(n2)")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test(
    "Fixed-length relationship and undirected QPP - process left-to-right - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)((n3)-[r2]-(n4)){0,3}(n5)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5")
      .expandExpr(
        "(n2)-[*0..3]-(n5)",
        relationshipPredicates = Seq(
          VariablePredicate(
            v"anon_0",
            not(equals(
              TraversalEndpoint(tempVar = v"anon_1", endpoint = From),
              TraversalEndpoint(tempVar = v"anon_2", endpoint = To)
            ))
          ),
          Predicate(
            "anon_0",
            "NOT n1 IN [startNode(anon_0), endNode(anon_0)]"
          ).asVariablePredicate
        ),
        pathMode = Acyclic
      )
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[]->(n2)")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test(
    "Fixed-length relationship and undirected QPP - process left-to-right - Should plan Repeat when the RHS of Repeat contains a filter before the expandAll and the repeat cannot be rewritten into a pruning VarExpand"
  ) {
    // The following shape is only translated to VarExpand when it can become a pruning varExpand
    // .repeat
    //  .|.filter
    //  .|.expandAll
    //  .|.filter     <-- This is the filter on the RHS of Repeat that is before the expandAll
    //  .|.argument
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)((n3 {p:1})-[r2]-(n4)){0,3}(n5)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "n2",
        end = "n5",
        innerStart = "n3",
        innerEnd = "n4",
        groupNodes = Set(),
        innerNodes = Set("n3", "n4"),
        previouslyBoundNodes = Set("n1", "n2"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n4"), "NOT n3 = n4")
      .|.expandAll("(n3)-[r2]-(n4)")
      .|.filter("n3.p = 1")
      .|.argument("n3")
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[]->(n2)")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test(
    "Fixed-length relationship and undirected QPP - process left-to-right - Should plan VarExpand when the RHS contains a filter before the repeat and the repeat can be rewritten into a pruning VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)((n3 {p:1})-[r2]-(n4)){0,3}(n5)
         |RETURN DISTINCT n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .orderedDistinct(Seq("n1"), "n1 AS n1", "n5 AS n5")
      .filter("NOT n1 = n5")
      .bfsPruningVarExpandExpr(
        "(n2)-[*0..3]-(n5)",
        relationshipPredicates = Seq(
          VariablePredicate(
            v"anon_0",
            not(equals(
              TraversalEndpoint(varFor("anon_1"), Endpoint.From),
              TraversalEndpoint(varFor("anon_2"), Endpoint.To)
            ))
          ),
          VariablePredicate(
            v"anon_0",
            equals(propExpression(TraversalEndpoint(varFor("anon_3"), Endpoint.From), "p"), literalInt(1))
          ),
          Predicate("anon_0", "NOT n1 IN [startNode(anon_0), endNode(anon_0)]").asVariablePredicate
        ),
        mode = ExpandAll,
        pathMode = Acyclic
      )
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[]->(n2)")
      .nodeByLabelScan("n1", "S", IndexOrderAscending)
      .build()
  }

  test(
    "Fixed-length relationship and outgoing QPP - process right-to-left - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1)-[r1]->(n2)((n3)-[r2]->(n4)){0,3}(n5:S)
        |RETURN n1, n5
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter(
        "none(anon_1 IN r2 WHERE n1 = startNode(anon_1))",
        "NOT n1 = n2",
        "NOT n1 = n5"
      )
      .expandAll("(n2)<-[]-(n1)")
      .expand(
        "(n5)<-[r2*0..3]-(n2)",
        expandMode = ExpandAll,
        relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)")),
        pathMode = Acyclic
      )
      .nodeByLabelScan("n5", "S")
      .build()
  }

  test(
    "Fixed-length relationship and incoming QPP - process right-to-left - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1)-[r1]->(n2)((n3)<-[r2]-(n4)){0,3}(n5:S)
        |RETURN n1, n5
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter(
        "none(anon_1 IN r2 WHERE n1 = endNode(anon_1))",
        "NOT n1 = n5",
        "NOT n1 = n2"
      )
      .expandAll("(n2)<-[]-(n1)")
      .expand(
        "(n5)-[r2*0..3]->(n2)",
        expandMode = ExpandAll,
        projectedDir = INCOMING,
        relationshipPredicates = Seq(Predicate("anon_0", "NOT endNode(anon_0) = startNode(anon_0)")),
        pathMode = Acyclic
      )
      .nodeByLabelScan("n5", "S")
      .build()
  }

  test(
    "Fixed-length relationship and undirected QPP - process right-to-left - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1)-[r1]->(n2)((n3)-[r2]-(n4)){0,3}(n5:S)
        |RETURN n1, n5
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter(
        "none(anon_3 IN r2 WHERE n1 IN [startNode(anon_3), endNode(anon_3)])",
        "NOT n1 = n5",
        "NOT n1 = n2"
      )
      .expandAll("(n2)<-[]-(n1)")
      .expandExpr(
        "(n5)-[r2*0..3]-(n2)",
        expandMode = ExpandAll,
        projectedDir = INCOMING,
        relationshipPredicates = Seq(VariablePredicate(
          v"anon_0",
          not(equals(
            TraversalEndpoint(varFor("anon_1"), Endpoint.To),
            TraversalEndpoint(varFor("anon_2"), Endpoint.From)
          ))
        )),
        pathMode = Acyclic
      )
      .nodeByLabelScan("n5", "S")
      .build()
  }

  test(
    "Fixed-length relationship and QPP - process right-to-left - Should plan Repeat with filter before the expand when it cannot become a pruning VarExpand"
  ) {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1)-[r1]->(n2)((n3)-[r2]->(n4 {p:1})){0,3}(n5:S)
        |RETURN DISTINCT n1, n5
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .orderedDistinct(Seq("n5"), "n1 AS n1", "n5 AS n5")
      .filter(
        // The Uniqueness checks require access to the individual paths, which are not being kept with pruning VarExpands.
        "NOT n1 IN n4 + n3",
        "NOT n1 = n5",
        "NOT n1 = n2"
      )
      .expandAll("(n2)<-[]-(n1)")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "n5",
        end = "n2",
        innerStart = "n4",
        innerEnd = "n3",
        groupNodes = Set(("n3", "n3"), ("n4", "n4")),
        innerNodes = Set("n3", "n4"),
        previouslyBoundNodes = Set("n5"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = true,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n3"), "NOT n3 = n4")
      .|.expandAll("(n4)<-[r2]-(n3)")
      .|.filter("n4.p = 1")
      .|.argument("n4")
      .nodeByLabelScan("n5", "S", IndexOrderAscending)
      .build()
  }

  test(
    "outgoing QPP and Fixed-length relationship - process right-to-left - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1)((n2)-[r1]->(n3)){0,3}(n4)-[r2]->(n5:S)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5")
      .expand(
        "(n4)<-[*0..3]-(n1)",
        relationshipPredicates = Seq(
          Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)"),
          Predicate("anon_0", "NOT n5 = startNode(anon_0)")
        ),
        pathMode = Acyclic
      )
      .filter("NOT n4 = n5")
      .expandAll("(n5)<-[]-(n4)")
      .nodeByLabelScan("n5", "S", IndexOrderNone)
      .build()
  }

  test(
    "incoming QPP and Fixed-length relationship - process right-to-left - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1)((n2)<-[r1]-(n3)){0,3}(n4)-[r2]->(n5:S)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5")
      .expand(
        "(n4)-[*0..3]->(n1)",
        projectedDir = INCOMING,
        relationshipPredicates = Seq(
          Predicate("anon_0", "NOT endNode(anon_0) = startNode(anon_0)"),
          Predicate("anon_0", "NOT n5 = endNode(anon_0)")
        ),
        pathMode = Acyclic
      )
      .filter("NOT n4 = n5")
      .expandAll("(n5)<-[]-(n4)")
      .nodeByLabelScan("n5", "S", IndexOrderNone)
      .build()
  }

  test(
    "undirected QPP and Fixed-length relationship - process right-to-left - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1)((n2)-[r1]-(n3)){0,3}(n4)-[r2]->(n5:S)
         |RETURN n1, n5
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT n1 = n5")
      .expandExpr(
        "(n4)-[*0..3]-(n1)",
        projectedDir = INCOMING,
        relationshipPredicates = Seq(
          VariablePredicate(
            v"anon_0",
            not(equals(
              TraversalEndpoint(tempVar = v"anon_1", endpoint = To),
              TraversalEndpoint(tempVar = v"anon_2", endpoint = From)
            ))
          ),
          Predicate("anon_0", "NOT n5 IN [startNode(anon_0), endNode(anon_0)]").asVariablePredicate
        ),
        pathMode = Acyclic
      )
      .filter("NOT n4 = n5")
      .expandAll("(n5)<-[]-(n4)")
      .nodeByLabelScan("n5", "S", IndexOrderNone)
      .build()
  }

  test("QPP with more than one relationship - process left-to-right") {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1:S)
        |(
        | (n2)<-[r1]-(n3)
        |  -[r2]->(n4)
        |  -[r3]->(n5)
        |){0,3}
        |(n6)
        |RETURN n1, n6
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n6")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "n1",
        end = "n6",
        innerStart = "n2",
        innerEnd = "n5",
        groupNodes = Set(),
        innerNodes = Set("n2", "n3", "n4", "n5"),
        previouslyBoundNodes = Set("n1"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r1", "r2", "r3"), // This prevents the rewrite to VarExpand
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n5"), "NOT n2 = n5", "NOT n4 = n5", "NOT n3 = n5")
      .|.expandAll("(n4)-[r3]->(n5)")
      .|.filter(isRepeatAcyclic("n4"), "NOT n2 = n4", "NOT n3 = n4")
      .|.expandAll("(n3)-[r2]->(n4)")
      .|.filter(isRepeatAcyclic("n3"), "NOT n2 = n3")
      .|.expandAll("(n2)<-[r1]-(n3)")
      .|.argument("n2")
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test("QPP with more than one relationship - process right-to-left") {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1)
        |(
        | (n2)<-[r1]-(n3)
        |  -[r2]->(n4)
        |  -[r3]->(n5)
        |){0,3}
        |(n6:S)
        |RETURN n1, n6
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n6")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "n6",
        end = "n1",
        innerStart = "n5",
        innerEnd = "n2",
        groupNodes = Set(),
        innerNodes = Set("n2", "n3", "n4", "n5"),
        previouslyBoundNodes = Set("n6"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r1", "r2", "r3"), // This prevents the rewrite to VarExpand
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = true,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n2"), "NOT n2 = n3", "NOT n2 = n4", "NOT n2 = n5")
      .|.expandAll("(n3)-[r1]->(n2)")
      .|.filter(isRepeatAcyclic("n3"), "NOT n3 = n4", "NOT n3 = n5")
      .|.expandAll("(n4)<-[r2]-(n3)")
      .|.filter(isRepeatAcyclic("n4"), "NOT n4 = n5")
      .|.expandAll("(n5)<-[r3]-(n4)")
      .|.argument("n5")
      .nodeByLabelScan("n6", "S", IndexOrderNone)
      .build()
  }

  test("Directly connected QPPs") {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1:S)
        |((n2)-[r1]->(n3)){0,2}
        |(n4)
        |((n5)-[r2]->(n6)){0,3}
        |(n7)
        |RETURN n1, n7
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n7")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "n4",
        end = "n7",
        innerStart = "n5",
        innerEnd = "n6",
        groupNodes = Set(),
        innerNodes = Set("n5", "n6"),
        previouslyBoundNodes = Set("n4"),
        // n1 was also previously bound. It is not strictly needed,
        // since it's the same as n4 in the case of zero-iterations of the first QPP
        // and the same as the last n2 in the case of at least one iteration of the first QPP
        previouslyBoundNodeGroups = Set("n3", "n2"), // This prevents the rewrite to VarExpand (for both Repeats)
        groupRelationships = Set(),
        innerRelationships = Set("r2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n6"), "NOT n5 = n6")
      .|.expandAll("(n5)-[r2]->(n6)")
      .|.argument("n5")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(2),
        start = "n1",
        end = "n4",
        innerStart = "n2",
        innerEnd = "n3",
        groupNodes = Set(("n2", "n2"), ("n3", "n3")),
        innerNodes = Set("n2", "n3"),
        previouslyBoundNodes = Set("n1"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r1"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n3"), "NOT n2 = n3")
      .|.expandAll("(n2)-[r1]->(n3)")
      .|.argument("n2")
      .nodeByLabelScan("n1", "S")
      .build()
  }

  test("QPPs with a relationship in between - processing QPP1, rel, QPP2") {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (n1:S)
        |  ((n2)-[r1:R]->(n3)){0,2}
        |  (n4)-[r2]->(n5)
        |  ((n6)-[r3]->(n7)){0,3}
        |  (n8)
        |RETURN n1, n8
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n8")
      .filter("NOT n8 IN n3 + n2", "NOT n1 = n8", "NOT n4 = n8") // Not strictly needed
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "n5",
        end = "n8",
        innerStart = "n6",
        innerEnd = "n7",
        groupNodes = Set(),
        innerNodes = Set("n6", "n7"),
        previouslyBoundNodes = Set("n1", "n4", "n5"),
        previouslyBoundNodeGroups = Set("n3", "n2"), // This prevents the rewrite to VarExpand (for both Repeats)
        groupRelationships = Set(),
        innerRelationships = Set("r3"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n7"), "NOT n6 = n7")
      .|.expandAll("(n6)-[r3]->(n7)")
      .|.argument("n6")
      .filter("NOT n5 IN n3 + n2", "NOT n1 = n5", "NOT n4 = n5")
      .expandAll("(n4)-[]->(n5)")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(2),
        start = "n1",
        end = "n4",
        innerStart = "n2",
        innerEnd = "n3",
        groupNodes = Set(("n2", "n2"), ("n3", "n3")),
        innerNodes = Set("n2", "n3"),
        previouslyBoundNodes = Set("n1"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r1"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n3"), "NOT n2 = n3")
      .|.expandAll("(n2)-[r1:R]->(n3)")
      .|.argument("n2")
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test("QPPs with a relationship in between - processing rel, QPP1, QPP2") {
    val planner = pb
      .setRelationshipCardinality("(:S)-[:R]->(:S)", 3)
      .build()
    val query =
      """
        |MATCH ACYCLIC (n1)
        |((n2)-[r1]->(n3)){0,2}
        |(n4)-[r2:R]->(n5:S)
        |((n6)-[r3]->(n7)){0,3}
        |(n8)
        |RETURN n1, n8
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n8")
      .filter("NOT n8 IN n3 + n2", "NOT n1 = n8", "NOT n4 = n8")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "n5",
        end = "n8",
        innerStart = "n6",
        innerEnd = "n7",
        groupNodes = Set(),
        innerNodes = Set("n6", "n7"),
        previouslyBoundNodes = Set("n1", "n4", "n5"),
        previouslyBoundNodeGroups = Set("n2", "n3"), // This prevents the rewrite to VarExpand (for both Repeats)
        groupRelationships = Set(),
        innerRelationships = Set("r3"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n7"), "NOT n6 = n7")
      .|.expandAll("(n6)-[r3]->(n7)")
      .|.argument("n6")
      .filter("NOT n1 = n5")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(2),
        start = "n4",
        end = "n1",
        innerStart = "n3",
        innerEnd = "n2",
        groupNodes = Set(("n2", "n2"), ("n3", "n3")),
        innerNodes = Set("n2", "n3"),
        previouslyBoundNodes = Set("n5", "n4"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r1"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = true,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n2"), "NOT n2 = n3")
      .|.expandAll("(n3)<-[r1]-(n2)")
      .|.argument("n3")
      .filter("NOT n4 = n5")
      .expandAll("(n5)<-[:R]-(n4)")
      .nodeByLabelScan("n5", "S", IndexOrderNone)
      .build()
  }

  test("Relationship before and after QPP") {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (a:S)-[q:R]->(b) ((c)-[r]->(d)-[s]->(e)){1,10} (f)-[t]->(g)
        |RETURN a, g
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "g")
      .filter("NOT g IN (e + d) + c", "NOT a = g", "NOT b = g", "NOT f = g")
      // The following should have been enough: "NOT g IN (e + d)", "NOT a = g", "NOT b = g"
      .expandAll("(f)-[]->(g)")
      .filter("NOT a = f") // Not strictly needed
      .repeatAcyclic(AcyclicParameters(
        min = 1,
        max = Limited(10),
        start = "b",
        end = "f",
        innerStart = "c",
        innerEnd = "e",
        groupNodes = Set(("c", "c"), ("d", "d"), ("e", "e")),
        innerNodes = Set("c", "d", "e"),
        previouslyBoundNodes = Set("a", "b"), // This prevents the rewrite to VarExpand
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r", "s"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("e"), "NOT d = e", "NOT c = e")
      .|.expandAll("(d)-[s]->(e)")
      .|.filter(isRepeatAcyclic("d"), "NOT c = d")
      .|.expandAll("(c)-[r]->(d)")
      .|.argument("c")
      .filter("NOT a = b")
      .expandAll("(a)-[:R]->(b)")
      .nodeByLabelScan("a", "S", IndexOrderNone)
      .build()
  }

  test("Relationship before and after QPP - VarExpand") {
    val planner = pb.build()
    val query =
      """
        |MATCH ACYCLIC (a:S)-[q:R]->(b) ((c)-[r]->(d)){1,10} (f)-[t]->(g)
        |RETURN a, g
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "g")
      .filter("none(anon_1 IN r WHERE g = endNode(anon_1))", "NOT b = g", "NOT a = g", "NOT f = g")
      .expandAll("(f)-[]->(g)")
      .filter("NOT a = f")
      .expand(
        "(b)-[r*1..10]->(f)",
        expandMode = ExpandAll,
        relationshipPredicates = Seq(
          Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)"),
          Predicate("anon_0", "NOT a = endNode(anon_0)")
        ),
        pathMode = Acyclic
      )
      .filter("NOT a = b")
      .expandAll("(a)-[:R]->(b)")
      .nodeByLabelScan("a", "S", IndexOrderNone)
      .build()
  }

  test("Multiple ACYCLIC path patterns - Two path patterns with single fixed-length relationships") {
    val planner = pb.build()
    val query =
      """
        |MATCH
        |  ACYCLIC (n1:S)-[r1:R]->(n2),
        |  ACYCLIC (n2)-[r2]->(n3)
        |RETURN n1, n3
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n3")
      .filter("NOT r1 = r2", "NOT n2 = n3")
      .expandAll("(n2)-[r2]->(n3)")
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[r1:R]->(n2)")
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test(
    "Multiple ACYCLIC path patterns - Two path patterns with single fixed-length relationships - anonymous variables"
  ) {
    val planner = pb.build()
    val query =
      """
        |MATCH
        |  ACYCLIC ()-[]-(),
        |  ACYCLIC ()-[]-()
        |RETURN 1
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("1")
      .projection("1 AS 1")
      .filter("NOT anon_1 = anon_4")
      .cartesianProduct()
      .|.filter("NOT anon_3 = anon_5")
      .|.allRelationshipsScan("(anon_3)-[anon_4]-(anon_5)")
      .filter("NOT anon_0 = anon_2")
      .allRelationshipsScan("(anon_0)-[anon_1]-(anon_2)")
      .build()
  }

  test("Multiple ACYCLIC path patterns - Two path patterns with multiple fixed-length relationships") {
    val planner = pb.build()
    val query =
      """
        |MATCH
        |   ACYCLIC (n1:S)-[r1]->(n2)<-[r2]-(n3),
        |   ACYCLIC (n3)-[r3]->(n4)-[r4]->(n5)
        |RETURN n1, n5
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n5")
      .filter("NOT r2 = r4", "NOT r1 = r4", "NOT n3 = n5", "NOT n4 = n5")
      .expandAll("(n4)-[r4]->(n5)")
      .filter("NOT r2 = r3", "NOT r1 = r3", "NOT n3 = n4")
      .expandAll("(n3)-[r3]->(n4)")
      .filter("NOT n1 = n3", "NOT n2 = n3")
      .expandAll("(n2)<-[r2]-(n3)")
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[r1]->(n2)")
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test(
    "Multiple ACYCLIC path patterns - Two path patterns both with a fixed-length relationship and a QPP - Repeat"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)
         |((n3)<-[r2]-(n4)){0,${intMaxValuePlus1}}
         |(n5),
         |ACYCLIC (n5)-[r3]->(n6)
         |((n7)-[r4]->(n8)){0,${intMaxValuePlus1}}
         |(n9)
         |RETURN n1, n9
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n9")
      .filter("NOT n5 = n9")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(intMaxValuePlus1), // This prevents the rewrite to VarExpand
        start = "n6",
        end = "n9",
        innerStart = "n7",
        innerEnd = "n8",
        groupNodes = Set(),
        innerNodes = Set("n7", "n8"),
        previouslyBoundNodes = Set("n5", "n6"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r4"),
        previouslyBoundRelationships = Set("r1"),
        previouslyBoundRelationshipGroups = Set("r2"),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n8"), "NOT n7 = n8", isRepeatTrailUnique("r4"))
      .|.expandAll("(n7)-[r4]->(n8)")
      .|.argument("n7")
      .filter("NOT r3 IN r2", "NOT n5 = n6", "NOT r1 = r3")
      .expandAll("(n5)-[r3]->(n6)")
      .filter("NOT n1 = n5") // Not strictly needed
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(intMaxValuePlus1), // This prevents the rewrite to VarExpand
        start = "n2",
        end = "n5",
        innerStart = "n3",
        innerEnd = "n4",
        groupNodes = Set(),
        innerNodes = Set("n3", "n4"),
        previouslyBoundNodes = Set("n1", "n2"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(("r2", "r2")),
        innerRelationships = Set("r2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("n4"), "NOT n3 = n4", isRepeatTrailUnique("r2"))
      .|.expandAll("(n3)<-[r2]-(n4)")
      .|.argument("n3")
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[r1]->(n2)")
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test(
    "Multiple ACYCLIC path patterns - Two path patterns both with a fixed-length relationship and a QPP - VarExpand"
  ) {
    val planner = pb.build()
    val query =
      s"""
         |MATCH ACYCLIC (n1:S)-[r1]->(n2)
         |((n3)<-[r2]-(n4)){0,2}
         |(n5),
         |ACYCLIC (n5)-[r3]->(n6)
         |((n7)-[r4]->(n8)){0,1}
         |(n9)
         |RETURN n1, n9
         |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("n1", "n9")
      .filter("NOT r1 IN r4", "none(anon_2 IN r4 WHERE anon_2 IN r2)", "NOT n5 = n9")
      .expandExpr(
        "(n6)-[r4*0..1]->(n9)",
        relationshipPredicates = Seq(
          Predicate("anon_1", "NOT startNode(anon_1) = endNode(anon_1)").asVariablePredicate,
          Predicate(
            "anon_1",
            "NOT n5 = endNode(anon_1)"
          ).asVariablePredicate
        ),
        pathMode = Acyclic
      )
      .filter("NOT r3 IN r2", "NOT n5 = n6", "NOT r1 = r3")
      .expandAll("(n5)-[r3]->(n6)")
      .filter("NOT n1 = n5")
      .expandExpr(
        "(n2)<-[r2*0..2]-(n5)",
        projectedDir = INCOMING,
        relationshipPredicates = Seq(
          Predicate("anon_0", "NOT endNode(anon_0) = startNode(anon_0)").asVariablePredicate,
          Predicate(
            "anon_0",
            "NOT n1 = startNode(anon_0)"
          ).asVariablePredicate
        ),
        pathMode = Acyclic
      )
      .filter("NOT n1 = n2")
      .expandAll("(n1)-[r1]->(n2)")
      .nodeByLabelScan("n1", "S", IndexOrderNone)
      .build()
  }

  test("ACYCLIC base test") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC (a:S)-[r1:R]->(c),
        |  ACYCLIC (c)-[r2:R]->(d)<-[r3:R]-(e)
        |  ((e_inner)-[r4:R]->(f)<-[r5:R]-(g_inner1)){0,3}
        |  (g)
        |  ((g_inner2)-[r6:R]->(h_inner)){0,2} (h)
        |RETURN a, h
        |
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "h")
      .filter("NOT c = h", "NOT d = h") // DifferentNodes(c,h), DifferentNodes(d,h)
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(2),
        start = "g",
        end = "h",
        innerStart = "g_inner2",
        innerEnd = "h_inner",
        groupNodes = Set(),
        innerNodes = Set("g_inner2", "h_inner"),
        // e is a previously bound node, but it has the same equivalence class as this Repeat and is not the start node of the Repeat. Therefore, it should not be included. Including it would add useless checks, but will also break our rewriter to VarExpand.
        previouslyBoundNodes = Set("c", "d", "g"),
        previouslyBoundNodeGroups = Set("g_inner1", "f", "e_inner"), // This prevents the rewrite to VarExpand
        groupRelationships = Set(),
        innerRelationships = Set("r6"),
        previouslyBoundRelationships = Set("r1"),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      // IsRepeatAcyclic(h_inner) with previouslyBoundNodes = Set("c", "d", "g")
      // and previouslyBoundNodeGroups = Set("g_inner1", "f", "e_inner") solves:
      // - UniqueNodes(g_inner2+h_inner)
      // - DistinctNodes(e_inner+f+g_inner1, g_inner2+h_inner)
      // - NoneOfNodes(c, g_inner2+h_inner)
      // - NoneOfNodes(d, g_inner2+h_inner)
      // isRepeatTrailUnique(r6) with previouslyBoundRelationships = Set("r1") solves:
      // - NoneOfRelationships(r1, r6)
      .|.filter(isRepeatAcyclic("h_inner"), "NOT g_inner2 = h_inner", isRepeatTrailUnique("r6"))
      .|.expandAll("(g_inner2)-[r6:R]->(h_inner)")
      .|.argument("g_inner2")
      .filter("NOT c = g", "NOT d = g") // DifferentNodes(c,g), DifferentNodes(d,g)
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(3),
        start = "e",
        end = "g",
        innerStart = "e_inner",
        innerEnd = "g_inner1",
        groupNodes =
          Set(("e_inner", "e_inner"), ("f", "f"), ("g_inner1", "g_inner1")),
        innerNodes = Set("e_inner", "f", "g_inner1"),
        previouslyBoundNodes = Set("c", "d", "e"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r4", "r5"), // This prevents the rewrite to VarExpand
        previouslyBoundRelationships = Set("r1"),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      // IsRepeatAcyclic(v"f"), IsRepeatAcyclic(v"g_inner1") with previouslyBoundNodes = Set("c", "d", "e") solves:
      // - UniqueNodes(e_inner+f+g_inner1)
      // - NoneOfNodes(c, e_inner+f+g_inner1)
      // - NoneOfNodes(d, e_inner+f+g_inner1)
      // IsRepeatTrailUnique(r4), IsRepeatTrailUnique(r5) with previouslyBoundRelationships = Set("r1") solves:
      // - NoneOfRelationships(r1, r4+r5)
      .|.filter(
        isRepeatAcyclic("g_inner1"),
        "NOT f = g_inner1",
        "NOT e_inner = g_inner1",
        isRepeatTrailUnique("r5")
      )
      .|.expandAll("(f)<-[r5:R]-(g_inner1)")
      .|.filter(isRepeatAcyclic("f"), "NOT e_inner = f", isRepeatTrailUnique("r4"))
      .|.expandAll("(e_inner)-[r4:R]->(f)")
      .|.argument("e_inner")
      .filter(
        "NOT r1 = r3", // DifferentRelationships(r1,r3)
        "NOT c = e", // DifferentNodes(c,e)
        "NOT d = e" // DifferentNodes(d,e)
      )
      .expandAll("(d)<-[r3:R]-(e)")
      .filter("NOT r1 = r2", "NOT c = d") // DifferentRelationships(r1,r2), DifferentNodes(c,d)
      .expandAll("(c)-[r2:R]->(d)")
      .filter("NOT a = c") // DifferentNodes(a,c)
      .expandAll("(a)-[r1:R]->(c)")
      .nodeByLabelScan("a", "S")
      .build()
  }

  test("Single ACYCLIC path pattern - with subquery expression") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC (a)-[r]-(b)((c)-[s]-(d)
        |  WHERE EXISTS { MATCH ACYCLIC (d)((x1)-[x2]->(x3))+(c) RETURN * } )+ (p)
        |RETURN *
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "b", "c", "d", "p", "r", "s")
      .filter("NOT a = p")
      .repeatAcyclic(AcyclicParameters(
        min = 1,
        max = Unlimited,
        start = "b",
        end = "p",
        innerStart = "c",
        innerEnd = "d",
        groupNodes = Set(("c", "c"), ("d", "d")),
        innerNodes = Set("c", "d"),
        previouslyBoundNodes = Set("a", "b"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(("s", "s")),
        innerRelationships = Set("s"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.semiApply() // This prevents the rewrite to VarExpand for the Repeat in the outer query
      .|.|.expand(
        "(d)-[*1..]->(c)",
        expandMode = ExpandInto,
        relationshipPredicates = Seq(Predicate("anon_0", "NOT startNode(anon_0) = endNode(anon_0)")),
        pathMode = Acyclic
      )
      .|.|.argument("d", "c")
      .|.filter(isRepeatAcyclic("d"), "NOT c = d")
      .|.expandAll("(c)-[s]-(d)")
      .|.argument("c")
      .filter("NOT a = b")
      .allRelationshipsScan("(a)-[r]-(b)")
      .build()
  }

  test("Single ACYCLIC path pattern - with subquery expression with TRAIL path mode") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC (a)-[r]-(b)((c)-[s]-(d)
        |  WHERE EXISTS { MATCH (d:T)((e)-[x2 {prop: 1}]->(f))+(c) RETURN * } )+ (p)
        |RETURN *
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "b", "c", "d", "p", "r", "s")
      .filter("NOT a = p")
      .repeatAcyclic(AcyclicParameters(
        min = 1,
        max = Unlimited,
        start = "b",
        end = "p",
        innerStart = "c",
        innerEnd = "d",
        groupNodes = Set(("c", "c"), ("d", "d")),
        innerNodes = Set("c", "d"),
        previouslyBoundNodes = Set("a", "b"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(("s", "s")),
        innerRelationships = Set("s"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("d"), "NOT c = d")
      .|.semiApply() // This prevents the rewrite to VarExpand for the Repeat in the outer query
      .|.|.repeatTrail(TrailParameters(
        min = 1,
        max = Unlimited,
        start = "d",
        end = "c",
        innerStart = "e",
        innerEnd = "f",
        groupNodes = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("x2"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandInto,
        accumulators = Set()
      ))
      .|.|.|.filter("NOT e = f", "x2.prop = 1", isRepeatTrailUnique("x2"))
      .|.|.|.expandAll("(e)-[x2]->(f)")
      .|.|.|.argument("e")
      .|.|.filter("d:T")
      .|.|.argument("d", "c")
      .|.expandAll("(c)-[s]-(d)")
      .|.argument("c")
      .filter("NOT a = b")
      .allRelationshipsScan("(a)-[r]-(b)")
      .build()
  }

  test("Single ACYCLIC path pattern - expandInto due to previous graph pattern") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC (a)--(e)
        |WITH * SKIP 0
        |MATCH ACYCLIC (a)((b)--(c)--(d))*(e)
        |RETURN *
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "b", "c", "d", "e")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Unlimited,
        start = "a",
        end = "e",
        innerStart = "b",
        innerEnd = "d",
        groupNodes = Set(("b", "b"), ("c", "c"), ("d", "d")),
        innerNodes = Set("b", "c", "d"),
        previouslyBoundNodes = Set("a"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("anon_0", "anon_1"), // This prevents the rewrite to VarExpand
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandInto,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("d"), "NOT b = d", "NOT c = d")
      .|.expandAll("(c)-[anon_1]-(d)")
      .|.filter(isRepeatAcyclic("c"), "NOT b = c")
      .|.expandAll("(b)-[anon_0]-(c)")
      .|.argument("b")
      .skip(0)
      .filter("NOT a = e")
      .allRelationshipsScan("(a)-[]-(e)")
      .build()
  }

  test("Single ACYCLIC path pattern - expandInto in same GRAPH pattern") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC (a)--(e), ACYCLIC (a)((b)--(c)--(d))*(e)
        |RETURN *
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    plan shouldEqual planner.subPlanBuilder()
      .produceResults("a", "b", "c", "d", "e")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Unlimited,
        start = "a",
        end = "e",
        innerStart = "b",
        innerEnd = "d",
        groupNodes = Set(("b", "b"), ("c", "c"), ("d", "d")),
        innerNodes = Set("b", "c", "d"),
        previouslyBoundNodes = Set("a"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("anon_1", "anon_2"), // This prevents the rewrite to VarExpand
        previouslyBoundRelationships = Set("anon_0"),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandInto,
        accumulators = Set()
      ))
      .|.filter(isRepeatAcyclic("d"), "NOT b = d", "NOT c = d", isRepeatTrailUnique("anon_2"))
      .|.expandAll("(c)-[anon_2]-(d)")
      .|.filter(isRepeatAcyclic("c"), "NOT b = c", isRepeatTrailUnique("anon_1"))
      .|.expandAll("(b)-[anon_1]-(c)")
      .|.argument("b")
      .filter("NOT a = e")
      .allRelationshipsScan("(a)-[anon_0]-(e)")
      .build()
  }

  test("should be able to combine ACYCLIC with allReduce") {
    val planner = pb.build()

    val query =
      """
        |MATCH ACYCLIC ()-[r]-+()
        |  WHERE allReduce(acc = [], rel IN r | acc + rel, size(acc) < 10)
        |RETURN count(*)
        |""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)
    val acyclicParameters =
      AcyclicParameters(
        min = 1,
        max = Unlimited,
        start = "anon_0",
        end = "anon_1",
        innerStart = "anon_2",
        innerEnd = "anon_3",
        groupNodes = Set(),
        innerNodes = Set("anon_2", "anon_3"),
        previouslyBoundNodes = Set("anon_0"),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(),
        innerRelationships = Set("r"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = false,
        expansionMode = ExpandAll,
        // this comes from the allReduce
        accumulators = Set(("[]", "acc", "acc"))
      )
    plan shouldEqual
      planner.planBuilder()
        .produceResults("`count(*)`")
        .aggregation(Seq(), Seq("count(*) AS `count(*)`"))
        .repeatAcyclic(acyclicParameters)
        .|.filter("size(acc) < 10", isRepeatAcyclic("anon_3"), "NOT anon_2 = anon_3")
        .|.projection("acc + r AS acc")
        .|.expandAll("(anon_2)-[r]-(anon_3)")
        .|.argument("anon_2", "acc")
        .allNodeScan("anon_0")
        .build()
  }
}
