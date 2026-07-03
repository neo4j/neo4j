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
package org.neo4j.cypher.internal.logical.builder

import dotty.tools.repl.ReplDriver
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.ir.SelectivePathPattern.CountInteger
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.AcyclicParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.PushdownOperators
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.WalkParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithDynamicLabels
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipExpression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipFull
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipWithDynamicType
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeDynamicLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setDynamicLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setDynamicProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.DynamicElement.All
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SkipSameNode
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.UpperBound.Limited
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.graphdb.schema.IndexType

import java.lang.reflect.Modifier

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.MILLISECONDS
import scala.util.DynamicVariable

/**
 * If you reference something new and a type was not found an import needs to be added to [[interpretPlanBuilder]]
 */
class LogicalPlanToPlanBuilderStringTest
    extends CypherFunSuite
    with TestName
    with AstConstructionTestSupport
    with QueryExpressionConstructionTestSupport {

  private val testedOperators = mutable.Set[String]()

  testPlan(
    "statefulShortestPath",
    new TestPlanBuilder()
      .produceResults("a", "b")
      .statefulShortestPath(
        "a",
        "b",
        "",
        None,
        Set.empty,
        Set.empty,
        Set("b_expr" -> "b"),
        Set("r_expr" -> "r"),
        StatefulShortestPath.Selector.Shortest(CountInteger(1)),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[r_expr]->(b_expr)")
          .setFinalState(1)
          .build(),
        ExpandAll,
        false
      )
      .allNodeScan("a")
      .build()
  )

  testPlan(
    "statefulShortestPath with more NFA variation",
    new TestPlanBuilder()
      .produceResults("a", "b", "c")
      .statefulShortestPath(
        "a",
        "c",
        "",
        Some("a.prop + c.prop = 5"),
        Set(("b_in", "b_group"), ("c_in", "c_group")),
        Set(("r2", "r2_group")),
        Set("b_expr" -> "b", "c_expr" -> "c", "d_expr" -> "d"),
        Set("r1_expr" -> "r1", "r3_expr" -> "r3"),
        StatefulShortestPath.Selector.ShortestGroups(CountInteger(5)),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[r1_expr WHERE r1_expr.prop > 5]->(b_expr:A&B WHERE b_expr.prop = 10)")
          .addTransition(1, 2, "(b_expr) (b_in WHERE b_in.prop = 10)")
          .addTransition(2, 3, "(b_in)<-[r2:R2 WHERE r2 < 7]-(c_in)")
          .addTransition(3, 2, "(c_in) (b_in:A|C)")
          .addTransition(3, 4, "(c_in) (c_expr:C&D WHERE c_expr.prop = 5)")
          .addTransition(1, 4, "(b_expr) (c_expr)")
          .addTransition(4, 5, "(c_expr)-[r3_expr]-(d_expr)")
          .setFinalState(5)
          .build(),
        ExpandAll,
        false
      )
      .allNodeScan("a")
      .build()
  )

  testPlan(
    "statefulShortestPathExpr",
    new TestPlanBuilder()
      .produceResults("a", "b", "c")
      .statefulShortestPathExpr(
        "a",
        "c",
        "",
        Some(
          // a.prop = c.prop
          AstConstructionTestSupport.equals(
            AstConstructionTestSupport.prop("a", "prop"),
            AstConstructionTestSupport.prop("c", "prop")
          )
        ),
        Set(("b_in", "b_group"), ("c_in", "c_group")),
        Set(("r2", "r2_group")),
        Set("b_expr" -> "b", "c_expr" -> "c", "d_expr" -> "d"),
        Set("r1_expr" -> "r1", "r3_expr" -> "r3"),
        StatefulShortestPath.Selector.ShortestGroups(CountInteger(5)),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[r1_expr WHERE r1_expr.prop > 5]->(b_expr:A&B WHERE b_expr.prop = 10)")
          .addTransition(1, 2, "(b_expr) (b_in WHERE b_in.prop = 10)")
          .addTransition(2, 3, "(b_in)<-[r2:R2 WHERE r2 < 7]-(c_in)")
          .addTransition(3, 2, "(c_in) (b_in:A|C)")
          .addTransition(3, 4, "(c_in) (c_expr:C&D WHERE c_expr.prop = 5)")
          .addTransition(1, 4, "(b_expr) (c_expr)")
          .addTransition(4, 5, "(c_expr)-[r3_expr]-(d_expr)")
          .setFinalState(5)
          .build(),
        ExpandAll,
        false
      )
      .allNodeScan("a")
      .build()
  )

  testPlan(
    "statefulShortestPath with non-default match mode",
    new TestPlanBuilder()
      .produceResults("a", "b")
      .statefulShortestPath(
        "a",
        "b",
        "",
        None,
        Set.empty,
        Set.empty,
        Set("b_expr" -> "b"),
        Set("r_expr" -> "r"),
        StatefulShortestPath.Selector.Shortest(CountInteger(1)),
        new TestNFABuilder(0, "a")
          .addTransition(0, 1, "(a)-[r_expr]->(b_expr)")
          .setFinalState(1)
          .build(),
        ExpandAll,
        false,
        pathMode = TraversalPathMode.Walk
      )
      .allNodeScan("a")
      .build()
  )

  testPlan(
    "pathPropagatingBFS",
    new TestPlanBuilder()
      .produceResults("b", "c")
      .pathPropagatingBFS("(a)-[*]->(b)", projectedDir = INCOMING, Seq.empty, Seq.empty)
      .|.filter("c:C")
      .|.expand("(b)-[]-(c)")
      .|.argument("b")
      .allNodeScan("a")
      .build()
  )

  testPlan(
    "pathPropagatingBFS with extras",
    new TestPlanBuilder()
      .produceResults("b", "c")
      .pathPropagatingBFS(
        "(a)-[:R*7..99]->(b)",
        projectedDir = OUTGOING,
        nodePredicates = Seq(Predicate("n", "id(n) <> 5"), Predicate("n2", "id(n2) > 5")),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> 5"), Predicate("r2", "id(r2) > 5"))
      )
      .|.filter("c:C")
      .|.expand("(b)-[]-(c)")
      .|.argument("b")
      .allNodeScan("a")
      .build()
  )

  testPlan(
    "letSelectOrSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .letSelectOrSemiApply("idName", "false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "letSelectOrAntiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .letSelectOrAntiSemiApply("idName", "false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "produceResults",
    new TestPlanBuilder()
      .produceResults("x")
      .argument("x")
      .build()
  )

  testPlan(
    "produceResults of property",
    new TestPlanBuilder()
      .produceResults("`x.prop`")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "produceResults with cached properties",
    new TestPlanBuilder()
      .produceResults(column("x", "cacheN[x.p1]", "cacheN[x.p2]"))
      .cacheProperties("x.p1", "x.p2")
      .argument("x")
      .build()
  )

  testPlan(
    "argument",
    new TestPlanBuilder()
      .produceResults("x")
      .argument("x", "y")
      .build()
  )

  testPlan(
    "input",
    new TestPlanBuilder()
      .produceResults("x")
      .input(Seq("n", "m"), Seq("r", "q", "p"), Seq("v1"), nullable = false)
      .build()
  )

  testPlan(
    "allNodeScan",
    new TestPlanBuilder()
      .produceResults("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "partitionedAllNodeScan",
    new TestPlanBuilder()
      .produceResults("x")
      .partitionedAllNodeScan("x")
      .build()
  )

  testPlan(
    "nodeByLabelScan",
    new TestPlanBuilder()
      .produceResults("x")
      .nodeByLabelScan("x", "X")
      .build()
  )

  testPlan(
    "dynamicLabelNodeLookup",
    new TestPlanBuilder()
      .produceResults("x")
      .apply()
      .|.dynamicLabelNodeLookup("y", "['A', 'B']", All, "x")
      .dynamicLabelNodeLookup("x", "['A', 'B']", All, Map("foo" -> "1"))
      .build()
  )

  testPlan(
    "partitionedNodeByLabelScan",
    new TestPlanBuilder()
      .produceResults("x")
      .partitionedNodeByLabelScan("x", "X")
      .build()
  )

  testPlan(
    "nodeByLabelScan anonymous variable",
    new TestPlanBuilder()
      .produceResults("x")
      .projection("`  UNNAMED0` AS x")
      .nodeByLabelScan("`  UNNAMED0`", "X")
      .build()
  )

  testPlan(
    "nodeByLabelScan full",
    new TestPlanBuilder()
      .produceResults("x")
      .nodeByLabelScan("x", "X", IndexOrderDescending, "foo")
      .build()
  )

  testPlan(
    "unionNodeByLabelsScan",
    new TestPlanBuilder()
      .produceResults("x")
      .unionNodeByLabelsScan("x", Seq("X", "Y", "Z"), IndexOrderNone)
      .build()
  )

  testPlan(
    "partitionedUnionNodeByLabelsScan",
    new TestPlanBuilder()
      .produceResults("x")
      .partitionedUnionNodeByLabelsScan("x", Seq("X", "Y", "Z"))
      .build()
  )

  testPlan(
    "intersectionNodeByLabelsScan",
    new TestPlanBuilder()
      .produceResults("x")
      .intersectionNodeByLabelsScan("x", Seq("X", "Y", "Z"), IndexOrderNone)
      .build()
  )

  testPlan(
    "partitionedIntersectionNodeByLabelsScan",
    new TestPlanBuilder()
      .produceResults("x")
      .partitionedIntersectionNodeByLabelsScan("x", Seq("X", "Y", "Z"))
      .build()
  )

  testPlan(
    "subtractionNodeByLabelsScan",
    new TestPlanBuilder()
      .produceResults("x")
      .subtractionNodeByLabelsScan("x", "X", "Y", IndexOrderNone)
      .build()
  )

  testPlan(
    "partitionedSubtractionNodeByLabelsScan",
    new TestPlanBuilder()
      .produceResults("x")
      .partitionedSubtractionNodeByLabelsScan("x", Seq("X", "Y"), Seq("A", "B"))
      .build()
  )

  testPlan(
    "unionRelationshipTypesScan",
    new TestPlanBuilder()
      .produceResults("r")
      .unionRelationshipTypesScan("(a)-[r:A|B|C]->(b)", IndexOrderNone)
      .build()
  )

  testPlan(
    "partitionedUnionRelationshipTypesScan",
    new TestPlanBuilder()
      .produceResults("r")
      .partitionedUnionRelationshipTypesScan("(a)-[r:A|B|C]->(b)")
      .build()
  )

  testPlan(
    "expandAll",
    new TestPlanBuilder()
      .produceResults("x")
      .expandAll("(x)-[r]->(y)")
      .expandAll("(x)-[r]->(y)")
      .expandAll("(x)<-[r]-(y)")
      .expandAll("(x)-[r]-(y)")
      .expandAll("(x)-[r:R]-(y)")
      .expandAll("(x)-[r:R|L]-(y)")
      .argument()
      .build()
  )

  // Var expand
  testPlan(
    "expand",
    new TestPlanBuilder()
      .produceResults("x")
      .expand("(x)-[r*0..0]->(y)", expandMode = ExpandAll, projectedDir = OUTGOING, pathMode = TraversalPathMode.Walk)
      .expand("(x)<-[r*0..1]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r*2..5]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r:REL*1..2]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r:REL|LER*1..2]-(y)", expandMode = ExpandAll, projectedDir = OUTGOING)
      .expand("(x)-[r*1..2]-(y)", expandMode = ExpandInto, projectedDir = OUTGOING)
      .expand("(x)-[r*1..2]->(y)", expandMode = ExpandAll, projectedDir = INCOMING)
      .expand("(x)-[r*1..2]->(y)", expandMode = ExpandAll, projectedDir = BOTH)
      .expand(
        "(x)-[r*1..2]->(y)",
        expandMode = ExpandAll,
        projectedDir = BOTH,
        nodePredicates = Seq(Predicate("n", "id(n) <> 5"))
      )
      .expand(
        "(x)-[r*1..3]->(y)",
        expandMode = ExpandAll,
        projectedDir = BOTH,
        relationshipPredicates = Seq(Predicate("r", "id(r) <> 5"))
      )
      .expand(
        "(x)-[r*1..2]->(y)",
        expandMode = ExpandAll,
        projectedDir = BOTH,
        nodePredicates = Seq(Predicate("n", "id(n) <> 5"), Predicate("n2", "id(n2) > 5"))
      )
      .expand(
        "(x)-[r*1..3]->(y)",
        expandMode = ExpandAll,
        projectedDir = BOTH,
        relationshipPredicates = Seq(Predicate("r", "id(r) <> 5"), Predicate("r2", "id(r2) > 5"))
      )
      .argument()
      .build()
  )

  testPlan(
    "expandExpr",
    new TestPlanBuilder()
      .produceResults("x")
      .expandExpr(
        "(start)-[r*0..]->(end)",
        expandMode = ExpandAll,
        projectedDir = OUTGOING,
        nodePredicates = Seq(),
        relationshipPredicates = Seq(VariablePredicate(varFor("r"), isNotNull(prop(endNode("r"), "foo")))),
        pathMode = TraversalPathMode.Walk
      )
      .nodeByLabelScan("start", "A", IndexOrderNone)
      .build()
  )

  testPlan(
    "shortestPath",
    new TestPlanBuilder()
      .produceResults("x")
      .shortestPath("(x)<-[r]-(y)")
      .shortestPath("(x)-[r*0..0]->(y)", pathName = Some("path"))
      .shortestPath("(x)<-[r*0..1]-(y)")
      .shortestPath("(x)-[r*2..5]-(y)", all = true)
      .shortestPath("(x)-[r:REL*1..2]-(y)", relationshipPredicates = Seq(Predicate("n", "id(n) <> 5")))
      .shortestPath("(x)-[r:REL|LER*1..2]-(y)")
      .shortestPath("(x)-[r*1..2]-(y)")
      .shortestPath(
        "(x)-[r*1..2]->(y)",
        pathName = Some("path"),
        all = true,
        nodePredicates = Seq(Predicate("n", "id(n) <> 5")),
        relationshipPredicates = Seq(Predicate("rel", "id(rel) <> 7"))
      )
      .shortestPath("(x)-[r*1..2]->(y)", sameNodeMode = SkipSameNode)
      .shortestPath("(x)-[r*1..3]->(y)", withFallback = true)
      .argument()
      .build()
  )

  testPlan(
    "pruningVarExpand",
    new TestPlanBuilder()
      .produceResults("x")
      .pruningVarExpand("(x)-[*0..0]->(y)")
      .pruningVarExpand("(x)<-[*0..1]-(y)")
      .pruningVarExpand("(x)-[*2..5]-(y)")
      .pruningVarExpand("(x)-[:REL*1..2]-(y)")
      .pruningVarExpand("(x)-[:REL|LER*1..2]-(y)")
      .pruningVarExpand("(x)-[*1..2]-(y)")
      .pruningVarExpand("(x)-[*1..2]->(y)")
      .pruningVarExpand("(x)-[*1..2]->(y)")
      .pruningVarExpand("(x)-[*1..2]->(y)", nodePredicates = Seq(Predicate("n", "id(n) <> 5")))
      .pruningVarExpand("(x)-[*1..3]->(y)", relationshipPredicates = Seq(Predicate("r", "id(r) <> 5")))
      .pruningVarExpand(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> 5"), Predicate("n2", "id(n2) > 5"))
      )
      .pruningVarExpand(
        "(x)-[*1..3]->(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> 5"), Predicate("r2", "id(r2) > 5"))
      )
      .argument()
      .build()
  )

  testPlan(
    "bfsPruningVarExpand",
    new TestPlanBuilder()
      .produceResults("x")
      .bfsPruningVarExpand("(x)-[*0..0]->(y)")
      .bfsPruningVarExpand("(x)<-[*0..1]-(y)")
      .bfsPruningVarExpand("(x)-[*1..5]->(y)")
      .bfsPruningVarExpand("(x)-[:REL*1..2]->(y)")
      .bfsPruningVarExpand("(x)<-[:REL|LER*1..2]-(y)")
      .bfsPruningVarExpand("(x)-[*1..2]->(y)")
      .bfsPruningVarExpand("(x)-[*1..2]->(y)")
      .bfsPruningVarExpand("(x)-[*1..2]->(y)", nodePredicates = Seq(Predicate("n", "id(n) <> 5")))
      .bfsPruningVarExpand("(x)-[*1..3]->(y)", relationshipPredicates = Seq(Predicate("r", "id(r) <> 5")))
      .bfsPruningVarExpand(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> 5"), Predicate("n2", "id(n2) > 5"))
      )
      .bfsPruningVarExpand(
        "(x)-[*1..3]->(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> 5"), Predicate("r2", "id(r2) > 5"))
      )
      .bfsPruningVarExpand(
        "(x)-[*1..3]->(y)",
        depthName = Some("depth")
      )
      .argument()
      .build()
  )

  testPlan(
    "bfsPruningVarExpand - walk mode",
    new TestPlanBuilder()
      .produceResults("x")
      .bfsPruningVarExpand("(x)-[*0..0]->(y)", pathMode = TraversalPathMode.Walk)
      .bfsPruningVarExpand("(x)<-[*0..1]-(y)", pathMode = TraversalPathMode.Walk)
      .bfsPruningVarExpand("(x)-[*1..5]->(y)", pathMode = TraversalPathMode.Walk)
      .bfsPruningVarExpand("(x)-[:REL*1..2]->(y)", pathMode = TraversalPathMode.Walk)
      .bfsPruningVarExpand("(x)<-[:REL|LER*1..2]-(y)", pathMode = TraversalPathMode.Walk)
      .bfsPruningVarExpand("(x)-[*1..2]->(y)", pathMode = TraversalPathMode.Walk)
      .bfsPruningVarExpand("(x)-[*1..2]->(y)", pathMode = TraversalPathMode.Walk)
      .bfsPruningVarExpand(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> 5")),
        pathMode = TraversalPathMode.Walk
      )
      .bfsPruningVarExpand(
        "(x)-[*1..3]->(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> 5")),
        pathMode = TraversalPathMode.Walk
      )
      .bfsPruningVarExpand(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> 5"), Predicate("n2", "id(n2) > 5")),
        pathMode = TraversalPathMode.Walk
      )
      .bfsPruningVarExpand(
        "(x)-[*1..3]->(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> 5"), Predicate("r2", "id(r2) > 5")),
        pathMode = TraversalPathMode.Walk
      )
      .bfsPruningVarExpand(
        "(x)-[*1..3]->(y)",
        depthName = Some("depth"),
        pathMode = TraversalPathMode.Walk
      )
      .argument()
      .build()
  )

  testPlan(
    "bfsPruningVarExpandExpr",
    new TestPlanBuilder()
      .produceResults("x")
      .bfsPruningVarExpandExpr("(x)-[*0..0]->(y)")
      .bfsPruningVarExpandExpr("(x)<-[*0..1]-(y)")
      .bfsPruningVarExpandExpr("(x)-[*1..5]->(y)")
      .bfsPruningVarExpandExpr("(x)-[:REL*1..2]->(y)")
      .bfsPruningVarExpandExpr("(x)<-[:REL|LER*1..2]-(y)")
      .bfsPruningVarExpandExpr("(x)-[*1..2]->(y)")
      .bfsPruningVarExpandExpr("(x)-[*1..2]->(y)")
      .bfsPruningVarExpandExpr(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(VariablePredicate(varFor("n"), notEquals(id(varFor("n")), literal(5))))
      )
      .bfsPruningVarExpandExpr(
        "(x)-[*1..3]->(y)",
        relationshipPredicates = Seq(VariablePredicate(varFor("r"), notEquals(id(varFor("r")), literal(5))))
      )
      .bfsPruningVarExpandExpr(
        "(x)-[*1..2]->(y)",
        nodePredicates = Seq(
          VariablePredicate(varFor("n"), notEquals(id(varFor("n")), literal(5))),
          VariablePredicate(varFor("n2"), greaterThan(id(varFor("n2")), literal(5)))
        )
      )
      .bfsPruningVarExpandExpr(
        "(x)-[*1..3]->(y)",
        relationshipPredicates = Seq(
          VariablePredicate(varFor("r"), notEquals(id(varFor("r")), literal(5))),
          VariablePredicate(varFor("r2"), greaterThan(id(varFor("r2")), literal(5)))
        )
      )
      .bfsPruningVarExpandExpr(
        "(x)-[*1..3]->(y)",
        depthName = Some("depth")
      )
      .argument()
      .build()
  )

  testPlan(
    "expandInto",
    new TestPlanBuilder()
      .produceResults("x")
      .expandInto("(x)-[r]->(y)")
      .expandInto("(x)-[r]->(y)")
      .expandInto("(x)<-[r]-(y)")
      .expandInto("(x)-[r]-(y)")
      .expandInto("(x)-[r:REL]-(y)")
      .expandInto("(x)-[r:REL|LER]-(y)")
      .argument()
      .build()
  )

  testPlan(
    "optionalExpandAll",
    new TestPlanBuilder()
      .produceResults("x")
      .optionalExpandAll("(x)-[r]->(y)")
      .optionalExpandAll("(x)-[r]->(y)")
      .optionalExpandAll("(x)-[r:REL]->(y)")
      .optionalExpandAll("(x)-[r:REL|LER]->(y)")
      .optionalExpandAll("(x)-[r]->(y)", Some("y.num > 20"))
      .optionalExpandAll("(x)-[r]->(y)", Some("x:X"))
      .optionalExpandAll("(x)-[r]->(y)", Some("r:R"))
      .optionalExpandAll("(x)-[r]->(y)", Some("a:A"))
      .argument()
      .build()
  )

  testPlan(
    "optionalExpandInto",
    new TestPlanBuilder()
      .produceResults("x")
      .optionalExpandInto("(x)-[r]->(y)")
      .optionalExpandInto("(x)-[r:REL]->(y)")
      .optionalExpandInto("(x)-[r:REL|LER]->(y)")
      .optionalExpandInto("(x)-[r]->(y)", Some("y.num > 20"))
      .argument()
      .build()
  )

  testPlan(
    "limit",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .limit(5)
      .argument()
      .build()
  )

  testPlan(
    "exhaustiveLimit",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .exhaustiveLimit(5)
      .argument()
      .build()
  )

  testPlan(
    "skip",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .skip(5)
      .argument()
      .build()
  )

  testPlan(
    "aggregation",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .aggregation(Seq("x AS x"), Seq("collect(y) AS y"))
      .argument()
      .build()
  )

  testPlan(
    "aggregation with order",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .aggregation(Seq("x AS x"), Seq("collect(y) aSc AS y"))
      .argument()
      .build()
  )

  testPlan(
    "orderedAggregation",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedAggregation(Seq("x AS x"), Seq("collect(y) AS y"), Seq("x"))
      .orderedAggregation(Seq("x AS x", "1 + n.foo AS y"), Seq("collect(y) AS y"), Seq("x", "1 + n.foo"))
      .argument()
      .build()
  )

  testPlan(
    "orderedAggregation with order",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedAggregation(Seq("x AS x"), Seq("collect(y) deSc AS y"), Seq("x"))
      .orderedAggregation(Seq("x AS x", "1 + n.foo AS y"), Seq("collect(y) asC AS y"), Seq("x", "1 + n.foo"))
      .argument()
      .build()
  )

  testPlan(
    "apply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "semiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .semiApply()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "antiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .antiSemiApply()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "letSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "idName")
      .letSemiApply("idName")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "letAntiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "let")
      .letAntiSemiApply("let")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "conditionalApply",
    new TestPlanBuilder()
      .produceResults("x")
      .conditionalApply("x")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "antiConditionalApply",
    new TestPlanBuilder()
      .produceResults("x")
      .antiConditionalApply("x")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "selectOrSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .selectOrSemiApply("false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "selectOrAntiSemiApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .selectOrAntiSemiApply("false")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "rollUpApply",
    new TestPlanBuilder()
      .produceResults("x", "list")
      .rollUpApply("list", "y")
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "foreachApply",
    new TestPlanBuilder()
      .produceResults("x", "list")
      .foreachApply("i", "[1, 2, 3]")
      .|.setNodeProperty("n", "n.prop", "i")
      .|.create(createNode("n"))
      .|.argument("i")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "foreach",
    new TestPlanBuilder()
      .produceResults("x", "list")
      .foreach(
        "i",
        "[1, 2, 3]",
        Seq(
          createPattern(nodes = Seq(createNode("n"))),
          removeLabel("x", "L"),
          removeDynamicLabel("x", "'M'"),
          AbstractLogicalPlanBuilder.delete("x", forced = true),
          setNodeProperty("n", "prop", "i"),
          setDynamicProperty("n", "'foo'", "i")
        )
      )
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "subqueryForeach",
    new TestPlanBuilder()
      .produceResults("x")
      .subqueryForeach()
      .|.emptyResult()
      .|.create(createNode("n"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "merge",
    new TestPlanBuilder()
      .produceResults("x")
      .merge(
        Seq(createNode("x"), createNode("y")),
        Seq(createRelationship("r", "x", "R", "y")),
        Seq(setNodeProperty("x", "prop", "42"), setNodePropertiesFromMap("x", "{prop: 42}")),
        Seq(
          setLabel("x", "L", "M"),
          setDynamicLabel("x", "'N'", "$p"),
          setRelationshipProperty("r", "prop", "42"),
          setRelationshipPropertiesFromMap("r", "{prop: 42}")
        )
      )
      .expand("(x)-[r:R]->(y)")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "merge with multiple properties",
    new TestPlanBuilder()
      .produceResults("x")
      .merge(
        Seq(createNode("x"), createNode("y")),
        Seq(createRelationship("r", "x", "R", "y")),
        Seq(
          setNodeProperties("x", ("prop1", "42"), ("prop2", "hej")),
          setProperties("y", ("prop1", "42"), ("prop2", "hej"))
        ),
        Seq(setRelationshipPropertiesFromMap("r", "{prop1: 42, prop2: 'hej'}"))
      )
      .expand("(x)-[r:R]->(y)")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "merge with lock",
    new TestPlanBuilder()
      .produceResults("x")
      .merge(
        relationships = Seq(createRelationship("r", "x", "R", "y")),
        lockNodes = Set("x", "y")
      )
      .expand("(x)-[r:R]->(y)")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "fusedMerge",
    new TestPlanBuilder()
      .produceResults("x")
      .fusedMerge(
        Seq(createNode("x"), createNode("y")),
        Seq(createRelationship("r", "x", "R", "y")),
        Seq(setNodeProperty("x", "prop", "42"), setNodePropertiesFromMap("x", "{prop: 42}")),
        Seq(
          setLabel("x", "L", "M"),
          setDynamicLabel("x", "'N'", "$p"),
          setRelationshipProperty("r", "prop", "42"),
          setRelationshipPropertiesFromMap("r", "{prop: 42}")
        )
      )
      .expand("(x)-[r:R]->(y)")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "mergeUniqueNode",
    new TestPlanBuilder()
      .produceResults("x")
      .apply()
      .|.mergeUniqueNode(
        "x",
        "L",
        Seq("p1" -> "i", "p2" -> "true"),
        onMatch = Seq("created" -> "false"),
        onCreate = Seq("created" -> "true"),
        args = Set("i"),
        indexType = IndexType.POINT
      )
      .unwind("[1, 2, 3] AS i")
      .argument()
      .build()
  )

  testPlan(
    "mergeUniqueNodeExpression",
    new TestPlanBuilder()
      .produceResults("x")
      .apply()
      .|.mergeUniqueNodeExpression(
        "x",
        "L",
        Seq("p1" -> "i", "p2" -> "true"),
        onMatch = Seq("created" -> literalBoolean(false)),
        onCreate = Seq("created" -> literalBoolean(true)),
        args = Set("i"),
        indexType = IndexType.POINT
      )
      .unwind("[1, 2, 3] AS i")
      .argument()
      .build()
  )

  testPlan(
    "mergeInto",
    new TestPlanBuilder()
      .produceResults("x")
      .mergeInto(
        "(x)-[r:R]->(y)",
        onCreate = Seq("p1" -> "42", "p2" -> "false")
      )
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "mergeIntoExpression",
    new TestPlanBuilder()
      .produceResults("x")
      .mergeIntoExpression(
        "(x)-[r:R]->(y)",
        onCreate = Seq("p1" -> literalInt(42), "p2" -> literalBoolean(false))
      )
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "lockNodes",
    new TestPlanBuilder()
      .produceResults("x")
      .apply()
      .|.merge(
        relationships = Seq(createRelationship("r", "x", "R", "y")),
        lockNodes = Set("x", "y")
      )
      .|.expand("(x)-[r:R]->(y)")
      .|.lockNodes("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "anti",
    new TestPlanBuilder()
      .produceResults("x")
      .anti()
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "optional",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.optional("x")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "cacheProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .cacheProperties("n.prop")
      .argument()
      .build()
  )

  testPlan(
    "remoteBatchProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .remoteBatchProperties("n.prop")
      .argument()
      .build()
  )

  testPlan(
    "remoteBatchPropertiesWithFilter",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .remoteBatchPropertiesWithFilter("n.prop", "m.prop", "r.prop")("n.prop = 5", "r.prop = 5")
      .argument()
      .build()
  )

  testPlan(
    "remoteBatchPropertiesWithFilterExpression",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .remoteBatchPropertiesWithFilter("n.prop", "m.prop", "r.prop")(
        equals(prop("n", "prop"), literalInt(5)),
        equals(prop("r", "prop"), literalInt(5))
      )
      .argument()
      .build()
  )

  testPlan(
    "remoteBatchPropertiesWithPushdownOperatorsOnNode",
    new TestPlanBuilder()
      .produceResults("x")
      .remoteBatchPropertiesWithPushdownOperatorsOnNode("x", "prop1", "prop2")(PushdownOperators()
        .limit("10")
        .orderBy("x.prop3")
        .distinct("x")
        .filter("x.prop1=foo", "x.prop2=anon_0")
        .importedConstantValues("foo")
        .importedPerRowValues(Map("anon_0" -> "cacheN[y.prop]", "perRowVar" -> "perRowVar")))
      .argument("y", "foo")
      .build()
  )

  testPlan(
    "remoteBatchPropertiesWithPushdownOperatorsOnRelationship",
    new TestPlanBuilder()
      .produceResults("x")
      .remoteBatchPropertiesWithPushdownOperatorsOnRelationship("x", "prop1", "prop2")(PushdownOperators()
        .limit("10")
        .orderBy("x.prop3", "x.prop2 ASC", "x.prop1 DESC")
        .distinct("x")
        .filter("x.prop1=foo", "x.prop2=anon_0")
        .importedConstantValues("foo")
        .importedPerRowValues(Map("anon_0" -> "cacheR[y.prop]", "perRowVar" -> "perRowVar")))
      .argument("y", "foo")
      .build()
  )

  testPlan(
    "cartesianProduct",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .cartesianProduct()
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "create",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .create(createNode("a", "A"), createNode("b"))
      .argument()
      .build()
  )

  testPlan(
    "create nodes and relationships",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .create(
        createNode("a", "A"),
        createNodeWithProperties("b", Seq("B"), "{node: true}"),
        createRelationship("r", "a", "R", "b", INCOMING, Some("{rel: true}")),
        createRelationshipExpression("r2", "a", "R", "b", INCOMING, Some(mapOfInt("baz" -> 42))),
        createRelationshipWithDynamicType("r2", "a", "label", "b", INCOMING, Some("{node: true}")),
        createRelationshipFull(
          "r2",
          "a",
          DynamicRelTypeExpression(varFor("label"))(pos),
          "b",
          INCOMING,
          Some(mapOfInt("baz" -> 42))
        )
      )
      .argument()
      .build()
  )

  testPlan(
    "create with properties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .create(
        createNodeWithProperties("a", Seq("A"), "{foo: 42}"),
        createNodeWithProperties("b", Seq.empty, "{bar: 'hello'}"),
        createNodeWithProperties("c", Seq.empty, mapOfInt("baz" -> 42))
      )
      .argument()
      .build()
  )

  testPlan(
    "create with dynamic labels",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .create(
        createNodeWithDynamicLabels("a", literal("L")),
        createNodeWithDynamicLabels("b", varFor("label"), prop(varFor("n"), "label"))
      )
      .argument()
      .build()
  )

  testPlan(
    "procedureCall",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .procedureCall("test.proc2(5) YIELD foo")
      .procedureCall("test.proc1()")
      .argument()
      .build()
  )

  testPlan(
    "projectEndpoints",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .projectEndpoints("(a)-[r]-(b)", startInScope = true, endInScope = true)
      .projectEndpoints("(a)-[r]->(b)", startInScope = false, endInScope = true)
      .projectEndpoints("(a)<-[r]-(b)", startInScope = false, endInScope = true)
      .projectEndpoints("(a)-[r]-(b)", startInScope = true, endInScope = false)
      .projectEndpoints("(a)-[r*1..5]-(b)", startInScope = true, endInScope = false)
      .projectEndpoints("(a)<-[r*1..5]-(b)", startInScope = true, endInScope = false)
      .projectEndpoints("(a)-[r:A|B*1..5]-(b)", startInScope = true, endInScope = false)
      .argument()
      .build()
  )

  testPlan(
    "valueHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .valueHashJoin("x.bar = y.foo")
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "nodeHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nodeHashJoin("x", "y")
      .|.nodeHashJoin("x")
      .|.|.argument()
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "rightOuterHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .rightOuterHashJoin("x", "y")
      .|.rightOuterHashJoin("x")
      .|.|.argument()
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "leftOuterHashJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .leftOuterHashJoin("x", "y")
      .|.leftOuterHashJoin("x")
      .|.|.argument()
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "valueMergeJoin",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .valueMergeJoin("x.bar = y.foo")
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "emptyResult",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .emptyResult()
      .argument()
      .build()
  )

  testPlan(
    "eager",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .eager()
      .eager(ListSet(EagernessReason.UpdateStrategyEager))
      .eager(ListSet(EagernessReason.Unknown))
      .eager(ListSet(
        EagernessReason.LabelReadSetConflict(LabelName("X")(InputPosition.NONE)),
        EagernessReason.TypeReadSetConflict(RelTypeName("X")(InputPosition.NONE)),
        EagernessReason.LabelReadRemoveConflict(LabelName("Bar")(InputPosition.NONE)),
        EagernessReason.ReadDeleteConflict("n"),
        EagernessReason.ReadCreateConflict,
        EagernessReason.PropertyReadSetConflict(PropertyKeyName("Foo")(InputPosition.NONE)),
        EagernessReason.UnknownPropertyReadSetConflict,
        EagernessReason.LabelReadSetConflict(LabelName("X")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(1), Id(2))),
        EagernessReason.TypeReadSetConflict(RelTypeName("X")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(1), Id(2))),
        EagernessReason.LabelReadRemoveConflict(LabelName("Bar")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(1), Id(2))),
        EagernessReason.ReadDeleteConflict("n")
          .withConflict(EagernessReason.Conflict(Id(1), Id(2))),
        EagernessReason.ReadCreateConflict
          .withConflict(EagernessReason.Conflict(Id(1), Id(2))),
        EagernessReason.PropertyReadSetConflict(PropertyKeyName("Foo")(InputPosition.NONE))
          .withConflict(EagernessReason.Conflict(Id(1), Id(2))),
        EagernessReason.UnknownPropertyReadSetConflict
          .withConflict(EagernessReason.Conflict(Id(1), Id(2))),
        EagernessReason.Summarized(Map(
          EagernessReason.ReadDeleteConflict("ident") ->
            EagernessReason.SummaryEntry(EagernessReason.Conflict(Id(7), Id(8)), 123),
          EagernessReason.ReadCreateConflict ->
            EagernessReason.SummaryEntry(EagernessReason.Conflict(Id(9), Id(10)), 321)
        ))
      ))
      .argument()
      .build()
  )

  testPlan(
    "errorPlan",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .errorPlan(TestException())
      .argument()
      .build()
  )

  testPlan(
    "sort",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .sort("x ASC", "y ASC")
      .sort("x DESC")
      .sort("`n.p` ASC")
      .argument()
      .build()
  )

  testPlan(
    "sortColumns",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .sortColumns(Seq(Ascending(varFor("x")), Ascending(varFor("y"))))
      .sortColumns(Seq(Descending(varFor("x"))))
      .argument()
      .build()
  )

  testPlan(
    "sortStrings",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .sort("x ASC", "y ASC")
      .sort("x DESC")
      .argument()
      .build()
  )

  testPlan(
    "partialSort",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .partialSort(
        Seq("x ASC", "y ASC"),
        Seq("xxx ASC", "y DESC")
      )
      .partialSort(Seq("x DESC"), Seq("x ASC", "y ASC"))
      .partialSort(Seq("x DESC"), Seq("x ASC"), 10)
      .argument()
      .build()
  )

  testPlan(
    "partialSortColumns",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .partialSortColumns(
        Seq(Ascending(varFor("x")), Ascending(varFor("y"))),
        Seq(Descending(varFor("a")), Ascending(varFor("f")))
      )
      .argument()
      .build()
  )

  testPlan(
    "top",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .top(Seq(Ascending(varFor("xxx")), Descending(varFor("y"))), 100)
      .top(Seq(Ascending(varFor("x")), Ascending(varFor("y"))), 42)
      .argument()
      .build()
  )

  testPlan(
    "top1WithTies",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .top1WithTies("xxx ASC", "y DESC")
      .top1WithTies("x ASC", "y ASC")
      .argument()
      .build()
  )

  testPlan(
    "top1WithTiesColumns",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .top1WithTiesColumns(Seq(Ascending(varFor("xxx")), Descending(varFor("y"))))
      .top1WithTiesColumns(Seq(Ascending(varFor("x")), Ascending(varFor("y"))))
      .argument()
      .build()
  )

  testPlan(
    "partialTop",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .partialTop(
        Seq(Ascending(varFor("x")), Ascending(varFor("y"))),
        Seq(Ascending(varFor("xxx")), Descending(varFor("y"))),
        100
      )
      .partialTop(Seq(Descending(varFor("x"))), Seq(Ascending(varFor("x")), Ascending(varFor("y"))), 42)
      .partialTop(Seq(Descending(varFor("x"))), Seq(Ascending(varFor("x")), Ascending(varFor("y"))), 42, 17)
      .argument()
      .build()
  )

  testPlan(
    "distinct",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .distinct("x AS y", "1 + n.foo AS z")
      .argument()
      .build()
  )

  testPlan(
    "orderedDistinct",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedDistinct(Seq("x"), "x AS y", "1 + n.foo AS z")
      .orderedDistinct(Seq("1 + n.foo"), "x AS y", "1 + n.foo AS z")
      .argument()
      .build()
  )

  testPlan(
    "projection",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .projection("x AS y", "1 + n.foo AS z")
      .projection("cacheR[r.prop] AS rel")
      .projection("cacheR[r.prop] AS `r.prop`")
      .projection("cacheN[n.prop] AS node")
      .projection("cache[n.prop] AS node")
      .projection("cacheFromStore[n.prop] AS node")
      .projection("cacheNFromStore[n.prop] AS node")
      .projection("cacheRFromStore[r.prop] AS node")
      .argument()
      .build()
  )

  testPlan(
    "unwind",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .unwind("[x, 42, y.prop] AS y")
      .argument()
      .build()
  )

  testPlan(
    "partitionedUnwind",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .partitionedUnwind("[x, 42, y.prop] AS y")
      .argument()
      .build()
  )

  testPlan(
    "union",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .union()
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "orderedUnion",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedUnion("x ASC")
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "orderedUnionColumns",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .orderedUnionColumns(Seq(Ascending(varFor("x"))))
      .|.argument()
      .argument()
      .build()
  )

  testPlan(
    "relationshipCountFromCountStore",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipCountFromCountStore("x", None, Seq("RelType"), None, "a", "b")
      .relationshipCountFromCountStore("x", Some("Start"), Seq("RelType", "FooBar"), Some("End"))
      .build()
  )

  testPlan(
    "nodeCountFromCountStore",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.nodeCountFromCountStore("x", Seq(Some("Label")), "a", "b")
      .apply()
      .|.nodeCountFromCountStore("x", Seq(Some("Label"), None, Some("Babel")), "a")
      .nodeCountFromCountStore("x", Seq())
      .build()
  )

  testPlan(
    "detachDeleteNode",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .detachDeleteNode("x")
      .argument()
      .build()
  )

  testPlan(
    "deleteRelationship",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .deleteRelationship("x")
      .argument()
      .build()
  )

  testPlan(
    "setProperty",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setProperty("x", "prop", "42")
      .setProperty("head([x])", "prop", "42")
      .setProperty(varFor("x"), "prop", varFor("42"))
      .argument()
      .build()
  )

  testPlan(
    "setDynamicProperty",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setDynamicProperty("x", "prop", "42")
      .setDynamicProperty("head([x])", "prop", "42")
      .setDynamicProperty("x", "'prop'", "42")
      .argument()
      .build()
  )

  testPlan(
    "setNodeProperty",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setNodeProperty("x", "prop", "42")
      .setNodeProperty("x", "prop", varFor("42"))
      .argument()
      .build()
  )

  testPlan(
    "setRelationshipProperty",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setRelationshipProperty("x", "prop", "42")
      .setRelationshipProperty("x", "prop", varFor("42"))
      .argument()
      .build()
  )

  testPlan(
    "setProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setProperties("x", ("p1", "42"), ("p1", "42"))
      .argument()
      .build()
  )

  testPlan(
    "setPropertiesExpression",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setProperties(varFor("x"), ("p1", varFor("42")), ("p1", varFor("42")))
      .argument()
      .build()
  )

  testPlan(
    "setNodeProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setNodeProperties("x", ("p1", "42"), ("p1", "42"))
      .argument()
      .build()
  )

  testPlan(
    "setNodePropertiesExpression",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setNodeProperties("x", ("p1", varFor("42")), ("p1", varFor("42")))
      .argument()
      .build()
  )

  testPlan(
    "setRelationshipProperties",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setRelationshipProperties("x", ("p1", "42"), ("p1", "42"))
      .argument()
      .build()
  )

  testPlan(
    "setRelationshipPropertiesExpression",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setRelationshipProperties("x", ("p1", varFor("42")), ("p1", varFor("42")))
      .argument()
      .build()
  )

  testPlan(
    "setPropertiesFromMap",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setPropertiesFromMap("x", "{prop: 42, foo: x.bar}", removeOtherProps = true)
      .setPropertiesFromMap("x", mapOfInt("prop" -> 42), removeOtherProps = true)
      .argument()
      .build()
  )

  testPlan(
    "setNodePropertiesFromMap",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setNodePropertiesFromMap("x", "{prop: 42, foo: x.bar}", removeOtherProps = true)
      .setNodePropertiesFromMap("x", mapOfInt("prop" -> 42), removeOtherProps = true)
      .argument()
      .build()
  )

  testPlan(
    "setRelationshipPropertiesFromMap",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .setRelationshipPropertiesFromMap("x", "{prop: 42, foo: x.bar}", removeOtherProps = false)
      .setRelationshipPropertiesFromMap("x", mapOfInt("prop" -> 42), removeOtherProps = true)
      .argument()
      .build()
  )

  testPlan(
    "filter",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .filter("x.foo > 42", "true <> false")
      .filter("CoerceToPredicate(x.foo)")
      .argument()
      .build()
  )

  testPlan(
    "nonFuseable",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nonFuseable()
      .argument()
      .build()
  )

  testPlan(
    "injectCompilationError",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .injectCompilationError()
      .argument()
      .build()
  )

  testPlan(
    "nonPipelined",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nonPipelined()
      .argument()
      .build()
  )

  testPlan(
    "nonPipelinedStreaming",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .nonPipelinedStreaming()
      .argument()
      .build()
  )

  testPlan(
    "prober",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .prober(Prober.NoopProbe)
      .argument()
      .build()
  )

  testPlan(
    "nodeByIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.nodeByIdSeek("m", Set.empty, "variable")
      .apply()
      .|.nodeByIdSeek("m", Set.empty, "var1", "var2")
      .apply()
      .|.nodeByIdSeek("y", Set("x"), 25)
      .nodeByIdSeek("x", Set(), 23, 22.0, -1)
      .build()
  )

  testPlan(
    "nodeByElementIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.nodeByElementIdSeek("m", Set.empty, "variable")
      .apply()
      .|.nodeByElementIdSeek("m", Set.empty, "var1", "var2")
      .apply()
      .|.nodeByElementIdSeek("y", Set("x"), 25)
      .nodeByElementIdSeek("x", Set(), 23, 22.0, -1)
      .build()
  )

  testPlan(
    "undirectedRelationshipByIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipByIdSeek("(x)-[r2]-(y)", Set("x"), 25)
      .relationshipByIdSeek("(x)-[r1]-(y)", Set(), 23, 22.0, -1)
      .build()
  )

  testPlan(
    "undirectedRelationshipByElementIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipByElementIdSeek("(x)-[r2]-(y)", Set("x"), 25)
      .relationshipByElementIdSeek("(x)-[r1]-(y)", Set(), 23, 22.0, -1)
      .build()
  )

  testPlan(
    "pointDistanceNodeIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.pointDistanceNodeIndexSeek(
        "y",
        "L",
        "prop",
        "{x: 1.0, y: 2.0, crs: 'cartesian'}",
        10,
        argumentIds = Set("x"),
        getValue = GetValue
      )
      .pointDistanceNodeIndexSeek(
        "x",
        "L",
        "prop",
        "{x: 0.0, y: 1.0, crs: 'cartesian'}",
        100,
        indexOrder = IndexOrderDescending
      )
      .build()
  )

  testPlan(
    "cachedPropertyPointDistanceNodeIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.cachedPropertyPointDistanceNodeIndexSeek(
        "y",
        "L",
        "prop",
        cachedNodePropFromStore("x", "prop"),
        literalInt(10),
        argumentIds = Set("x"),
        getValue = GetValue
      )
      .nodeByLabelScan("x", "X")
      .build()
  )

  testPlan(
    "nodeVectorIndexSearch",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.nodeVectorIndexSearch(
        "y",
        Seq("L"),
        Seq("prop", "prop2"),
        "'rhsIndex'",
        "[1, 2, 3]",
        "10",
        score = "score",
        argumentIds = Set("x"),
        getValueFromIndex = Map("prop" -> GetValue, "prop2" -> DoNotGetValue),
        propertyFilter = Some(rangeExpression(gte(5)))
      )
      .apply()
      .|.nodeVectorIndexSearch(
        "y",
        Seq("L"),
        Seq("prop", "prop2"),
        "'rhsIndex'",
        "[1, 2, 3]",
        "10",
        score = "score",
        argumentIds = Set("x"),
        getValueFromIndex = Map("prop" -> GetValue, "prop2" -> DoNotGetValue),
        propertyFilter = Some(single(5))
      )
      .apply()
      .|.nodeVectorIndexSearch(
        "y",
        Seq("L"),
        Seq("prop", "prop2"),
        "'rhsIndex'",
        "[1, 2, 3]",
        "10",
        score = "score",
        argumentIds = Set("x"),
        getValueFromIndex = Map("prop" -> GetValue, "prop2" -> DoNotGetValue),
        propertyFilter = Some(between(gte(5), lt(10)))
      )
      .apply()
      .|.nodeVectorIndexSearch(
        "y",
        Seq("L"),
        Seq("prop"),
        "'rhsIndex'",
        "[1, 2, 3]",
        "10",
        score = "score",
        argumentIds = Set("x"),
        getValueFromIndex = Map("prop" -> GetValue)
      )
      .nodeVectorIndexSearch(
        "x",
        Seq("L"),
        Seq("prop"),
        "'lhsIndex'",
        "[1, 2, 3]",
        "10",
        getValueFromIndex = Map("prop" -> GetValue)
      )
      .build()
  )

  testPlan(
    "relationshipVectorIndexSearch",
    new TestPlanBuilder()
      .produceResults("r1", "r2")
      .apply()
      .|.relationshipVectorIndexSearch(
        "(x1)-[r1]->()",
        Seq("L"),
        Seq("prop"),
        "'rhsIndex'",
        "[1, 2, 3]",
        "10",
        score = "score",
        argumentIds = Set("x1", "r1", "y1"),
        propertyFilter = Some(rangeExpression(gte(5)))
      )
      .apply()
      .|.relationshipVectorIndexSearch(
        "(x1)-[r1]->()",
        Seq("L"),
        Seq("prop"),
        "'rhsIndex'",
        "[1, 2, 3]",
        "10",
        score = "score",
        argumentIds = Set("x1", "r1", "y1")
      )
      .relationshipVectorIndexSearch(
        "(x1)-[r1]-(y1)",
        Seq("R1", "R2"),
        Seq("prop", "prop2"),
        "'lhsIndex'",
        "[1, 2, 3]",
        "10"
      )
      .build()
  )

  testPlan(
    "nodeFulltextIndexSearch",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.nodeFulltextIndexSearch(
        "y",
        Seq("L"),
        Seq("prop", "prop2"),
        "'rhsIndex'",
        "'hello'",
        limit = "10",
        analyzer = Some("'standard'"),
        skip = Some("5"),
        score = "score",
        argumentIds = Set("x"),
        getValueFromIndex = Map("prop" -> GetValue, "prop2" -> DoNotGetValue)
      )
      .nodeFulltextIndexSearch(
        "x",
        Seq("L"),
        Seq("prop"),
        "'lhsIndex'",
        "'world'",
        getValueFromIndex = Map("prop" -> GetValue)
      )
      .build()
  )

  testPlan(
    "relationshipFulltextIndexSearch",
    new TestPlanBuilder()
      .produceResults("r1", "r2")
      .apply()
      .|.relationshipFulltextIndexSearch(
        "(x1)-[r1]->()",
        Seq("L"),
        Seq("prop"),
        "'rhsIndex'",
        "'hello'",
        limit = "10",
        analyzer = Some("'standard'"),
        skip = Some("5"),
        score = "score",
        argumentIds = Set("x1", "r1", "y1")
      )
      .relationshipFulltextIndexSearch(
        "(x1)-[r1]-(y1)",
        Seq("R1", "R2"),
        Seq("prop", "prop2"),
        "'lhsIndex'",
        "'world'"
      )
      .build()
  )

  testPlan(
    "pointBoundingBoxNodeIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.pointBoundingBoxNodeIndexSeek(
        "y",
        "L",
        "prop",
        "{x: 1.0, y: 2.0, crs: 'cartesian'}",
        "{x: 10.0, y: 20.0, crs: 'cartesian'}",
        argumentIds = Set("x"),
        getValue = GetValue
      )
      .pointBoundingBoxNodeIndexSeek(
        "x",
        "L",
        "prop",
        "{x: 0.0, y: 1.0, crs: 'cartesian'}",
        "{x: 100.0, y: 100.0, crs: 'cartesian'}",
        indexOrder = IndexOrderDescending
      )
      .build()
  )

  testPlan(
    "pointDistanceRelationshipIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.pointDistanceRelationshipIndexSeek(
        "(x)-[r:OTHER_REL(prop2)]->(y)",
        "{x: 1.0, y: 2.0, crs: 'cartesian'}",
        10,
        argumentIds = Set("r1"),
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .pointDistanceRelationshipIndexSeek(
        "(a)-[r1:REL(prop1)]-(b)",
        "{x: 0.0, y: 1.0, crs: 'cartesian'}",
        100,
        indexOrder = IndexOrderDescending,
        inclusive = true
      )
      .build()
  )

  testPlan(
    "pointBoundingBoxRelationshipIndexSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.pointBoundingBoxRelationshipIndexSeek(
        "(y1)-[y:L(prop)]->(y2)",
        "{x: 1.0, y: 2.0, crs: 'cartesian'}",
        "{x: 10.0, y: 20.0, crs: 'cartesian'}",
        argumentIds = Set("x"),
        getValue = GetValue
      )
      .pointBoundingBoxRelationshipIndexSeek(
        "(x1)-[x:L(prop)]-(y1)",
        "{x: 0.0, y: 1.0, crs: 'cartesian'}",
        "{x: 100.0, y: 100.0, crs: 'cartesian'}",
        indexOrder = IndexOrderDescending
      )
      .build()
  )

  testPlan(
    "relationshipByIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipByIdSeek("(x2)-[r2]->(y)", Set("x"), 25)
      .relationshipByIdSeek("(x)-[r1]->(y)", Set(), 23, 22.0, -1)
      .build()
  )

  testPlan(
    "relationshipByElementIdSeek",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipByElementIdSeek("(x)-[r2]->(y)", Set("x"), 25)
      .relationshipByElementIdSeek("(x)-[r1]->(y)", Set(), 23, 22.0, -1)
      .build()
  )

  testPlan(
    "allRelationshipsScan",
    new TestPlanBuilder()
      .produceResults("x1", "y1")
      .apply()
      .|.allRelationshipsScan("(x2)-[r2]-(y2)", "x1", "r1", "y1")
      .allRelationshipsScan("(x1)-[r1]->(y1)")
      .build()
  )

  testPlan(
    "allRelationshipsScan without end-nodes",
    new TestPlanBuilder()
      .produceResults("r1", "r21")
      .apply()
      .|.allRelationshipsScan("()-[r2]-()", "x1", "r1", "y1")
      .allRelationshipsScan("(x1)-[r1]->()")
      .build()
  )

  testPlan(
    "allRelationshipsScan without relationships",
    new TestPlanBuilder()
      .produceResults("y")
      .apply()
      .|.allRelationshipsScan("()-[]-()", "x1", "r", "y")
      .allRelationshipsScan("()-[]->(y)")
      .build()
  )

  testPlan(
    "partitionedAllRelationshipsScan",
    new TestPlanBuilder()
      .produceResults("x1", "y1")
      .apply()
      .|.partitionedAllRelationshipsScan("(x2)-[r2]-(y2)", "x1", "r1", "y1")
      .partitionedAllRelationshipsScan("(x1)-[r1]->(y1)")
      .build()
  )

  testPlan(
    "relationshipTypeScan",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipTypeScan("(x)-[r:R]-(y)")
      .relationshipTypeScan("(x)-[r:R]->(y)")
      .build()
  )

  testPlan(
    "dynamicRelationshipTypeLookup",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.dynamicRelationshipTypeLookup("(x)-[r]-(y)", "$all('R')", IndexOrderAscending, argumentIds = Set("x", "y"))
      .dynamicRelationshipTypeLookup("(x)-[r]->(y)", "$any('R')", propertyPredicates = Map("prop" -> "123"))
      .build()
  )

  testPlan(
    "partitionedRelationshipTypeScan",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.partitionedRelationshipTypeScan("(x)-[r:R]-(y)")
      .partitionedRelationshipTypeScan("(x)-[r:R]->(y)")
      .build()
  )

  testPlan(
    "relationshipTypeScan with providedOrder",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .apply()
      .|.relationshipTypeScan("(x)-[r:R]-(y)", IndexOrderDescending)
      .relationshipTypeScan("(x)-[r:R]->(y)", IndexOrderAscending)
      .build()
  )

  // Formatting paramExpr for complex expressions and customQueryExpression is currently not supported.
  // These cases will need manual fixup.
  testPlan(
    "nodeIndexOperator", {
      val builder = new TestPlanBuilder().produceResults("x", "y")

      // NodeIndexSeek
      builder
        .apply()
        .|.nodeIndexOperator("x:Honey(prop = 20)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(prop = 20 OR 30)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(prop > 20)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 < prop < 20)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 < prop <= 20)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 <= prop < 20)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 <= prop <= 20)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(prop >= 20)", indexOrder = IndexOrderNone, indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(prop < 20)", getValue = _ => DoNotGetValue, indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(prop <= 20)", getValue = Map("prop" -> GetValue), indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(prop = 10, prop2 = '20')",
          indexOrder = IndexOrderDescending,
          getValue = Map("prop" -> GetValue, "prop2" -> DoNotGetValue),
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Label(text STARTS WITH 'as')",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.TEXT
        )
        .apply()
        .|.nodeIndexOperator("x:Honey(prop2 = 10, prop)", indexType = IndexType.RANGE)
        .nodeIndexOperator("x:Honey(prop = variable)", argumentIds = Set("variable"), indexType = IndexType.RANGE)
        .build()
    }
  )

  testPlan(
    "remoteNodeIndexOperator", {
      val builder = new TestPlanBuilder().produceResults("x", "y")

      builder
        .apply()
        .|.remoteNodeIndexOperator("x:Honey(prop = 20)", indexType = IndexType.RANGE)
        .remoteNodeIndexOperator("x:Honey(prop = variable)", argumentIds = Set("variable"), indexType = IndexType.RANGE)
        .build()
    }
  )

  testPlan(
    "partitionedNodeIndexOperator", {
      val builder = new TestPlanBuilder().produceResults("x", "y")

      // NodeIndexSeek
      builder
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(prop = 20)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(prop = 20 OR 30)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(prop > 20)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(10 < prop < 20)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(10 < prop <= 20)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(10 <= prop < 20)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(10 <= prop <= 20)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(prop >= 20)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedNodeIndexOperator(
          "x:Honey(prop < 20)",
          getValue = _ => DoNotGetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.partitionedNodeIndexOperator(
          "x:Honey(prop <= 20)",
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.RANGE
        )
        .apply()
        .|.partitionedNodeIndexOperator(
          "x:Honey(prop = 10, prop2 = '20')",
          getValue = Map("prop" -> GetValue, "prop2" -> DoNotGetValue),
          indexType = IndexType.RANGE
        )
        .apply()
        .|.partitionedNodeIndexOperator(
          "x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .apply()
        .|.partitionedNodeIndexOperator(
          "x:Label(text STARTS WITH 'as')",
          indexType = IndexType.TEXT
        )
        .apply()
        .|.partitionedNodeIndexOperator("x:Honey(prop2 = 10, prop)", indexType = IndexType.RANGE)
        .partitionedNodeIndexOperator(
          "x:Honey(prop = variable)",
          argumentIds = Set("variable"),
          indexType = IndexType.RANGE
        )
        .build()
    }
  )

  // Split into 2 tests to avoid StackOverflow exceptions on too long method call chains.
  testPlan(
    "nodeIndexOperator 2", {
      val builder = new TestPlanBuilder().produceResults("r")

      // NodeUniqueIndexSeek
      builder
        .apply()
        .|.nodeIndexOperator("x:Honey(prop = 20)", unique = true, indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(prop = 20 OR 30)", unique = true, indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(prop > 20)", unique = true, indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(prop >= 20)",
          indexOrder = IndexOrderNone,
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(prop < 20)",
          getValue = _ => DoNotGetValue,
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(prop <= 20)",
          getValue = _ => GetValue,
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Comb(poo = ???)",
          paramExpr = Some(parameter("parameter", CTString)),
          getValue = _ => GetValue,
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(prop = 10, prop2 = '20')",
          indexOrder = IndexOrderDescending,
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(prop = 10 OR 20, prop2 = '10' OR '30')",
          argumentIds = Set("a", "b"),
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Label(text STARTS WITH 'as')",
          indexOrder = IndexOrderAscending,
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Label(text STARTS WITH ???)",
          indexOrder = IndexOrderAscending,
          paramExpr = Some(parameter("parameter", CTString)),
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Label(text ENDS WITH ???)",
          indexOrder = IndexOrderAscending,
          paramExpr = Some(parameter("parameter", CTString)),
          unique = true,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.nodeIndexOperator(
          "x:Label(prop < ???)",
          indexOrder = IndexOrderAscending,
          paramExpr = Some(parameter("parameter", CTString)),
          unique = true,
          indexType = IndexType.RANGE
        )
        .nodeIndexOperator(
          "x:Label(??? < prop >= ???)",
          indexOrder = IndexOrderAscending,
          paramExpr = Seq(parameter("exclusiveBound", CTInteger), parameter("inclusiveBound", CTInteger)),
          unique = true,
          indexType = IndexType.RANGE
        )
        .build()
    }
  )

  testPlan(
    "nodeIndexOperator - multiple range bounds of different types", {
      val builder = new TestPlanBuilder().produceResults("r")

      builder
        .apply()
        .|.nodeIndexOperator("x:Honey(10 < prop > 'foo')", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 <= prop > 'foo')", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 < prop >= 'foo')", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey('foo' <= prop >= 20)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 > prop < 'foo')", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 >= prop < 'foo')", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(10 > prop <= 'foo')", indexType = IndexType.RANGE)
        .nodeIndexOperator("x:Honey('foo' >= prop <= 20)", indexType = IndexType.RANGE)
        .build()
    }
  )

  testPlan(
    "nodeIndexOperator - scans", {
      val builder = new TestPlanBuilder().produceResults("r")

      // NodeIndexScan
      builder
        .apply()
        .|.nodeIndexOperator("x:Honey(calories)", indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(calories, taste)", getValue = _ => GetValue, indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(calories, taste)", indexOrder = IndexOrderDescending, indexType = IndexType.RANGE)
        .apply()
        .|.nodeIndexOperator("x:Honey(calories, taste)", argumentIds = Set("a", "b"), indexType = IndexType.RANGE)

      // NodeIndexContainsScan
      builder
        .apply()
        .|.nodeIndexOperator("x:Label(text CONTAINS 'as')", indexType = IndexType.TEXT)
        .apply()
        .|.nodeIndexOperator("x:Honey(text CONTAINS 'as')", getValue = _ => GetValue, indexType = IndexType.TEXT)
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(text CONTAINS 'as')",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.TEXT
        )
        .apply()
        .|.nodeIndexOperator("x:Honey(text CONTAINS 'as')", argumentIds = Set("a", "b"), indexType = IndexType.TEXT)

      // NodeIndexEndsWithScan
      builder
        .apply()
        .|.nodeIndexOperator("x:Label(text ENDS WITH 'as')", indexType = IndexType.TEXT)
        .apply()
        .|.nodeIndexOperator("x:Honey(text ENDS WITH 'as')", getValue = _ => GetValue, indexType = IndexType.TEXT)
        .apply()
        .|.nodeIndexOperator(
          "x:Honey(text ENDS WITH 'as')",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.TEXT
        )
        .nodeIndexOperator("x:Honey(text ENDS WITH 'as')", argumentIds = Set("a", "b"), indexType = IndexType.TEXT)

      builder.build()
    }
  )

  // Formatting paramExpr for complex expressions and customQueryExpression is currently not supported.
  // These cases will need manual fixup.
  testPlan(
    "relationshipIndexOperator", {
      val builder = new TestPlanBuilder().produceResults("r")

      // directed RelationshipIndexSeek
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20 OR 30)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop > 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop < 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop <= 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop < 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop <= 20)->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop >= 20)]->(y)",
          indexOrder = IndexOrderNone,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop < 20)->(y)",
          getValue = _ => DoNotGetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop <= 20)->(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10, prop2 = '20')->(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10 OR 20, prop2 = '10' OR '30')->(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = ???, prop2 = '10' OR '30')->(y)",
          paramExpr = Some(parameter("parameter", CTInteger)),
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .relationshipIndexOperator(
          "(x)-[r:Label(text STARTS WITH 'as')->(y)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .build()
    }
  )

  // Split into 2 tests to avoid StackOverflow exceptions on too long method call chains.
  testPlan(
    "relationshipIndexOperator 2", {
      val builder = new TestPlanBuilder().produceResults("r")

      // undirected RelationshipIndexSeek
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20 OR 30)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop > 20)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop < 20)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop <= 20)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop < 20)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop <= 20)-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop >= 20)]-(y)",
          indexOrder = IndexOrderNone,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop < 20)-(y)",
          getValue = _ => DoNotGetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop <= 20)-(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10, prop2 = '20')-(y)",
          indexOrder = IndexOrderDescending,
          getValue = { case "prop" => GetValue; case "prop2" => DoNotGetValue },
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10 OR 20, prop2 = '10' OR '30')-(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .relationshipIndexOperator(
          "(x)-[r:Label(text STARTS WITH 'as')-(y)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .build()
    }
  )

  testPlan(
    "relationshipIndexOperator - scan", {
      val builder = new TestPlanBuilder().produceResults("r")

      // directed indexScan
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(calories)->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(calories, taste)->(y)",
          getValue = { case "calories" => GetValue; case _ => DoNotGetValue },
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(calories, taste)->(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(calories, taste)->(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )

      // undirected indexScan
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(calories)-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(calories, taste)-(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(calories, taste)-(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE
        )
        .relationshipIndexOperator(
          "(x)-[r:Honey(calories, taste)-(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .build()
    }
  )

  testPlan(
    "relationshipIndexOperator - contains", {
      val builder = new TestPlanBuilder().produceResults("r")

      // directed contains scan
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Label(text CONTAINS 'as')->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text CONTAINS 'as')->(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text CONTAINS 'as')->(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text CONTAINS 'as')->(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )

      // undirected contains scan
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Label(text CONTAINS 'as')-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text CONTAINS 'as')-(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text CONTAINS 'as')-(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE
        )
        .relationshipIndexOperator(
          "(x)-[r:Honey(text CONTAINS 'as')-(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .build()
    }
  )

  testPlan(
    "relationshipIndexOperator - ends with", {
      val builder = new TestPlanBuilder().produceResults("r")

      // directed ends with scan
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Label(text ENDS WITH 'as')->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text ENDS WITH 'as')->(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text ENDS WITH 'as')->(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text ENDS WITH 'as')->(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )

      // undirected ends with scan
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Label(text ENDS WITH 'as')-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text ENDS WITH 'as')-(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(text ENDS WITH 'as')-(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE
        )
        .relationshipIndexOperator(
          "(x)-[r:Honey(text ENDS WITH 'as')-(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )

      builder.build()
    }
  )

  testPlan(
    "relationshipIndexOperator - unique", {
      val builder = new TestPlanBuilder().produceResults("r")

      // DirectedRelationshipUniqueIndexSeek
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20)]->(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20 OR 30)]->(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop > 20)]->(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop < 20)]->(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop <= 20)]->(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop < 20)]->(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop <= 20)->(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop >= 20)]->(y)",
          indexOrder = IndexOrderNone,
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop < 20)->(y)",
          getValue = _ => DoNotGetValue,
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop <= 20)->(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10, prop2 = '20')->(y)",
          indexOrder = IndexOrderDescending,
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10 OR 20, prop2 = '10' OR '30')->(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE,
          unique = true
        )
        .relationshipIndexOperator(
          "(x)-[r:Label(text STARTS WITH 'as')->(y)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE,
          unique = true
        )
        .build()
    }
  )

  testPlan(
    "relationshipIndexOperator - unique 2", {
      val builder = new TestPlanBuilder().produceResults("r")

      // UndirectedRelationshipUniqueIndexSeek
      builder
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20)]-(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop = 20 OR 30)]-(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(prop > 20)]-(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop < 20)]-(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 < prop <= 20)]-(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop < 20)]-(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator("(x)-[r:Honey(10 <= prop <= 20)-(y)", indexType = IndexType.RANGE, unique = true)
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop >= 20)]-(y)",
          indexOrder = IndexOrderNone,
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop < 20)-(y)",
          getValue = _ => DoNotGetValue,
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop <= 20)-(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10, prop2 = '20')-(y)",
          indexOrder = IndexOrderDescending,
          getValue = {
            case "prop"  => GetValue;
            case "prop2" => DoNotGetValue
          },
          indexType = IndexType.RANGE,
          unique = true
        )
        .apply()
        .|.relationshipIndexOperator(
          "(x)-[r:Honey(prop = 10 OR 20, prop2 = '10' OR '30')-(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE,
          unique = true
        )
        .relationshipIndexOperator(
          "(x)-[r:Label(text STARTS WITH 'as')-(y)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE,
          unique = true
        )
        .build()
    }
  )

  testPlan(
    "partitionedRelationshipIndexOperator", {
      val builder = new TestPlanBuilder().produceResults("r")

      builder.apply()
        .|.partitionedRelationshipIndexOperator("(x)-[r:Honey(prop = 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedRelationshipIndexOperator("(x)-[r:Honey(prop = 20 OR 30)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedRelationshipIndexOperator("(x)-[r:Honey(prop > 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedRelationshipIndexOperator("(x)-[r:Honey(10 < prop < 20)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedRelationshipIndexOperator("(x)-[r:Honey(10 < prop <= 20)]->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedRelationshipIndexOperator("(x)-[r:Honey(10 <= prop < 20)]-(y)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedRelationshipIndexOperator("(x)-[r:Honey(10 <= prop <= 20)->(y)", indexType = IndexType.RANGE)
        .apply()
        .|.partitionedRelationshipIndexOperator(
          "(x)-[r:Honey(prop >= 20)]-(y)",
          indexType = IndexType.RANGE
        ).apply()
        .|.partitionedRelationshipIndexOperator(
          "(x)-[r:Honey(prop < 20)->(y)",
          getValue = _ => DoNotGetValue,
          indexType = IndexType.RANGE
        ).apply()
        .|.partitionedRelationshipIndexOperator(
          "(x)-[r:Honey(prop <= 20)->(y)",
          getValue = _ => GetValue,
          indexType = IndexType.RANGE
        ).apply()
        .|.partitionedRelationshipIndexOperator(
          "(x)-[r:Honey(prop = 10, prop2 = '20')->(y)",
          indexType = IndexType.RANGE
        ).apply()
        .|.partitionedRelationshipIndexOperator(
          "(x)-[r:Honey(prop = 10 OR 20, prop2 = '10' OR '30')->(y)",
          argumentIds = Set("a", "b"),
          indexType = IndexType.RANGE
        )
        .partitionedRelationshipIndexOperator(
          "(x)-[r:Label(text STARTS WITH 'as')->(y)",
          indexType = IndexType.RANGE
        )
        .build()
    }
  )

  testPlan(
    "multiNodeIndexSeekOperator",
    new TestPlanBuilder()
      .produceResults("n", "m")
      .multiNodeIndexSeekOperator(
        _.nodeIndexSeek("n:Label(prop=5)", indexType = IndexType.RANGE),
        _.nodeIndexSeek("m:Label(prop=6)", indexType = IndexType.RANGE)
      )
      .build()
  )

  testPlan(
    "triadicSelection",
    new TestPlanBuilder()
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.expandAll("(y)-[r2]->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-[r1]->(y)")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "triadicBuild",
    new TestPlanBuilder()
      .produceResults("n")
      .triadicFilter(42, positivePredicate = true, "n", "n")
      .triadicBuild(42, "n", "n")
      .allNodeScan("n")
      .build()
  )

  testPlan(
    "triadicFilter",
    new TestPlanBuilder()
      .produceResults("n")
      .triadicFilter(42, positivePredicate = true, "n", "n")
      .triadicBuild(42, "n", "n")
      .allNodeScan("n")
      .build()
  )

  testPlan(
    "injectValue",
    new TestPlanBuilder()
      .produceResults("x")
      .injectValue("x", "null")
      .argument()
      .build()
  )

  testPlan(
    "assertSameNode",
    new TestPlanBuilder()
      .produceResults("x")
      .assertSameNode("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "assertSameRelationship",
    new TestPlanBuilder()
      .produceResults("x")
      .assertSameRelationship("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "deleteNode",
    new TestPlanBuilder()
      .produceResults("n")
      .deleteNode("n")
      .allNodeScan("n")
      .build()
  )

  testPlan(
    "deletePath",
    new TestPlanBuilder()
      .produceResults("p")
      .deletePath("p")
      .argument("p")
      .build()
  )

  testPlan(
    "detachDeletePath",
    new TestPlanBuilder()
      .produceResults("p")
      .deletePath("p")
      .argument("p")
      .build()
  )

  testPlan(
    "deleteExpression",
    new TestPlanBuilder()
      .produceResults("e")
      .deletePath("e")
      .argument("e")
      .build()
  )

  testPlan(
    "detachDeleteExpression",
    new TestPlanBuilder()
      .produceResults("e")
      .deletePath("e")
      .argument("e")
      .build()
  )

  testPlan(
    "removeLabels",
    new TestPlanBuilder()
      .produceResults("n")
      .removeLabels("n", "Label")
      .argument("n")
      .build()
  )

  testPlan(
    "removeDynamicLabels",
    new TestPlanBuilder()
      .produceResults("n")
      .removeDynamicLabels("n", "'Label'")
      .argument("n")
      .build()
  )

  testPlan(
    "removeDynamicLabelsWithExpressions",
    new TestPlanBuilder()
      .produceResults("n")
      .removeDynamicLabelsWithExpressions("n", Set(literalString("OtherLabel")))
      .argument("n")
      .build()
  )

  testPlan(
    "removeLabels dynamic and static",
    new TestPlanBuilder()
      .produceResults("n")
      .removeLabels("n", Seq("Label1"), Seq("'Label2'"))
      .argument("n")
      .build()
  )

  testPlan(
    "setLabels",
    new TestPlanBuilder()
      .produceResults("n")
      .setLabels("n", "Label", "OtherLabel")
      .argument("n")
      .build()
  )

  testPlan(
    "setDynamicLabels",
    new TestPlanBuilder()
      .produceResults("n")
      .setDynamicLabels("n", "'Label'", "'OtherLabel'")
      .argument("n")
      .build()
  )

  testPlan(
    "setLabels dynamic and static",
    new TestPlanBuilder()
      .produceResults("n")
      .setLabels("n", Seq("Label"), Seq("'OtherLabel'"))
      .argument("n")
      .build()
  )

  testPlan(
    "setDynamicLabelsWithExpression",
    new TestPlanBuilder()
      .produceResults("n")
      .setDynamicLabels("n", literalString("OtherLabel"))
      .argument("n")
      .build()
  )

  testPlan(
    "loadCSV",
    new TestPlanBuilder()
      .produceResults("x", "var")
      .loadCSV("url", "var", NoHeaders, Some("-"))
      .loadCSV("url", "var", HasHeaders, None)
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeach",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeach()
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeach with batchSize",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeach(10)
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeach with disjointBy",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionForeach(effectiveDisjointBy = Seq("x"))
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeach with OnErrorContinue",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeach(onErrorBehaviour = OnErrorContinue)
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeach with OnErrorFail",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeach(onErrorBehaviour = OnErrorFail)
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeach with OnErrorBreak",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeach(onErrorBehaviour = OnErrorBreak)
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeachWithRetry",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionForeachWithRetry(
        onErrorBehaviour = OnErrorRetryThenContinue,
        maybeRetryTimeout = Some(FiniteDuration(250, MILLISECONDS))
      )
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionApplyWithRetry",
    new TestPlanBuilder()
      .produceResults("x")
      .transactionApplyWithRetry(
        onErrorBehaviour = OnErrorRetryThenBreak,
        maybeRetryTimeout = Some(FiniteDuration(1500, MILLISECONDS))
      )
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionApply",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionApply with batchSize",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply(42)
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionApply with disjointBy",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply(effectiveDisjointBy = Seq("x"))
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionApply with DISJOINT BY AUTO",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply(maybeDisjointByParameters = Some("auto"))
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionApply with DISJOINT BY NONE",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply(maybeDisjointByParameters = Some("none"))
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionApply with DISJOINT BY expressions",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionApply(maybeDisjointByParameters = Some("(a.id, b.id)"))
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "transactionForeach with DISJOINT BY expressions",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .transactionForeach(maybeDisjointByParameters = Some("(a.id, b.id)"))
      .|.emptyResult()
      .|.create(createNode("y"))
      .|.argument("x")
      .allNodeScan("x")
      .build()
  )

  testPlan(
    "argumentTracker",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .argumentTracker()
      .argument()
      .build()
  )

  testPlan(
    "repeatTrail",
    new TestPlanBuilder()
      .produceResults("me", "you", "a", "b", "r")
      .repeatTrail(TrailParameters(
        min = 0,
        max = Limited(2),
        start = "me",
        end = "you",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
        groupRelationships = Set(("r_inner", "r")),
        innerRelationships = Set("r_inner"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set("r_group"),
        reverseGroupVariableProjections = true,
        expansionMode = ExpandAll,
        accumulators = Set(("1", "b", "c"))
      ))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()
  )

  testPlan(
    "bidirectionalRepeatTrail",
    new TestPlanBuilder()
      .produceResults("me", "you", "a", "b", "r")
      .bidirectionalRepeatTrail(TrailParameters(
        min = 0,
        max = Limited(2),
        start = "me",
        end = "you",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
        groupRelationships = Set(("r_inner", "r")),
        innerRelationships = Set("r_inner"),
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set("r_group"),
        reverseGroupVariableProjections = true,
        expansionMode = ExpandAll,
        accumulators = Set.empty
      ))
      .|.repeatOptions()
      .|.|.expandAll("(b_inner)<-[r_inner]-(a_inner)")
      .|.|.argument("you", "b_inner")
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .cartesianProduct()
      .|.nodeByLabelScan("you", "END", IndexOrderNone)
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()
  )

  testPlan(
    "repeatWalk",
    new TestPlanBuilder()
      .produceResults("me", "you", "a", "b", "r")
      .repeatWalk(WalkParameters(
        min = 0,
        max = Limited(2),
        start = "me",
        end = "you",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
        groupRelationships = Set(("r_inner", "r")),
        reverseGroupVariableProjections = true,
        innerRelationships = Set("r_inner"),
        expansionMode = ExpandAll,
        accumulators = Set(("a", "b", "c"))
      ))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()
  )

  testPlan(
    "repeatAcyclic",
    new TestPlanBuilder()
      .produceResults("me", "you", "a", "b", "r")
      .repeatAcyclic(AcyclicParameters(
        min = 0,
        max = Limited(2),
        start = "me",
        end = "you",
        innerStart = "a",
        innerEnd = "b",
        groupNodes = Set(("a_inner", "a"), ("b_inner", "b")),
        innerNodes = Set("a_inner", "b_inner"),
        previouslyBoundNodes = Set(),
        previouslyBoundNodeGroups = Set(),
        groupRelationships = Set(("r_inner", "r")),
        innerRelationships = Set("r_inner"),
        previouslyBoundRelationships = Set(),
        previouslyBoundRelationshipGroups = Set(),
        reverseGroupVariableProjections = true,
        expansionMode = ExpandAll,
        accumulators = Set(("a", "b", "c"))
      ))
      .|.expandAll("(a_inner)-[r_inner]->(b_inner)")
      .|.argument("me", "a_inner")
      .nodeByLabelScan("me", "START", IndexOrderNone)
      .build()
  )

  testPlan(
    "simulatedNodeScan",
    new TestPlanBuilder()
      .produceResults("x")
      .simulatedNodeScan("x", 1000)
      .build()
  )

  testPlan(
    "simulatedExpand",
    new TestPlanBuilder()
      .produceResults("x")
      .simulatedExpand("x", "r", "y", 1.0)
      .argument()
      .build()
  )

  testPlan(
    "simulatedFilter",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .simulatedFilter(1.0)
      .argument()
      .build()
  )

  testPlan(
    "runQueryAt",
    new TestPlanBuilder()
      .produceResults("article")
      .apply()
      .|.runQueryAt(
        query =
          """WITH $`i` AS `i`
            |MATCH (`article`)
            |  WHERE (((`article`).`id`) IN ([$`articleID`])) AND (((`article`).`version`) IN ([`i`])) AND ((`article`):`Article`)
            |RETURN `article` AS `article`""".stripMargin,
        graphReference = "db.articles",
        parameters = Set("$articleID"),
        importsAsParameters = Map("$i" -> "i"),
        columns = Set("article")
      )
      .|.argument("i")
      .projection("1 AS i")
      .argument()
      .build()
  )

  testPlan(
    "planIf",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .planIf(true)(_.argument())
      .build()
  )

  testPlan(
    "planAny",
    new TestPlanBuilder()
      .produceResults("x", "y")
      .planAny(_.argument())
      .build()
  )

  testPlan(
    "pipelineBreaker",
    new TestPlanBuilder()
      .produceResults()
      .pipelineBreaker()
      .argument()
      .build()
  )

  private def imports: String = {
    """import org.neo4j.cypher.internal.util.collection.immutable.ListSet
      |import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.cachedNodePropFromStore
      |import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.labelName
      |import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.literalInt
      |import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.literalFloat
      |import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.parameter
      |import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.propName
      |import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.relTypeName
      |import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
      |import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
      |import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
      |import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
      |import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
      |import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.between
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.gt
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.gte
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.lte
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.lt
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.matchAll
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.matchEntities
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.rangeExpression
      |import org.neo4j.cypher.internal.compiler.helpers.QueryExpressionConstructionTestSupport.single
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createPattern
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipWithDynamicType
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.delete
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setLabel
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setDynamicProperty
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodePropertiesFromMap
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperty
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipPropertiesFromMap
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperty
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setProperties
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperties
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setRelationshipProperties
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setPropertyFromMap
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.removeLabel
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.AcyclicParameters
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.WalkParameters
      |import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.PushdownOperators
      |import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
      |import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
      |import org.neo4j.cypher.internal.expressions.SemanticDirection.{INCOMING, OUTGOING, BOTH}
      |import org.neo4j.cypher.internal.expressions.LabelName
      |import org.neo4j.cypher.internal.expressions.RelTypeName
      |import org.neo4j.cypher.internal.expressions.PropertyKeyName
      |import org.neo4j.cypher.internal.logical.plans.*
      |import org.neo4j.cypher.internal.logical.plans.Expand.*
      |import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Concurrent
      |import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency.Serial
      |import org.neo4j.cypher.internal.logical.builder.TestException
      |import org.neo4j.cypher.internal.ir.HasHeaders
      |import org.neo4j.cypher.internal.ir.NoHeaders
      |import org.neo4j.cypher.internal.ir.EagernessReason
      |import org.neo4j.cypher.internal.ir.SelectivePathPattern.CountInteger
      |import org.neo4j.cypher.internal.util.attribution.Id
      |import org.neo4j.cypher.internal.util.InputPosition
      |import org.neo4j.cypher.internal.util.UpperBound.Limited
      |import org.neo4j.cypher.internal.util.Repetition
      |// For Cypher types CT...
      |import org.neo4j.cypher.internal.util.symbols.*
      |import org.neo4j.cypher.internal.util.UpperBound.Unlimited
      |import org.neo4j.graphdb.schema.IndexType
      |import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.*
      |import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.*
      |""".stripMargin
  }

  private def interpretPlanBuilder(code: String): LogicalPlan = {
    scalaInterpreter.get.evalPlanBuilderString(code)
  }

  /**
   * Test that for each method of AbstractLogicalPlanBuilder that generated a plan
   * we have a test calling [[testPlan()]] here.
   * This is done via reflection.
   */
  test("all the tests exist") {
    val methodsWeCannotTest = Set(
      "filterExpression",
      "filterExpressionOrString",
      "appendAtCurrentIndent",
      "nestedPlanExistsExpressionProjection",
      "nestedPlanCollectExpressionProjection",
      "nestedPlanGetByNameExpressionProjection",
      "nestedPlanGetByNameExpressionInListComprehensionProjection",
      "pointDistanceNodeIndexSeekExpr",
      "exprPointDistanceNodeIndexSeek",
      "pointDistanceNodeIndexSeekParam",
      "pointDistanceRelationshipIndexSeekExpr",
      "pointBoundingBoxNodeIndexSeekExpr",
      "pointBoundingBoxRelationshipIndexSeekExpr",
      "shortestPathExpr",
      "undirectedRelationshipByIdSeekExpr",
      "directedRelationshipByIdSeekExpr",
      "repeatOptions",
      "varExpandAsShortest",
      "resetIndent",
      "planIf",
      "planAny",
      "remoteBatchPropertiesByExpr",
      "foreachWithExpression",
      "___CONDITION_BEGIN___",
      "___CONDITION_END___"
    )
    withClue("tests missing for these operators:") {
      val methods = classOf[AbstractLogicalPlanBuilder[_, _]].getDeclaredMethods.filter { m =>
        val modifiers = m.getModifiers
        m.getGenericReturnType.getTypeName == "IMPL" &&
        Modifier.isPublic(modifiers) &&
        !Modifier.isStatic(modifiers) &&
        !Modifier.isAbstract(modifiers)
      }
      methods should not be empty
      val m = methods.map { m =>
        val name = m.getName
        val index = name.indexOf('$') // filter out the $bar method
        val end = if (index == -1) name.length else index
        name.substring(0, end)
      }

      m.filter(_.nonEmpty).toSet[String] -- methodsWeCannotTest -- testedOperators should be(empty)
    }
  }

  /**
   * Tests a plan by getting the string representation and then using scala REPL to execute that code, which yields a `rebuiltPlan`.
   * Compare that plan against the original plan.
   *
   * @param buildPlan is pass-by-name to avoid running out of stack while compiling the huge LogicalPlanToLogicalPlanBuilderStringTest constructor
   */
  private def testPlan(name: String, buildPlan: => LogicalPlan): Unit = {
    testedOperators.add(name)

    def roundtripTest(plan: LogicalPlan) = {
      val code = LogicalPlanToPlanBuilderString(plan)
      val rebuiltPlan = interpretPlanBuilder(code)
      if (rebuiltPlan == null) {
        throw new RuntimeException(s"This code did not produce a plan:\n$code")
      }
      rebuiltPlan should equal(plan)
    }

    test(name) {
      val plan = buildPlan
      roundtripTest(plan)

      withClue("with spaced variables") {
        val planWithDifficultVars =
          plan.endoRewrite(topDown(Rewriter.lift {
            case variable @ Variable(name) =>
              variable.copy(s"  $name")(variable.position, variable.isIsolated)
          }))

        roundtripTest(planWithDifficultVars)
      }
    }
  }

  // relies on the current thread's class loader, so make it thread-local to make sure we don't accidentally use the wrong class loader
  private lazy val scalaInterpreter: ThreadLocal[ScalaInterpreter] = ThreadLocal.withInitial(() => ScalaInterpreter())

  class ScalaInterpreter() {

    private val errStream = new java.io.ByteArrayOutputStream()

    // The Scala 3 REPL creates a fresh URLClassLoader (child of the system classloader) for
    // generated code, so without an explicit parent it would load a second copy of
    // LogicalPlanToPlanBuilderStringTest with a different RESULT_ARRAY instance.
    // Passing the test classloader as parent ensures both sides share the same instance.
    private val repl = new ReplDriver(
      Array("-usejavacp", "-color:never"),
      out = new java.io.PrintStream(errStream),
      classLoader = Some(Thread.currentThread().getContextClassLoader)
    ) {
      override protected def redirectOutput: Boolean =
        false // don't redirect output, ScalaTest uses it for communicating test results
    }

    private var replState: dotty.tools.repl.State = repl.initialState

    loadImports()

    private def loadImports(): Unit = {
      val result = eval(preludeCode = imports, resultExpressionCode = "Seq()")
      if (result != Seq()) {
        throw new RuntimeException(s"Failed to load imports:\n$errStream")
      }
    }

    def evalPlanBuilderString(code: String): LogicalPlan = {
      val completeCode =
        s"""new org.neo4j.cypher.internal.logical.builder.TestPlanBuilder()
           |$code""".stripMargin

      eval(preludeCode = "", resultExpressionCode = completeCode) match {
        case plan: LogicalPlan =>
          plan
        case other =>
          throw new RuntimeException(
            s"This code did not produce a plan:\n$code\nResult: $other\nREPL output:\n$errStream"
          )
      }
    }

    private def eval(preludeCode: String, resultExpressionCode: String): AnyRef = {
      val codeToRun =
        s"""$preludeCode
           |
           |org.neo4j.cypher.internal.logical.builder.LogicalPlanToPlanBuilderStringTest.RESULT_ARRAY.value(0) = $resultExpressionCode""".stripMargin

      errStream.reset()
      val res = Array[AnyRef](null)
      LogicalPlanToPlanBuilderStringTest.RESULT_ARRAY.withValue(res) {
        try {
          replState = repl.run(codeToRun)(using replState)
        } catch {
          case t: Throwable =>
            fail(s"Failed to interpret generated code: $t")
        }
      }
      res.head
    }
  }
}

object LogicalPlanToPlanBuilderStringTest {
  // Passes the result array into the REPL's generated code via a thread-local storage.
  val RESULT_ARRAY: DynamicVariable[Array[AnyRef]] = new DynamicVariable[Array[AnyRef]](null)
}

case class TestException() extends RuntimeException()
