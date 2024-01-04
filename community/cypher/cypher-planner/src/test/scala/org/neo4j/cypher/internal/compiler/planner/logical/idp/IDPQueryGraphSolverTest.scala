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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlannerWithCaching
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.exceptions.TestFailedException

import scala.language.reflectiveCalls

class IDPQueryGraphSolverTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {
  self =>

  case class EmptySolverConfig() extends SingleComponentIDPSolverConfig() {
    override def solvers(qppInnerPlanner: QPPInnerPlanner): Seq[Nothing] = Seq.empty
  }

  test("should plan for a single node pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(patternNodes = Set(v"a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        AllNodesScan(v"a", Set.empty)
      )

      verify(monitor).noIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan cartesian product between 3 pattern nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = IDPQueryGraphSolver(
        SingleComponentPlanner(solverConfig = EmptySolverConfig())(monitor),
        cartesianProductsOrValueJoins,
        ExistsSubqueryPlannerWithCaching()
      )(monitor)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val allNodeScanA = AllNodesScan(v"a", Set.empty)
      val allNodeScanB = AllNodesScan(v"b", Set.empty)
      val allNodeScanC = AllNodesScan(v"c", Set.empty)
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result

      val possiblePlans = List(allNodeScanA, allNodeScanB, allNodeScanC).permutations.map { l =>
        val (a, b, c) = (l.head, l(1), l(2))
        CartesianProduct(
          a,
          CartesianProduct(
            b,
            c
          )
        )
      }.toList

      possiblePlans should contain(plan)

      val qgs = cfg.qg.connectedComponents.toArray
      val plans = Array(allNodeScanA, allNodeScanB, allNodeScanC)

      (0 to 2).foreach { i =>
        verify(monitor).noIDPIterationFor(qgs(i), plans(i))
      }
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for a single relationship pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(hasLabels("b", "B"))
      )

      labelCardinality = Map(
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Expand(
          NodeByLabelScan(v"b", labelName("B"), Set.empty, IndexOrderNone),
          v"b",
          SemanticDirection.INCOMING,
          Seq.empty,
          v"a",
          v"r"
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verify(monitor).foundPlanAfter(0)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for a single relationship pattern with labels on both sides") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    val labelBPredicate = hasLabels("b", "B")
    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(Seq(
          hasLabels("a", "A"),
          labelBPredicate
        ))
      )

      labelCardinality = Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(1000)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Selection(
          Seq(labelBPredicate),
          Expand(
            NodeByLabelScan(v"a", labelName("A"), Set.empty, IndexOrderNone),
            v"a",
            SemanticDirection.OUTGOING,
            Seq.empty,
            v"b",
            v"r"
          )
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for a join between two pattern relationships") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    // MATCH (a:A)-[r1]->(c)-[r2]->(b:B)
    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = JoinOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c"),
        patternRelationships = Set(
          PatternRelationship(v"r1", (v"a", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship(v"r2", (v"c", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(Seq(
          hasLabels("a", "A"),
          hasLabels("b", "B")
        ))
      )

      labelCardinality = Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        NodeHashJoin(
          Set(v"c"),
          Expand(
            NodeByLabelScan(v"a", labelName("A"), Set.empty, IndexOrderNone),
            v"a",
            SemanticDirection.OUTGOING,
            Seq.empty,
            v"c",
            v"r1"
          ),
          Expand(
            NodeByLabelScan(v"b", labelName("B"), Set.empty, IndexOrderNone),
            v"b",
            SemanticDirection.INCOMING,
            Seq.empty,
            v"c",
            v"r2"
          )
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).startIteration(1)
      verify(monitor).endIteration(1, 2, 3)
      verify(monitor).foundPlanAfter(1)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for a join between two pattern relationships and apply a selection") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    // MATCH (a:A)-[r1]->(c)-[r2]->(b:B) WHERE r1.foo = r2.foo
    new givenConfig {
      val predicate: Expression = self.equals(prop("r1", "foo"), prop("r2", "foo"))
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = JoinOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c"),
        patternRelationships = Set(
          PatternRelationship(v"r1", (v"a", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship(v"r2", (v"c", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(Seq(
          predicate,
          hasLabels("a", "A"),
          hasLabels("b", "B")
        ))
      )

      labelCardinality = Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
      addTypeToSemanticTable(v"r1", CTRelationship)
      addTypeToSemanticTable(v"r2", CTRelationship)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Selection(
          Seq(cfg.predicate),
          NodeHashJoin(
            Set(v"c"),
            Expand(
              NodeByLabelScan(v"a", labelName("A"), Set.empty, IndexOrderNone),
              v"a",
              SemanticDirection.OUTGOING,
              Seq.empty,
              v"c",
              v"r1"
            ),
            Expand(
              NodeByLabelScan(v"b", labelName("B"), Set.empty, IndexOrderNone),
              v"b",
              SemanticDirection.INCOMING,
              Seq.empty,
              v"c",
              v"r2"
            )
          )
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).startIteration(1)
      verify(monitor).endIteration(1, 2, 3)
      verify(monitor).foundPlanAfter(1)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should solve self looping pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set(v"a"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"a"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Selection(
          ands(equals(v"a", v"  UNNAMED0")),
          DirectedAllRelationshipsScan(v"r", v"a", v"  UNNAMED0", Set.empty)
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should solve double expand") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c"),
        patternRelationships = Set(
          PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship(v"r2", (v"b", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        )
      )
      // make it expensive to do allRelationshipsScans
      cost = {
        case (_: DirectedAllRelationshipsScan, _, _, _) => 1000
        case _                                          => 1
      }
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should beLike {
        case Expand(
            Expand(
              AllNodesScan(_, _),
              _,
              _,
              Seq(),
              _,
              _,
              ExpandAll
            ),
            LogicalVariable("b"),
            _,
            Seq(),
            _,
            _,
            ExpandAll
          ) => ()
      }

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).startIteration(1)
      verify(monitor).endIteration(1, 2, 3)
      verify(monitor).foundPlanAfter(1)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should solve empty graph with Argument") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph.empty
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Argument()
      )

      verify(monitor).emptyComponentPlanned(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan a simple argument row when everything is covered") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(argumentIds = Set(v"a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Argument(Set(v"a"))
      )

      verify(monitor).emptyComponentPlanned(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should handle projected endpoints") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set(v"r")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        ProjectEndpoints(
          Argument(Set(v"r")),
          v"r",
          v"a",
          startInScope = false,
          v"b",
          endInScope = false,
          Seq.empty,
          SemanticDirection.OUTGOING,
          SimplePatternLength
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should expand from projected endpoints") {
    /*
    START r1 = rel(0)
    MATCH (a)-[r1]->(b)-[r2]->(c)
     */
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      cost = {
        case (ProjectEndpoints(Expand(_, _, _, _, _, _, _), _, _, _, _, _, _, _, _), _, _, _) => 10.0
        case (Expand(ProjectEndpoints(_, _, _, _, _, _, _, _, _), _, _, _, _, _, _), _, _, _) => 1.0
      }

      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      val pattern1 =
        PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      val pattern2 =
        PatternRelationship(v"r2", (v"b", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c"),
        patternRelationships = Set(pattern1, pattern2),
        argumentIds = Set(v"r1")
      )

    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Expand(
          ProjectEndpoints(
            Argument(Set(v"r1")),
            v"r1",
            v"a",
            startInScope = false,
            v"b",
            endInScope = false,
            Seq.empty,
            SemanticDirection.OUTGOING,
            SimplePatternLength
          ),
          v"b",
          SemanticDirection.OUTGOING,
          Seq.empty,
          v"c",
          v"r2",
          ExpandAll
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).startIteration(1)
      verify(monitor).endIteration(1, 2, 3)
      verify(monitor).foundPlanAfter(1)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan a relationship pattern based on an argument row since part of the node pattern is already solved") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set(v"a")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Expand(
          Argument(Set(v"a")),
          v"a",
          SemanticDirection.OUTGOING,
          Seq.empty,
          v"b",
          v"r",
          ExpandAll
        )
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test(
    "should plan a very long relationship pattern without combinatorial explosion using various compaction strategies"
  ) {
    val monitor = mock[IDPQueryGraphSolverMonitor]
    val numberOfPatternRelationships = 15

    val solverConfigsToTest = Seq(
      ExpandOnlyIDPSolverConfig,
      new ConfigurableIDPSolverConfig(maxTableSize = 32, iterationDurationLimit = Long.MaxValue), // table limited
      new ConfigurableIDPSolverConfig(maxTableSize = Int.MaxValue, iterationDurationLimit = 500) // time limited
    )

    solverConfigsToTest.foreach { solverConfig =>
      new givenConfig {

        queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = solverConfig)

        val patternNodes: Seq[LogicalVariable] = for (i <- 0 to numberOfPatternRelationships) yield {
          v"n$i"
        }

        val patternRels: Seq[PatternRelationship] = for (i <- 1 to numberOfPatternRelationships) yield {
          PatternRelationship(
            v"r$i",
            (v"n${i - 1}", v"n$i"),
            SemanticDirection.OUTGOING,
            Seq.empty,
            SimplePatternLength
          )
        }

        qg = QueryGraph(patternNodes = patternNodes.toSet, patternRelationships = patternRels.toSet)
        // make it expensive to do allRelationshipsScans
        cost = {
          case (_: DirectedAllRelationshipsScan, _, _, _) => 1000
          case _                                          => 1
        }
      }.withLogicalPlanningContext { (cfg, ctx) =>
        try {
          val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
          // We disallow joins in a couple of configurations
          val joinsPossible: Boolean = solverConfig match {
            case ExpandOnlyIDPSolverConfig => false
            case _                         => true
          }
          assertMinExpandsAndJoins(plan, numberOfPatternRelationships, joinsPossible, numberOfPatternRelationships)
        } catch {
          case e: TestFailedException => fail(s"Failed to plan with config '$solverConfig': ${e.getMessage}")
          case e: Throwable           => throw new RuntimeException(s"Failed to plan with config '$solverConfig'", e)
        }
      }
    }
  }

  private def assertMinExpandsAndJoins(
    plan: LogicalPlan,
    numberOfExpands: Int,
    joinsPossible: Boolean,
    numberOfPatternRelationships: Int
  ): Unit = {
    val counts = countExpandsAndJoins(plan)
    withClue("Expected expands: ") {
      counts("expands") should be >= numberOfExpands
    }

    if (!joinsPossible) withClue(s"No joins expected: $numberOfPatternRelationships") {
      counts("joins") should equal(0)
    }
  }

  private def countExpandsAndJoins(plan: LogicalPlan) = {
    def addCounts(map1: Map[String, Int], map2: Map[String, Int]) = map1 ++ map2.map { case (k, v) =>
      k -> (v + map1.getOrElse(k, 0))
    }
    def incrCount(map: Map[String, Int], key: String) = addCounts(map, Map(key -> 1))
    def expandsAndJoinsCount(plan: Option[LogicalPlan], counts: Map[String, Int]): Map[String, Int] = plan match {
      case None => counts
      case Some(NodeHashJoin(_, left, right)) =>
        incrCount(
          addCounts(expandsAndJoinsCount(Some(left), counts), expandsAndJoinsCount(Some(right), counts)),
          "joins"
        )
      case Some(Expand(source, _, _, _, _, _, _)) =>
        incrCount(expandsAndJoinsCount(Some(source), counts), "expands")
      case Some(p: LogicalPlan) =>
        addCounts(expandsAndJoinsCount(p.lhs, counts), expandsAndJoinsCount(p.rhs, counts))
      case _ => counts
    }
    expandsAndJoinsCount(Some(plan), Map("expands" -> 0, "joins" -> 0))
  }

  test("should plan big star pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    // keep around, practical for investigating performance
    val numberOfPatternRelationships = 10

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      val patternRels: Seq[PatternRelationship] = for (i <- 1 to numberOfPatternRelationships) yield {
        PatternRelationship(v"r $i", (v"n $i", v"x"), SemanticDirection.INCOMING, Seq.empty, SimplePatternLength)
      }

      val patternNodes: Seq[LogicalVariable] = for (i <- 1 to numberOfPatternRelationships) yield {
        v"n$i"
      }

      qg = QueryGraph(patternNodes = patternNodes.toSet + v"x", patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx) // should not throw
    }
  }

  test("should plan big chain pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    // keep around, practical for investigating performance
    val numberOfPatternRelationships = 10

    new givenConfig {
      val patternRels: Seq[PatternRelationship] = for (i <- 1 until numberOfPatternRelationships) yield {
        PatternRelationship(
          v"r $i",
          (v"n $i", v"n ${i + 1}"),
          SemanticDirection.INCOMING,
          Seq.empty,
          SimplePatternLength
        )
      }

      val patternNodes: Seq[LogicalVariable] = for (i <- 1 to numberOfPatternRelationships) yield {
        v"n$i"
      }

      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      qg = QueryGraph(patternNodes = patternNodes.toSet, patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx) // should not throw
    }
  }

  test("should solve planning an empty QG with arguments") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(argumentIds = Set(v"a"), patternNodes = Set(v"a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        Argument(Set(v"a"))
      )

      verify(monitor).noIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan cartesian product between 3 pattern nodes and using a single predicate between 2 pattern nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]
    val predicate = equals(v"b", v"c")

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c"),
        selections = Selections.from(predicate)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val allNodeScanA = AllNodesScan(v"a", Set.empty)
      val allNodeScanB = AllNodesScan(v"b", Set.empty)
      val allNodeScanC = AllNodesScan(v"c", Set.empty)
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        CartesianProduct(
          allNodeScanA,
          ValueHashJoin(
            allNodeScanB,
            allNodeScanC,
            predicate
          )
        )
      )

      val qgs = cfg.qg.connectedComponents.toArray
      val plans = Array(allNodeScanA, allNodeScanB, allNodeScanC)

      (0 to 2).foreach { i =>
        verify(monitor).noIDPIterationFor(qgs(i), plans(i))
      }
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
    }
  }

  test("should plan cartesian product between 1 pattern nodes and 1 pattern relationship") {
    val monitor = mock[IDPQueryGraphSolverMonitor]
    val predicate = equals(v"b", v"c")

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c"),
        selections = Selections.from(predicate),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val allRelScanAB = DirectedAllRelationshipsScan(v"r", v"a", v"b", Set.empty)
      val allNodeScanC = AllNodesScan(v"c", Set.empty)
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        ValueHashJoin(
          allNodeScanC,
          allRelScanAB,
          predicate.switchSides
        )
      )
    }
  }

  test("should plan for optional single relationship pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      qg = QueryGraph( // MATCH a OPTIONAL MATCH (a)-[r]-(b)
        patternNodes = Set(v"a"),
        optionalMatches = Vector(QueryGraph(
          patternNodes = Set(v"a", v"b"),
          argumentIds = Set(v"a"),
          patternRelationships =
            Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val qgs = cfg.qg.connectedComponents
      val allNodeScanA: AllNodesScan = AllNodesScan(v"a", Set.empty)
      val allRelScan = DirectedAllRelationshipsScan(v"r", v"a", v"b", Set.empty)
      val expandAtoB =
        Expand(Argument(Set(v"a")), v"a", SemanticDirection.OUTGOING, Seq.empty, v"b", v"r")
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        OptionalExpand(
          allNodeScanA,
          v"a",
          SemanticDirection.OUTGOING,
          Seq.empty,
          v"b",
          v"r"
        )
      )

      verify(monitor).noIDPIterationFor(qgs.head, allNodeScanA)

      // optional match solving
      {
        // apply optional
        val omQG = cfg.qg.optionalMatches.head

        verify(monitor).initTableFor(omQG)
        verify(monitor).startIDPIterationFor(omQG)
        verify(monitor).endIDPIterationFor(omQG, expandAtoB)

        verify(monitor, times(2)).foundPlanAfter(0) // 1 time here

        verify(monitor).startConnectingComponents(omQG)
        verify(monitor).endConnectingComponents(omQG, expandAtoB)

        // all relationship scan
        val omQGWithoutArguments = omQG.removeArguments()
        verify(monitor).initTableFor(omQGWithoutArguments)
        verify(monitor).startIDPIterationFor(omQGWithoutArguments)
        verify(monitor).endIDPIterationFor(omQGWithoutArguments, allRelScan)

        verify(monitor, times(2)).foundPlanAfter(0) // 1 time here

        verify(monitor).startConnectingComponents(omQGWithoutArguments)
        verify(monitor).endConnectingComponents(omQGWithoutArguments, allRelScan)
      }

      // final result
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for optional single relationship pattern between two known nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      cost = {
        case (_: LeftOuterHashJoin, _, _, _) => 20.0
        case _                               => Double.MaxValue
      }

      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph( // MATCH a, b OPTIONAL MATCH a-[r]->b
        patternNodes = Set(v"a", v"b"),
        optionalMatches = Vector(QueryGraph(
          patternNodes = Set(v"a", v"b"),
          argumentIds = Set(v"a", v"b"),
          patternRelationships =
            Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result should equal(
        LeftOuterHashJoin(
          Set(v"a", v"b"),
          CartesianProduct(
            AllNodesScan(v"a", Set.empty),
            AllNodesScan(v"b", Set.empty)
          ),
          DirectedAllRelationshipsScan(v"r", v"a", v"b", Set.empty)
        )
      )
    }
  }

  test("should handle query starting with an optional match") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph( // OPTIONAL MATCH a-->b RETURN b a
        patternNodes = Set.empty,
        argumentIds = Set.empty,
        optionalMatches = Vector(QueryGraph(
          patternNodes = Set(v"a", v"b"),
          argumentIds = Set.empty,
          patternRelationships =
            Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
      // make it expensive to do allRelationshipsScans
      cost = {
        case (_: DirectedAllRelationshipsScan, _, _, _) => 1000
        case _                                          => 1
      }
    }.withLogicalPlanningContext { (cfg, ctx) =>
      queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result should equal(
        Apply(
          Argument(),
          Optional(
            Expand(
              AllNodesScan(v"a", Set.empty),
              v"a",
              SemanticDirection.OUTGOING,
              Seq.empty,
              v"b",
              v"r",
              ExpandAll
            )
          )
        )
      )
    }
  }

  test("should handle relationship by id") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph( // MATCH (a)-[r]->(b) WHERE id(r) = 42 RETURN *
        patternNodes = Set(v"a", v"b"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(
          in(id(v"r"), listOfInt(42))
        )
      )
      addTypeToSemanticTable(v"r", CTRelationship)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        DirectedRelationshipByIdSeek(v"r", ManySeekableArgs(listOfInt(42)), v"a", v"b", Set.empty)
      )

      verify(monitor).initTableFor(cfg.qg)
      verify(monitor).startIDPIterationFor(cfg.qg)
      verify(monitor).endIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should handle multiple project end points on arguments when creating leaf plans") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c"),
        patternRelationships = Set(
          PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship(v"r2", (v"b", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set(v"r1", v"r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result should equal(
        ProjectEndpoints(
          ProjectEndpoints(
            Argument(Set(v"r1", v"r2")),
            v"r2",
            v"b",
            startInScope = false,
            v"c",
            endInScope = false,
            Seq.empty,
            SemanticDirection.OUTGOING,
            SimplePatternLength
          ),
          v"r1",
          v"a",
          startInScope = false,
          v"b",
          endInScope = true,
          Seq.empty,
          SemanticDirection.OUTGOING,
          SimplePatternLength
        )
      )
    }
  }

  test("should handle passing multiple projectible relationships as arguments") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {

      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b", v"c", v"d"),
        patternRelationships = Set(
          PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship(v"r2", (v"c", v"d"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship(v"r3", (v"a", v"d"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set(v"a", v"b", v"c", v"d", v"r1", v"r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val plan = queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result
      plan should equal(
        new LogicalPlanBuilder(wholePlan = false)
          .projectEndpoints("(a)-[r1]->(b)", startInScope = true, endInScope = true)
          .projectEndpoints("(c)-[r2]->(d)", startInScope = true, endInScope = true)
          .expandInto("(a)-[r3]->(d)")
          .argument("a", "b", "r1", "r2", "c", "d")
          .build()
      )
    }
  }

  test("should not plan cartesian products by duplicating argument rows") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set(v"a", v"b"),
        argumentIds = Set(v"a", v"b")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      queryGraphSolver.plan(cfg.qg, InterestingOrderConfig.empty, ctx).result should equal(
        Argument(Set(v"a", v"b"))
      )
    }
  }

  test("can call hashCode on IDPQueryGraphSolver") {
    // Given
    val monitor = new IDPQueryGraphSolverMonitor {
      override def noIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit = ()
      override def initTableFor(graph: QueryGraph): Unit = ()
      override def startIDPIterationFor(graph: QueryGraph): Unit = ()
      override def endIDPIterationFor(graph: QueryGraph, result: LogicalPlan): Unit = ()
      override def emptyComponentPlanned(graph: QueryGraph, plan: LogicalPlan): Unit = ()
      override def startConnectingComponents(graph: QueryGraph): Unit = ()
      override def endConnectingComponents(graph: QueryGraph, result: LogicalPlan): Unit = ()
      override def startIteration(iteration: Int): Unit = ()
      override def endIteration(iteration: Int, depth: Int, tableSize: Int): Unit = ()
      override def foundPlanAfter(iterations: Int): Unit = ()
      // The Kernel monitors behave weirdly.
      // You can only call the methods directly defined in the interface on them, everything else throws a NullPointerException.
      override def hashCode(): Int = throw new NullPointerException()
    }
    val solver = createQueryGraphSolver(monitor, ExpandOnlyIDPSolverConfig)

    // When & Then
    noException should be thrownBy solver.hashCode()
  }

  test("should cache the result of EXISTS subquery planning when caching is enabled") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(
        monitor = monitor,
        solverConfig = ExpandOnlyIDPSolverConfig,
        existSubqueryPlanner = ExistsSubqueryPlannerWithCaching()
      )
    }.withLogicalPlanningContext { (_, ctx) =>
      val exists = ExistsIRExpression(
        RegularSinglePlannerQuery(
          queryGraph = QueryGraph(
            patternNodes = Set(v"a", v"x"),
            argumentIds = Set(v"a")
          )
        ),
        v"anon_0",
        "EXISTS { MATCH (a), (x) }"
      )(pos, Some(Set(v"x")), Some(Set(v"a")))

      val plan = ctx.staticComponents.queryGraphSolver.planInnerOfExistsSubquery(exists, LabelInfo.empty, ctx)
      val planAgain = ctx.staticComponents.queryGraphSolver.planInnerOfExistsSubquery(exists, LabelInfo.empty, ctx)
      val planWithDifferentLabelInfo =
        ctx.staticComponents.queryGraphSolver.planInnerOfExistsSubquery(
          exists,
          LabelInfo(v"a" -> Set(labelName("REL"))),
          ctx
        )

      (plan eq planAgain) shouldBe true
      (plan eq planWithDifferentLabelInfo) shouldBe false
    }
  }

  test("should not cache the result of EXISTS subquery planning when caching is disabled") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new givenConfig {
      queryGraphSolver = createQueryGraphSolver(
        monitor = monitor,
        solverConfig = ExpandOnlyIDPSolverConfig,
        existSubqueryPlanner = ExistsSubqueryPlanner
      )
    }.withLogicalPlanningContext { (_, ctx) =>
      val exists = ExistsIRExpression(
        RegularSinglePlannerQuery(
          queryGraph = QueryGraph(
            patternNodes = Set(v"a", v"x"),
            argumentIds = Set(v"a")
          )
        ),
        v"anon_0",
        "EXISTS { MATCH (a), (x) }"
      )(pos, Some(Set(v"x")), Some(Set(v"a")))

      val plan = ctx.staticComponents.queryGraphSolver.planInnerOfExistsSubquery(exists, LabelInfo.empty, ctx)
      val planAgain = ctx.staticComponents.queryGraphSolver.planInnerOfExistsSubquery(exists, LabelInfo.empty, ctx)

      (plan eq planAgain) shouldBe false
    }
  }

  private def createQueryGraphSolver(
    monitor: IDPQueryGraphSolverMonitor,
    solverConfig: SingleComponentIDPSolverConfig,
    existSubqueryPlanner: ExistsSubqueryPlanner = ExistsSubqueryPlannerWithCaching()
  ) =
    IDPQueryGraphSolver(
      SingleComponentPlanner(solverConfig)(monitor),
      cartesianProductsOrValueJoins,
      existSubqueryPlanner
    )(monitor)
}
