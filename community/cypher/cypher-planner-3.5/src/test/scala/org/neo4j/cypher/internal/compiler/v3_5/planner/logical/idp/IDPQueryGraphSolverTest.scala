/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp

import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions}
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5._
import org.opencypher.v9_0.util.Cardinality
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions.SemanticDirection.OUTGOING
import org.opencypher.v9_0.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.scalatest.exceptions.TestFailedException

import scala.collection.immutable
import scala.language.reflectiveCalls

class IDPQueryGraphSolverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  case class EmptySolverConfig() extends IDPSolverConfig() {
    override def solvers(queryGraph: QueryGraph) = Seq.empty
  }

  test("should plan for a single node pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        AllNodesScan("a", Set.empty)
      )

      verify(monitor).noIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan cartesian product between 3 pattern nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(SingleComponentPlanner(monitor, solverConfig = EmptySolverConfig()), cartesianProductsOrValueJoins, monitor)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c")
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val allNodeScanA = AllNodesScan("a", Set.empty)
      val allNodeScanB = AllNodesScan("b", Set.empty)
      val allNodeScanC = AllNodesScan("c", Set.empty)
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)

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

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(HasLabels(varFor("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Expand(NodeByLabelScan("b", lblName("B"), Set.empty), "b", SemanticDirection.INCOMING, Seq.empty, "a", "r")
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

    val labelBPredicate = HasLabels(varFor("b"), Seq(LabelName("B")(pos)))(pos)
    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(Seq(
          HasLabels(varFor("a"), Seq(LabelName("A")(pos)))(pos),
          labelBPredicate))
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(1000)
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Selection(Seq(labelBPredicate),
          Expand(
            NodeByLabelScan("a", lblName("A"), Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")
        ))

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
    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = JoinOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(Seq(
          HasLabels(varFor("a"), Seq(LabelName("A")(pos)))(pos),
          HasLabels(varFor("b"), Seq(LabelName("B")(pos)))(pos))
      ))

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        NodeHashJoin(Set("c"),
          Expand(
            NodeByLabelScan("a", lblName("A"), Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "c", "r1"),
          Expand(
            NodeByLabelScan("b", lblName("B"), Set.empty), "b", SemanticDirection.INCOMING, Seq.empty, "c", "r2")
        ))

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
    new given {
      val predicate = Equals(Property(varFor("r1"), PropertyKeyName("foo")(pos))(pos), Property(varFor("r2"), PropertyKeyName("foo")(pos))(pos))(pos)
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = JoinOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(Seq(
          predicate,
          HasLabels(varFor("a"), Seq(LabelName("A")(pos)))(pos),
          HasLabels(varFor("b"), Seq(LabelName("B")(pos)))(pos))
      ))

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Selection(Seq(cfg.predicate),
          NodeHashJoin(Set("c"),
            Expand(
              NodeByLabelScan("a", lblName("A"), Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "c", "r1"),
            Expand(
              NodeByLabelScan("b", lblName("B"), Set.empty), "b", SemanticDirection.INCOMING, Seq.empty, "c", "r2")
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

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a"),
        patternRelationships = Set(PatternRelationship("r", ("a", "a"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Expand(AllNodesScan("a", Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "a", "r", ExpandInto)
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

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Expand(
          Expand(
            AllNodesScan("c", Set.empty),
            "c", SemanticDirection.INCOMING, Seq.empty, "b", "r2", ExpandAll
          ),
          "b", SemanticDirection.INCOMING, Seq.empty, "a", "r1", ExpandAll
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

  test("should solve empty graph with Argument") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph.empty
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
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

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(argumentIds = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Argument(Set("a"))
      )

      verify(monitor).emptyComponentPlanned(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should handle projected endpoints") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("r"))
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        ProjectEndpoints(Argument(Set("r")), "r", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength)
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

    new given {
      cost = {
        case (ProjectEndpoints(Expand(_, _, _, _, _, _, _),_, _, _, _, _, _, _,_), _, _) => 10.0
        case (Expand(ProjectEndpoints(_,_, _, _, _, _, _, _, _), _, _, _, _, _, _), _, _) => 1.0
      }

      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      val pattern1 = PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      val pattern2 = PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(pattern1, pattern2),
        argumentIds = Set("r1"))

    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Expand(
          ProjectEndpoints(Argument(Set("r1")), "r1", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength),
          "b", SemanticDirection.OUTGOING, Seq.empty, "c", "r2", ExpandAll
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

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("a")
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Expand(Argument(Set("a")), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)
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

  test("should plan a very long relationship pattern without combinatorial explosion using various compaction strategies") {
    val monitor = mock[IDPQueryGraphSolverMonitor]
    val numberOfPatternRelationships = 15

    val solverConfigsToTest = Seq(
      ExpandOnlyIDPSolverConfig,
      new ConfigurableIDPSolverConfig(maxTableSize = 32, iterationDurationLimit = Long.MaxValue), // table limited
      new ConfigurableIDPSolverConfig(maxTableSize = Int.MaxValue, iterationDurationLimit = 500), // time limited
      AdaptiveChainPatternConfig(10), // default
      new AdaptiveChainPatternConfig(5) { // make sure it works on comprehensions for very long patterns
        override def iterationDurationLimit: Long = 20
      }
    )

    solverConfigsToTest.foreach { solverConfig =>
      new given {

        queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = solverConfig)

        val patternNodes = for (i <- 0 to numberOfPatternRelationships) yield {
          s"n$i"
        }

        val patternRels = for (i <- 1 to numberOfPatternRelationships) yield {
          PatternRelationship(s"r$i", (s"n${i - 1}", s"n$i"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        }

        qg = QueryGraph(patternNodes = patternNodes.toSet, patternRelationships = patternRels.toSet)
      }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
        implicit val x = ctx
        try {
          val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
          // We disallow joins in a couple of configurations
          val joinsPossible: Boolean = solverConfig match {
            case ExpandOnlyIDPSolverConfig => false
            case _ => true
          }
          assertMinExpandsAndJoins(plan, numberOfPatternRelationships, joinsPossible, numberOfPatternRelationships)
        } catch {
          case e: TestFailedException => fail(s"Failed to plan with config '$solverConfig': ${e.getMessage}")
          case e: Throwable => throw new RuntimeException(s"Failed to plan with config '$solverConfig'", e)
        }
      }
    }
  }

  private def assertMinExpandsAndJoins(plan: LogicalPlan, numberOfExpands: Int, joinsPossible: Boolean, numberOfPatternRelationships: Int) = {
    val counts = countExpandsAndJoins(plan)
    withClue("Expected expands: ") {
      counts("expands") should be >= numberOfExpands
    }

    if(!joinsPossible) withClue(s"No joins expected: $numberOfPatternRelationships") {
      counts("joins") should equal(0)
    }
  }

  def countExpandsAndJoins(plan: LogicalPlan) = {
    def addCounts(map1: Map[String, Int], map2: Map[String, Int]) = map1 ++ map2.map { case (k, v) => k -> (v + map1.getOrElse(k, 0)) }
    def incrCount(map: Map[String, Int], key: String) = addCounts(map, Map(key -> 1))
    def expandsAndJoinsCount(plan: Option[LogicalPlan], counts: Map[String, Int]): Map[String, Int] = plan match {
      case None => counts
      case Some(NodeHashJoin(_, left, right)) =>
        incrCount(addCounts(expandsAndJoinsCount(Some(left), counts), expandsAndJoinsCount(Some(right), counts)), "joins")
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

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      val patternRels = for (i <- 1 to numberOfPatternRelationships) yield {
        PatternRelationship("r" + i, ("n" + i, "x"), SemanticDirection.INCOMING, Seq.empty, SimplePatternLength)
      }

      val patternNodes = for (i <- 1 to numberOfPatternRelationships) yield {
        "n" + i
      }

      qg = QueryGraph(patternNodes = patternNodes.toSet + "x", patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      implicit val x = ctx
      queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities) // should not throw
    }
  }

  test("should plan big chain pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    // keep around, practical for investigating performance
    val numberOfPatternRelationships = 10

    new given {
      val patternRels = for (i <- 1 until numberOfPatternRelationships) yield {
        PatternRelationship("r" + i, ("n" + i, "n" + (i + 1)), SemanticDirection.INCOMING, Seq.empty, SimplePatternLength)
      }

      val patternNodes = for (i <- 1 to numberOfPatternRelationships) yield {
        "n" + i
      }

      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      qg = QueryGraph(patternNodes = patternNodes.toSet, patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      implicit val x = ctx
      queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities) // should not throw
    }
  }

  test("should solve planning an empty QG with arguments") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(argumentIds = Set("a"), patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Argument(Set("a"))
      )

      verify(monitor).noIDPIterationFor(cfg.qg, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan cartesian product between 3 pattern nodes and using a single predicate between 2 pattern nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]
    val predicate: Equals = Equals(varFor("b"), varFor("c"))(pos)

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(predicate))
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val allNodeScanA = AllNodesScan("a", Set.empty)
      val allNodeScanB = AllNodesScan("b", Set.empty)
      val allNodeScanC = AllNodesScan("c", Set.empty)
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
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
    val predicate: Equals = Equals(varFor("b"), varFor("c"))(pos)

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(predicate),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val expandAtoB = Expand(AllNodesScan("a", Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)
      val allNodeScanC = AllNodesScan("c", Set.empty)
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        ValueHashJoin(
          allNodeScanC,
          expandAtoB,
          predicate.switchSides
        )
      )
    }
  }

  test("should plan for optional single relationship pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      qg = QueryGraph(// MATCH a OPTIONAL MATCH (a)-[r]-(b)
        patternNodes = Set("a"),
        optionalMatches = Vector(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val qgs = cfg.qg.connectedComponents
      val allNodeScanA: AllNodesScan = AllNodesScan("a", Set.empty)
      val expandAtoB = Expand(Argument(Set("a")), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")
      val expandAtoB2 = Expand(AllNodesScan("a", Set.empty), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Apply(
          allNodeScanA,
          Optional(
            expandAtoB, Set("a")
          )
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

        verify(monitor, times(3)).foundPlanAfter(0) // 1 time here

        verify(monitor).startConnectingComponents(omQG)
        verify(monitor).endConnectingComponents(omQG, expandAtoB)

        // outer hash joins (left and right)
        val omQGWithoutArguments = omQG.withoutArguments()

        verify(monitor, times(2)).initTableFor(omQGWithoutArguments)
        verify(monitor, times(2)).startIDPIterationFor(omQGWithoutArguments)
        verify(monitor, times(2)).endIDPIterationFor(omQGWithoutArguments, expandAtoB2)

        verify(monitor, times(3)).foundPlanAfter(0) // 1 time here

        verify(monitor, times(2)).startConnectingComponents(omQGWithoutArguments)
        verify(monitor, times(2)).endConnectingComponents(omQGWithoutArguments, expandAtoB2)
      }

      // final result
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for optional single relationship pattern between two known nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      cost = {
        case (_: LeftOuterHashJoin, _, _) => 20.0
        case _ => Double.MaxValue
      }

      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(// MATCH a, b OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a", "b"),
        optionalMatches = Vector(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a", "b"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities) should equal(
        LeftOuterHashJoin(
          Set("a", "b"),
          CartesianProduct(
            AllNodesScan("a", Set.empty),
            AllNodesScan("b", Set.empty)
          ),
          Expand(
            AllNodesScan("a", Set.empty),
            "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll
          )
        )
      )
    }
  }

  test("should handle query starting with an optional match") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(// OPTIONAL MATCH a-->b RETURN b a
        patternNodes = Set.empty,
        argumentIds = Set.empty,
        optionalMatches = Vector(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set.empty,
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)))
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities) should equal(
        Apply(
          Argument(),
          Optional(
            Expand(
              AllNodesScan("a", Set.empty),
              "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll
            )
          )
        )
      )
    }
  }

  test("should handle relationship by id") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(// MATCH (a)-[r]->(b) WHERE id(r) = 42 RETURN *
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(In(FunctionInvocation(FunctionName("id")(pos), varFor("r"))(pos), ListLiteral(Seq(SignedDecimalIntegerLiteral("42")(pos)))(pos))(pos))
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        DirectedRelationshipByIdSeek("r", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("42")(pos)))(pos)), "a", "b", Set.empty)
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

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor, DefaultIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities) should equal(
        ProjectEndpoints(
          ProjectEndpoints(
            Argument(Set("r1", "r2")),
            "r2", "b", startInScope = false, "c", endInScope = false, None, directed = true, SimplePatternLength),
          "r1", "a", startInScope = false, "b", endInScope = true, None, directed = true, SimplePatternLength))
    }
  }

  test("should handle passing multiple projectible relationships as arguments") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {

      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c", "d"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "d"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", ("a", "d"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("a", "b", "c", "d", "r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val plan = queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities)
      plan should equal(
        Expand(
          ProjectEndpoints(
            ProjectEndpoints(
              Argument(Set("r2", "r1", "a", "d", "b", "c")),
              "r2", "c", startInScope = true, "d", endInScope = true, None, directed = true, SimplePatternLength),
            "r1", "a", startInScope = true, "b", endInScope = true, None, directed = true, SimplePatternLength),
          "a", OUTGOING, List(), "d", "r3", ExpandInto))
    }
  }

  test("should not plan cartesian products by duplicating argument rows") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = createQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        argumentIds = Set("a", "b")
      )
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      queryGraphSolver.plan(cfg.qg, ctx, solveds, cardinalities) should equal(
        Argument(Set("a", "b"))
      )
    }
  }

  private def createQueryGraphSolver(monitor: IDPQueryGraphSolverMonitor, solverConfig: IDPSolverConfig) =
    IDPQueryGraphSolver(SingleComponentPlanner(monitor, solverConfig), cartesianProductsOrValueJoins, monitor)
}
