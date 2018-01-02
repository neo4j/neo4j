/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, LogicalPlanningContext}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport2, PlannerQuery, QueryGraph, Selections}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

import scala.collection.immutable
import scala.language.reflectiveCalls

class IDPQueryGraphSolverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  case class EmptySolverConfig() extends IDPSolverConfig() {
    override def solvers(queryGraph: QueryGraph) = Seq.empty
  }

  test("should plan for a single node pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        AllNodesScan("a", Set.empty)(solved)
      )

      verify(monitor).noIDPIterationFor(cfg.qg, 0, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan cartesian product between 3 pattern nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val allNodeScanA = AllNodesScan("a", Set.empty)(solved)
      val allNodeScanB = AllNodesScan("b", Set.empty)(solved)
      val allNodeScanC = AllNodesScan("c", Set.empty)(solved)
      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        CartesianProduct(
          allNodeScanA,
          CartesianProduct(
            allNodeScanB,
            allNodeScanC
          )(solved)
        )(solved)
      )

      val qgs = cfg.qg.connectedComponents.toArray
      val plans = Array(allNodeScanA, allNodeScanB, allNodeScanC)

      (0 to 2).foreach { i =>
        verify(monitor).noIDPIterationFor(qgs(i), i, plans(i))
      }
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for a single relationship pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(NodeByLabelScan("b", LazyLabel("B"), Set.empty)(solved), "b", SemanticDirection.INCOMING, Seq.empty, "a", "r")(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verify(monitor).foundPlanAfter(0)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for a single relationship pattern with labels on both sides") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    val labelBPredicate = HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos)
    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(
          HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos),
          labelBPredicate)
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(1000)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Selection(Seq(labelBPredicate),
          Expand(
            NodeByLabelScan("a", LazyLabel("A"), Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")(solved)
        )(solved))

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = JoinOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(
          HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos),
          HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        NodeHashJoin(Set("c"),
          Expand(
            NodeByLabelScan("a", LazyLabel("A"), Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq.empty, "c", "r1")(solved),
          Expand(
            NodeByLabelScan("b", LazyLabel("B"), Set.empty)(solved), "b", SemanticDirection.INCOMING, Seq.empty, "c", "r2")(solved)
        )(solved))

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
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
      val predicate = Equals(Property(ident("r1"), PropertyKeyName("foo")(pos))(pos), Property(ident("r2"), PropertyKeyName("foo")(pos))(pos))(pos)
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = JoinOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        selections = Selections.from(
          predicate,
          HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos),
          HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "A" -> Cardinality(10),
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Selection(Seq(cfg.predicate),
          NodeHashJoin(Set("c"),
            Expand(
              NodeByLabelScan("a", LazyLabel("A"), Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq.empty, "c", "r1")(solved),
            Expand(
              NodeByLabelScan("b", LazyLabel("B"), Set.empty)(solved), "b", SemanticDirection.INCOMING, Seq.empty, "c", "r2")(solved)
          )(solved)
        )(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a"),
        patternRelationships = Set(PatternRelationship("r", ("a", "a"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(AllNodesScan("a", Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq.empty, "a", IdName("r"), ExpandInto)(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should solve double expand") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(
          Expand(
            AllNodesScan("b", Set.empty)(solved),
            "b", SemanticDirection.OUTGOING, Seq.empty, "c", IdName("r2"), ExpandAll
          )(solved),
          "b", SemanticDirection.INCOMING, Seq.empty, "a", IdName("r1"), ExpandAll
        )(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).startIteration(1)
      verify(monitor).endIteration(1, 2, 3)
      verify(monitor).foundPlanAfter(1)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should solve empty graph with SingleRow") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph.empty
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        SingleRow()(solved)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(argumentIds = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Argument(Set("a"))(solved)()
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("r"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        ProjectEndpoints(Argument(Set("r"))(solved)(), "r", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength)(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should expand from projected endpoints") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      cost = {
        case (ProjectEndpoints(Expand(_, _, _, _, _, _, _),_, _, _, _, _, _, _,_), _) => 10.0
        case (Expand(ProjectEndpoints(_,_, _, _, _, _, _, _, _), _, _, _, _, _, _), _) => 1.0
      }

      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      val pattern1 = PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      val pattern2 = PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(pattern1, pattern2),
        argumentIds = Set("r1"))

    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(
          ProjectEndpoints(Argument(Set("r1"))(solved)(), "r1", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength)(solved),
          "b", SemanticDirection.OUTGOING, Seq.empty, "c", "r2", ExpandAll
        )(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("a")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(Argument(Set("a"))(solved)(), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r", ExpandAll)(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
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
      ExpandOnlyWhenPatternIsLong,
      new IDPSolverConfig {
        override def solvers(queryGraph: QueryGraph): Seq[(QueryGraph) => IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext]] =
          ExpandOnlyWhenPatternIsLong.solvers(queryGraph)
        override def iterationDurationLimit: Long = 100
      },
      new ConfigurableIDPSolverConfig(maxTableSize = 32, iterationDurationLimit = Long.MaxValue), // table limited
      new ConfigurableIDPSolverConfig(maxTableSize = Int.MaxValue, iterationDurationLimit = 500), // time limited
      AdaptiveChainPatternConfig(10), // default
      new AdaptiveChainPatternConfig(5) { // make sure it works on comprehsions for very long patterns
        override def iterationDurationLimit: Long = 20
      }
    )

    solverConfigsToTest.foreach { solverConfig =>
      new given {
        queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = solverConfig)

        val patternNodes = for (i <- 0 to numberOfPatternRelationships) yield {
          IdName(s"n$i")
        }

        val patternRels = for (i <- 1 to numberOfPatternRelationships) yield {
          PatternRelationship(s"r$i", (s"n${i - 1}", s"n$i"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        }

        qg = QueryGraph(patternNodes = patternNodes.toSet, patternRelationships = patternRels.toSet)
      }.withLogicalPlanningContext { (cfg, ctx) =>
        implicit val x = ctx
        try {
          val plan = queryGraphSolver.plan(cfg.qg)
          val minJoinsExpected = solverConfig match {
            case ExpandOnlyIDPSolverConfig => 0
            case ExpandOnlyWhenPatternIsLong => 0
            case _ => 1
          }
          assertMinExpandsAndJoins(plan, Map("expands" -> numberOfPatternRelationships, "joins" -> minJoinsExpected))
        } catch {
          case e: Exception => fail(s"Failed to plan with config '$solverConfig': ${e.getMessage}")
        }
      }
    }
  }

  def assertMinExpandsAndJoins(plan: LogicalPlan, minCounts: Map[String, Int]) = {
    val counts = countExpandsAndJoins(plan)
    Seq("expands", "joins").foreach { op =>
      counts(op) should be >= minCounts(op)
    }
    counts
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor)
      val patternRels = for (i <- 1 to numberOfPatternRelationships) yield {
        PatternRelationship("r" + i, ("n" + i, "x"), SemanticDirection.INCOMING, Seq.empty, SimplePatternLength)
      }

      val patternNodes = for (i <- 1 to numberOfPatternRelationships) yield {
        IdName("n" + i)
      }

      qg = QueryGraph(patternNodes = patternNodes.toSet + IdName("x"), patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx
      queryGraphSolver.plan(cfg.qg) // should not throw
    }
  }

  test("should plan big chain pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    // keep around, practical for investigating performance
    val numberOfPatternRelationships = 10

    new given {
      val patternRels = for (i <- 1 to numberOfPatternRelationships - 1) yield {
        PatternRelationship("r" + i, ("n" + i, "n" + (i + 1)), SemanticDirection.INCOMING, Seq.empty, SimplePatternLength)
      }

      val patternNodes = for (i <- 1 to numberOfPatternRelationships) yield {
        IdName("n" + i)
      }

      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor)
      qg = QueryGraph(patternNodes = patternNodes.toSet, patternRelationships = patternRels.toSet)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx
      queryGraphSolver.plan(cfg.qg) // should not throw
    }
  }

  test("should solve planning an empty QG with arguments") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(argumentIds = Set("a"), patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Argument(Set("a"))(solved)()
      )

      verify(monitor).noIDPIterationFor(cfg.qg, 0, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan cartesian product between 3 pattern nodes and using a single predicate between 2 pattern nodes") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val allNodeScanA = AllNodesScan("a", Set.empty)(solved)
      val allNodeScanB = AllNodesScan("b", Set.empty)(solved)
      val allNodeScanC = AllNodesScan("c", Set.empty)(solved)
      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Selection(cfg.qg.selections.flatPredicates,
          CartesianProduct(
            allNodeScanB,
            CartesianProduct(
              allNodeScanA,
              allNodeScanC
            )(solved)
          )(solved)
        )(solved)
      )

      val qgs = cfg.qg.connectedComponents.toArray
      val plans = Array(allNodeScanA, allNodeScanB, allNodeScanC)

      (0 to 2).foreach { i =>
        verify(monitor).noIDPIterationFor(qgs(i), i, plans(i))
      }
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan cartesian product between 1 pattern nodes and 1 pattern relationship") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val expandBtoA = Expand(AllNodesScan("b", Set.empty)(solved), "b", SemanticDirection.INCOMING, Seq.empty, "a", "r", ExpandAll)(solved)
      val allNodeScanC = AllNodesScan("c", Set.empty)(solved)
      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Selection(cfg.qg.selections.flatPredicates,
          CartesianProduct(
            allNodeScanC,
            expandBtoA
          )(solved)
        )(solved)
      )
      val qgs = cfg.qg.connectedComponents.toArray
      val plans = Array(expandBtoA, allNodeScanC)

      verify(monitor).initTableFor(qgs(0), 0)
      verify(monitor).startIDPIterationFor(qgs(0), 0)
      verify(monitor).endIDPIterationFor(qgs(0), 0, expandBtoA)

      verify(monitor).foundPlanAfter(0)

      verify(monitor).noIDPIterationFor(qgs(1), 1, plans(1))

      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)
      verifyNoMoreInteractions(monitor)
    }
  }

  test("should plan for optional single relationship pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor)
      qg = QueryGraph(// MATCH a OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a"),
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val qgs = cfg.qg.connectedComponents
      val allNodeScanA: AllNodesScan = AllNodesScan("a", Set.empty)(solved)
      val expandAtoB = Expand(Argument(Set("a"))(solved)(), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")(solved)
      val expandBtoA = Expand(AllNodesScan("b", Set.empty)(solved), "b", SemanticDirection.INCOMING, Seq.empty, "a", "r")(solved)
      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Apply(
          allNodeScanA,
          Optional(
            expandAtoB
          )(solved)
        )(solved)
      )

      verify(monitor).noIDPIterationFor(qgs.head, 0, allNodeScanA)

      // optional match solving
      {
        // apply optional
        val omQG = cfg.qg.optionalMatches.head

        verify(monitor).initTableFor(omQG, 0)
        verify(monitor).startIDPIterationFor(omQG, 0)
        verify(monitor).endIDPIterationFor(omQG, 0, expandAtoB)

        verify(monitor, times(2)).foundPlanAfter(0) // 1 time here

        verify(monitor).startConnectingComponents(omQG)
        verify(monitor).endConnectingComponents(omQG, expandAtoB)

        // outer hash joins
        val omQGWithoutArguments = omQG.withoutArguments()

        verify(monitor).initTableFor(omQGWithoutArguments, 0)
        verify(monitor).startIDPIterationFor(omQGWithoutArguments, 0)
        verify(monitor).endIDPIterationFor(omQGWithoutArguments, 0, expandBtoA)

        verify(monitor, times(2)).foundPlanAfter(0) // 1 time here

        verify(monitor).startConnectingComponents(omQGWithoutArguments)
        verify(monitor).endConnectingComponents(omQGWithoutArguments, expandBtoA)
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
        case (_: OuterHashJoin, _) => 20.0
        case _ => Double.MaxValue
      }

      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(// MATCH a, b OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a", "b"),
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a", "b"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        OuterHashJoin(
          Set("a", "b"),
          CartesianProduct(
            AllNodesScan(IdName("a"), Set.empty)(solved),
            AllNodesScan(IdName("b"), Set.empty)(solved)
          )(solved),
          Expand(
            AllNodesScan(IdName("b"), Set.empty)(solved),
            "b", SemanticDirection.INCOMING, Seq.empty, "a", "r", ExpandAll
          )(solved)
        )(solved)
      )
    }
  }

  test("should handle query starting with an optional match") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(// OPTIONAL MATCH a-->b RETURN b a
        patternNodes = Set.empty,
        argumentIds = Set.empty,
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set.empty,
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)))
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Apply(
          SingleRow()(solved),
          Optional(
            Expand(
              AllNodesScan("b", Set.empty)(solved),
              "b", SemanticDirection.INCOMING, Seq.empty, "a", "r", ExpandAll
            )(solved)
          )(solved)
        )(solved)
      )
    }
  }

  test("should handle relationship by id") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = EmptySolverConfig())
      qg = QueryGraph(// MATCH (a)-[r]->(b) WHERE id(r) = 42 RETURN *
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(In(FunctionInvocation(FunctionName("id")(pos), ident("r"))(pos), Collection(Seq(SignedDecimalIntegerLiteral("42")(pos)))(pos))(pos))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        DirectedRelationshipByIdSeek("r", ManySeekableArgs(Collection(Seq(SignedDecimalIntegerLiteral("42")(pos)))(pos)), "a", "b", Set.empty)(solved)
      )

      verify(monitor).initTableFor(cfg.qg, 0)
      verify(monitor).startIDPIterationFor(cfg.qg, 0)
      verify(monitor).endIDPIterationFor(cfg.qg, 0, plan)
      verify(monitor).startConnectingComponents(cfg.qg)
      verify(monitor).endConnectingComponents(cfg.qg, plan)

      verify(monitor).foundPlanAfter(0)

      verifyNoMoreInteractions(monitor)
    }
  }

  test("should handle multiple project end points on arguments when creating leaf plans") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        ProjectEndpoints(
          ProjectEndpoints(
            Argument(Set("r1", "r2"))(solved)(),
            "r2", "b", startInScope = false, "c", endInScope = false, None, directed = true, SimplePatternLength)(solved),
          "r1", "a", startInScope = false, "b", endInScope = true, None, directed = true, SimplePatternLength)(solved))
    }
  }

  test("should handle passing multiple projectible relationships as arguments") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {

      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c", "d"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "d"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", ("a", "d"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("a", "b", "c", "d", "r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(
          ProjectEndpoints(
            ProjectEndpoints(
              Argument(Set("r2", "r1", "a", "d", "b", "c"))(solved)(),
              "r2", "c", startInScope = true, "d", endInScope = true, None, directed = true, SimplePatternLength)(solved),
            "r1", "a" , startInScope = true, "b", endInScope = true, None, directed = true, SimplePatternLength)(solved),
          "a", OUTGOING, List(), "d", "r3", ExpandInto)(solved))
    }
  }

  test("should not plan cartesian products by duplicating argument rows") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solverConfig = ExpandOnlyIDPSolverConfig)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        argumentIds = Set("a", "b")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Argument(Set("a", "b"))(solved)()
      )
    }
  }
}
