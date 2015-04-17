/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp

import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions}
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport2, QueryGraph, Selections}
import org.neo4j.graphdb.Direction

import scala.collection.immutable

import scala.language.reflectiveCalls

class IDPQueryGraphSolverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan for a single node pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        AllNodesScan("a", Set.empty)(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val allNodeScanA = AllNodesScan("a", Set.empty)(null)
      val allNodeScanB = AllNodesScan("b", Set.empty)(null)
      val allNodeScanC = AllNodesScan("c", Set.empty)(null)
      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        CartesianProduct(
          allNodeScanA,
          CartesianProduct(
            allNodeScanB,
            allNodeScanC
          )(null)
        )(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "B" -> Cardinality(10)
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "a", "r")(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
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
            NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "b", "r")(null)
        )(null))

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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq(joinSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
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
            NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "c", "r1")(null),
          Expand(
            NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "c", "r2")(null)
        )(null))

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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq(joinSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
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
              NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "c", "r1")(null),
            Expand(
              NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "c", "r2")(null)
          )(null)
        )(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a"),
        patternRelationships = Set(PatternRelationship("r", ("a", "a"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(AllNodesScan("a", Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "a", IdName("r"), ExpandInto)(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq(expandSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(
          Expand(
            AllNodesScan("b", Set.empty)(null),
            "b", Direction.OUTGOING, Seq.empty, "c", IdName("r2"), ExpandAll
          )(null),
          "b", Direction.INCOMING, Seq.empty, "a", IdName("r1"), ExpandAll
        )(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(argumentIds = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Argument(Set("a"))(null)()
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("r"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        ProjectEndpoints(Argument(Set("r"))(null)(), "r", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength)(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq(expandSolverStep(_)))
      val pattern1 = PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
      val pattern2 = PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(pattern1, pattern2),
        argumentIds = Set("r1"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan: LogicalPlan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(
          ProjectEndpoints(Argument(Set("r1"))(null)(), "r1", "a", startInScope = false, "b", endInScope = false, None, directed = true, SimplePatternLength)(null),
          "b", Direction.OUTGOING, Seq.empty, "c", "r2", ExpandAll
        )(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq(expandSolverStep(_)))
      qg = QueryGraph(patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("a")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Expand(Argument(Set("a"))(null)(), "a", Direction.OUTGOING, Seq.empty, "b", "r", ExpandAll)(null)
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

  test("should plan big star pattern") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    // keep around, practical for investigating performance
    val numberOfPatternRelationships = 10

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor)
      val patternRels = for (i <- 1 to numberOfPatternRelationships) yield {
        PatternRelationship("r" + i, ("n" + i, "x"), Direction.INCOMING, Seq.empty, SimplePatternLength)
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
        PatternRelationship("r" + i, ("n" + i, "n" + (i + 1)), Direction.INCOMING, Seq.empty, SimplePatternLength)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(argumentIds = Set("a"), patternNodes = Set("a"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Argument(Set("a"))(null)()
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val allNodeScanA = AllNodesScan("a", Set.empty)(null)
      val allNodeScanB = AllNodesScan("b", Set.empty)(null)
      val allNodeScanC = AllNodesScan("c", Set.empty)(null)
      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Selection(cfg.qg.selections.flatPredicates,
          CartesianProduct(
            allNodeScanB,
            CartesianProduct(
              allNodeScanA,
              allNodeScanC
            )(null)
          )(null)
        )(null)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val expandBtoA = Expand(AllNodesScan("b", Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "a", "r", ExpandAll)(null)
      val allNodeScanC = AllNodesScan("c", Set.empty)(null)
      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        Selection(cfg.qg.selections.flatPredicates,
          CartesianProduct(
            allNodeScanC,
            expandBtoA
          )(null)
        )(null)
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
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val qgs = cfg.qg.connectedComponents
      val allNodeScanA: AllNodesScan = AllNodesScan("a", Set.empty)(solved)
      val expandAtoB = Expand(Argument(Set("a"))(solved)(), "a", Direction.OUTGOING, Seq.empty, "b", "r")(solved)
      val expandBtoA = Expand(AllNodesScan("b", Set.empty)(solved), "b", Direction.INCOMING, Seq.empty, "a", "r")(solved)
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
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(// MATCH a, b OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a", "b"),
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a", "b"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        OuterHashJoin(
          Set("a", "b"),
          CartesianProduct(
            AllNodesScan(IdName("a"), Set.empty)(null),
            AllNodesScan(IdName("b"), Set.empty)(null)
          )(null),
          Expand(
            AllNodesScan(IdName("b"), Set.empty)(null),
            "b", Direction.INCOMING, Seq.empty, "a", "r", ExpandAll
          )(null)
        )(null)
      )
    }
  }

  test("should handle query starting with an optional match") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(// OPTIONAL MATCH a-->b RETURN b a
        patternNodes = Set.empty,
        argumentIds = Set.empty,
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set.empty,
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
        )
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        Apply(
          SingleRow()(solved),
          Optional(
            Expand(
              AllNodesScan("b", Set.empty)(null),
              "b", Direction.INCOMING, Seq.empty, "a", "r", ExpandAll
            )(solved)
          )(solved)
        )(solved)
      )
    }
  }

  test("should handle relationship by id") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq.empty)
      qg = QueryGraph(// MATCH (a)-[r]->(b) WHERE id(r) = 42 RETURN *
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(In(FunctionInvocation(FunctionName("id")(pos), ident("r"))(pos), Collection(Seq(SignedDecimalIntegerLiteral("42")(pos)))(pos))(pos))
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      val plan = queryGraphSolver.plan(cfg.qg)
      plan should equal(
        DirectedRelationshipByIdSeek("r", EntityByIdRhs(Collection(Seq(SignedDecimalIntegerLiteral("42")(pos)))(pos), Some(1)), "a", "b", Set.empty)(null)
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
          PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        ProjectEndpoints(
          ProjectEndpoints(
            Argument(Set("r1", "r2"))(null)(),
            "r2", "b", startInScope = false, "c", endInScope = false, None, directed = true, SimplePatternLength)(null),
          "r1", "a", startInScope = false, "b", endInScope = true, None, directed = true, SimplePatternLength)(null))
    }
  }

  test("should handle passing multiple projectible relationships as arguments") {
    val monitor = mock[IDPQueryGraphSolverMonitor]

    new given {
      queryGraphSolver = IDPQueryGraphSolver(monitor = monitor, solvers = Seq(expandSolverStep(_)))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c", "d"),
        patternRelationships = Set(
          PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("c", "d"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", ("a", "d"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        ),
        argumentIds = Set("a", "b", "c", "d", "r1", "r2")
      )
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx

      queryGraphSolver.plan(cfg.qg) should equal(
        ProjectEndpoints(
          Expand(
            ProjectEndpoints(
              Argument(Set("a", "b", "c", "d", "r1", "r2"))(null)(),
              "r2", "c", startInScope = true, "d", endInScope = true, None, directed = true, SimplePatternLength)(null),
            "a", Direction.OUTGOING, Seq.empty, "d", "r3", ExpandInto)(null),
          "r1", "a", startInScope = true, "b", endInScope = true, None, directed = true, SimplePatternLength)(null))
    }
  }
}
