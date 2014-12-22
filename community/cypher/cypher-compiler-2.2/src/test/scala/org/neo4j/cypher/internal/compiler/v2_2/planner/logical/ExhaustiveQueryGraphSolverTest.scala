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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Equals, HasLabels, LabelName}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.ExhaustiveQueryGraphSolver.PlanProducer
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.applyOptional
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport2, PlannerQuery, QueryGraph, Selections}
import org.neo4j.graphdb.Direction

import scala.collection.{Map, immutable}

class ExhaustiveQueryGraphSolverTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val A = IdName("a")

  test("should plan for a single node pattern") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(
        generatePlanTable(AllNodesScan("a", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a"))))),
        Seq(undefinedPlanProducer))
      qg = QueryGraph(patternNodes = Set("a"))
      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          AllNodesScan("a", Set.empty)(null)
        )
      }
    }
  }

  test("should plan for a single relationship pattern") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(generatePlanTable(
        AllNodesScan("a", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a")))),
        NodeByLabelScan("b", LazyLabel("B"), Set.empty)(PlannerQuery.empty.withGraph(
          QueryGraph(patternNodes = Set("b"),
            selections = Selections.from(HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos)))))
      ), Seq(expandOptions))
      qg = QueryGraph(
        patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        selections = Selections.from(HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))
      )

      labelCardinality = immutable.Map(
        "B" -> Cardinality(10)
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Expand(NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "a", "r")(null)
        )
      }
    }
  }

  test("should plan for a single relationship pattern with labels on both sides") {
    val planTableGenerator = generatePlanTable(
      NodeByLabelScan("a", LazyLabel("A"), Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a"), selections = Selections.from(
        HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos))))),
      NodeByLabelScan("b", LazyLabel("B"), Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("b"), selections =
        Selections.from(
        HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos)))))
    )
    val labelBPredicate = HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos)

    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(leafPlanTableGenerator = planTableGenerator, planProducers = Seq(expandOptions))
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

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Selection(Seq(labelBPredicate),
            Expand(
              NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "b", "r")(null)
          )(null))
      }
    }
  }

  test("should plan for a join between two pattern relationships") {
    // MATCH (a:A)-[r1]->(c)-[r2]->(b:B)
    val planTableGenerator = generatePlanTable(
      NodeByLabelScan("a", LazyLabel("A"), Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a"), selections = Selections.from(
        HasLabels(ident("a"), Seq(LabelName("A")(pos)))(pos))))),
      NodeByLabelScan("b", LazyLabel("B"), Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("b"), selections = Selections.from(
        HasLabels(ident("b"), Seq(LabelName("B")(pos)))(pos))))),
      AllNodesScan("c", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("c"))))
    )
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(leafPlanTableGenerator = planTableGenerator, planProducers =
        Seq(expandOptions, joinOptions))
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

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          NodeHashJoin(Set("c"),
            Expand(
              NodeByLabelScan("a", LazyLabel("A"), Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "c", "r1")(null),
            Expand(
              NodeByLabelScan("b", LazyLabel("B"), Set.empty)(null), "b", Direction.INCOMING, Seq.empty, "c", "r2")(null)
          )(null))
      }
    }
  }

  test("should plan a simple argument row when everything is covered") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(
        generatePlanTable(
          Argument(Set("a"))(PlannerQuery(graph = QueryGraph(patternNodes = Set("a"), argumentIds = Set("a"))))()),
        Seq(undefinedPlanProducer))
      qg = QueryGraph(patternNodes = Set("a"), argumentIds = Set("a"))
      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Argument(Set("a"))(null)()
        )
      }
    }
  }

  test("should plan a relationship pattern based on an argument row since part of the node pattern is already solved") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(
        generatePlanTable(
          Argument(Set("a"))(PlannerQuery(graph = QueryGraph(patternNodes = Set("a"), argumentIds = Set("a"))))(),
          AllNodesScan("b", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("b"), argumentIds = Set("a"))))
        ),
        Seq(expandOptions))
      qg = QueryGraph(patternNodes = Set("a", "b"),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)),
        argumentIds = Set("a"))

      cardinality = Map(
        Argument(Set("a"))(null)() -> Cardinality(10),
        AllNodesScan("b", Set.empty)(null) -> Cardinality(100)
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Expand(Argument(Set("a"))(null)(), "a", Direction.OUTGOING, Seq.empty, "b", "r", ExpandAll)(null)
        )
      }
    }
  }

  test("should produce no plans for expand and join when the considered sub-query are not solved") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(
        generatePlanTable(
          AllNodesScan("corp", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("corp")))),
          AllNodesScan("a1", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a1")))),
          AllNodesScan("a2", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a2")))),
          AllNodesScan("c", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("c")))),
          AllNodesScan("v", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("v"))))
        ),
        Seq(expandOptions, joinOptions))
      qg = QueryGraph(patternNodes = Set("corp", "a1", "a2", "c", "v"),
        patternRelationships = Set(
          PatternRelationship("r1", ("corp", "a1"), Direction.INCOMING, Seq.empty, SimplePatternLength),
          PatternRelationship("r2", ("a1", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r3", ("c", "v"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
          PatternRelationship("r4", ("corp", "a2"), Direction.INCOMING, Seq.empty, SimplePatternLength),
          PatternRelationship("r5", ("a2", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
        ))

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) // should not throw
      }
    }
  }

  test("should plan cartesian product between 3 pattern nodes") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(
        generatePlanTable(
          AllNodesScan("a", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a")))),
          AllNodesScan("b", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("b")))),
          AllNodesScan("c", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("c"))))
        ),
        Seq(expandOptions,joinOptions))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)))

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          CartesianProduct(
            AllNodesScan("a", Set.empty)(null),
            Selection(qg.selections.flatPredicates,
              CartesianProduct(
                AllNodesScan("b", Set.empty)(null),
                AllNodesScan("c", Set.empty)(null)
              )(null)
            )(null)
          )(null)
        )
      }
    }
  }

  test("should plan cartesian product between 1 pattern nodes and 1 pattern relationship") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(
        generatePlanTable(
          AllNodesScan("a", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("a")))),
          AllNodesScan("b", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("b")))),
          AllNodesScan("c", Set.empty)(PlannerQuery(graph = QueryGraph(patternNodes = Set("c"))))
        ),
        Seq(expandOptions,joinOptions))
      qg = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Equals(ident("b"), ident("c"))(pos)),
        patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Selection(qg.selections.flatPredicates,
            CartesianProduct(
                Expand(AllNodesScan("a", Set.empty)(null), "a", Direction.OUTGOING, Seq.empty, "b", "r", ExpandAll)(null),
              AllNodesScan("c", Set.empty)(null)
            )(null)
          )(null)
        )
      }
    }
  }

  test("should plan for optional single relationship pattern") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults()
      qg = QueryGraph(// MATCH a OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a"),
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )

      labelCardinality = immutable.Map(
        "B" -> Cardinality(10)
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Apply(
            AllNodesScan("a", Set.empty)(null),
            Optional(
              Expand(Argument(Set("a"))(null)(), "a", Direction.OUTGOING, Seq.empty, "b", "r")(null)
            )(null)
          )(null)
        )
      }
    }
  }

  test("should solve empty graph with SingleRow") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults()
      qg = QueryGraph.empty

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          SingleRow()
        )
      }
    }
  }

  test("should solve plan an empty QG with arguments") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults()
      qg = QueryGraph(argumentIds = Set(A), patternNodes = Set(A))

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Argument(Set(A))(PlannerQuery(graph = qg))()
        )
      }
    }
  }

  test("should solve self looping pattern") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults()
      qg = QueryGraph(
        patternNodes = Set(A),
        patternRelationships = Set(PatternRelationship("r", ("a", "a"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Expand(AllNodesScan(A, Set.empty)(null), A, Direction.OUTGOING, Seq.empty, A, IdName("r"), ExpandInto)(null)
        )
      }
    }
  }

  test("should plan for optional single relationship pattern between two known nodes") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(optionalSolvers = Seq(applyOptional))
      qg = QueryGraph(// MATCH a, b OPTIONAL MATCH a-[r]->b
        patternNodes = Set("a", "b"),
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a", "b"),
          argumentIds = Set("a", "b"),
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength))
        ))
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Apply(
            CartesianProduct(
              AllNodesScan(IdName("a"), Set.empty)(null),
              AllNodesScan(IdName("b"), Set.empty)(null)
            )(null),
            Optional(
              Expand(
                Argument(Set("a", "b"))(null)(),
                "a", Direction.OUTGOING, Seq.empty, "b", "r", ExpandInto
              )(null)
            )(null)
          )(null)
        )
      }
    }
  }

  test("should handle query starting with an optional match") {
    new given {
      queryGraphSolver = ExhaustiveQueryGraphSolver.withDefaults(optionalSolvers = Seq(applyOptional))
      qg = QueryGraph( // OPTIONAL MATCH a-->b RETURN b a
        patternNodes = Set.empty,
        argumentIds = Set.empty,
        optionalMatches = Seq(QueryGraph(
          patternNodes = Set("a","b"),
          argumentIds = Set.empty,
          patternRelationships = Set(PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)))
        )
      )

      withLogicalPlanningContext { (ctx) =>
        implicit val x = ctx

        queryGraphSolver.plan(qg) should equal(
          Apply(
            SingleRow(),
            Optional(
              Expand(
                AllNodesScan("a",Set.empty)(null),
                "a", Direction.OUTGOING, Seq.empty, "b", "r", ExpandAll
              )(null)
            )(null)
          )(null)
        )
      }
    }
  }

  private val undefinedPlanProducer: PlanProducer = new PlanProducer {
    def apply(qg: QueryGraph, cache: PlanTable): Seq[LogicalPlan] = ???
  }

  private def generatePlanTable(plans: LogicalPlan*): PlanTableGenerator = {
    new PlanTableGenerator {
      def apply(qg: QueryGraph, plan: Option[LogicalPlan])(implicit context: LogicalPlanningContext): PlanTable =
        ExhaustivePlanTable(plans: _*)
    }
  }
}
