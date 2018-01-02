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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.QueryGraphProducer
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{OUTGOING, INCOMING}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class TriadicSelectionFinderTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer {

  implicit val ctx = newMockedLogicalPlanningContext(mock[PlanContext])

  test("empty plan passes through") {
    val plan = newMockedLogicalPlan()

    triadicSelectionFinder(plan, QueryGraph.empty) shouldBe empty
  }

  // Negative Predicate Expression

  test("MATCH (a:X)-->(b)-->(c) WHERE NOT (a)-->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b)-[r2]->(c) WHERE NOT (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase()
    val triadic = produceTriadicTestPlan(expand1)

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("B"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE NOT (a)<-[:A]-(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE NOT (a)<-[:A]-(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("A"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE NOT (a:X)-[:A]->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)-[r2:A]->(c) WHERE NOT (a)-[:A]->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("A"))
    val triadic = produceTriadicTestPlan(expand1, r1Types = Seq("A"), r2Types = Seq("A"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE NOT (a:X)-[:A]->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)-[r2:B]->(c) WHERE NOT (a)-[:A]->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("B"))
    val triadic = produceTriadicTestPlan(expand1, r1Types = Seq("A"), r2Types = Seq("B"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-[:A]->(b)<-[:B]-(c) WHERE NOT (a:X)-[:A]->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)<-[r2:B]-(c) WHERE NOT (a)-[:A]->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("B"), r2Direction = INCOMING)
    val triadic = produceTriadicTestPlan(expand1, r1Types = Seq("A"), r2Types = Seq("B"), r2Direction = INCOMING)

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  // Positive Predicate Expression

  test("MATCH (a:X)-->(b)-[:A]->(c) WHERE (a:X)-[:A]->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b)-[r2:A]->(c) WHERE (a)-[:A]->(c)")
    val (expand1, selection) = produceTriadicTestCase(r2Types = Seq("A"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-->(b)-->(c) WHERE (a)-->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b)-[r2]->(c) WHERE (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase()
    val triadic = produceTriadicTestPlan(expand1, predicateExpressionCase = true)

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)-[r2:B]->(c) WHERE (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("B"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE (a)<-[:A]-(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)-[r2:A]->(c) WHERE (a)<-[:A]-(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("A"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-[:A]->(b)-[:A]->(c) WHERE (a:X)-[:A]->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)-[r2:A]->(c) WHERE (a)-[:A]->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("A"))
    val triadic = produceTriadicTestPlan(expand1, predicateExpressionCase = true, r1Types = Seq("A"), r2Types = Seq("A"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-[:A]->(b)-[:B]->(c) WHERE (a:X)-[:A]->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)-[r2:B]->(c) WHERE (a)-[:A]->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("B"))
    val triadic = produceTriadicTestPlan(expand1, predicateExpressionCase = true, r1Types = Seq("A"), r2Types = Seq("B"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-[:A]->(b)<-[:B]-(c) WHERE (a:X)-[:A]->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1:A]->(b)<-[r2:B]-(c) WHERE (a)-[:A]->(c)")
    val (expand1, selection) = produceTriadicTestCase(r1Types = Seq("A"), r2Types = Seq("B"), r2Direction = INCOMING)
    val triadic = produceTriadicTestPlan(expand1, predicateExpressionCase = true, r1Types = Seq("A"), r2Types = Seq("B"), r2Direction = INCOMING)

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  // Negative Predicate Expression and matching labels

  test("MATCH (a:X)-->(b:Y)-->(c:Y) WHERE NOT (a)-->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Y) WHERE NOT (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y"), cLabels = Seq("Y"))
    val triadic = produceTriadicTestPlan(expand1, bLabels = Seq("Y"), cLabels = Seq("Y"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-->(b:Y)-->(c:Z) WHERE NOT (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Z) WHERE NOT (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y"), cLabels = Seq("Z"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-->(b:Y)-->(c) WHERE NOT (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c) WHERE NOT (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-->(b)-->(c:Z) WHERE NOT (a)-->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b)-[r2]->(c:Z) WHERE NOT (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(cLabels = Seq("Z"))
    val triadic = produceTriadicTestPlan(expand1, cLabels = Seq("Z"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-->(b:Y:Z)-->(c:Z) WHERE NOT (a)-->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y:Z)-[r2]->(c:Z) WHERE NOT (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y", "Z"), cLabels = Seq("Z"))
    val triadic = produceTriadicTestPlan(expand1, bLabels = Seq("Y", "Z"), cLabels = Seq("Z"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  // Positive Predicate Expression and matching labels

  test("MATCH (a:X)-->(b:Y)-->(c:Y) WHERE (a)-->(c)") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Y) WHERE (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y"), cLabels = Seq("Y"))
    val triadic = produceTriadicTestPlan(expand1, predicateExpressionCase = true, bLabels = Seq("Y"), cLabels = Seq("Y"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) should contain only triadic
  }

  test("MATCH (a:X)-->(b:Y)-->(c:Z) WHERE (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c:Z) WHERE (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y"), cLabels = Seq("Z"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-->(b:Y)-->(c) WHERE (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y)-[r2]->(c) WHERE (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-->(b)-->(c:Z) WHERE (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b)-[r2]->(c:Z) WHERE (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(cLabels = Seq("Z"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  test("MATCH (a:X)-->(b:Y:Z)-->(c:Z) WHERE (a)-->(c) passes through") {
    implicit val (plannerQuery, semanticTable) = producePlannerQueryForPattern("MATCH (a:X)-[r1]->(b:Y:Z)-[r2]->(c:Z) WHERE (a)-->(c)")
    val (expand1, selection) = produceTriadicTestCase(bLabels = Seq("Y", "Z"), cLabels = Seq("Z"))

    triadicSelectionFinder(selection, plannerQuery.lastQueryGraph) shouldBe empty
  }

  private def produceTriadicTestCase(aLabel: String = "X",
                                     r1Types: Seq[String] = Seq.empty, r1Direction: SemanticDirection = OUTGOING, bLabels: Seq[String] = Seq.empty,
                                     r2Types: Seq[String] = Seq.empty, r2Direction: SemanticDirection = OUTGOING, cLabels: Seq[String] = Seq.empty): (Expand, Selection) = {
    val lblScan = NodeByLabelScan(IdName("a"), LazyLabel(aLabel), Set.empty)(solved)
    val expand1 = Expand(lblScan, IdName("a"), OUTGOING, r1Types.map(RelTypeName(_)(pos)), IdName("b"), IdName("r1"), ExpandAll)(solved)
    val expand2 = Expand(expand1, IdName("b"), r2Direction, r2Types.map(RelTypeName(_)(pos)), IdName("c"), IdName("r2"), ExpandAll)(solved)
    val relationshipUniqueness = Not(Equals(Identifier("r1")(pos), Identifier("r2")(pos))(pos))(pos)
    val labelPredicates = cLabels.map(lbl => HasLabels(Identifier("c")(pos), Seq(LabelName(lbl)(pos)))(pos))
    val selection = Selection(labelPredicates :+ relationshipUniqueness, expand2)(solved)
    (expand1, selection)
  }

  private def produceTriadicTestPlan(expand1: Expand, predicateExpressionCase: Boolean = false,
                                     r1Types: Seq[String] = Seq.empty, r1Direction: SemanticDirection = OUTGOING, bLabels: Seq[String] = Seq.empty,
                                     r2Types: Seq[String] = Seq.empty, r2Direction: SemanticDirection = OUTGOING, cLabels: Seq[String] = Seq.empty) = {
    val argument = Argument(expand1.availableSymbols)(solved)()
    val expand2B = Expand(argument, IdName("b"), r2Direction, r2Types.map(RelTypeName(_)(pos)), IdName("c"), IdName("r2"), ExpandAll)(solved)
    val relationshipUniqueness = Not(Equals(Identifier("r1")(pos), Identifier("r2")(pos))(pos))(pos)
    val labelPredicates = cLabels.map(lbl => HasLabels(Identifier("c")(pos), Seq(LabelName(lbl)(pos)))(pos))
    val selectionB = Selection(labelPredicates :+ relationshipUniqueness, expand2B)(solved)
    TriadicSelection(predicateExpressionCase, expand1, IdName("a"), IdName("b"), IdName("c"), selectionB)(solved)
  }
}
