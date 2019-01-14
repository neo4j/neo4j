/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.planDescription

import org.neo4j.cypher.internal.planner.v3_4.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, ReadOnlies}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.util.v3_4._
import org.neo4j.cypher.internal.util.v3_4.attribution.{Id, SequentialIdGen}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.{SemanticDirection, LabelName => AstLabelName, _}
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.scalatest.prop.TableDrivenPropertyChecks

class LogicalPlan2PlanDescriptionTest extends CypherFunSuite with TableDrivenPropertyChecks {

  test("tests") {
    implicit val idGen = new SequentialIdGen()
    val readOnlies = new ReadOnlies
    val cardinalities = new Cardinalities
    val id = Id.INVALID_ID

    def attach(plan: LogicalPlan, cardinality: Cardinality): LogicalPlan = {
      readOnlies.set(plan.id, true)
      cardinalities.set(plan.id, cardinality)
      plan
    }

    val lhsLP = attach(AllNodesScan("a", Set.empty), 2.0)
    val lhsPD = PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(2)), Set("a"))

    val rhsLP = attach(AllNodesScan("b", Set.empty), 2.0)
    val rhsPD = PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(2)), Set("b"))

    val pos = InputPosition(0, 0, 0)
    val modeCombinations = Table(
      "logical plan" -> "expected plan description",

      attach(AllNodesScan("a", Set.empty), 1.0) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(1), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("a"))

      , attach(AllNodesScan("b", Set.empty), 42.0) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(42), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("b"))

      , attach(NodeByLabelScan("node", AstLabelName("X")(DummyPosition(0)), Set.empty), 33.0) ->
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(LabelName("X"), EstimatedRows(33), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("node"))

      , attach(NodeByIdSeek("node", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos)), Set.empty), 333.0) ->
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(EstimatedRows(333), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("node"))

      , attach(NodeIndexSeek("x", LabelToken("Label", LabelId(0)), Seq(PropertyKeyToken("Prop", PropertyKeyId(0))), ManyQueryExpression(ListLiteral(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeek", NoChildren, Seq(Index("Label", Seq("Prop")), EstimatedRows(23), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("x"))

      , attach(NodeUniqueIndexSeek("x", LabelToken("Lebal", LabelId(0)), Seq(PropertyKeyToken("Porp", PropertyKeyId(0))), ManyQueryExpression(ListLiteral(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty), 95.0) ->
        PlanDescriptionImpl(id, "NodeUniqueIndexSeek", NoChildren, Seq(Index("Lebal", Seq("Porp")), EstimatedRows(95), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("x"))

      , attach(Expand(lhsLP, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll), 95.0) ->
        PlanDescriptionImpl(id, "Expand(All)", SingleChild(lhsPD), Seq(ExpandExpression("a", "r1", Seq.empty, "b", SemanticDirection.OUTGOING, 1, Some(1)),
          EstimatedRows(95), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("a", "r1", "b"))

      , attach(Expand(lhsLP, "a", SemanticDirection.OUTGOING, Seq.empty, "a", "r1", ExpandInto), 113.0) ->
        PlanDescriptionImpl(id, "Expand(Into)", SingleChild(lhsPD), Seq(ExpandExpression("a", "r1", Seq.empty, "a", SemanticDirection.OUTGOING, 1, Some(1)),
          EstimatedRows(113), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("a", "r1"))

      , attach(NodeHashJoin(Set("a"), lhsLP, rhsLP), 2345.0) ->
        PlanDescriptionImpl(id, "NodeHashJoin", TwoChildren(lhsPD, rhsPD), Seq(KeyNames(Seq("a")), EstimatedRows(2345), Version("CYPHER 3.4"), RuntimeVersion("3.4"), Planner("COST"), PlannerImpl("IDP"), PlannerVersion("3.4")), Set("a", "b"))
    )

    forAll(modeCombinations) {
      case (logicalPlan: LogicalPlan, expectedPlanDescription: PlanDescriptionImpl) =>
        val producedPlanDescription = LogicalPlan2PlanDescription(logicalPlan, IDPPlannerName, readOnlies, cardinalities)

        def shouldBeEqual(a: InternalPlanDescription, b: InternalPlanDescription) = {
          withClue("name")(a.name should equal(b.name))
          withClue("arguments")(a.arguments should equal(b.arguments))
          withClue("variables")(a.variables should equal(b.variables))
        }

        shouldBeEqual(producedPlanDescription, expectedPlanDescription)

        withClue("children") {
          producedPlanDescription.children match {
            case NoChildren =>
              expectedPlanDescription.children should equal(NoChildren)
            case SingleChild(child) =>
              shouldBeEqual(child, lhsPD)
            case TwoChildren(l, r) =>
              shouldBeEqual(l, lhsPD)
              shouldBeEqual(r, rhsPD)
          }
        }
    }
  }
}
