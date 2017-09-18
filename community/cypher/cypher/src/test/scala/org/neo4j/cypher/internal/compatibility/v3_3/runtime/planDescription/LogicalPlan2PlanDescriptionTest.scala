/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments._
import org.neo4j.cypher.internal.compiler.v3_3.IDPPlannerName
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.ast.{LabelName => AstLabelName, _}
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.{Cardinality, CardinalityEstimation, IdName, PlannerQuery}
import org.neo4j.cypher.internal.v3_3.logical.plans._
import org.scalatest.prop.TableDrivenPropertyChecks

class LogicalPlan2PlanDescriptionTest extends CypherFunSuite with TableDrivenPropertyChecks {

  test("tests") {

    implicit def emptySolvedWithCardinality(i: Int): PlannerQuery with CardinalityEstimation =
      CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(i))

    val lhsLP = AllNodesScan(IdName("a"), Set.empty)(2)
    val lhsPD = PlanDescriptionImpl(LogicalPlanId.DEFAULT, "AllNodesScan", NoChildren, Seq(EstimatedRows(2)), Set("a"))

    val rhsPD = PlanDescriptionImpl(LogicalPlanId.DEFAULT, "AllNodesScan", NoChildren, Seq(EstimatedRows(2)), Set("b"))
    val rhsLP = AllNodesScan(IdName("b"), Set.empty)(2)

    val pos = InputPosition(0, 0, 0)
    val id = LogicalPlanId.DEFAULT
    val modeCombinations = Table(
      "logical plan" -> "expected plan description",

      AllNodesScan(IdName("a"), Set.empty)(1) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(1), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("a"))

      , AllNodesScan(IdName("b"), Set.empty)(42) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(42), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("b"))

      , NodeByLabelScan(IdName("node"), AstLabelName("X")(DummyPosition(0)), Set.empty)(33) ->
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(LabelName("X"), EstimatedRows(33), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("node"))

      , NodeByIdSeek(IdName("node"), ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos)), Set.empty)(333) ->
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(EstimatedRows(333), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("node"))

      , NodeIndexSeek(IdName("x"), LabelToken("Label", LabelId(0)), Seq(PropertyKeyToken("Prop", PropertyKeyId(0))), ManyQueryExpression(ListLiteral(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty)(23) ->
        PlanDescriptionImpl(id, "NodeIndexSeek", NoChildren, Seq(Index("Label", Seq("Prop")), EstimatedRows(23), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("x"))

      , NodeUniqueIndexSeek(IdName("x"), LabelToken("Lebal", LabelId(0)), Seq(PropertyKeyToken("Porp", PropertyKeyId(0))), ManyQueryExpression(ListLiteral(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty)(95) ->
        PlanDescriptionImpl(id, "NodeUniqueIndexSeek", NoChildren, Seq(Index("Lebal", Seq("Porp")), EstimatedRows(95), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("x"))

      , Expand(lhsLP, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(95) ->
        PlanDescriptionImpl(id, "Expand(All)", SingleChild(lhsPD), Seq(ExpandExpression("a", "r1", Seq.empty, "b", SemanticDirection.OUTGOING, 1, Some(1)),
                                                                       EstimatedRows(95), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("a", "r1", "b"))

      , Expand(lhsLP, IdName("a"), SemanticDirection.OUTGOING, Seq.empty, IdName("a"), IdName("r1"), ExpandInto)(113) ->
        PlanDescriptionImpl(id, "Expand(Into)", SingleChild(lhsPD), Seq(ExpandExpression("a", "r1", Seq.empty, "a", SemanticDirection.OUTGOING, 1, Some(1)),
                                                                        EstimatedRows(113), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("a", "r1"))

      , NodeHashJoin(Set(IdName("a")), lhsLP, rhsLP)(2345) ->
        PlanDescriptionImpl(id, "NodeHashJoin", TwoChildren(lhsPD, rhsPD), Seq(KeyNames(Seq("a")), EstimatedRows(2345), Version("CYPHER 3.3"), Planner("COST"), PlannerImpl("IDP")), Set("a", "b"))
    )

    forAll(modeCombinations) {
      case (logicalPlan: LogicalPlan, expectedPlanDescription: PlanDescriptionImpl) =>
        logicalPlan.assignIds()
        val producedPlanDescription = LogicalPlan2PlanDescription(logicalPlan, IDPPlannerName)

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
            case TwoChildren(l,r) =>
              shouldBeEqual(l, lhsPD)
              shouldBeEqual(r, rhsPD)
          }
        }
    }
  }
}
