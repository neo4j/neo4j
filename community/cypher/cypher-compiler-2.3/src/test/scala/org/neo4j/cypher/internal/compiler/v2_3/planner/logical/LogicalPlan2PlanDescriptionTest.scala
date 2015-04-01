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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.commands.ManyQueryExpression
import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{Index, EstimatedRows, LabelName}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{Id, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v2_3.{InputPosition, LabelId, PropertyKeyId}
import org.scalatest.prop.TableDrivenPropertyChecks

class LogicalPlan2PlanDescriptionTest extends CypherFunSuite with TableDrivenPropertyChecks {
  test("tests") {
    implicit def emptySolvedWithCardinality(i: Int): PlannerQuery with CardinalityEstimation =
      CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(i))
    val pos = InputPosition(0, 0, 0)
    val id = new Id
    val modeCombinations = Table(
      "logical plan" -> "expected plan description",

      AllNodesScan(IdName("a"), Set.empty)(1) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(1)), Set("a"))

      , AllNodesScan(IdName("b"), Set.empty)(42) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(42)), Set("b"))

      , NodeByLabelScan(IdName("node"), LazyLabel("X"), Set.empty)(33) ->
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(LabelName("X"), EstimatedRows(33)), Set("node"))

      , NodeByIdSeek(IdName("node"), EntityByIdExprs(Seq(SignedDecimalIntegerLiteral("1")(pos))), Set.empty)(333) ->
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(EstimatedRows(333)), Set("node"))

      , NodeIndexSeek(IdName("x"), LabelToken("Label", LabelId(0)), PropertyKeyToken("Prop", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty)(23) ->
        PlanDescriptionImpl(id, "NodeIndexSeek", NoChildren, Seq(Index("Label", "Prop"), EstimatedRows(23)), Set("x"))

      , NodeIndexUniqueSeek(IdName("x"), LabelToken("Lebal", LabelId(0)), PropertyKeyToken("Porp", PropertyKeyId(0)), ManyQueryExpression(Collection(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty)(95) ->
        PlanDescriptionImpl(id, "NodeIndexUniqueSeek", NoChildren, Seq(Index("Lebal", "Porp"), EstimatedRows(95)), Set("x"))
    )

    forAll(modeCombinations) {
      case (logicalPlan: LogicalPlan, expectedPlanDescription: PlanDescriptionImpl) =>
        val idMap = LogicalPlanIdentificationBuilder(logicalPlan)
        LogicalPlan2PlanDescription(logicalPlan, idMap) should equal(expectedPlanDescription.copy(id = idMap(logicalPlan)))
    }

  }
}
