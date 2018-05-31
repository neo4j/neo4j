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
package org.neo4j.cypher.internal.runtime.planDescription

import org.neo4j.cypher.internal.planner.v3_5.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments._
import org.opencypher.v9_0.util._
import org.opencypher.v9_0.util.attribution.{Id, IdGen, SequentialIdGen}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions.{SemanticDirection, LabelName => AstLabelName, _}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.scalatest.prop.TableDrivenPropertyChecks

class LogicalPlan2PlanDescriptionTest extends CypherFunSuite with TableDrivenPropertyChecks {

  private val CYPHER_VERSION = Version("CYPHER 3.5")
  private val RUNTIME_VERSION = RuntimeVersion("3.5")
  private val PLANNER_VERSION = PlannerVersion("3.5")

  test("tests") {
    implicit val idGen: IdGen = new SequentialIdGen()
    val readOnly = true
    val cardinalities = new Cardinalities
    val id = Id.INVALID_ID

    def attach(plan: LogicalPlan, cardinality: Cardinality): LogicalPlan = {
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
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren,
                            Seq(EstimatedRows(1), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"), PlannerImpl("IDP"),
                                PLANNER_VERSION), Set("a"))

      , attach(AllNodesScan("b", Set.empty), 42.0) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren,
                            Seq(EstimatedRows(42), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"), PlannerImpl("IDP"),
                                PLANNER_VERSION), Set("b"))

      , attach(NodeByLabelScan("node", AstLabelName("X")(DummyPosition(0)), Set.empty), 33.0) ->
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren,
                            Seq(LabelName("X"), EstimatedRows(33), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"),
                                PlannerImpl("IDP"), PLANNER_VERSION), Set("node"))

      , attach(
        NodeByIdSeek("node", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos)), Set.empty),
        333.0) ->
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren,
                            Seq(EstimatedRows(333), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"),
                                PlannerImpl("IDP"), PLANNER_VERSION), Set("node"))

      , attach(NodeIndexSeek("x", LabelToken("Label", LabelId(0)), Seq(PropertyKeyToken("Prop", PropertyKeyId(0))),
                             ManyQueryExpression(ListLiteral(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty),
               23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeek", NoChildren,
                            Seq(Index("Label", Seq("Prop")), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
                                Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(
        NodeUniqueIndexSeek("x", LabelToken("Lebal", LabelId(0)), Seq(PropertyKeyToken("Porp", PropertyKeyId(0))),
                            ManyQueryExpression(ListLiteral(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty),
        95.0) ->
        PlanDescriptionImpl(id, "NodeUniqueIndexSeek", NoChildren,
                            Seq(Index("Lebal", Seq("Porp")), EstimatedRows(95), CYPHER_VERSION, RUNTIME_VERSION,
                                Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(Expand(lhsLP, "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandAll), 95.0) ->
        PlanDescriptionImpl(id, "Expand(All)", SingleChild(lhsPD),
                            Seq(ExpandExpression("a", "r1", Seq.empty, "b", SemanticDirection.OUTGOING, 1, Some(1)),
                                EstimatedRows(95), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"), PlannerImpl("IDP"),
                                PLANNER_VERSION), Set("a", "r1", "b"))

      , attach(Expand(lhsLP, "a", SemanticDirection.OUTGOING, Seq.empty, "a", "r1", ExpandInto), 113.0) ->
        PlanDescriptionImpl(id, "Expand(Into)", SingleChild(lhsPD),
                            Seq(ExpandExpression("a", "r1", Seq.empty, "a", SemanticDirection.OUTGOING, 1, Some(1)),
                                EstimatedRows(113), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"),
                                PlannerImpl("IDP"), PLANNER_VERSION), Set("a", "r1"))

      , attach(NodeHashJoin(Set("a"), lhsLP, rhsLP), 2345.0) ->
        PlanDescriptionImpl(id, "NodeHashJoin", TwoChildren(lhsPD, rhsPD),
                            Seq(KeyNames(Seq("a")), EstimatedRows(2345), CYPHER_VERSION, RUNTIME_VERSION,
                                Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("a", "b"))
    )

    forAll(modeCombinations) {
      case (logicalPlan: LogicalPlan, expectedPlanDescription: PlanDescriptionImpl) =>
        val producedPlanDescription = LogicalPlan2PlanDescription(logicalPlan, IDPPlannerName, readOnly, cardinalities)

        def shouldBeEqual(a: InternalPlanDescription, b: InternalPlanDescription): Unit = {
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
