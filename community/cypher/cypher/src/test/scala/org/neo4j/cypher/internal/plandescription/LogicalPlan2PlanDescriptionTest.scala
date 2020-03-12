/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.QueryPlanTestSupport.StubExecutionPlan
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.plandescription.Arguments.Index
import org.neo4j.cypher.internal.plandescription.Arguments.KeyNames
import org.neo4j.cypher.internal.plandescription.Arguments.LabelName
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableFor2

class LogicalPlan2PlanDescriptionTest extends CypherFunSuite with TableDrivenPropertyChecks {

  private val CYPHER_VERSION = Version("CYPHER 4.1")
  private val RUNTIME_VERSION = RuntimeVersion("4.1")
  private val PLANNER_VERSION = PlannerVersion("4.1")

  test("tests") {
    implicit val idGen: IdGen = new SequentialIdGen()
    val readOnly = true
    val cardinalities = new Cardinalities
    val providedOrders = new ProvidedOrders
    val id = Id.INVALID_ID

    def attach(plan: LogicalPlan, cardinality: Cardinality, providedOrder: ProvidedOrder = ProvidedOrder.empty): LogicalPlan = {
      cardinalities.set(plan.id, cardinality)
      providedOrders.set(plan.id, providedOrder)
      plan
    }

    val lhsLP = attach(AllNodesScan("a", Set.empty), 2.0, ProvidedOrder.empty)
    val lhsPD = PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(2)), Set("a"))

    val rhsLP = attach(AllNodesScan("b", Set.empty), 2.0, ProvidedOrder.empty)
    val rhsPD = PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(EstimatedRows(2)), Set("b"))

    val pos = InputPosition(0, 0, 0)
    val modeCombinations: TableFor2[LogicalPlan, PlanDescriptionImpl] = Table(
      "logical plan" -> "expected plan description",

      attach(AllNodesScan("a", Set.empty), 1.0, ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("a"))))) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren,
          Seq(EstimatedRows(1), Order(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("a"))))), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"), PlannerImpl("IDP"),
            PLANNER_VERSION), Set("a"))

      , attach(AllNodesScan("b", Set.empty), 42.0, ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("b")), ProvidedOrder.Desc(prop("b", "foo"))))) ->
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren,
          Seq(EstimatedRows(42), Order(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("b")), ProvidedOrder.Desc(prop("b", "foo"))))), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"), PlannerImpl("IDP"),
            PLANNER_VERSION), Set("b"))

      , attach(NodeByLabelScan("node", expressions.LabelName("X")(pos), Set.empty), 33.0) ->
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren,
          Seq(LabelName("X"), EstimatedRows(33), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"),
            PlannerImpl("IDP"), PLANNER_VERSION), Set("node"))

      , attach(
        NodeByIdSeek("node", ManySeekableArgs(ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos)), Set.empty),
        333.0) ->
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren,
          Seq(EstimatedRows(333), CYPHER_VERSION, RUNTIME_VERSION, Planner("COST"),
            PlannerImpl("IDP"), PLANNER_VERSION), Set("node"))

      , attach(IndexSeek("x:Label(Prop)"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren,
          Seq(Index("x:Label(Prop) WHERE exists(Prop)"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop)", getValue = GetValue), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren,
          Seq(Index("x:Label(Prop) WHERE exists(Prop), cache[x.Prop]"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop,Foo)"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren,
          Seq(Index("x:Label(Prop, Foo) WHERE exists(Prop) AND exists(Foo)"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop,Foo)", getValue = GetValue), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren,
          Seq(Index("x:Label(Prop, Foo) WHERE exists(Prop) AND exists(Foo), cache[x.Prop], cache[x.Foo]"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop = 'Andres')"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeek", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop = \"Andres\""), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop = 'Andres')", getValue = GetValue), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeek", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop = \"Andres\", cache[x.Prop]"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop = 'Andres')", unique = true), 23.0) ->
        PlanDescriptionImpl(id, "NodeUniqueIndexSeek", NoChildren,
          Seq(Index("x:Label UNIQUE(Prop) WHERE Prop = \"Andres\""), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeek", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')", unique = true), 23.0) ->
        PlanDescriptionImpl(id, "NodeUniqueIndexSeek", NoChildren,
          Seq(Index("x:Label UNIQUE(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop > 9)"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeekByRange", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop > 9"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop < 9)"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeekByRange", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop < 9"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(9 <= Prop <= 11)"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeekByRange", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop >= 9 AND Prop <= 11"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      // This is ManyQueryExpression with only a single expression. That is possible to get, but the test utility IndexSeek cannot create those.
      , attach(
        NodeUniqueIndexSeek("x", LabelToken("Label", LabelId(0)), Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue)),
          ManyQueryExpression(ListLiteral(Seq(StringLiteral("Andres")(pos)))(pos)), Set.empty, IndexOrderNone),
        95.0) ->
        PlanDescriptionImpl(id, "NodeUniqueIndexSeek", NoChildren,
          Seq(Index("x:Label UNIQUE(Prop) WHERE Prop = \"Andres\""), EstimatedRows(95), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(
        NodeIndexSeek("x", LabelToken("Label", LabelId(0)), Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue)),
          RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(
            FunctionInvocation(MapExpression(Seq(
              (PropertyKeyName("x")(pos), SignedDecimalIntegerLiteral("1")(pos)),
              (PropertyKeyName("y")(pos), SignedDecimalIntegerLiteral("2")(pos)),
              (PropertyKeyName("crs")(pos), StringLiteral("cartesian")(pos))
            ))(pos), FunctionName(Point.name)(pos)), SignedDecimalIntegerLiteral("10")(pos), inclusive = true
          ))(pos)),
          Set.empty, IndexOrderNone),
        95.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeekByRange", NoChildren,
          Seq(Index("x:Label(Prop) WHERE distance(Prop, point(1, 2, \"cartesian\")) <= 10"), EstimatedRows(95), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop STARTS WITH 'Foo')"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeekByRange", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop STARTS WITH \"Foo\""), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop ENDS WITH 'Foo')"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexEndsWithScan", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop ENDS WITH \"Foo\""), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop CONTAINS 'Foo')"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexContainsScan", NoChildren,
          Seq(Index("x:Label(Prop) WHERE Prop CONTAINS \"Foo\""), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop = 10,Foo = 12)"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeek(equality,equality)", NoChildren,
          Seq(Index("x:Label(Prop, Foo) WHERE Prop = 10 AND Foo = 12"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop = 10,Foo = 12)", unique = true), 23.0) ->
        PlanDescriptionImpl(id, "NodeUniqueIndexSeek(equality,equality)", NoChildren,
          Seq(Index("x:Label UNIQUE(Prop, Foo) WHERE Prop = 10 AND Foo = 12"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("x"))

      , attach(IndexSeek("x:Label(Prop > 10,Foo)"), 23.0) ->
        PlanDescriptionImpl(id, "NodeIndexSeek(range,exists)", NoChildren,
          Seq(Index("x:Label(Prop, Foo) WHERE Prop > 10 AND exists(Foo)"), EstimatedRows(23), CYPHER_VERSION, RUNTIME_VERSION,
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

      , attach(Input(Seq("a", "b")), 4.0) ->
        PlanDescriptionImpl(id, "Input", NoChildren,
          Seq(EstimatedRows(4), CYPHER_VERSION, RUNTIME_VERSION,
            Planner("COST"), PlannerImpl("IDP"), PLANNER_VERSION), Set("a", "b"))
    )

    forAll(modeCombinations) {
      case (logicalPlan: LogicalPlan, expectedPlanDescription: PlanDescriptionImpl) =>
        val producedPlanDescription = LogicalPlan2PlanDescription(logicalPlan, IDPPlannerName, CypherVersion.default, readOnly, cardinalities, providedOrders, StubExecutionPlan())

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
  private val pos: InputPosition = DummyPosition(0)
  private def varFor(name: String): Variable = Variable(name)(pos)
  private def prop(varName: String, propName: String): Property = Property(varFor(varName), PropertyKeyName(propName)(pos))(pos)
}
