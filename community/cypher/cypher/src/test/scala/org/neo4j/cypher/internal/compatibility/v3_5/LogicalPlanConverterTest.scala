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
package org.neo4j.cypher.internal.compatibility.v3_5

import java.lang.reflect.Modifier

import org.neo4j.cypher.internal.compatibility.v3_5.SemanticTableConverter.ExpressionMapping4To5
import org.neo4j.cypher.internal.ir.{v3_5 => irV3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities => CardinalitiesV3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Solveds => SolvedsV3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{ProvidedOrders => ProvidedOrdersV3_5}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{Cardinalities => CardinalitiesV4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{Solveds => SolvedsV4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.{ProvidedOrders => ProvidedOrdersV4_0}
import org.neo4j.cypher.internal.planner.v4_0.spi.{PlanningAttributes => PlanningAttributesV4_0}
import org.neo4j.cypher.internal.planner.v3_5.spi.{PlanningAttributes => PlanningAttributesV3_5}
import org.neo4j.cypher.internal.v3_5.logical.{plans => plansV3_5}
import org.neo4j.cypher.internal.v4_0.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.v4_0.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.v4_0.logical.{plans => plansV4_0}
import org.neo4j.cypher.internal.v4_0.util.attribution.{SequentialIdGen => SequentialIdGenV4_0}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.NonEmptyList
import org.neo4j.cypher.internal.v4_0.util.{symbols => symbolsV4_0}
import org.neo4j.cypher.internal.v4_0.{ast => astV4_0}
import org.neo4j.cypher.internal.v4_0.{util => utilV4_0}
import org.neo4j.cypher.internal.v4_0.{expressions => expressionsV4_0}
import org.opencypher.v9_0.util.attribution.{SequentialIdGen => SequentialIdGenV3_5}
import org.opencypher.v9_0.util.{InputPosition => InputPositionV3_5}
import org.opencypher.v9_0.util.{symbols => symbolsV3_5}
import org.opencypher.v9_0.{ast => astV3_5}
import org.opencypher.v9_0.{expressions => expressionsV3_5}
import org.opencypher.v9_0.{util => utilV3_5}
import org.reflections.Reflections

import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class LogicalPlanConverterTest extends CypherFunSuite {

  private implicit val idGen3_5 = new SequentialIdGenV3_5()
  private implicit val idGen4_0 = new SequentialIdGenV4_0()

  private val pos3_5 = InputPositionV3_5(0,0,0)
  private val pos4_0 = InputPosition(0,0,0)
  // We use these package names to enumerate all classes of a certain type in these packages and test
  // for all of them.
  private val reflectExpressions = new Reflections("org.neo4j.cypher.internal.v3_5.expressions")
  private val reflectLogicalPlans = new Reflections("org.neo4j.cypher.internal.v3_5.logical.plans")

  test("should convert an IntegerLiteral with its position") {
    val i3_5 = expressionsV3_5.SignedDecimalIntegerLiteral("5")(InputPositionV3_5(1, 2, 3))
    val i4_0 = expressionsV4_0.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 3))

    val rewritten = convert[expressionsV4_0.SignedDecimalIntegerLiteral](i3_5)
    rewritten should be(i4_0)
    rewritten.position should be(i4_0.position)
  }

  test("should convert an Add with its position (recursively)") {
    val i3_5a = expressionsV3_5.SignedDecimalIntegerLiteral("2")(InputPositionV3_5(1, 2, 3))
    val i3_5b = expressionsV3_5.SignedDecimalIntegerLiteral("5")(InputPositionV3_5(1, 2, 5))
    val add3_5 = expressionsV3_5.Add(i3_5a, i3_5b)(InputPositionV3_5(1,2,3))
    val i4_0a = expressionsV4_0.SignedDecimalIntegerLiteral("2")(InputPosition(1, 2, 3))
    val i4_0b = expressionsV4_0.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 5))
    val add4_0 = expressionsV4_0.Add(i4_0a, i4_0b)(InputPosition(1,2,3))

    val rewritten = convert[expressionsV4_0.Add](add3_5)
    rewritten should be(add4_0)
    rewritten.position should equal(add4_0.position)
    rewritten.lhs.position should equal(i4_0a.position)
    rewritten.rhs.position should equal(i4_0b.position)
  }

  test("should convert Expression with Seq") {
    val i3_5a = expressionsV3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsV3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val l3_5 = expressionsV3_5.ListLiteral(Seq(i3_5a, i3_5b))(pos3_5)
    val i4_0a = expressionsV4_0.SignedDecimalIntegerLiteral("2")(pos4_0)
    val i4_0b = expressionsV4_0.SignedDecimalIntegerLiteral("5")(pos4_0)
    val l4_0 = expressionsV4_0.ListLiteral(Seq(i4_0a, i4_0b))(pos4_0)

    convert[expressionsV4_0.ListLiteral](l3_5) should be(l4_0)
  }

  test("should convert Expression with Option") {
    val i3_5 = expressionsV3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val v3_5 = expressionsV3_5.Variable("var")(pos3_5)
    val f3_5 = expressionsV3_5.FilterScope(v3_5, Some(i3_5))(pos3_5)
    val f3_5b = expressionsV3_5.FilterScope(v3_5, None)(pos3_5)

    val i4_0 = expressionsV4_0.SignedDecimalIntegerLiteral("2")(pos4_0)
    val v4_0 = expressionsV4_0.Variable("var")(pos4_0)
    val f4_0 = expressionsV4_0.FilterScope(v4_0, Some(i4_0))(pos4_0)
    val f4_0b = expressionsV4_0.FilterScope(v4_0, None)(pos4_0)

    convert[expressionsV4_0.FilterScope](f3_5) should be(f4_0)
    convert[expressionsV4_0.FilterScope](f3_5b) should be(f4_0b)
  }

  test("should convert Expression with Set") {
    val i3_5a = expressionsV3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsV3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val l3_5 = expressionsV3_5.Ands(Set(i3_5a, i3_5b))(pos3_5)
    val i4_0a = expressionsV4_0.SignedDecimalIntegerLiteral("2")(pos4_0)
    val i4_0b = expressionsV4_0.SignedDecimalIntegerLiteral("5")(pos4_0)
    val l4_0 = expressionsV4_0.Ands(Set(i4_0a, i4_0b))(pos4_0)

    convert[expressionsV4_0.Ands](l3_5) should be(l4_0)
  }

  test("should convert Expression with Seq of Tuple") {
    val i3_5a = expressionsV3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsV3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val i3_5c = expressionsV3_5.SignedDecimalIntegerLiteral("10")(pos3_5)
    val i3_5d = expressionsV3_5.SignedDecimalIntegerLiteral("11")(pos3_5)
    val c3_5 = expressionsV3_5.CaseExpression(None, List((i3_5a, i3_5b), (i3_5c, i3_5d)), None)(pos3_5)

    val i4_0a = expressionsV4_0.SignedDecimalIntegerLiteral("2")(pos4_0)
    val i4_0b = expressionsV4_0.SignedDecimalIntegerLiteral("5")(pos4_0)
    val i4_0c = expressionsV4_0.SignedDecimalIntegerLiteral("10")(pos4_0)
    val i4_0d = expressionsV4_0.SignedDecimalIntegerLiteral("11")(pos4_0)
    val c4_0 = expressionsV4_0.CaseExpression(None, List((i4_0a, i4_0b), (i4_0c, i4_0d)), None)(pos4_0)

    convert[expressionsV4_0.CaseExpression](c3_5) should be(c4_0)
  }

  test("should convert Expression with Seq of Tuple (MapExpression)") {
    val i3_5a = expressionsV3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsV3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val p3_5a = expressionsV3_5.PropertyKeyName("a")(pos3_5)
    val p3_5b = expressionsV3_5.PropertyKeyName("b")(pos3_5)
    val m3_5 = expressionsV3_5.MapExpression(Seq((p3_5a, i3_5a),(p3_5b, i3_5b)))(pos3_5)

    val i4_0a = expressionsV4_0.SignedDecimalIntegerLiteral("2")(pos4_0)
    val i4_0b = expressionsV4_0.SignedDecimalIntegerLiteral("5")(pos4_0)
    val p4_0a = expressionsV4_0.PropertyKeyName("a")(pos4_0)
    val p4_0b = expressionsV4_0.PropertyKeyName("b")(pos4_0)
    val m4_0 = expressionsV4_0.MapExpression(Seq((p4_0a, i4_0a),(p4_0b, i4_0b)))(pos4_0)

    convert[expressionsV4_0.CaseExpression](m3_5) should be(m4_0)
  }

  test("should convert PathExpression") {
    val var3_5 = expressionsV3_5.Variable("n")(pos3_5)
    val psv3_5a = expressionsV3_5.NilPathStep
    val psv3_5b = expressionsV3_5.MultiRelationshipPathStep(var3_5, expressionsV3_5.SemanticDirection.BOTH, psv3_5a)
    val psv3_5c = expressionsV3_5.SingleRelationshipPathStep(var3_5, expressionsV3_5.SemanticDirection.OUTGOING, psv3_5b)
    val psv3_5d = expressionsV3_5.NodePathStep(var3_5, psv3_5c)
    val pexpv3_5 = expressionsV3_5.PathExpression(psv3_5d)(pos3_5)

    val var4_0 = expressionsV4_0.Variable("n")(pos4_0)
    val psv4_0a = expressionsV4_0.NilPathStep
    val psv4_0b = expressionsV4_0.MultiRelationshipPathStep(var4_0, expressionsV4_0.SemanticDirection.BOTH, psv4_0a)
    val psv4_0c = expressionsV4_0.SingleRelationshipPathStep(var4_0, expressionsV4_0.SemanticDirection.OUTGOING, psv4_0b)
    val psv4_0d = expressionsV4_0.NodePathStep(var4_0, psv4_0c)
    val pexpv4_0 = expressionsV4_0.PathExpression(psv4_0d)(pos4_0)

    convert[expressionsV4_0.PathExpression](pexpv3_5) should be(pexpv4_0)
  }

  test("should convert AndedPropertyInequalities") {
    val var3_5 = expressionsV3_5.Variable("n")(pos3_5)
    val p3_5 = expressionsV3_5.Property(var3_5, expressionsV3_5.PropertyKeyName("n")(pos3_5))(pos3_5)
    val i3_5a = expressionsV3_5.LessThan(var3_5, var3_5)(pos3_5)
    val i3_5b = expressionsV3_5.LessThan(var3_5, var3_5)(pos3_5)
    val i3_5c = expressionsV3_5.GreaterThan(var3_5, var3_5)(pos3_5)
    val a3_5 = expressionsV3_5.AndedPropertyInequalities(var3_5, p3_5, utilV3_5.NonEmptyList(i3_5a, i3_5b, i3_5c))

    val var4_0 = expressionsV4_0.Variable("n")(pos4_0)
    val p4_0 = expressionsV4_0.Property(var4_0, expressionsV4_0.PropertyKeyName("n")(pos4_0))(pos4_0)
    val i4_0a = expressionsV4_0.LessThan(var4_0, var4_0)(pos4_0)
    val i4_0b = expressionsV4_0.LessThan(var4_0, var4_0)(pos4_0)
    val i4_0c = expressionsV4_0.GreaterThan(var4_0, var4_0)(pos4_0)
    val a4_0 = expressionsV4_0.AndedPropertyInequalities(var4_0, p4_0, NonEmptyList(i4_0a, i4_0b, i4_0c))

    convert[expressionsV4_0.PathExpression](a3_5) should be(a4_0)
  }

  test("should convert Parameter and CypherTypes") {
    val p3_5a = expressionsV3_5.Parameter("a", symbolsV3_5.CTBoolean)(pos3_5)
    val p3_5b = expressionsV3_5.Parameter("a", symbolsV3_5.CTList(symbolsV3_5.CTAny))(pos3_5)
    val p4_0a = expressionsV4_0.Parameter("a", symbolsV4_0.CTBoolean)(pos4_0)
    val p4_0b = expressionsV4_0.Parameter("a", symbolsV4_0.CTList(symbolsV4_0.CTAny))(pos4_0)

    convert[expressionsV4_0.PathExpression](p3_5a) should be(p4_0a)
    convert[expressionsV4_0.PathExpression](p3_5b) should be(p4_0b)
  }

  test("should not save expression mappings if seenBySemanticTable always returns false") {
    val var3_5 = expressionsV3_5.Variable("n")(pos3_5)
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val l3_5 = plansV3_5.Limit(a3_5, var3_5, plansV3_5.IncludeTies)

    expressionMapping(l3_5, expr => false) shouldBe empty
  }

  test("should save expression mappings if seenBySemanticTable always returns true") {
    val var3_5 = expressionsV3_5.Variable("n")(pos3_5)
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val l3_5 = plansV3_5.Limit(a3_5, var3_5, plansV3_5.IncludeTies)

    val var4_0 = expressionsV4_0.Variable("n")(pos4_0)

    expressionMapping(l3_5, expr => true) should contain only ((var3_5, var3_5.position) -> var4_0)
  }

  test("should save distinct expressions with different positions in expression mappings") {
    val var3_5a = expressionsV3_5.Variable("n")(InputPositionV3_5(0, 0, 0))
    val var3_5b = expressionsV3_5.Variable("n")(InputPositionV3_5(1, 1, 1))
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val l3_5a = plansV3_5.Limit(a3_5, var3_5a, plansV3_5.IncludeTies)
    val l3_5b = plansV3_5.Limit(l3_5a, var3_5b, plansV3_5.IncludeTies)

    val var4_0a = expressionsV4_0.Variable("n")(InputPosition(0, 0, 0))
    val var4_0b = expressionsV4_0.Variable("n")(InputPosition(1, 1, 1))

    expressionMapping(l3_5b, expr => true) should contain only(
      (var3_5a, var3_5a.position) -> var4_0a,
      (var3_5b, var3_5b.position) -> var4_0b
    )
  }

  test("should provide minimal implementation of planningAttributes after plan conversion") {
    val solveds3_5 = new SolvedsV3_5
    val cardinalities3_5 = new CardinalitiesV3_5
    val providedOrders3_5 = new ProvidedOrdersV3_5
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    solveds3_5.set(a3_5.id, irV3_5.PlannerQuery.empty)
    cardinalities3_5.set(a3_5.id, utilV3_5.Cardinality(5.0))
    providedOrders3_5.set(a3_5.id, irV3_5.ProvidedOrder(Seq(irV3_5.ProvidedOrder.Asc("foo"))))

    val solveds4_0 = new SolvedsV4_0
    val cardinalities4_0 = new CardinalitiesV4_0
    val providedOrders4_0 = new ProvidedOrdersV4_0
    val rewrittenPlan = LogicalPlanConverter.convertLogicalPlan[plansV4_0.AllNodesScan](
      a3_5,
      PlanningAttributesV3_5(solveds3_5, cardinalities3_5, providedOrders3_5),
      PlanningAttributesV4_0(solveds4_0, cardinalities4_0, providedOrders4_0),
      new MaxIdConverter
    )._1
    solveds4_0.get(rewrittenPlan.id).readOnly should equal(solveds3_5.get(a3_5.id).readOnly)
    cardinalities4_0.get(rewrittenPlan.id) should equal(helpers.as4_0(cardinalities3_5.get(a3_5.id)))
    providedOrders4_0.get(rewrittenPlan.id) should equal(helpers.as4_0(providedOrders3_5.get(a3_5.id)))
  }

  test("should convert AllNodeScan and keep id") {
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val id3_5 = a3_5.id
    val a4_0 = plansV4_0.AllNodesScan("n", Set.empty)

    val rewrittenPlan = convert[plansV4_0.AllNodesScan](a3_5)
    rewrittenPlan should be(a4_0)
    rewrittenPlan.id should be(helpers.as4_0(id3_5))
  }

  test("should convert Aggregation and keep ids") {
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val i3_5a = expressionsV3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsV3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val ag3_5 = plansV3_5.Aggregation(a3_5, Map("a" -> i3_5a), Map("b" -> i3_5b))
    val ans_id = a3_5.id
    val ag_id = ag3_5.id

    val a4_0 = plansV4_0.AllNodesScan("n", Set.empty)
    val i4_0a = expressionsV4_0.SignedDecimalIntegerLiteral("2")(pos4_0)
    val i4_0b = expressionsV4_0.SignedDecimalIntegerLiteral("5")(pos4_0)
    val ag4_0 = plansV4_0.Aggregation(a4_0, Map("a" -> i4_0a), Map("b" -> i4_0b))

    val rewrittenPlan = convert[plansV4_0.Aggregation](ag3_5)
    rewrittenPlan should be(ag4_0)
    rewrittenPlan.id should be(helpers.as4_0(ag_id))
    rewrittenPlan.lhs.get.id should be(helpers.as4_0(ans_id))
  }

  test("should convert ProduceResult and keep ids") {
    val s3_5 = plansV3_5.Argument()
    val p3_5 = plansV3_5.ProduceResult(s3_5, Seq("a"))

    val s3_5_id = s3_5.id
    val p3_5_id = p3_5.id

    val s4_0 = plansV4_0.Argument()
    val p4_0 = plansV4_0.ProduceResult(s4_0, Seq("a"))

    val rewrittenPlan = convert[plansV4_0.ProduceResult](p3_5)
    rewrittenPlan should be(p4_0)
    rewrittenPlan.id should be(helpers.as4_0(p3_5_id))
    rewrittenPlan.lhs.get.id should be(helpers.as4_0(s3_5_id))
  }

  test("should convert ErrorPlan") {
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val e3_5 = plansV3_5.ErrorPlan(a3_5, new utilV3_5.ExhaustiveShortestPathForbiddenException)

    val a4_0 = plansV4_0.AllNodesScan("n", Set.empty)

    val rewrittenPlan = convert[plansV4_0.ErrorPlan](e3_5)
    rewrittenPlan.asInstanceOf[plansV4_0.ErrorPlan].source should be(a4_0)
    rewrittenPlan.asInstanceOf[plansV4_0.ErrorPlan].exception shouldBe an[utilV4_0.ExhaustiveShortestPathForbiddenException]
  }

  test("should convert NodeIndexSeek") {
    val var3_5 = expressionsV3_5.Variable("n")(pos3_5)
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val n3_5 = plansV3_5.NodeIndexSeek("a",
      expressionsV3_5.LabelToken("b", utilV3_5.LabelId(2)),
      Seq(plansV3_5.IndexedProperty(expressionsV3_5.PropertyKeyToken("c", utilV3_5.PropertyKeyId(3)), plansV3_5.DoNotGetValue)),
      plansV3_5.SingleQueryExpression(var3_5), Set.empty, plansV3_5.IndexOrderAscending)

    val var4_0 = expressionsV4_0.Variable("n")(pos4_0)
    val a4_0 = plansV4_0.AllNodesScan("n", Set.empty)
    val n4_0 = plansV4_0.NodeIndexSeek("a",
      expressionsV4_0.LabelToken("b", utilV4_0.LabelId(2)),
      Seq(plansV4_0.IndexedProperty(expressionsV4_0.PropertyKeyToken("c", utilV4_0.PropertyKeyId(3)), plansV4_0.DoNotGetValue)),
      plansV4_0.SingleQueryExpression(var4_0), Set.empty, IndexOrderAscending)

    convert[plansV4_0.NodeIndexSeek](n3_5) should be(n4_0)
  }

  test("should convert ProcedureCall") {
    val var3_5 = expressionsV3_5.Variable("n")(pos3_5)
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val inputv3_5 = plansV3_5.FieldSignature("d", symbolsV3_5.CTString, Some(plansV3_5.CypherValue("e", symbolsV3_5.CTString)))
    val sigv3_5 = plansV3_5.ProcedureSignature(plansV3_5.QualifiedName(Seq("a", "b"), "c"), IndexedSeq(inputv3_5), None, None, plansV3_5.ProcedureReadWriteAccess(Array("foo", "bar")))
    val pres3_5 = astV3_5.ProcedureResultItem(Some(expressionsV3_5.ProcedureOutput("f")(pos3_5)), var3_5)(pos3_5)
    val rc3_5 = plansV3_5.ResolvedCall(sigv3_5, Seq(var3_5), IndexedSeq(pres3_5))(pos3_5)
    val pc3_5 = plansV3_5.ProcedureCall(a3_5, rc3_5)

    val var4_0 = expressionsV4_0.Variable("n")(pos4_0)
    val a4_0 = plansV4_0.AllNodesScan("n", Set.empty)
    val inputv4_0 = plansV4_0.FieldSignature("d", symbolsV4_0.CTString, Some(plansV4_0.CypherValue("e", symbolsV4_0.CTString)))
    val sigv4_0 = plansV4_0.ProcedureSignature(plansV4_0.QualifiedName(Seq("a", "b"), "c"), IndexedSeq(inputv4_0), None, None, plansV4_0.ProcedureReadWriteAccess(Array("foo", "bar")))
    val pres4_0 = astV4_0.ProcedureResultItem(Some(expressionsV4_0.ProcedureOutput("f")(pos4_0)), var4_0)(pos4_0)
    val rc4_0 = plansV4_0.ResolvedCall(sigv4_0, Seq(var4_0), IndexedSeq(pres4_0))(pos4_0)
    val pc4_0 = plansV4_0.ProcedureCall(a4_0, rc4_0)

    convert[ProcedureCall](pc3_5) should be(pc4_0)
  }

  test("should convert Sort") {
    val a3_5 = plansV3_5.AllNodesScan("n", Set.empty)
    val s3_5 = plansV3_5.Sort(a3_5, Seq(plansV3_5.Ascending("n")))

    val a4_0 = plansV4_0.AllNodesScan("n", Set.empty)
    val s4_0 = plansV4_0.Sort(a4_0, Seq(plansV4_0.Ascending("n")))

    convert[ProcedureCall](s3_5) should be(s4_0)
  }

  test("should convert function call with 'null' default value") {
    val allowed = Array.empty[String] // this is passed through as the same instance - so shared
    val name3_5 = plansV3_5.QualifiedName(Seq.empty, "foo")
    val call3_5 = plansV3_5.ResolvedFunctionInvocation(name3_5,
      Some(plansV3_5.UserFunctionSignature(name3_5, Vector(plansV3_5.FieldSignature("input", symbolsV3_5.CTAny,
        default = Some(plansV3_5.CypherValue(null, symbolsV3_5.CTAny)))),
        symbolsV3_5.CTAny, None, allowed, None, isAggregate = false)), Vector())(InputPositionV3_5(1, 2, 3))

    val name4_0 = plansV4_0.QualifiedName(Seq.empty, "foo")
    val call4_0 = plansV4_0.ResolvedFunctionInvocation(name4_0,
      Some(plansV4_0.UserFunctionSignature(name4_0, Vector(plansV4_0.FieldSignature("input", symbolsV4_0.CTAny,
        default = Some(plansV4_0.CypherValue(null, symbolsV4_0.CTAny)))),
        symbolsV4_0.CTAny, None, allowed, None, isAggregate = false)), Vector())(InputPosition(1, 2, 3))

    convert[plansV4_0.ResolvedFunctionInvocation](call3_5) should be(call4_0)
  }

  test("should convert all expressions") {
    val subTypes = reflectExpressions.getSubTypesOf(classOf[expressionsV3_5.Expression]).asScala ++
      reflectLogicalPlans.getSubTypesOf(classOf[expressionsV3_5.Expression]).asScala
    subTypes.filter { c => !Modifier.isAbstract(c.getModifiers) }
      .toList.sortBy(_.getName)
      .foreach { subType =>
      val constructor = subType.getConstructors.head
      val paramTypes = constructor.getParameterTypes
      Try {
        val constructorArgs = paramTypes.asInstanceOf[Array[Class[AnyRef]]].map(x => argumentProvider(x))
        constructor.newInstance(constructorArgs: _*).asInstanceOf[expressionsV3_5.Expression]
      } match {
        case Success(expressionV3_5) =>
          val rewritten = convert[expressionsV4_0.Expression](expressionV3_5)
          rewritten shouldBe an[expressionsV4_0.Expression]
        case Failure(e: InstantiationException) => fail(s"could not instantiate 3.5 expression: ${subType.getSimpleName} with arguments ${paramTypes.toList}", e)
        case Failure(e) => fail(s"Converting ${subType.getName} failed", e)
      }
    }
  }

  test("should convert all logical plans") {
    val subTypes = reflectLogicalPlans.getSubTypesOf(classOf[plansV3_5.LogicalPlan]).asScala
    subTypes.filter(c => !Modifier.isAbstract(c.getModifiers))
      .toList.sortBy(_.getName)
      .foreach { subType =>
        val constructor = subType.getConstructors.head
        val paramTypes = constructor.getParameterTypes
        val planV3_5 = {
          if (subType.getSimpleName.equals("Selection")) {
            // To avoid AssertionError when we would create empty Selection in the else branch otherwise
            Try(plansV3_5.Selection(Seq(expressionsV3_5.Variable("n")(pos3_5)), argumentProvider[plansV3_5.LogicalPlan](classOf[plansV3_5.LogicalPlan])))
          } else {
            Try {
              val constructorArgs = paramTypes.asInstanceOf[Array[Class[AnyRef]]].map(x => argumentProvider(x))
              constructor.newInstance(constructorArgs: _*).asInstanceOf[plansV3_5.LogicalPlan]
            }
          }
        }
        planV3_5 match {
          case Success(plan) =>
            val rewritten = convert[plansV4_0.LogicalPlan](plan)
            rewritten shouldBe an[plansV4_0.LogicalPlan]
          case Failure(e: InstantiationException) => fail(s"could not instantiate 4.0 logical plan: ${subType.getSimpleName} with arguments ${paramTypes.toList}", e)
          case Failure(e) => fail(s"Converting ${subType.getName} failed", e)
        }
      }
  }

  /**
    * While enumerating all expressions and logical plans with reflection, we need to be able
    * to instantiate the old types and thus need to provide them with the correct constructor arguments.
    * This method return a suitable object given the simple class name.
    */
  private def argumentProvider[T <: AnyRef](clazz: Class[T]): T = {
    val variable = expressionsV3_5.Variable("n")(pos3_5)
    val value = clazz.getSimpleName match {
      case "Variable" => variable
      case "LogicalVariable" => variable
      case "Property" => expressionsV3_5.Property(variable, argumentProvider(classOf[expressionsV3_5.PropertyKeyName]))(pos3_5)
      case "PropertyKeyName" => expressionsV3_5.PropertyKeyName("n")(pos3_5)
      case "PathStep" => expressionsV3_5.NilPathStep
      case "Expression" => variable
      case "InputPosition" => pos3_5
      case "ReduceScope" => expressionsV3_5.ReduceScope(variable, variable, variable)(pos3_5)
      case "FilterScope" => expressionsV3_5.FilterScope(variable, Some(variable))(pos3_5)
      case "ExtractScope" => expressionsV3_5.ExtractScope(variable, Some(variable), Some(variable))(pos3_5)
      case "RelationshipsPattern" => expressionsV3_5.RelationshipsPattern(argumentProvider(classOf[expressionsV3_5.RelationshipChain]))(pos3_5)
      case "RelationshipChain" => expressionsV3_5.RelationshipChain(argumentProvider(classOf[expressionsV3_5.PatternElement]), argumentProvider(classOf[expressionsV3_5.RelationshipPattern]), argumentProvider(classOf[expressionsV3_5.NodePattern]))(pos3_5)
      case "RelationshipPattern" => expressionsV3_5.RelationshipPattern(Some(variable), Seq.empty, None, None, expressionsV3_5.SemanticDirection.OUTGOING)(pos3_5)
      case "NodePattern" => new expressionsV3_5.InvalidNodePattern(variable)(pos3_5)
      case "PatternElement" => new expressionsV3_5.InvalidNodePattern(variable)(pos3_5)
      case "NonEmptyList" => utilV3_5.NonEmptyList(1)
      case "Namespace" => expressionsV3_5.Namespace()(pos3_5)
      case "FunctionName" => expressionsV3_5.FunctionName("a")(pos3_5)
      case "SemanticDirection" => expressionsV3_5.SemanticDirection.OUTGOING
      case "ShortestPaths" => expressionsV3_5.ShortestPaths(argumentProvider(classOf[expressionsV3_5.PatternElement]), single = true)(pos3_5)
      case "CypherType" => symbolsV3_5.CTBoolean
      case "Scope" => astV3_5.semantics.Scope.empty
      case "Equals" => expressionsV3_5.Equals(variable, variable)(pos3_5)
      case "InequalitySeekRange" => plansV3_5.RangeGreaterThan(utilV3_5.NonEmptyList(plansV3_5.InclusiveBound(variable)))
      case "PrefixRange" => plansV3_5.PrefixRange(variable)
      case "PointDistanceRange" => plansV3_5.PointDistanceRange(1, 1, inclusive = true)
      case "LogicalProperty" => expressionsV3_5.Property(variable, argumentProvider(classOf[expressionsV3_5.PropertyKeyName]))(pos3_5)
      case "IndexedProperty" => plansV3_5.IndexedProperty(argumentProvider(classOf[expressionsV3_5.PropertyKeyToken]), argumentProvider(classOf[plansV3_5.GetValueFromIndexBehavior]))
      case "GetValueFromIndexBehavior" => plansV3_5.GetValue
      case "IndexOrder" => plansV3_5.IndexOrderAscending

      case "QueryExpression" => plansV3_5.SingleQueryExpression(variable)
      case "LogicalPlan" => plansV3_5.AllNodesScan("n", Set.empty)
      case "IdGen" => idGen3_5
      case "Exception" => new utilV3_5.ExhaustiveShortestPathForbiddenException
      case "IdName" => "n"
      case "LabelName" => expressionsV3_5.LabelName("n")(pos3_5)
      case "LabelToken" => expressionsV3_5.LabelToken("a", argumentProvider(classOf[utilV3_5.LabelId]))
      case "LabelId" => utilV3_5.LabelId(5)
      case "PropertyKeyToken" => expressionsV3_5.PropertyKeyToken("a", argumentProvider(classOf[utilV3_5.PropertyKeyId]))
      case "PropertyKeyId" => utilV3_5.PropertyKeyId(5)
      case "ResolvedCall" => plansV3_5.ResolvedCall(argumentProvider(classOf[plansV3_5.ProcedureSignature]), Seq.empty, IndexedSeq.empty)(pos3_5)
      case "ProcedureSignature" => plansV3_5.ProcedureSignature(argumentProvider(classOf[plansV3_5.QualifiedName]), IndexedSeq.empty, None, None, argumentProvider(classOf[plansV3_5.ProcedureAccessMode]))
      case "QualifiedName" => plansV3_5.QualifiedName(Seq.empty, "c")
      case "ProcedureAccessMode" => plansV3_5.ProcedureReadWriteAccess(Array())
      case "RelTypeName" => expressionsV3_5.RelTypeName("x")(pos3_5)
      case "SeekableArgs" => plansV3_5.SingleSeekableArg(variable)
      case "ExpansionMode" => plansV3_5.ExpandAll
      case "ShortestPathPattern" => irV3_5.ShortestPathPattern(None, argumentProvider(classOf[irV3_5.PatternRelationship]), single = true)(argumentProvider(classOf[expressionsV3_5.ShortestPaths]))
      case "PatternRelationship" => irV3_5.PatternRelationship("n", ("n", "n"), expressionsV3_5.SemanticDirection.OUTGOING, Seq.empty, irV3_5.SimplePatternLength)
      case "PatternLength" => irV3_5.SimplePatternLength
      case "Ties" => plansV3_5.IncludeTies
      case "CSVFormat" => irV3_5.HasHeaders
      case "VarPatternLength" => irV3_5.VarPatternLength(0, None)

      case "IndexedSeq" => IndexedSeq.empty
      case "boolean" => true
      case "String" => "test"
      case "Option" => None
      case "Set" => Set.empty
      case "List" => List.empty
      case "Seq" => Seq.empty
      case "Map" => Map.empty
      case "int" => 42
    }
    value.asInstanceOf[T]
  }

  /**
    * Converts an expression.
    */
  private def convert[T <: expressionsV4_0.Expression](input: expressionsV3_5.Expression): T = {
    val planningAttributes3_5 = PlanningAttributesV3_5(new SolvedsV3_5, new CardinalitiesV3_5, new ProvidedOrdersV3_5)
    val planningAttributes4_0 = PlanningAttributesV4_0(new SolvedsV4_0, new CardinalitiesV4_0, new ProvidedOrdersV4_0)
    input match {
      case nestedPlan: plansV3_5.NestedPlanExpression =>
        assignAttributesRecursivelyWithDefaultValues(nestedPlan.plan, planningAttributes3_5)
      case _ =>
    }
    LogicalPlanConverter.convertExpression(input, planningAttributes3_5, planningAttributes4_0, new MaxIdConverter)
  }

  /**
    * Converts a logical plan with default solved and cardinality.
    */
  private def convert[T <: plansV4_0.LogicalPlan](input: plansV3_5.LogicalPlan): T = {
    val planningAttributes3_5 = PlanningAttributesV3_5(new SolvedsV3_5, new CardinalitiesV3_5, new ProvidedOrdersV3_5)
    val planningAttributes4_0 = PlanningAttributesV4_0(new SolvedsV4_0, new CardinalitiesV4_0, new ProvidedOrdersV4_0)
    assignAttributesRecursivelyWithDefaultValues(input, planningAttributes3_5)
    LogicalPlanConverter.convertLogicalPlan[T](input, planningAttributes3_5, planningAttributes4_0, new MaxIdConverter)._1
  }

  /**
    * Given a plan and a lambda deciding which expressions are important, returns the expression mapping
    */
  private def expressionMapping(input: plansV3_5.LogicalPlan,
                                seenBySemanticTable: expressionsV3_5.Expression => Boolean): ExpressionMapping4To5 = {
    val planningAttributes3_5 = PlanningAttributesV3_5(new SolvedsV3_5, new CardinalitiesV3_5, new ProvidedOrdersV3_5)
    val planningAttributes4_0 = PlanningAttributesV4_0(new SolvedsV4_0, new CardinalitiesV4_0, new ProvidedOrdersV4_0)
    assignAttributesRecursivelyWithDefaultValues(input, planningAttributes3_5)
    LogicalPlanConverter.convertLogicalPlan(input,
      planningAttributes3_5,
      planningAttributes4_0,
      new MaxIdConverter,
      seenBySemanticTable
    )._2
  }

  /**
    * Sets a default attributes for the input and all its subplans, recursively.
    */
  private def assignAttributesRecursivelyWithDefaultValues(input: plansV3_5.LogicalPlan,
                                                           attributes: PlanningAttributesV3_5) : Unit = {
    attributes.solveds.set(input.id, irV3_5.PlannerQuery.empty)
    attributes.cardinalities.set(input.id, utilV3_5.Cardinality(1.0))
    attributes.providedOrders.set(input.id, irV3_5.ProvidedOrder(Seq.empty))
    input.children.foreach {
      case p:plansV3_5.LogicalPlan => assignAttributesRecursivelyWithDefaultValues(p, attributes)
      case _ =>
    }
  }
}
