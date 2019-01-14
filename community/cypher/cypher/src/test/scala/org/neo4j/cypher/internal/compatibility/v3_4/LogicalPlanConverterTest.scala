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
package org.neo4j.cypher.internal.compatibility.v3_4

import java.lang.reflect.Modifier

import org.neo4j.cypher.internal.compatibility.v3_4.SemanticTableConverter.ExpressionMapping4To5
import org.neo4j.cypher.internal.frontend.v3_4.{ast => astV3_4}
import org.neo4j.cypher.internal.frontend.{v3_4 => frontendV3_4}
import org.neo4j.cypher.internal.ir.{v3_4 => irV3_4}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities => CardinalitiesV3_4}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Solveds => SolvedsV3_4}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities => CardinalitiesV3_5}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Solveds => SolvedsV3_5}
import org.neo4j.cypher.internal.util.v3_4.attribution.{SequentialIdGen => SequentialIdGenv3_4}
import org.neo4j.cypher.internal.util.v3_4.{InputPosition => InputPositionV3_4}
import org.neo4j.cypher.internal.util.v3_4.{symbols => symbolsV3_4}
import org.neo4j.cypher.internal.util.{v3_4 => utilv3_4}
import org.neo4j.cypher.internal.v3_4.logical.{plans => plansV3_4}
import org.neo4j.cypher.internal.v3_4.{expressions => expressionsv3_4}
import org.neo4j.cypher.internal.v3_5.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.v3_5.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.v3_5.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.v3_5.logical.{plans => plansv3_5}
import org.neo4j.cypher.internal.v3_5.expressions.PathExpression
import  org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_5.util.attribution.{SequentialIdGen => SequentialIdGenv3_5}
import  org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import  org.neo4j.cypher.internal.v3_5.util.NonEmptyList
import  org.neo4j.cypher.internal.v3_5.util.{symbols => symbolsv3_5}
import org.neo4j.cypher.internal.v3_5.{ast => astv3_5}
import  org.neo4j.cypher.internal.v3_5.{util => utilv3_5}
import  org.neo4j.cypher.internal.v3_5.{expressions => expressionsv3_5}
import org.reflections.Reflections

import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class LogicalPlanConverterTest extends CypherFunSuite {

  private implicit val idGen3_4 = new SequentialIdGenv3_4()
  private implicit val idGen3_5 = new SequentialIdGenv3_5()

  private val pos3_4 = InputPositionV3_4(0,0,0)
  private val pos3_5 = InputPosition(0,0,0)
  // We use these package names to enumerate all classes of a certain type in these packages and test
  // for all of them.
  private val reflectExpressions = new Reflections("org.neo4j.cypher.internal.v3_4.expressions")
  private val reflectLogicalPlans = new Reflections("org.neo4j.cypher.internal.v3_4.logical.plans")

  test("should convert an IntegerLiteral with its position") {
    val i3_4 = expressionsv3_4.SignedDecimalIntegerLiteral("5")(InputPositionV3_4(1, 2, 3))
    val i3_5 = expressionsv3_5.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 3))

    val rewritten = convert[expressionsv3_5.SignedDecimalIntegerLiteral](i3_4)
    rewritten should be(i3_5)
    rewritten.position should be(i3_5.position)
  }

  test("should convert an Add with its position (recursively)") {
    val i3_4a = expressionsv3_4.SignedDecimalIntegerLiteral("2")(InputPositionV3_4(1, 2, 3))
    val i3_4b = expressionsv3_4.SignedDecimalIntegerLiteral("5")(InputPositionV3_4(1, 2, 5))
    val add3_4 = expressionsv3_4.Add(i3_4a, i3_4b)(InputPositionV3_4(1,2,3))
    val i3_5a = expressionsv3_5.SignedDecimalIntegerLiteral("2")(InputPosition(1, 2, 3))
    val i3_5b = expressionsv3_5.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 5))
    val add3_5 = expressionsv3_5.Add(i3_5a, i3_5b)(InputPosition(1,2,3))

    val rewritten = convert[expressionsv3_5.Add](add3_4)
    rewritten should be(add3_5)
    rewritten.position should equal(add3_5.position)
    rewritten.lhs.position should equal(i3_5a.position)
    rewritten.rhs.position should equal(i3_5b.position)
  }

  test("should convert Expression with Seq") {
    val i3_4a = expressionsv3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsv3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val l3_4 = expressionsv3_4.ListLiteral(Seq(i3_4a, i3_4b))(pos3_4)
    val i3_5a = expressionsv3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsv3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val l3_5 = expressionsv3_5.ListLiteral(Seq(i3_5a, i3_5b))(pos3_5)

    convert[expressionsv3_5.ListLiteral](l3_4) should be(l3_5)
  }

  test("should convert Expression with Option") {
    val i3_4 = expressionsv3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val v3_4 = expressionsv3_4.Variable("var")(pos3_4)
    val f3_4 = expressionsv3_4.FilterScope(v3_4, Some(i3_4))(pos3_4)
    val f3_4b = expressionsv3_4.FilterScope(v3_4, None)(pos3_4)

    val i3_5 = expressionsv3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val v3_5 = expressionsv3_5.Variable("var")(pos3_5)
    val f3_5 = expressionsv3_5.FilterScope(v3_5, Some(i3_5))(pos3_5)
    val f3_5b = expressionsv3_5.FilterScope(v3_5, None)(pos3_5)

    convert[expressionsv3_5.FilterScope](f3_4) should be(f3_5)
    convert[expressionsv3_5.FilterScope](f3_4b) should be(f3_5b)
  }

  test("should convert Expression with Set") {
    val i3_4a = expressionsv3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsv3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val l3_4 = expressionsv3_4.Ands(Set(i3_4a, i3_4b))(pos3_4)
    val i3_5a = expressionsv3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsv3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val l3_5 = expressionsv3_5.Ands(Set(i3_5a, i3_5b))(pos3_5)

    convert[expressionsv3_5.Ands](l3_4) should be(l3_5)
  }

  test("should convert Expression with Seq of Tuple") {
    val i3_4a = expressionsv3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsv3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val i3_4c = expressionsv3_4.SignedDecimalIntegerLiteral("10")(pos3_4)
    val i3_4d = expressionsv3_4.SignedDecimalIntegerLiteral("11")(pos3_4)
    val c3_4 = expressionsv3_4.CaseExpression(None, List((i3_4a, i3_4b), (i3_4c, i3_4d)), None)(pos3_4)

    val i3_5a = expressionsv3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsv3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val i3_5c = expressionsv3_5.SignedDecimalIntegerLiteral("10")(pos3_5)
    val i3_5d = expressionsv3_5.SignedDecimalIntegerLiteral("11")(pos3_5)
    val c3_5 = expressionsv3_5.CaseExpression(None, List((i3_5a, i3_5b), (i3_5c, i3_5d)), None)(pos3_5)

    convert[expressionsv3_5.CaseExpression](c3_4) should be(c3_5)
  }

  test("should convert Expression with Seq of Tuple (MapExpression)") {
    val i3_4a = expressionsv3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsv3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val p3_4a = expressionsv3_4.PropertyKeyName("a")(pos3_4)
    val p3_4b = expressionsv3_4.PropertyKeyName("b")(pos3_4)
    val m3_4 = expressionsv3_4.MapExpression(Seq((p3_4a, i3_4a),(p3_4b, i3_4b)))(pos3_4)

    val i3_5a = expressionsv3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsv3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val p3_5a = expressionsv3_5.PropertyKeyName("a")(pos3_5)
    val p3_5b = expressionsv3_5.PropertyKeyName("b")(pos3_5)
    val m3_5 = expressionsv3_5.MapExpression(Seq((p3_5a, i3_5a),(p3_5b, i3_5b)))(pos3_5)

    convert[expressionsv3_5.CaseExpression](m3_4) should be(m3_5)
  }

  test("should convert PathExpression") {
    val var3_4 = expressionsv3_4.Variable("n")(pos3_4)
    val psv3_4a = expressionsv3_4.NilPathStep
    val psv3_4b = expressionsv3_4.MultiRelationshipPathStep(var3_4, expressionsv3_4.SemanticDirection.BOTH, psv3_4a)
    val psv3_4c = expressionsv3_4.SingleRelationshipPathStep(var3_4, expressionsv3_4.SemanticDirection.OUTGOING, psv3_4b)
    val psv3_4d = expressionsv3_4.NodePathStep(var3_4, psv3_4c)
    val pexpv3_4 = expressionsv3_4.PathExpression(psv3_4d)(pos3_4)

    val var3_5 = expressionsv3_5.Variable("n")(pos3_5)
    val psv3_5a = expressionsv3_5.NilPathStep
    val psv3_5b = expressionsv3_5.MultiRelationshipPathStep(var3_5, SemanticDirection.BOTH, psv3_5a)
    val psv3_5c = expressionsv3_5.SingleRelationshipPathStep(var3_5, SemanticDirection.OUTGOING, psv3_5b)
    val psv3_5d = expressionsv3_5.NodePathStep(var3_5, psv3_5c)
    val pexpv3_5 = expressionsv3_5.PathExpression(psv3_5d)(pos3_5)

    convert[PathExpression](pexpv3_4) should be(pexpv3_5)
  }

  test("should convert AndedPropertyInequalities") {
    val var3_4 = expressionsv3_4.Variable("n")(pos3_4)
    val p3_4 = expressionsv3_4.Property(var3_4, expressionsv3_4.PropertyKeyName("n")(pos3_4))(pos3_4)
    val i3_4a = expressionsv3_4.LessThan(var3_4, var3_4)(pos3_4)
    val i3_4b = expressionsv3_4.LessThan(var3_4, var3_4)(pos3_4)
    val i3_4c = expressionsv3_4.GreaterThan(var3_4, var3_4)(pos3_4)
    val a3_4 = expressionsv3_4.AndedPropertyInequalities(var3_4, p3_4, utilv3_4.NonEmptyList(i3_4a, i3_4b, i3_4c))

    val var3_5 = expressionsv3_5.Variable("n")(pos3_5)
    val p3_5 = expressionsv3_5.Property(var3_5, expressionsv3_5.PropertyKeyName("n")(pos3_5))(pos3_5)
    val i3_5a = expressionsv3_5.LessThan(var3_5, var3_5)(pos3_5)
    val i3_5b = expressionsv3_5.LessThan(var3_5, var3_5)(pos3_5)
    val i3_5c = expressionsv3_5.GreaterThan(var3_5, var3_5)(pos3_5)
    val a3_5 = expressionsv3_5.AndedPropertyInequalities(var3_5, p3_5, NonEmptyList(i3_5a, i3_5b, i3_5c))

    convert[PathExpression](a3_4) should be(a3_5)
  }

  test("should convert Parameter and CypherTypes") {
    val p3_4a = expressionsv3_4.Parameter("a", symbolsV3_4.CTBoolean)(pos3_4)
    val p3_4b = expressionsv3_4.Parameter("a", symbolsV3_4.CTList(symbolsV3_4.CTAny))(pos3_4)
    val p3_5a = expressionsv3_5.Parameter("a", symbolsv3_5.CTBoolean)(pos3_5)
    val p3_5b = expressionsv3_5.Parameter("a", symbolsv3_5.CTList(symbolsv3_5.CTAny))(pos3_5)

    convert[PathExpression](p3_4a) should be(p3_5a)
    convert[PathExpression](p3_4b) should be(p3_5b)
  }

  test("should not save expression mappings if seenBySemanticTable always returns false") {
    val var3_4 = expressionsv3_4.Variable("n")(pos3_4)
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val l3_4 = plansV3_4.Limit(a3_4, var3_4, plansV3_4.IncludeTies)

    expressionMapping(l3_4, expr => false) shouldBe empty
  }

  test("should save expression mappings if seenBySemanticTable always returns true") {
    val var3_4 = expressionsv3_4.Variable("n")(pos3_4)
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val l3_4 = plansV3_4.Limit(a3_4, var3_4, plansV3_4.IncludeTies)

    val var3_5 = expressionsv3_5.Variable("n")(pos3_5)

    expressionMapping(l3_4, expr => true) should contain only ((var3_4, var3_4.position) -> var3_5)
  }

  test("should save distinct expressions with different positions in expression mappings") {
    val var3_4a = expressionsv3_4.Variable("n")(InputPositionV3_4(0, 0, 0))
    val var3_4b = expressionsv3_4.Variable("n")(InputPositionV3_4(1, 1, 1))
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val l3_4a = plansV3_4.Limit(a3_4, var3_4a, plansV3_4.IncludeTies)
    val l3_4b = plansV3_4.Limit(l3_4a, var3_4b, plansV3_4.IncludeTies)

    val var3_5a = expressionsv3_5.Variable("n")(InputPosition(0, 0, 0))
    val var3_5b = expressionsv3_5.Variable("n")(InputPosition(1, 1, 1))

    expressionMapping(l3_4b, expr => true) should contain only(
      (var3_4a, var3_4a.position) -> var3_5a,
      (var3_4b, var3_4b.position) -> var3_5b
    )
  }

  test("should provide minimal implementation of solved after plan conversion") {
    val solveds3_4 = new SolvedsV3_4
    val cardinalities3_4 = new CardinalitiesV3_4
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    solveds3_4.set(a3_4.id, irV3_4.PlannerQuery.empty)
    cardinalities3_4.set(a3_4.id, utilv3_4.Cardinality(5.0))

    val solveds3_5 = new SolvedsV3_5
    val cardinalities3_5 = new CardinalitiesV3_5
    val rewrittenPlan = LogicalPlanConverter.convertLogicalPlan[plansv3_5.AllNodesScan](
                          a3_4, solveds3_4, cardinalities3_4, solveds3_5, cardinalities3_5, new MaxIdConverter
                        )._1
    solveds3_5.get(rewrittenPlan.id).readOnly should equal(solveds3_4.get(a3_4.id).readOnly)
    cardinalities3_5.get(rewrittenPlan.id) should equal(helpers.as3_5(cardinalities3_4.get(a3_4.id)))
  }

  test("should convert AllNodeScan and keep id") {
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val id3_4 = a3_4.id
    val a3_5 = plansv3_5.AllNodesScan("n", Set.empty)

    val rewrittenPlan = convert[plansv3_5.AllNodesScan](a3_4)
    rewrittenPlan should be(a3_5)
    rewrittenPlan.id should be(helpers.as3_5(id3_4))
  }

  test("should convert Aggregation and keep ids") {
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val i3_4a = expressionsv3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsv3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val ag3_4 = plansV3_4.Aggregation(a3_4, Map("a" -> i3_4a), Map("b" -> i3_4b))
    val ans_id = a3_4.id
    val ag_id = ag3_4.id

    val a3_5 = plansv3_5.AllNodesScan("n", Set.empty)
    val i3_5a = expressionsv3_5.SignedDecimalIntegerLiteral("2")(pos3_5)
    val i3_5b = expressionsv3_5.SignedDecimalIntegerLiteral("5")(pos3_5)
    val ag3_5 = plansv3_5.Aggregation(a3_5, Map("a" -> i3_5a), Map("b" -> i3_5b))

    val rewrittenPlan = convert[plansv3_5.Aggregation](ag3_4)
    rewrittenPlan should be(ag3_5)
    rewrittenPlan.id should be(helpers.as3_5(ag_id))
    rewrittenPlan.lhs.get.id should be(helpers.as3_5(ans_id))
  }

  test("should convert ProduceResult and keep ids") {
    val s3_4 = plansV3_4.Argument()
    val p3_4 = plansV3_4.ProduceResult(s3_4, Seq("a"))

    val s3_4_id = s3_4.id
    val p3_4_id = p3_4.id

    val s3_5 = plansv3_5.Argument()
    val p3_5 = plansv3_5.ProduceResult(s3_5, Seq("a"))

    val rewrittenPlan = convert[plansv3_5.ProduceResult](p3_4)
    rewrittenPlan should be(p3_5)
    rewrittenPlan.id should be(helpers.as3_5(p3_4_id))
    rewrittenPlan.lhs.get.id should be(helpers.as3_5(s3_4_id))
  }

  test("should convert ErrorPlan") {
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val e3_4 = plansV3_4.ErrorPlan(a3_4, new utilv3_4.ExhaustiveShortestPathForbiddenException)

    val a3_5 = plansv3_5.AllNodesScan("n", Set.empty)

    val rewrittenPlan = convert[ErrorPlan](e3_4)
    rewrittenPlan shouldBe an[plansv3_5.ErrorPlan]
    rewrittenPlan.asInstanceOf[plansv3_5.ErrorPlan].source should be(a3_5)
    rewrittenPlan.asInstanceOf[plansv3_5.ErrorPlan].exception shouldBe an[utilv3_5.ExhaustiveShortestPathForbiddenException]
  }

  test("should convert NodeIndexSeek") {
    val var3_4 = expressionsv3_4.Variable("n")(pos3_4)
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val n3_4 = plansV3_4.NodeIndexSeek("a",
      expressionsv3_4.LabelToken("b", utilv3_4.LabelId(2)),
      Seq(expressionsv3_4.PropertyKeyToken("c", utilv3_4.PropertyKeyId(3))),
      plansV3_4.SingleQueryExpression(var3_4), Set.empty)

    val var3_5 = expressionsv3_5.Variable("n")(pos3_5)
    val a3_5 = plansv3_5.AllNodesScan("n", Set.empty)
    val n3_5 = plansv3_5.NodeIndexSeek("a",
      expressionsv3_5.LabelToken("b", utilv3_5.LabelId(2)),
      Seq(plansv3_5.IndexedProperty(expressionsv3_5.PropertyKeyToken("c", utilv3_5.PropertyKeyId(3)), plansv3_5.DoNotGetValue)),
      plansv3_5.SingleQueryExpression(var3_5), Set.empty, IndexOrderNone)

    convert[ErrorPlan](n3_4) should be(n3_5)
  }

  test("should convert ProcedureCall") {
    val var3_4 = expressionsv3_4.Variable("n")(pos3_4)
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val inputv3_4 = plansV3_4.FieldSignature("d", symbolsV3_4.CTString, Some(plansV3_4.CypherValue("e", symbolsV3_4.CTString)))
    val sigv3_4 = plansV3_4.ProcedureSignature(plansV3_4.QualifiedName(Seq("a", "b"), "c"), IndexedSeq(inputv3_4), None, None, plansV3_4.ProcedureReadWriteAccess(Array("foo", "bar")))
    val pres3_4 = astV3_4.ProcedureResultItem(Some(expressionsv3_4.ProcedureOutput("f")(pos3_4)), var3_4)(pos3_4)
    val rc3_4 = plansV3_4.ResolvedCall(sigv3_4, Seq(var3_4), IndexedSeq(pres3_4))(pos3_4)
    val pc3_4 = plansV3_4.ProcedureCall(a3_4, rc3_4)

    val var3_5 = expressionsv3_5.Variable("n")(pos3_5)
    val a3_5 = plansv3_5.AllNodesScan("n", Set.empty)
    val inputv3_5 = plansv3_5.FieldSignature("d", symbolsv3_5.CTString, Some(plansv3_5.CypherValue("e", symbolsv3_5.CTString)))
    val sigv3_5 = plansv3_5.ProcedureSignature(plansv3_5.QualifiedName(Seq("a", "b"), "c"), IndexedSeq(inputv3_5), None, None, plansv3_5.ProcedureReadWriteAccess(Array("foo", "bar")))
    val pres3_5 = astv3_5.ProcedureResultItem(Some(expressionsv3_5.ProcedureOutput("f")(pos3_5)), var3_5)(pos3_5)
    val rc3_5 = plansv3_5.ResolvedCall(sigv3_5, Seq(var3_5), IndexedSeq(pres3_5))(pos3_5)
    val pc3_5 = plansv3_5.ProcedureCall(a3_5, rc3_5)

    convert[ProcedureCall](pc3_4) should be(pc3_5)
  }

  test("should convert Sort") {
    val a3_4 = plansV3_4.AllNodesScan("n", Set.empty)
    val s3_4 = plansV3_4.Sort(a3_4, Seq(plansV3_4.Ascending("n")))

    val a3_5 = plansv3_5.AllNodesScan("n", Set.empty)
    val s3_5 = plansv3_5.Sort(a3_5, Seq(plansv3_5.Ascending("n")))

    convert[ProcedureCall](s3_4) should be(s3_5)
  }

  test("should convert function call with 'null' default value") {
    val allowed = Array.empty[String] // this is passed through as the same instance - so shared
    val name3_4 = plansV3_4.QualifiedName(Seq.empty, "foo")
    val call3_4 = plansV3_4.ResolvedFunctionInvocation(name3_4,
      Some(plansV3_4.UserFunctionSignature(name3_4, Vector(plansV3_4.FieldSignature("input", symbolsV3_4.CTAny,
        default = Some(plansV3_4.CypherValue(null, symbolsV3_4.CTAny)))),
        symbolsV3_4.CTAny, None, allowed, None, isAggregate = false)), Vector())(InputPositionV3_4(1, 2, 3))

    val name3_5 = plansv3_5.QualifiedName(Seq.empty, "foo")
    val call3_5 = plansv3_5.ResolvedFunctionInvocation(name3_5,
      Some(plansv3_5.UserFunctionSignature(name3_5, Vector(plansv3_5.FieldSignature("input", symbolsv3_5.CTAny,
        default = Some(plansv3_5.CypherValue(null, symbolsv3_5.CTAny)))),
        symbolsv3_5.CTAny, None, allowed, None, isAggregate = false)), Vector())(InputPosition(1, 2, 3))

    convert[plansv3_5.ResolvedFunctionInvocation](call3_4) should be(call3_5)
  }

  test("should convert all expressions") {
    val subTypes = reflectExpressions.getSubTypesOf(classOf[expressionsv3_4.Expression]).asScala ++
      reflectLogicalPlans.getSubTypesOf(classOf[expressionsv3_4.Expression]).asScala
    subTypes.filter { c => !Modifier.isAbstract(c.getModifiers) }
      .toList.sortBy(_.getName)
      .foreach { subType =>
      val constructor = subType.getConstructors.head
      val paramTypes = constructor.getParameterTypes
      Try {
        val constructorArgs = paramTypes.asInstanceOf[Array[Class[AnyRef]]].map(x => argumentProvider(x))
        constructor.newInstance(constructorArgs: _*).asInstanceOf[expressionsv3_4.Expression]
      } match {
        case Success(expressionV3_4) =>
          val rewritten = convert[expressionsv3_5.Expression](expressionV3_4)
          rewritten shouldBe an[expressionsv3_5.Expression]
        case Failure(e: InstantiationException) => fail(s"could not instantiate 3.4 expression: ${subType.getSimpleName} with arguments ${paramTypes.toList}", e)
        case Failure(e) => fail(s"Converting ${subType.getName} failed", e)
      }
    }
  }

  test("should convert all logical plans") {
    val subTypes = reflectLogicalPlans.getSubTypesOf(classOf[plansV3_4.LogicalPlan]).asScala
    subTypes.filter(c => !Modifier.isAbstract(c.getModifiers))
      .toList.sortBy(_.getName)
      .foreach { subType =>
        val constructor = subType.getConstructors.head
        val paramTypes = constructor.getParameterTypes
        val planV3_4 = {
          if (subType.getSimpleName.equals("Selection")) {
            // To avoid AssertionError when we would create empty Selection in the else branch otherwise
            Try(plansV3_4.Selection(Seq(expressionsv3_4.Variable("n")(pos3_4)), argumentProvider[plansV3_4.LogicalPlan](classOf[plansV3_4.LogicalPlan])))
          } else {
            Try {
              val constructorArgs = paramTypes.asInstanceOf[Array[Class[AnyRef]]].map(x => argumentProvider(x))
              constructor.newInstance(constructorArgs: _*).asInstanceOf[plansV3_4.LogicalPlan]
            }
          }
        }
        planV3_4 match {
          case Success(plan) =>
            val rewritten = convert[plansv3_5.LogicalPlan](plan)
            rewritten shouldBe an[plansv3_5.LogicalPlan]
          case Failure(e: InstantiationException) => fail(s"could not instantiate 3.5 logical plan: ${subType.getSimpleName} with arguments ${paramTypes.toList}", e)
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
    val variable = expressionsv3_4.Variable("n")(pos3_4)
    val value = clazz.getSimpleName match {
      case "Variable" => variable
      case "LogicalVariable" => variable
      case "Property" => expressionsv3_4.Property(variable, argumentProvider(classOf[expressionsv3_4.PropertyKeyName]))(pos3_4)
      case "PropertyKeyName" => expressionsv3_4.PropertyKeyName("n")(pos3_4)
      case "PathStep" => expressionsv3_4.NilPathStep
      case "Expression" => variable
      case "InputPosition" => pos3_4
      case "ReduceScope" => expressionsv3_4.ReduceScope(variable, variable, variable)(pos3_4)
      case "FilterScope" => expressionsv3_4.FilterScope(variable, Some(variable))(pos3_4)
      case "ExtractScope" => expressionsv3_4.ExtractScope(variable, Some(variable), Some(variable))(pos3_4)
      case "RelationshipsPattern" => expressionsv3_4.RelationshipsPattern(argumentProvider(classOf[expressionsv3_4.RelationshipChain]))(pos3_4)
      case "RelationshipChain" => expressionsv3_4.RelationshipChain(argumentProvider(classOf[expressionsv3_4.PatternElement]), argumentProvider(classOf[expressionsv3_4.RelationshipPattern]), argumentProvider(classOf[expressionsv3_4.NodePattern]))(pos3_4)
      case "RelationshipPattern" => expressionsv3_4.RelationshipPattern(Some(variable), Seq.empty, None, None, expressionsv3_4.SemanticDirection.OUTGOING)(pos3_4)
      case "NodePattern" => new expressionsv3_4.InvalidNodePattern(variable)(pos3_4)
      case "PatternElement" => new expressionsv3_4.InvalidNodePattern(variable)(pos3_4)
      case "NonEmptyList" => utilv3_4.NonEmptyList(1)
      case "Namespace" => expressionsv3_4.Namespace()(pos3_4)
      case "FunctionName" => expressionsv3_4.FunctionName("a")(pos3_4)
      case "SemanticDirection" => expressionsv3_4.SemanticDirection.OUTGOING
      case "ShortestPaths" => expressionsv3_4.ShortestPaths(argumentProvider(classOf[expressionsv3_4.PatternElement]), single = true)(pos3_4)
      case "CypherType" => symbolsV3_4.CTBoolean
      case "Scope" => frontendV3_4.semantics.Scope.empty
      case "Equals" => expressionsv3_4.Equals(variable, variable)(pos3_4)
      case "InequalitySeekRange" => plansV3_4.RangeGreaterThan(utilv3_4.NonEmptyList(plansV3_4.InclusiveBound(variable)))
      case "PrefixRange" => plansV3_4.PrefixRange(variable)
      case "PointDistanceRange" => plansV3_4.PointDistanceRange(1, 1, inclusive = true)
      case "LogicalProperty" => expressionsv3_4.Property(variable, argumentProvider(classOf[expressionsv3_4.PropertyKeyName]))(pos3_4)

      case "QueryExpression" => plansV3_4.SingleQueryExpression(variable)
      case "LogicalPlan" => plansV3_4.AllNodesScan("n", Set.empty)
      case "IdGen" => idGen3_4
      case "Exception" => new utilv3_4.ExhaustiveShortestPathForbiddenException
      case "IdName" => "n"
      case "LabelName" => expressionsv3_4.LabelName("n")(pos3_4)
      case "LabelToken" => expressionsv3_4.LabelToken("a", argumentProvider(classOf[utilv3_4.LabelId]))
      case "LabelId" => utilv3_4.LabelId(5)
      case "PropertyKeyToken" => expressionsv3_4.PropertyKeyToken("a", argumentProvider(classOf[utilv3_4.PropertyKeyId]))
      case "PropertyKeyId" => utilv3_4.PropertyKeyId(5)
      case "ResolvedCall" => plansV3_4.ResolvedCall(argumentProvider(classOf[plansV3_4.ProcedureSignature]), Seq.empty, IndexedSeq.empty)(pos3_4)
      case "ProcedureSignature" => plansV3_4.ProcedureSignature(argumentProvider(classOf[plansV3_4.QualifiedName]), IndexedSeq.empty, None, None, argumentProvider(classOf[plansV3_4.ProcedureAccessMode]))
      case "QualifiedName" => plansV3_4.QualifiedName(Seq.empty, "c")
      case "ProcedureAccessMode" => plansV3_4.ProcedureReadWriteAccess(Array())
      case "RelTypeName" => expressionsv3_4.RelTypeName("x")(pos3_4)
      case "SeekableArgs" => plansV3_4.SingleSeekableArg(variable)
      case "ExpansionMode" => plansV3_4.ExpandAll
      case "ShortestPathPattern" => irV3_4.ShortestPathPattern(None, argumentProvider(classOf[irV3_4.PatternRelationship]), single = true)(argumentProvider(classOf[expressionsv3_4.ShortestPaths]))
      case "PatternRelationship" => irV3_4.PatternRelationship("n", ("n", "n"), expressionsv3_4.SemanticDirection.OUTGOING, Seq.empty, irV3_4.SimplePatternLength)
      case "PatternLength" => irV3_4.SimplePatternLength
      case "Ties" => plansV3_4.IncludeTies
      case "CSVFormat" => irV3_4.HasHeaders
      case "VarPatternLength" => irV3_4.VarPatternLength(0, None)

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
  private def convert[T <: expressionsv3_5.Expression](input: expressionsv3_4.Expression): T = {
    val solveds = new SolvedsV3_4
    val cardinalities = new CardinalitiesV3_4
    input match {
      case nestedPlan: plansV3_4.NestedPlanExpression =>
        assignAttributesRecursivelyWithDefaultValues(nestedPlan.plan, solveds, cardinalities)
      case _ =>
    }
    LogicalPlanConverter.convertExpression(input, solveds, cardinalities, new SolvedsV3_5, new CardinalitiesV3_5, new MaxIdConverter)
  }

  /**
    * Converts a logical plan with default solved and cardinality.
    */
  private def convert[T <: plansv3_5.LogicalPlan](input: plansV3_4.LogicalPlan): plansv3_5.LogicalPlan = {
    val solveds = new SolvedsV3_4
    val cardinalities = new CardinalitiesV3_4
    assignAttributesRecursivelyWithDefaultValues(input, solveds, cardinalities)
    LogicalPlanConverter.convertLogicalPlan(input, solveds, cardinalities, new SolvedsV3_5, new CardinalitiesV3_5, new MaxIdConverter)._1
  }

  /**
    * Given a plan and a lambda deciding which expressions are important, returns the expression mapping
    */
  private def expressionMapping(input: plansV3_4.LogicalPlan,
                                seenBySemanticTable: expressionsv3_4.Expression => Boolean): ExpressionMapping4To5 = {
    val solveds = new SolvedsV3_4
    val cardinalities = new CardinalitiesV3_4
    assignAttributesRecursivelyWithDefaultValues(input, solveds, cardinalities)
    LogicalPlanConverter.convertLogicalPlan(input,
                                            solveds,
                                            cardinalities,
                                            new SolvedsV3_5,
                                            new CardinalitiesV3_5,
                                            new MaxIdConverter,
                                            seenBySemanticTable
                                          )._2
  }

  /**
    * Sets a default solved and cardinality for the input and all its subplans, recursively.
    */
  private def assignAttributesRecursivelyWithDefaultValues(input: plansV3_4.LogicalPlan,
                                                           solveds: SolvedsV3_4,
                                                           cardinalities: CardinalitiesV3_4) : Unit = {
    solveds.set(input.id, irV3_4.PlannerQuery.empty)
    cardinalities.set(input.id, utilv3_4.Cardinality(1.0))
    input.children.foreach {
      case p:plansV3_4.LogicalPlan => assignAttributesRecursivelyWithDefaultValues(p, solveds, cardinalities)
      case _ =>
    }
  }
}
