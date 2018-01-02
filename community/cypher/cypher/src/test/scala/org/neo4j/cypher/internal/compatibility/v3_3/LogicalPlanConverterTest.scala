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
package org.neo4j.cypher.internal.compatibility.v3_3

import java.lang.reflect.Modifier

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition => InputPositionV3_3, SemanticDirection => SemanticDirectionV3_3, ast => astV3_3, symbols => symbolsV3_3}
import org.neo4j.cypher.internal.frontend.v3_4.{ast => astV3_4}
import org.neo4j.cypher.internal.frontend.{v3_3 => frontendV3_3}
import org.neo4j.cypher.internal.compiler.{v3_3 => compilerV3_3}
import org.neo4j.cypher.internal.ir.v3_3.{IdName => IdNameV3_3}
import org.neo4j.cypher.internal.ir.v3_4.{IdName => IdNameV3_4}
import org.neo4j.cypher.internal.ir.{v3_3 => irV3_3, v3_4 => irV3_4}
import org.neo4j.cypher.internal.util.v3_4.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.v3_4.{InputPosition, NonEmptyList, symbols => symbolsV3_4}
import org.neo4j.cypher.internal.util.{v3_4 => utilV3_4}
import org.neo4j.cypher.internal.v3_3.logical.{plans => plansV3_3}
import org.neo4j.cypher.internal.v3_4.expressions.{PathExpression, SemanticDirection}
import org.neo4j.cypher.internal.v3_4.logical.plans.{ErrorPlan, ProcedureCall}
import org.neo4j.cypher.internal.v3_4.logical.{plans => plansV3_4}
import org.neo4j.cypher.internal.v3_4.{expressions => expressionsV3_4}
import org.reflections.Reflections
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class LogicalPlanConverterTest extends FunSuite with Matchers {

  implicit val idGen = new SequentialIdGen()

  val pos3_3 = InputPositionV3_3(0,0,0)
  val pos3_4 = InputPosition(0,0,0)
  val reflectExpressions = new Reflections("org.neo4j.cypher.internal.frontend.v3_3.ast")
  val reflectLogicalPlanExpressions = new Reflections("org.neo4j.cypher.internal.compiler.v3_3.ast")
  val reflectLogicalPlans = new Reflections("org.neo4j.cypher.internal.v3_3.logical.plans")
  val solved3_3 = irV3_3.CardinalityEstimation.lift(irV3_3.PlannerQuery.empty, irV3_3.Cardinality.EMPTY)
  val solved3_4 = irV3_4.CardinalityEstimation.lift(irV3_4.PlannerQuery.empty, utilV3_4.Cardinality.EMPTY)

  test("should convert an IntegerLiteral with its position") {
    val i3_3 = astV3_3.SignedDecimalIntegerLiteral("5")(InputPositionV3_3(1, 2, 3))
    val i3_4 = expressionsV3_4.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 3))

    val rewritten = LogicalPlanConverter.convertExpression[expressionsV3_4.SignedDecimalIntegerLiteral](i3_3)
    rewritten should be(i3_4)
    rewritten.position should be(i3_4.position)
  }

  test("should convert an Add with its position (recursively)") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(InputPositionV3_3(1, 2, 3))
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(InputPositionV3_3(1, 2, 5))
    val add3_3 = astV3_3.Add(i3_3a, i3_3b)(InputPositionV3_3(1,2,3))
    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(InputPosition(1, 2, 3))
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(InputPosition(1, 2, 5))
    val add3_4 = expressionsV3_4.Add(i3_4a, i3_4b)(InputPosition(1,2,3))

    val rewritten = LogicalPlanConverter.convertExpression[expressionsV3_4.Add](add3_3)
    rewritten should be(add3_4)
    rewritten.position should equal(add3_4.position)
    rewritten.lhs.position should equal(i3_4a.position)
    rewritten.rhs.position should equal(i3_4b.position)
  }

  test("should convert Expression with Seq") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val l3_3 = astV3_3.ListLiteral(Seq(i3_3a, i3_3b))(pos3_3)
    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val l3_4 = expressionsV3_4.ListLiteral(Seq(i3_4a, i3_4b))(pos3_4)

    LogicalPlanConverter.convertExpression[expressionsV3_4.ListLiteral](l3_3) should be(l3_4)
  }

  test("should convert Expression with Option") {
    val i3_3 = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val v3_3 = astV3_3.Variable("var")(pos3_3)
    val f3_3 = astV3_3.FilterScope(v3_3, Some(i3_3))(pos3_3)
    val f3_3b = astV3_3.FilterScope(v3_3, None)(pos3_3)

    val i3_4 = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val v3_4 = expressionsV3_4.Variable("var")(pos3_4)
    val f3_4 = expressionsV3_4.FilterScope(v3_4, Some(i3_4))(pos3_4)
    val f3_4b = expressionsV3_4.FilterScope(v3_4, None)(pos3_4)

    LogicalPlanConverter.convertExpression[expressionsV3_4.FilterScope](f3_3) should be(f3_4)
    LogicalPlanConverter.convertExpression[expressionsV3_4.FilterScope](f3_3b) should be(f3_4b)
  }

  test("should convert Expression with Set") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val l3_3 = astV3_3.Ands(Set(i3_3a, i3_3b))(pos3_3)
    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val l3_4 = expressionsV3_4.Ands(Set(i3_4a, i3_4b))(pos3_4)

    LogicalPlanConverter.convertExpression[expressionsV3_4.Ands](l3_3) should be(l3_4)
  }

  test("should convert Expression with Seq of Tuple") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val i3_3c = astV3_3.SignedDecimalIntegerLiteral("10")(pos3_3)
    val i3_3d = astV3_3.SignedDecimalIntegerLiteral("11")(pos3_3)
    val c3_3 = astV3_3.CaseExpression(None, List((i3_3a, i3_3b), (i3_3c, i3_3d)), None)(pos3_3)

    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val i3_4c = expressionsV3_4.SignedDecimalIntegerLiteral("10")(pos3_4)
    val i3_4d = expressionsV3_4.SignedDecimalIntegerLiteral("11")(pos3_4)
    val c3_4 = expressionsV3_4.CaseExpression(None, List((i3_4a, i3_4b), (i3_4c, i3_4d)), None)(pos3_4)

    LogicalPlanConverter.convertExpression[expressionsV3_4.CaseExpression](c3_3) should be(c3_4)
  }

  test("should convert Expression with Seq of Tuple (MapExpression)") {
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val p3_3a = astV3_3.PropertyKeyName("a")(pos3_3)
    val p3_3b = astV3_3.PropertyKeyName("b")(pos3_3)
    val m3_3 = astV3_3.MapExpression(Seq((p3_3a, i3_3a),(p3_3b, i3_3b)))(pos3_3)

    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val p3_4a = expressionsV3_4.PropertyKeyName("a")(pos3_4)
    val p3_4b = expressionsV3_4.PropertyKeyName("b")(pos3_4)
    val m3_4 = expressionsV3_4.MapExpression(Seq((p3_4a, i3_4a),(p3_4b, i3_4b)))(pos3_4)

    LogicalPlanConverter.convertExpression[expressionsV3_4.CaseExpression](m3_3) should be(m3_4)
  }

  test("should convert PathExpression") {
    val var3_3 = astV3_3.Variable("n")(pos3_3)
    val psv3_3a = astV3_3.NilPathStep
    val psv3_3b = astV3_3.MultiRelationshipPathStep(var3_3, SemanticDirectionV3_3.BOTH, psv3_3a)
    val psv3_3c = astV3_3.SingleRelationshipPathStep(var3_3, SemanticDirectionV3_3.OUTGOING, psv3_3b)
    val psv3_3d = astV3_3.NodePathStep(var3_3, psv3_3c)
    val pexpv3_3 = astV3_3.PathExpression(psv3_3d)(pos3_3)

    val var3_4 = expressionsV3_4.Variable("n")(pos3_4)
    val psv3_4a = expressionsV3_4.NilPathStep
    val psv3_4b = expressionsV3_4.MultiRelationshipPathStep(var3_4, SemanticDirection.BOTH, psv3_4a)
    val psv3_4c = expressionsV3_4.SingleRelationshipPathStep(var3_4, SemanticDirection.OUTGOING, psv3_4b)
    val psv3_4d = expressionsV3_4.NodePathStep(var3_4, psv3_4c)
    val pexpv3_4 = expressionsV3_4.PathExpression(psv3_4d)(pos3_4)

    LogicalPlanConverter.convertExpression[PathExpression](pexpv3_3) should be(pexpv3_4)
  }

  test("should convert AndedPropertyInequalities") {
    val var3_3 = astV3_3.Variable("n")(pos3_3)
    val p3_3 = astV3_3.Property(var3_3, astV3_3.PropertyKeyName("n")(pos3_3))(pos3_3)
    val i3_3a = astV3_3.LessThan(var3_3, var3_3)(pos3_3)
    val i3_3b = astV3_3.LessThan(var3_3, var3_3)(pos3_3)
    val i3_3c = astV3_3.GreaterThan(var3_3, var3_3)(pos3_3)
    val a3_3 = astV3_3.AndedPropertyInequalities(var3_3, p3_3, frontendV3_3.helpers.NonEmptyList(i3_3a, i3_3b, i3_3c))

    val var3_4 = expressionsV3_4.Variable("n")(pos3_4)
    val p3_4 = expressionsV3_4.Property(var3_4, expressionsV3_4.PropertyKeyName("n")(pos3_4))(pos3_4)
    val i3_4a = expressionsV3_4.LessThan(var3_4, var3_4)(pos3_4)
    val i3_4b = expressionsV3_4.LessThan(var3_4, var3_4)(pos3_4)
    val i3_4c = expressionsV3_4.GreaterThan(var3_4, var3_4)(pos3_4)
    val a3_4 = expressionsV3_4.AndedPropertyInequalities(var3_4, p3_4, NonEmptyList(i3_4a, i3_4b, i3_4c))

    LogicalPlanConverter.convertExpression[PathExpression](a3_3) should be(a3_4)
  }

  test("should convert Parameter and CypherTypes") {
    val p3_3a = astV3_3.Parameter("a", symbolsV3_3.CTBoolean)(pos3_3)
    val p3_3b = astV3_3.Parameter("a", symbolsV3_3.CTList(symbolsV3_3.CTAny))(pos3_3)
    val p3_4a = expressionsV3_4.Parameter("a", symbolsV3_4.CTBoolean)(pos3_4)
    val p3_4b = expressionsV3_4.Parameter("a", symbolsV3_4.CTList(symbolsV3_4.CTAny))(pos3_4)

    LogicalPlanConverter.convertExpression[PathExpression](p3_3a) should be(p3_4a)
    LogicalPlanConverter.convertExpression[PathExpression](p3_3b) should be(p3_4b)
  }

  test("should not save expression mappings if isImportant always returns false") {
    def isImportant(expression: astV3_3.Expression): Boolean = false

    val var3_3 = astV3_3.Variable("n")(pos3_3)
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val l3_3 = plansV3_3.Limit(a3_3, var3_3, plansV3_3.IncludeTies)(solved3_3)
    l3_3.assignIds()

    val expressionMapping = LogicalPlanConverter.convertLogicalPlan[plansV3_4.NodeIndexSeek](l3_3, isImportant)._2
    expressionMapping shouldBe empty
  }

  test("should save expression mappings if isImportant always returns true") {
    def isImportant(expression: astV3_3.Expression): Boolean = true

    val var3_3 = astV3_3.Variable("n")(pos3_3)
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val l3_3 = plansV3_3.Limit(a3_3, var3_3, plansV3_3.IncludeTies)(solved3_3)
    l3_3.assignIds()

    val var3_4 = expressionsV3_4.Variable("n")(pos3_4)

    val expressionMapping = LogicalPlanConverter.convertLogicalPlan[plansV3_4.NodeIndexSeek](l3_3, isImportant)._2
    expressionMapping should contain only ((var3_3, var3_3.position) -> var3_4)
  }

  test("should save distinct expressions with different positions in expression mappings") {
    def isImportant(expression: astV3_3.Expression): Boolean = true

    val var3_3a = astV3_3.Variable("n")(InputPositionV3_3(0, 0, 0))
    val var3_3b = astV3_3.Variable("n")(InputPositionV3_3(1, 1, 1))
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val l3_3a = plansV3_3.Limit(a3_3, var3_3a, plansV3_3.IncludeTies)(solved3_3)
    val l3_3b = plansV3_3.Limit(l3_3a, var3_3b, plansV3_3.IncludeTies)(solved3_3)
    l3_3b.assignIds()

    val var3_4a = expressionsV3_4.Variable("n")(InputPosition(0, 0, 0))
    val var3_4b = expressionsV3_4.Variable("n")(InputPosition(1, 1, 1))

    val expressionMapping = LogicalPlanConverter.convertLogicalPlan[plansV3_4.NodeIndexSeek](l3_3b, isImportant)._2
    expressionMapping should contain only((var3_3a, var3_3a.position) -> var3_4a, (var3_3b, var3_3b.position) -> var3_4b)
  }

  test("should provide minimal implementation of solved after plan conversion") {
    val solved = irV3_3.CardinalityEstimation.lift(irV3_3.PlannerQuery.empty, irV3_3.Cardinality(5.0))
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved)
    a3_3.assignIds()

    val rewrittenPlan = LogicalPlanConverter.convertLogicalPlan[plansV3_4.AllNodesScan](a3_3)._1
    rewrittenPlan.solved.readOnly should equal(solved.readOnly)
    rewrittenPlan.solved.estimatedCardinality should equal(helpers.as3_4(solved.estimatedCardinality))
  }

  test("should convert AllNodeScan and keep id") {
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    a3_3.assignIds()
    val id3_3 = a3_3.assignedId
    val a3_4 = plansV3_4.AllNodesScan(IdNameV3_4("n"), Set.empty)(solved3_4)

    val rewrittenPlan = LogicalPlanConverter.convertLogicalPlan[plansV3_4.AllNodesScan](a3_3)._1
    rewrittenPlan should be(a3_4)
    rewrittenPlan.id should be(helpers.as3_4(id3_3))
  }

  test("should convert Aggregation and keep ids") {
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val i3_3a = astV3_3.SignedDecimalIntegerLiteral("2")(pos3_3)
    val i3_3b = astV3_3.SignedDecimalIntegerLiteral("5")(pos3_3)
    val ag3_3 = plansV3_3.Aggregation(a3_3, Map("a" -> i3_3a), Map("b" -> i3_3b))(solved3_3)
    ag3_3.assignIds()
    val ans_id = a3_3.assignedId
    val ag_id = ag3_3.assignedId

    val a3_4 = plansV3_4.AllNodesScan(IdNameV3_4("n"), Set.empty)(solved3_4)
    val i3_4a = expressionsV3_4.SignedDecimalIntegerLiteral("2")(pos3_4)
    val i3_4b = expressionsV3_4.SignedDecimalIntegerLiteral("5")(pos3_4)
    val ag3_4 = plansV3_4.Aggregation(a3_4, Map("a" -> i3_4a), Map("b" -> i3_4b))(solved3_4)

    val rewrittenPlan = LogicalPlanConverter.convertLogicalPlan[plansV3_4.Aggregation](ag3_3)._1
    rewrittenPlan should be(ag3_4)
    rewrittenPlan.id should be(helpers.as3_4(ag_id))
    rewrittenPlan.lhs.get.id should be(helpers.as3_4(ans_id))
  }

  test("should convert ProduceResult and keep ids") {
    val s3_3 = plansV3_3.SingleRow()(solved3_3)
    val p3_3 = plansV3_3.ProduceResult(Seq("a"), s3_3)
    p3_3.assignIds()

    val s3_3_id = s3_3.assignedId
    val p3_3_id = p3_3.assignedId

    val s3_4 = plansV3_4.Argument()(solved3_4)
    val p3_4 = plansV3_4.ProduceResult(s3_4, Seq("a"))

    val rewrittenPlan = LogicalPlanConverter.convertLogicalPlan[plansV3_4.ProduceResult](p3_3)._1
    rewrittenPlan should be(p3_4)
    rewrittenPlan.id should be(helpers.as3_4(p3_3_id))
    rewrittenPlan.lhs.get.id should be(helpers.as3_4(s3_3_id))
  }

  test("should convert ErrorPlan") {
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val e3_3 = plansV3_3.ErrorPlan(a3_3, new frontendV3_3.ExhaustiveShortestPathForbiddenException)(solved3_3)
    e3_3.assignIds()

    val a3_4 = plansV3_4.AllNodesScan(IdNameV3_4("n"), Set.empty)(solved3_4)

    val rewrittenPlan = LogicalPlanConverter.convertLogicalPlan[ErrorPlan](e3_3)._1
    rewrittenPlan shouldBe an[plansV3_4.ErrorPlan]
    rewrittenPlan.asInstanceOf[plansV3_4.ErrorPlan].source should be(a3_4)
    rewrittenPlan.asInstanceOf[plansV3_4.ErrorPlan].exception shouldBe an[utilV3_4.ExhaustiveShortestPathForbiddenException]
  }

  test("should convert NodeIndexSeek") {
    val var3_3 = astV3_3.Variable("n")(pos3_3)
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val n3_3 = plansV3_3.NodeIndexSeek(IdNameV3_3("a"),
      astV3_3.LabelToken("b", frontendV3_3.LabelId(2)),
      Seq(astV3_3.PropertyKeyToken("c", frontendV3_3.PropertyKeyId(3))),
      plansV3_3.ScanQueryExpression(var3_3), Set.empty)(solved3_3)
    n3_3.assignIds()

    val var3_4 = expressionsV3_4.Variable("n")(pos3_4)
    val a3_4 = plansV3_4.AllNodesScan(IdNameV3_4("n"), Set.empty)(solved3_4)
    val n3_4 = plansV3_4.NodeIndexSeek(IdNameV3_4("a"),
      expressionsV3_4.LabelToken("b", utilV3_4.LabelId(2)),
      Seq(expressionsV3_4.PropertyKeyToken("c", utilV3_4.PropertyKeyId(3))),
      plansV3_4.ScanQueryExpression(var3_4), Set.empty)(solved3_4)

    LogicalPlanConverter.convertLogicalPlan[ErrorPlan](n3_3)._1 should be(n3_4)
  }

  test("should convert ProcedureCall") {
    val var3_3 = astV3_3.Variable("n")(pos3_3)
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val inputv3_3 = plansV3_3.FieldSignature("d", symbolsV3_3.CTString, Some(plansV3_3.CypherValue("e", symbolsV3_3.CTString)))
    val sigv3_3 = plansV3_3.ProcedureSignature(plansV3_3.QualifiedName(Seq("a", "b"), "c"), IndexedSeq(inputv3_3), None, None, plansV3_3.ProcedureReadWriteAccess(Array("foo", "bar")))
    val pres3_3 = astV3_3.ProcedureResultItem(Some(astV3_3.ProcedureOutput("f")(pos3_3)), var3_3)(pos3_3)
    val rc3_3 = plansV3_3.ResolvedCall(sigv3_3, Seq(var3_3), IndexedSeq(pres3_3))(pos3_3)
    val pc3_3 = plansV3_3.ProcedureCall(a3_3, rc3_3)(solved3_3)
    pc3_3.assignIds()

    val var3_4 = expressionsV3_4.Variable("n")(pos3_4)
    val a3_4 = plansV3_4.AllNodesScan(IdNameV3_4("n"), Set.empty)(solved3_4)
    val inputv3_4 = plansV3_4.FieldSignature("d", symbolsV3_4.CTString, Some(plansV3_4.CypherValue("e", symbolsV3_4.CTString)))
    val sigv3_4 = plansV3_4.ProcedureSignature(plansV3_4.QualifiedName(Seq("a", "b"), "c"), IndexedSeq(inputv3_4), None, None, plansV3_4.ProcedureReadWriteAccess(Array("foo", "bar")))
    val pres3_4 = astV3_4.ProcedureResultItem(Some(expressionsV3_4.ProcedureOutput("f")(pos3_4)), var3_4)(pos3_4)
    val rc3_4 = plansV3_4.ResolvedCall(sigv3_4, Seq(var3_4), IndexedSeq(pres3_4))(pos3_4)
    val pc3_4 = plansV3_4.ProcedureCall(a3_4, rc3_4)(solved3_4)

    val plan = LogicalPlanConverter.convertLogicalPlan[ProcedureCall](pc3_3)._1
    plan should be(pc3_4)
  }

  test("should convert Sort") {
    val a3_3 = plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
    val s3_3 = plansV3_3.Sort(a3_3, Seq(plansV3_3.Ascending(IdNameV3_3("n"))))(solved3_3)
    s3_3.assignIds()

    val a3_4 = plansV3_4.AllNodesScan(IdNameV3_4("n"), Set.empty)(solved3_4)
    val s3_4 = plansV3_4.Sort(a3_4, Seq(plansV3_4.Ascending(IdNameV3_4("n"))))(solved3_4)

    val plan = LogicalPlanConverter.convertLogicalPlan[ProcedureCall](s3_3)._1
    plan should be(s3_4)
  }

  test("should convert all expressions") {
    val subTypes = reflectExpressions.getSubTypesOf(classOf[astV3_3.Expression]).asScala ++
      reflectLogicalPlanExpressions.getSubTypesOf(classOf[astV3_3.Expression]).asScala
    subTypes.filter { c => !Modifier.isAbstract(c.getModifiers) }
      .toList.sortBy(_.getName)
      .foreach { subType =>
      val constructor = subType.getConstructors.head
      val paramTypes = constructor.getParameterTypes
      Try {
        val constructorArgs = paramTypes.asInstanceOf[Array[Class[AnyRef]]].map(x => argumentProvider(x))
        constructor.newInstance(constructorArgs: _*).asInstanceOf[astV3_3.Expression]
      } match {
        case Success(expressionV3_3) =>
          if(expressionV3_3.isInstanceOf[compilerV3_3.ast.NestedPlanExpression]) {
            expressionV3_3.asInstanceOf[compilerV3_3.ast.NestedPlanExpression].plan.assignIds()
          }
          val rewritten = LogicalPlanConverter.convertExpression[expressionsV3_4.Expression](expressionV3_3)
          rewritten shouldBe an[expressionsV3_4.Expression]
        case Failure(e: InstantiationException) => fail(s"could not instantiate 3.3 expression: ${subType.getSimpleName} with arguments ${paramTypes.toList}", e)
        case Failure(e) => fail(s"Converting ${subType.getName} failed", e)
      }
    }
  }

  test("should convert all logical plans") {
    val subTypes = reflectLogicalPlans.getSubTypesOf(classOf[plansV3_3.LogicalPlan]).asScala
    subTypes.filter { c => !Modifier.isAbstract(c.getModifiers) }
      .toList.sortBy(_.getName)
      .foreach { subType =>
        val constructor = subType.getConstructors.head
        val paramTypes = constructor.getParameterTypes
        val planV3_3 = {
          if (subType.getSimpleName.equals("Selection")) {
            // To avoid AssertionError
            Try(plansV3_3.Selection(Seq(astV3_3.Variable("n")(pos3_3)), argumentProvider[plansV3_3.LogicalPlan](classOf[plansV3_3.LogicalPlan]))(solved3_3))
          } else {
            Try {
              val constructorArgs = paramTypes.asInstanceOf[Array[Class[AnyRef]]].map(x => argumentProvider(x))
              constructor.newInstance(constructorArgs: _*).asInstanceOf[plansV3_3.LogicalPlan]
            }
          }
        }
        planV3_3 match {
          case Success(plan) =>
            plan.assignIds()
            val rewritten = LogicalPlanConverter.convertLogicalPlan[plansV3_4.LogicalPlan](plan)._1
            rewritten shouldBe an[plansV3_4.LogicalPlan]
          case Failure(e: InstantiationException) => fail(s"could not instantiate 3.4 logical plan: ${subType.getSimpleName} with arguments ${paramTypes.toList}", e)
          case Failure(e) => fail(s"Converting ${subType.getName} failed", e)
        }
      }
  }

  private def argumentProvider[T <: AnyRef](clazz: Class[T]): T = {
    val variable = astV3_3.Variable("n")(pos3_3)
    val value = clazz.getSimpleName match {
      case "Variable" => variable
      case "Property" => astV3_3.Property(variable, argumentProvider(classOf[astV3_3.PropertyKeyName]))(pos3_3)
      case "PropertyKeyName" => astV3_3.PropertyKeyName("n")(pos3_3)
      case "PathStep" => astV3_3.NilPathStep
      case "Expression" => variable
      case "InputPosition" => pos3_3
      case "ReduceScope" => astV3_3.ReduceScope(variable, variable, variable)(pos3_3)
      case "FilterScope" => astV3_3.FilterScope(variable, Some(variable))(pos3_3)
      case "ExtractScope" => astV3_3.ExtractScope(variable, Some(variable), Some(variable))(pos3_3)
      case "RelationshipsPattern" => astV3_3.RelationshipsPattern(argumentProvider(classOf[astV3_3.RelationshipChain]))(pos3_3)
      case "RelationshipChain" => astV3_3.RelationshipChain(argumentProvider(classOf[astV3_3.PatternElement]), argumentProvider(classOf[astV3_3.RelationshipPattern]), argumentProvider(classOf[astV3_3.NodePattern]))(pos3_3)
      case "RelationshipPattern" => astV3_3.RelationshipPattern(Some(variable), Seq.empty, None, None, frontendV3_3.SemanticDirection.OUTGOING)(pos3_3)
      case "NodePattern" => new astV3_3.InvalidNodePattern(variable)(pos3_3)
      case "PatternElement" => new astV3_3.InvalidNodePattern(variable)(pos3_3)
      case "NonEmptyList" => frontendV3_3.helpers.NonEmptyList(1)
      case "Namespace" => astV3_3.Namespace()(pos3_3)
      case "FunctionName" => astV3_3.FunctionName("a")(pos3_3)
      case "SemanticDirection" => frontendV3_3.SemanticDirection.OUTGOING
      case "ShortestPaths" => astV3_3.ShortestPaths(argumentProvider(classOf[astV3_3.PatternElement]), single = true)(pos3_3)
      case "CypherType" => symbolsV3_3.CTBoolean
      case "Scope" => frontendV3_3.Scope.empty
      case "Equals" => astV3_3.Equals(variable, variable)(pos3_3)
      case "InequalitySeekRange" => compilerV3_3.RangeGreaterThan(frontendV3_3.helpers.NonEmptyList(frontendV3_3.InclusiveBound(variable)))
      case "PrefixRange" => compilerV3_3.PrefixRange(variable)

      case "LogicalPlan" => plansV3_3.AllNodesScan(IdNameV3_3("n"), Set.empty)(solved3_3)
      case "PlannerQuery" => solved3_3
      case "Exception" => new frontendV3_3.ExhaustiveShortestPathForbiddenException
      case "IdName" => IdNameV3_3("n")
      case "LabelName" => astV3_3.LabelName("n")(pos3_3)
      case "LabelToken" => astV3_3.LabelToken("a", argumentProvider(classOf[frontendV3_3.LabelId]))
      case "LabelId" => frontendV3_3.LabelId(5)
      case "PropertyKeyToken" => astV3_3.PropertyKeyToken("a", argumentProvider(classOf[frontendV3_3.PropertyKeyId]))
      case "PropertyKeyId" => frontendV3_3.PropertyKeyId(5)
      case "ResolvedCall" => plansV3_3.ResolvedCall(argumentProvider(classOf[plansV3_3.ProcedureSignature]), Seq.empty, IndexedSeq.empty)(pos3_3)
      case "ProcedureSignature" => plansV3_3.ProcedureSignature(argumentProvider(classOf[plansV3_3.QualifiedName]), IndexedSeq.empty, None, None, argumentProvider(classOf[plansV3_3.ProcedureAccessMode]))
      case "QualifiedName" => plansV3_3.QualifiedName(Seq.empty, "c")
      case "ProcedureAccessMode" => plansV3_3.ProcedureReadWriteAccess(Array())
      case "RelTypeName" => astV3_3.RelTypeName("x")(pos3_3)
      case "QueryExpression" => plansV3_3.ScanQueryExpression(variable)
      case "SeekableArgs" => plansV3_3.SingleSeekableArg(variable)
      case "ExpansionMode" => plansV3_3.ExpandAll
      case "ShortestPathPattern" => irV3_3.ShortestPathPattern(None, argumentProvider(classOf[irV3_3.PatternRelationship]), single = true)(argumentProvider(classOf[astV3_3.ShortestPaths]))
      case "PatternRelationship" => irV3_3.PatternRelationship(IdNameV3_3("n"), (IdNameV3_3("n"), IdNameV3_3("n")), frontendV3_3.SemanticDirection.OUTGOING, Seq.empty, irV3_3.SimplePatternLength)
      case "PatternLength" => irV3_3.SimplePatternLength
      case "Ties" => plansV3_3.IncludeTies
      case "CSVFormat" => irV3_3.HasHeaders
      case "VarPatternLength" => irV3_3.VarPatternLength(0, None)

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
}
