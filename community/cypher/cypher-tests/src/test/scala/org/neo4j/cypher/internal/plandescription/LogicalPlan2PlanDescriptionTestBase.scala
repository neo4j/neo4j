/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.QueryPlanTestSupport.StubExecutionPlan
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersionArgument
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.language.implicitConversions

object LogicalPlan2PlanDescriptionTestBase {

  def anonVar(number: String) = s"anon_$number"

  def details(infos: Seq[String]): Details = Details(infos.map(asPrettyString.raw))

  def details(info: String): Details = Details(asPrettyString.raw(info))

  def planDescription(
    id: Id,
    name: String,
    children: Seq[InternalPlanDescription],
    arguments: Seq[Argument] = Seq.empty,
    variables: Set[String] = Set.empty
  ): PlanDescriptionImpl = PlanDescriptionImpl(id, name, children, arguments, variables.map(asPrettyString.raw))
}

class LogicalPlan2PlanDescriptionTestBase extends CommunityCypherTestSuite with TableDrivenPropertyChecks
    with AstConstructionTestSupport {

  protected val RUNTIME_VERSION: RuntimeVersion = RuntimeVersion.currentVersion
  protected val PLANNER_VERSION: PlannerVersionArgument = PlannerVersionArgument.currentVersion

  implicit val idGen: IdGen = new SequentialIdGen()
  protected val effectiveCardinalities = new EffectiveCardinalities
  protected val providedOrders = new ProvidedOrders
  protected val id: Id = Id.INVALID_ID

  implicit def lift(amount: Double): EffectiveCardinality = EffectiveCardinality(amount)

  /**
   * attaches given meta-info to the given plan
   */
  protected def attach[P <: LogicalPlan](
    plan: P,
    effectiveCardinality: EffectiveCardinality,
    providedOrder: ProvidedOrder = ProvidedOrder.empty
  ): P = {
    effectiveCardinalities.set(plan.id, effectiveCardinality)
    providedOrders.set(plan.id, providedOrder)
    plan
  }

  // Help methods

  protected def assertGood(
    logicalPlan: LogicalPlan,
    expectedPlanDescription: InternalPlanDescription,
    validateAllArgs: Boolean = false,
    readOnly: Boolean = true,
    cypherVersion: CypherVersion = CypherVersion.Legacy.legacyVersion()
  ): Unit = {
    val producedPlanDescription = LogicalPlan2PlanDescription.create(
      logicalPlan,
      IDPPlannerName,
      readOnly,
      ImmutablePlanningAttributes.EffectiveCardinalities(effectiveCardinalities),
      withRawCardinalities = false,
      withDistinctness = false,
      renderNestedPlanExpressions = false,
      providedOrders = ImmutablePlanningAttributes.ProvidedOrders(providedOrders),
      StubExecutionPlan().operatorMetadata,
      cypherVersion
    )

    def shouldValidateArg(arg: Argument) =
      validateAllArgs ||
        !(arg.isInstanceOf[PlannerImpl] ||
          arg.isInstanceOf[Planner] ||
          arg.isInstanceOf[EstimatedRows] ||
          arg.isInstanceOf[Version] ||
          arg.isInstanceOf[RuntimeVersion] ||
          arg.isInstanceOf[PlannerVersionArgument])

    def shouldBeEqual(a: InternalPlanDescription, b: InternalPlanDescription): Unit = {
      withClue("name")(a.name should equal(b.name))
      if (validateAllArgs) {
        withClue("arguments(all)")(a.arguments should equal(b.arguments))
      } else {
        val aArgsToValidate = a.arguments.filter(shouldValidateArg)
        val bArgsToValidate = b.arguments.filter(shouldValidateArg)

        withClue("arguments")(aArgsToValidate should equal(bArgsToValidate))
      }
      withClue("variables")(a.variables should equal(b.variables))
    }

    shouldBeEqual(producedPlanDescription, expectedPlanDescription)

    withClue("children") {
      (expectedPlanDescription.children, producedPlanDescription.children) match {
        case (Seq(), Seq()) =>
        case (Seq(expectedChild), Seq(producedChild)) =>
          shouldBeEqual(expectedChild, producedChild)
        case (Seq(expectedLhs, expectedRhs), Seq(producedLhs, producedRhs)) =>
          shouldBeEqual(expectedLhs, producedLhs)
          shouldBeEqual(expectedRhs, producedRhs)
        case (expected, produced) =>
          fail(s"${expected.getClass} does not equal ${produced.getClass}")
      }
    }
  }

  protected def cachedProp(varName: String, propName: String): CachedProperty =
    CachedProperty(varFor(varName), varFor(varName), PropertyKeyName(propName)(pos), NODE_TYPE)(pos)

  protected def label(name: String): LabelName = LabelName(name)(pos)

  protected def relType(name: String): RelTypeName = RelTypeName(name)(pos)
  protected def relType(expr: Expression): DynamicRelTypeExpression = DynamicRelTypeExpression(expr)(pos)

  protected def key(name: String): PropertyKeyName = PropertyKeyName(name)(pos)

  protected def number(i: String): SignedDecimalIntegerLiteral = SignedDecimalIntegerLiteral(i)(pos)
  protected def float(i: String): DecimalDoubleLiteral = DecimalDoubleLiteral(i)(pos)

  protected def stringLiteral(s: String): StringLiteral = StringLiteral(s)(pos.withInputLength(0))
}
