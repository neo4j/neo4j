/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionItem
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.notification.DeprecatedAmbiguousReferenceInSubclauseExpression
import org.neo4j.cypher.internal.notification.DeprecatedComplexAndAmbiguousReferenceInSubclauseExpression
import org.neo4j.cypher.internal.notification.DeprecatedComplexGroupingExpressionInSubclauseExpression
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.topDown

/**
 * Shared infrastructure for classifying subclause expressions per CIP-248.
 *
 * Classification of a subclause expression produces either a [[SemanticError]],
 * an [[InternalNotification]], or nothing. Errors and notifications can be raised
 * by either the implicit-grouping decision (Table A) or the explicit-GROUP-BY
 * decision (Table B); the inputs computed here are the same in both cases.
 */
object SubclauseExpressionClassifier {

  private val stringifier: ExpressionStringifier = ExpressionStringifier.pretty(_ => "")

  def aggArgRefs(scope: WorkingScope): Set[LogicalVariable] = scope match {
    case ExpressionScope(IsAggregate(_), _, _, _, _) => outerRefs(scope)
    case _                                           => scope.children.flatMap(aggArgRefs).toSet
  }

  /**
   * Variable use sites the user actually wrote inside the subclause expression's
   * scope tree.
   *
   *   - For an [[ExpressionScope]] whose AST node is a [[LogicalVariable]] we use
   *     `referenced` — that's the real use site.
   *   - For any other scope (including [[recognizedLeafScope]] for non-variable
   *     expressions) we read `hiddenReferences` only.
   *
   * Then recurse into children to pick up nested recognized leaves and ordinary
   * variable scopes.
   */
  def outerRefs(scope: WorkingScope): Set[LogicalVariable] = {
    val here = scope match {
      case ExpressionScope(_: LogicalVariable, _, referenced, _, _) =>
        referenced.getVariables.toSet
      case _ =>
        scope.hiddenReferences.getVariables.toSet
    }
    here ++ scope.children.flatMap(outerRefs).toSet
  }

  /**
   * Outcomes produced by either decision table for a single subclause expression.
   */
  sealed trait Classification {
    def expression: Expression
    def errors: Seq[SemanticError]
    def notifications: Seq[InternalNotification]
  }

  case class NonProblematic(expression: Expression) extends Classification {
    def errors: Seq[SemanticError] = Seq.empty
    def notifications: Seq[InternalNotification] = Seq.empty
  }

  case class Ambiguous(
    expression: Expression,
    problematicReferences: Set[LogicalVariable]
  ) extends Classification {
    def errors: Seq[SemanticError] = Seq.empty

    def notifications: Seq[InternalNotification] = Seq(
      DeprecatedAmbiguousReferenceInSubclauseExpression(
        expression.position,
        stringifier.apply(expression),
        problematicReferences.map(_.name).mkString(", ")
      )
    )
  }

  case class Complex(expression: Expression, item: ProjectionItem) extends Classification {
    def errors: Seq[SemanticError] = Seq.empty

    def notifications: Seq[InternalNotification] = Seq(
      DeprecatedComplexGroupingExpressionInSubclauseExpression(
        expression.position,
        stringifier.apply(expression),
        stringifier.apply(item.expression)
      )
    )
  }

  case class AmbiguousAndComplex(
    expression: Expression,
    problematicReferences: Set[LogicalVariable],
    item: ProjectionItem
  ) extends Classification {
    def errors: Seq[SemanticError] = Seq.empty

    def notifications: Seq[InternalNotification] = Seq(
      DeprecatedComplexAndAmbiguousReferenceInSubclauseExpression(
        expression.position,
        stringifier.apply(expression),
        problematicReferences.map(_.name).mkString(", "),
        stringifier.apply(item.expression)
      )
    )
  }

  case class AggregationReferencingDeclared(
    expression: Expression,
    problematicReferences: Set[LogicalVariable]
  ) extends Classification {

    def errors: Seq[SemanticError] = Seq(
      SemanticError.invalidReferenceInSubclauseExpression(
        problematicReferences.map(_.name).toSeq.sorted,
        expression.position
      )
    )
    def notifications: Seq[InternalNotification] = Seq.empty
  }

  /**
   * Aggregating subclause expression in a non-aggregating projection clause.
   * CIP-248 disallows this since aggregations require an aggregating
   * projection.
   */
  case class AggregationInNonAggregatingProjection(expression: Expression) extends Classification {

    def errors: Seq[SemanticError] = Seq(
      SemanticError.aggregateExpressionInSubclauseExpression(
        Seq(stringifier.apply(expression)),
        expression.position
      )
    )
    def notifications: Seq[InternalNotification] = Seq.empty
  }

  /**
   * Pre-computed inputs for a single subclause expression, assembled by
   * [[buildInputs]]. The decision tables ([[classifyImplicit]] /
   * [[classifyWithGroupBy]]) consume only this — they never touch the scope
   * tree or the projection specification directly.
   */
  case class ClassifierInputs(
    expression: Expression,
    item: Option[ProjectionItem],
    hasGroupBy: Boolean,
    isProjectionAggregating: Boolean,
    hasUnrecognizedAggregate: Boolean,
    aggArgs: Set[LogicalVariable],
    outer: Set[LogicalVariable],
    shadowing: Set[LogicalVariable],
    introduced: Set[LogicalVariable],
    groupingKeyAliases: Set[LogicalVariable]
  )

  /**
   * Assemble [[ClassifierInputs]] for one subclause expression: use-site
   * references from the scope tree, alias/grouping-key metadata from the
   * projection spec, and whether a *fresh* aggregate survives substitution of
   * recognised sub-expressions to their projected alias.
   */
  def apply(
    expression: Expression,
    expressionScope: WorkingScope,
    clauseScope: WorkingScope,
    spec: ProjectionSpecification,
    scopeState: ScopeState
  ): Classification = {
    val shadowing = spec.getShadowingDeclarations(clauseScope.incoming.allSymbols)
    val introduced = spec.getIntroducedSymbols
    val groupingKeyAliases = spec.groupingKeys.collect {
      case gk if gk.isRecognized && gk.alias.isDefined => gk.alias.get
    }

    val outer = outerRefs(expressionScope)
    val aggArgs = aggArgRefs(expressionScope)

    def transitive(refs: Set[LogicalVariable]): Set[LogicalVariable] = {
      val matched = refs.flatMap(r => spec.allItems.find(_.referenceableVariable == r))
      matched.flatMap(item => scopeState.getReferenced(item.expression))
    }

    val hasUnrecognizedAggregate = expression.containsAggregate && {
      val substituteRecognized: Rewriter = topDown(
        Rewriter.lift { case e: Expression => spec.substituteSubExpression(e, scopeState) },
        stopper = RewriterStopper.stopOn[ScopeExpression]
      )
      expression.endoRewrite(substituteRecognized).containsAggregate
    }

    classify(ClassifierInputs(
      expression = expression,
      item = spec.getContainingItem(expression),
      hasGroupBy = spec.hasGroupBy,
      isProjectionAggregating = spec.isAggregating,
      hasUnrecognizedAggregate = hasUnrecognizedAggregate,
      aggArgs = aggArgs union transitive(aggArgs),
      outer = outer,
      shadowing = shadowing,
      introduced = introduced,
      groupingKeyAliases = groupingKeyAliases
    ))
  }

  def classify(inputs: ClassifierInputs): Classification =
    if (inputs.hasGroupBy) classifyWithGroupBy(inputs)
    else classifyImplicit(inputs)

  /**
   * Table A — implicit grouping (no GROUP BY).
   */
  def classifyImplicit(inputs: ClassifierInputs): Classification = {
    import inputs.*
    val exprContainsAggregate = expression.containsAggregate
    val freshlyIntroduced = introduced -- groupingKeyAliases
    val outerShadowing = outer intersect shadowing
    val outerDeclared = outer intersect freshlyIntroduced
    val argShadowing = aggArgs intersect shadowing
    val argDeclared = aggArgs intersect freshlyIntroduced

    expression match {
      case _: Literal         => NonProblematic(expression)
      case _: Parameter       => NonProblematic(expression)
      case _: LogicalVariable => NonProblematic(expression)

      case _ if item.exists(_.expression == expression) && outerShadowing.nonEmpty =>
        Ambiguous(expression, outerShadowing)

      case _ if hasUnrecognizedAggregate =>
        val problematic = outerShadowing union outerDeclared union argShadowing union argDeclared
        if (!isProjectionAggregating)
          AggregationInNonAggregatingProjection(expression)
        else if (problematic.nonEmpty)
          AggregationReferencingDeclared(expression, problematic)
        else
          NonProblematic(expression)

      case _ if exprContainsAggregate && outerShadowing.nonEmpty =>
        Ambiguous(expression, outerShadowing)
      case _ if exprContainsAggregate =>
        NonProblematic(expression)

      case _ if item.exists(_.expression == expression) => NonProblematic(expression)

      case _ if item.exists(i => !i.isConstantOrReference) && outerShadowing.isEmpty =>
        Complex(expression, item.get)

      case _ if item.exists(i => !i.isConstantOrReference) && outerShadowing.nonEmpty =>
        AmbiguousAndComplex(expression, outerShadowing, item.get)

      case _ =>
        NonProblematic(expression)
    }
  }

  /**
   * Table B — explicit GROUP BY (strict mode).
   */
  def classifyWithGroupBy(inputs: ClassifierInputs): Classification = {
    import inputs.*
    val freshlyIntroduced = introduced -- groupingKeyAliases
    val problematic = (aggArgs union outer) intersect (shadowing union freshlyIntroduced)
    expression match {
      case _ if hasUnrecognizedAggregate && problematic.nonEmpty =>
        AggregationReferencingDeclared(expression, problematic)
      case _ =>
        NonProblematic(expression)
    }
  }
}
