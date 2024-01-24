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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.CreateOrInsert
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_TYPE_CHECK
import org.neo4j.cypher.internal.frontend.phases.ListCoercedToBooleanCheck.listCoercedToBooleanCheck
import org.neo4j.cypher.internal.frontend.phases.PatternExpressionInNonExistenceCheck.patternExpressionInNonExistenceCheck
import org.neo4j.cypher.internal.frontend.phases.SemanticTypeCheck.SemanticErrorCheck
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * Checks for semantic errors when semantic table has been initialized.
 *
 * Does not change the State, just checks for semantic errors.
 */
case object SemanticTypeCheck extends VisitorPhase[BaseContext, BaseState]
    with StepSequencer.Step
    with DefaultPostCondition
    with ParsePipelineTransformerFactory {

  type SemanticErrorCheck = (BaseState, BaseContext) => Seq[SemanticError]

  val checks: Seq[SemanticErrorCheck] = Seq(
    patternExpressionInNonExistenceCheck,
    CreatePatternSelfReferenceCheck.check,
    listCoercedToBooleanCheck
  )

  override def visit(from: BaseState, context: BaseContext): Unit = {
    context.errorHandler(checks.flatMap(_.apply(from, context)))
  }

  override val phase = SEMANTIC_TYPE_CHECK

  override def preConditions: Set[StepSequencer.Condition] = Set(
    BaseContains[Statement](),
    BaseContains[SemanticTable]()
  ) ++ SemanticInfoAvailable

  // necessary because VisitorPhase defines empty postConditions
  override def postConditions: Set[StepSequencer.Condition] = Set(completed)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    literalExtractionStrategy: LiteralExtractionStrategy,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    semanticFeatures: Seq[SemanticFeature],
    obfuscateLiterals: Boolean
  ): Transformer[BaseContext, BaseState, BaseState] = this

}

trait ExpectedBooleanTypeCheck {

  def isExpectedTypeBoolean(semanticTable: SemanticTable, e: Expression): Boolean =
    semanticTable.types.get(e)
      .flatMap(_.expected)
      .exists(CTBoolean.covariant.containsAll)
}

object PatternExpressionInNonExistenceCheck extends ExpectedBooleanTypeCheck {

  def patternExpressionInNonExistenceCheck: SemanticErrorCheck = (baseState, _) => {

    baseState.statement().folder.treeFold(Seq.empty[SemanticError]) {
      case Exists(_) =>
        // Don't look inside exists()
        errors => SkipChildren(errors)

      // The replacement for size(PatternExpression) is COUNT {PatternExpression} and not size(PatternComprehension).
      case FunctionInvocation(_, FunctionName("size"), _, Vector(p: PatternExpression), _, _)
        if !isExpectedTypeBoolean(baseState.semanticTable(), p) =>
        errors => SkipChildren(errors :+ SemanticError(errorMessageForSizeFunction, p.position))

      case p: PatternExpression if !isExpectedTypeBoolean(baseState.semanticTable(), p) =>
        errors => SkipChildren(errors :+ SemanticError(errorMessage, p.position))
    }
  }

  val errorMessage: String = "A pattern expression should only be used in order to test the existence of a pattern. " +
    "It should therefore only be used in contexts that evaluate to a boolean, e.g. inside the function exists() or in a WHERE-clause. " +
    "No other uses are allowed, instead they should be replaced by a pattern comprehension."

  val errorMessageForSizeFunction: String =
    "A pattern expression should only be used in order to test the existence of a pattern. " +
      "It can no longer be used inside the function size(), an alternative is to replace size() with COUNT {}."
}

object CreatePatternSelfReferenceCheck {

  def check: SemanticErrorCheck = (baseState, baseContext) => {
    val semanticTable = baseState.semanticTable()
    baseState.statement().folder.treeFold(Seq.empty[SemanticError]) {
      case c: CreateOrInsert =>
        accErrors =>
          val errors = findSelfReferenceVariablesInPattern(c.pattern, semanticTable)
            .map(createError(_, semanticTable, baseContext.errorMessageProvider))
            .toSeq
          SkipChildren(accErrors ++ errors)
    }
  }

  private def findSelfReferenceVariablesInPattern(
    pattern: Pattern,
    semanticTable: SemanticTable
  ): Set[LogicalVariable] = {
    val allSymbolDefinitions = semanticTable.recordedScopes(pattern).allSymbolDefinitions

    def findAllVariables(e: Any): Set[LogicalVariable] =
      e.folder.findAllByClass[LogicalVariable].toSet.filter(v => !isDefinition(v))
    def isDefinition(variable: LogicalVariable): Boolean =
      allSymbolDefinitions(variable.name).map(_.use).contains(Ref(variable))

    pattern.patternParts.flatMap { patternParts =>
      val (declaredVariables, referencedVariables) =
        patternParts.folder.treeFold[(Set[LogicalVariable], Set[LogicalVariable])]((Set.empty, Set.empty)) {
          case NodePattern(maybeVariable, _, maybeProperties, _) => acc =>
              SkipChildren((acc._1 ++ maybeVariable.filter(isDefinition), acc._2 ++ findAllVariables(maybeProperties)))
          case RelationshipPattern(maybeVariable, _, _, maybeProperties, _, _) => acc =>
              SkipChildren((acc._1 ++ maybeVariable.filter(isDefinition), acc._2 ++ findAllVariables(maybeProperties)))
          case NamedPatternPart(variable, _) => acc => TraverseChildren((acc._1 + variable, acc._2))
        }
      referencedVariables.intersect(declaredVariables)
    }.toSet
  }

  private def createError(
    variable: LogicalVariable,
    semanticTable: SemanticTable,
    errorMessageProvider: ErrorMessageProvider
  ): SemanticError = {
    val msg = semanticTable.typeFor(variable).typeInfo.map(_.toShortString) match {
      case Some(typ) => errorMessageProvider.createSelfReferenceError(variable.name, typ)
      case None      => errorMessageProvider.createSelfReferenceError(variable.name)
    }

    SemanticError(msg, variable.position)
  }
}

object ListCoercedToBooleanCheck extends ExpectedBooleanTypeCheck {

  private def isListCoercedToBoolean(semanticTable: SemanticTable, e: Expression): Boolean = {
    semanticTable.types.get(e).exists(typeInfo =>
      CTList(CTAny).covariant.containsAll(typeInfo.specified) && isExpectedTypeBoolean(semanticTable, e)
    )
  }

  def listCoercedToBooleanCheck: SemanticErrorCheck = (baseState, _) => {

    baseState.statement().folder.treeFold(Seq.empty[SemanticError]) {
      case p: Expression
        if isListCoercedToBoolean(baseState.semanticTable(), p) && !p.isInstanceOf[PatternExpression] =>
        errors => SkipChildren(errors :+ SemanticError(errorMessage, p.position))
    }
  }

  val errorMessage: String = "Coercion of list to boolean is not allowed. Please use `NOT isEmpty(...)` instead."
}
