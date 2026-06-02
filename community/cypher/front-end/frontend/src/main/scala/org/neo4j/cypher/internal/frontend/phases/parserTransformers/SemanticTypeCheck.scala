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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateOrInsert
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UpdateClause
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.VectorFilterExpression
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_TYPE_CHECK
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.VisitorPhase
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ListCoercedToBooleanCheck.listCoercedToBooleanCheck
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.MatchChecks.SearchCheck
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PatternExpressionInNonExistenceCheck.patternExpressionInNonExistenceCheck
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticTypeCheck.SemanticErrorCheck
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.DynamicLeaf
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTList

case object SemanticTypeCheckCompleted extends Condition

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
    SelfReferenceCheckWithinPatternPart.check,
    SelfReferenceCheckAcrossPatternParts.check,
    listCoercedToBooleanCheck,
    MatchChecks.checkMatchMode,
    SearchCheck.check
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
  override def postConditions: Set[StepSequencer.Condition] = Set(SemanticTypeCheckCompleted)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

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
      case FunctionInvocation(FunctionName(_, "size"), _, IndexedSeq(p: PatternExpression), _, _, _, _)
        if !isExpectedTypeBoolean(baseState.semanticTable(), p) =>
        errors => SkipChildren(errors :+ SemanticError.patternExpressionInSize(p.position))

      case p: PatternExpression if !isExpectedTypeBoolean(baseState.semanticTable(), p) =>
        errors => SkipChildren(errors :+ SemanticError.invalidUseOfPatternExpression(p.position))
    }
  }
}

trait VariableReferenceCheck {

  /**
   * Check for self references either within a pattern part (disallowed for CREATE and INSERT) or across multiple
   * pattern parts (disallowed for INSERT, deprecated for CREATE).
   * @param ast The pattern part or (in case of checking across pattern parts) the full pattern to be checked
   * @param pattern The full pattern, needed to fetch all symbol definitions in scope from the semantic table
   * @param semanticTable Semantic table, containing symbol definitions
   * @return
   */
  def findSelfReferenceVariables(
    ast: ASTNode,
    pattern: Pattern,
    semanticTable: SemanticTable
  ): Set[LogicalVariable] = {

    val allSymbolDefinitions = semanticTable.recordedScopes(pattern).allSymbolDefinitions

    def isDefinition(variable: LogicalVariable): Boolean = {
      allSymbolDefinitions(variable.name).map(_.use).contains(Ref(variable))
    }

    def findRefVariables(e: Option[Expression]): Set[LogicalVariable] =
      e.fold(Set.empty[LogicalVariable])(_.dependencies)

    def findDynamicVariables(e: Option[LabelExpression]): Set[LogicalVariable] =
      e.folder.findAllByClass[DynamicLeaf].flatten(_.expr.expression.dependencies).toSet

    val (declaredVariables, referencedVariables) =
      ast.folder.treeFold[(Set[LogicalVariable], Set[LogicalVariable])]((Set.empty, Set.empty)) {
        case NodePattern(maybeVariable, labelExpression, maybeProperties, _) => acc =>
            SkipChildren((
              acc._1 ++ maybeVariable.filter(isDefinition),
              acc._2 ++ findRefVariables(maybeProperties) ++ findDynamicVariables(labelExpression)
            ))
        case RelationshipPattern(maybeVariable, labelExpression, _, maybeProperties, _, _) => acc =>
            SkipChildren((
              acc._1 ++ maybeVariable.filter(isDefinition),
              acc._2 ++ findRefVariables(maybeProperties) ++ findDynamicVariables(labelExpression)
            ))
        case NamedPatternPart(variable, _) => acc => TraverseChildren((acc._1 + variable, acc._2))
      }
    referencedVariables.intersect(declaredVariables)
  }
}

object SelfReferenceCheckWithinPatternPart extends VariableReferenceCheck {

  def check: SemanticErrorCheck = (baseState, baseContext) => {
    val semanticTable = baseState.semanticTable()

    baseState.statement().folder.treeFold(Seq.empty[SemanticError]) {
      case c: CreateOrInsert =>
        accErrors =>
          val errors = checkPattern(c, c.pattern, semanticTable)
          SkipChildren(accErrors ++ errors)

      case m: Merge if Merge.SelfReference.errorIn(baseContext.cypherVersion) =>
        accErrors =>
          val pattern = Pattern.ForUpdate(Seq(m.pattern))(m.pattern.position)
          val errors = checkPattern(m, pattern, semanticTable)
          SkipChildren(accErrors ++ errors)
    }
  }

  private def checkPattern(
    clause: UpdateClause,
    pattern: Pattern,
    semanticTable: SemanticTable
  ): Seq[SemanticError] = {
    findSelfReferenceVariablesWithinPatternParts(pattern, semanticTable)
      .map(createError(clause, _))
      .toSeq
  }

  private def findSelfReferenceVariablesWithinPatternParts(
    pattern: Pattern,
    semanticTable: SemanticTable
  ): Set[LogicalVariable] = {
    pattern.patternParts.flatMap { patternPart =>
      findSelfReferenceVariables(patternPart, pattern, semanticTable)
    }.toSet
  }

  private def createError(
    clause: UpdateClause,
    variable: LogicalVariable
  ): SemanticError = {
    SemanticError.invalidEntityReference(variable.name, clause.name, variable.position)
  }
}

object SelfReferenceCheckAcrossPatternParts extends VariableReferenceCheck {

  def check: SemanticErrorCheck = (baseState, ctx) => {
    val semanticTable = baseState.semanticTable()
    baseState.statement().folder.treeFold(Seq.empty[SemanticError]) {
      case i: Insert =>
        accErrors =>
          val errors = checkPattern(i, i.pattern, semanticTable)
          SkipChildren(accErrors ++ errors)

      case c: Create if Create.SelfReferenceAcrossPatterns.errorIn(ctx.cypherVersion) =>
        accErrors =>
          val errors = checkPattern(c, c.pattern, semanticTable)
          SkipChildren(accErrors ++ errors)
    }
  }

  private def checkPattern(clause: UpdateClause, pattern: Pattern, semanticTable: SemanticTable): Seq[SemanticError] = {
    // Returns the set of variables that are defined in a pattern and used in the same pattern for property read
    // E.g. `INSERT (a {prop: 5}), (b {prop: a.prop})
    findSelfReferenceVariables(pattern, pattern, semanticTable)
      .map(e => SemanticError.invalidEntityReference(e.name, clause.name, e.position)).toSeq
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
      // ResolvedFunctionInvocation calls are exempted, due to legacy behavior when
      // callables were not resolved before type checking.
      case p: Expression
        if isListCoercedToBoolean(baseState.semanticTable(), p)
          && !p.isInstanceOf[PatternExpression]
          && !p.isInstanceOf[ResolvedFunctionInvocation] =>
        errors =>
          SkipChildren(errors :+ SemanticError.invalidCoercion(
            "LIST",
            "BOOLEAN",
            errorMessage,
            p.position
          ))
    }
  }

  val errorMessage: String = "Coercion of list to boolean is not allowed. Please use `NOT isEmpty(...)` instead."
}

/**
 * Checks on Match clauses that can be done separately from Semantic Analysis.
 */
object MatchChecks {

  def checkMatchMode: SemanticErrorCheck = (baseState: BaseState, baseContext) => {
    val matchClauses =
      baseState.statement().folder.treeFold(Seq.empty[Match]) {
        case clause: Match =>
          clauses => SkipChildren(clauses :+ clause)
      }

    matchClauses.flatMap { clause =>
      clause.checkMatchMode(baseState.semantics(), baseContext.cypherVersion)
    }
  }

  object SearchCheck {

    def check: SemanticErrorCheck = (baseState, _) => {
      baseState.statement().folder.treeFold(Seq.empty[SemanticError]) {
        case Search(bindingVariable, _, _, embedding, where, _) =>
          errors =>
            val newErrors = Seq.empty[SemanticError] ++
              Option.when(embedding.dependencies.contains(bindingVariable)) {
                // To be removed again in PLAN-3087
                SemanticError.singleStageWithEmbeddingReferencingEntity(
                  embedding.asCanonicalStringVal,
                  bindingVariable.name,
                  embedding.position
                )
              } ++ where.toSeq.map(_.expression).flatMap(checkWhereClause)

            SkipChildren(errors ++ newErrors)
      }
    }
  }

  private def checkWhereClause(expression: Expression): Seq[SemanticError] =
    expression match {
      case VectorFilterExpression(variable: LogicalVariable, rhs: Expression, _: VectorFilterExpression) =>
        Option.when(rhs.dependencies.contains(variable)) {
          // To be removed again in PLAN-3087
          SemanticError.singleStageWithPredicateReferencingEntity(
            rhs.asCanonicalStringVal,
            variable.name,
            rhs.position
          )
        }.toSeq
      case And(lhs, rhs) => checkWhereClause(lhs) ++ checkWhereClause(rhs)
      case Ands(exprs) =>
        exprs.map(checkWhereClause).foldLeft(Seq.empty[SemanticError])(_ ++ _)
      case _ =>
        // Ignore. Will be caught during semantic analysis in Search.asFilterExpressions.
        Seq.empty
    }
}
