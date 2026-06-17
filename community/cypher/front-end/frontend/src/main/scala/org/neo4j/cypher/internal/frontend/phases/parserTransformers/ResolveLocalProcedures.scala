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

import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.LocalFunctionBody
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.ProjectingUnionAll
import org.neo4j.cypher.internal.ast.ProjectingUnionDistinct
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.semantics.scoping.LocalProcedureScopeSignature
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.ResolvedLocalCall
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ReturnItemsAreAliased
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

/**
 * Resolves local procedure calls structurally from scope information only.
 * It leaves update classification to ResolveLocalProceduresStep2.
 */
case object ResolveLocalProceduresStep1 extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val recordedScopes = from.scopeState().recordedScopes
    val rewriter = topDown(
      Rewriter.lift {
        case uc: UnresolvedCall =>
          recordedScopes.get(Ref(uc)).flatMap { ws =>
            ws.incoming.localCallables.collectFirst {
              case sig: LocalProcedureScopeSignature if sig.name.fullNameEqual(uc.procedureName) => sig
            }
          }.map(sig => ResolvedLocalCall(uc, sig).coerceArguments).getOrElse(uc)
      }
    )
    from.withStatement(from.statement().endoRewrite(rewriter))
  }

  override def phase = AST_REWRITE

  override def preConditions: Set[StepSequencer.Condition] =
    Set(BaseContains[Statement](), ReturnItemsAreAliased, UpToDateScopes)

  override def postConditions: Set[StepSequencer.Condition] = Set(LocalProceduresPartiallyResolved)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable + UpToDateScopes

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this
}

case object LocalProceduresPartiallyResolved extends StepSequencer.Condition
case object LocalProceduresFullyResolved extends StepSequencer.Condition

/**
 * Propagates update information into already-resolved local procedure calls by walking local
 * definitions in scope order. Duplicate local callable names are rejected later by VariableChecker,
 * so this step intentionally uses simple sets keyed by callable name.
 */
case object ResolveLocalProceduresStep2 extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  private case class VisibleCallables(
    updatingProcedures: Set[ProcedureName],
    updatingFunctions: Set[FunctionName]
  ) {

    def withProcedure(name: ProcedureName, containsUpdates: Boolean): VisibleCallables =
      if (containsUpdates) copy(updatingProcedures = updatingProcedures + name) else this

    def withFunction(name: FunctionName, containsUpdates: Boolean): VisibleCallables =
      if (containsUpdates) copy(updatingFunctions = updatingFunctions + name) else this
  }

  private object VisibleCallables {
    val empty: VisibleCallables = VisibleCallables(Set.empty, Set.empty)
  }

  override def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(rewriteStatement(from.statement()))

  private def rewriteStatement(statement: Statement): Statement =
    statement match {
      // note this assumes no local callables in admin and schema commands
      case query: Query => rewriteQuery(query, VisibleCallables.empty)._1
      case _            => statement
    }

  private def rewriteDefinitions(
    definitions: Seq[LocalCallableDefinition],
    visibleCallables: VisibleCallables
  ): (Seq[LocalCallableDefinition], VisibleCallables) =
    definitions.foldLeft((Vector.empty[LocalCallableDefinition], visibleCallables)) {
      case ((rewrittenDefinitions, visible), definition) =>
        val (rewrittenDefinition, updatedVisible) = rewriteDefinition(definition, visible)
        (rewrittenDefinitions :+ rewrittenDefinition, updatedVisible)
    }

  private def rewriteDefinition(
    definition: LocalCallableDefinition,
    visibleCallables: VisibleCallables
  ): (LocalCallableDefinition, VisibleCallables) =
    definition match {
      case procedure @ LocalProcedureDefinition(name, _, _, body) =>
        val (rewrittenBody, procedureContainsUpdates) = rewriteQuery(body, visibleCallables)
        (
          procedure.copy(body = rewrittenBody)(procedure.position),
          visibleCallables.withProcedure(name, procedureContainsUpdates)
        )

      case function @ LocalFunctionDefinition(_, _, _, body) =>
        val (rewrittenBody, functionContainsUpdates) = rewriteFunctionBody(body, visibleCallables)
        (
          function.copy(body = rewrittenBody)(function.position),
          visibleCallables.withFunction(function.name, functionContainsUpdates)
        )
    }

  private def rewriteFunctionBody(
    body: LocalFunctionBody,
    visibleCallables: VisibleCallables
  ): (LocalFunctionBody, Boolean) = body match {
    case qb @ QueryBody(query) =>
      val (rewrittenQuery, containsUpdates) = rewriteQuery(query, visibleCallables)
      (qb.copy(query = rewrittenQuery)(qb.position), containsUpdates)
    case eb @ ExpressionBody(expression) =>
      val (rewrittenExpression, containsUpdates) = rewriteExpression(expression, visibleCallables)
      (eb.copy(expression = rewrittenExpression)(eb.position), containsUpdates)
  }

  private def rewriteQuery(query: Query, visibleCallables: VisibleCallables): (Query, Boolean) =
    query match {
      case qwld @ QueryWithLocalDefinitions(definitions, innerQuery) =>
        val (rewrittenDefinitions, visibleAfterDefinitions) = rewriteDefinitions(definitions, visibleCallables)
        val (rewrittenQuery, containsUpdates) = rewriteQuery(innerQuery, visibleAfterDefinitions)
        (
          qwld.copy(definitions = rewrittenDefinitions, query = rewrittenQuery)(qwld.position),
          containsUpdates
        )

      case braces @ TopLevelBraces(innerQuery, use) =>
        val (rewrittenQuery, _) = rewriteQuery(innerQuery, visibleCallables)
        val rewrittenBraces =
          braces.copy(query = rewrittenQuery, use = use)(braces.position)
        (rewrittenBraces, containsUpdates(rewrittenBraces, visibleCallables))

      case union @ UnionAll(lhs, rhs) =>
        val (rewrittenLhs, _) = rewriteQuery(lhs, visibleCallables)
        val (rewrittenRhs, _) = rewritePartQuery(rhs, visibleCallables)
        val rewrittenUnion =
          union.copy(lhs = rewrittenLhs, rhs = rewrittenRhs)(union.position)
        (rewrittenUnion, containsUpdates(rewrittenUnion, visibleCallables))

      case union @ UnionDistinct(lhs, rhs) =>
        val (rewrittenLhs, _) = rewriteQuery(lhs, visibleCallables)
        val (rewrittenRhs, _) = rewritePartQuery(rhs, visibleCallables)
        val rewrittenUnion =
          union.copy(lhs = rewrittenLhs, rhs = rewrittenRhs)(union.position)
        (rewrittenUnion, containsUpdates(rewrittenUnion, visibleCallables))

      case union @ ProjectingUnionAll(lhs, rhs, unionMappings) =>
        val (rewrittenLhs, _) = rewriteQuery(lhs, visibleCallables)
        val (rewrittenRhs, _) = rewritePartQuery(rhs, visibleCallables)
        val rewrittenUnion =
          union.copy(lhs = rewrittenLhs, rhs = rewrittenRhs, unionMappings = unionMappings)(union.position)
        (rewrittenUnion, containsUpdates(rewrittenUnion, visibleCallables))

      case union @ ProjectingUnionDistinct(lhs, rhs, unionMappings) =>
        val (rewrittenLhs, _) = rewriteQuery(lhs, visibleCallables)
        val (rewrittenRhs, _) = rewritePartQuery(rhs, visibleCallables)
        val rewrittenUnion =
          union.copy(lhs = rewrittenLhs, rhs = rewrittenRhs, unionMappings = unionMappings)(union.position)
        (rewrittenUnion, containsUpdates(rewrittenUnion, visibleCallables))

      case conditional @ ConditionalQueryWhen(branches, default) =>
        val rewrittenBranches = branches.map(rewriteConditionalBranch(_, visibleCallables))
        val rewrittenDefault = default.map(rewriteConditionalBranch(_, visibleCallables))
        val rewrittenConditional =
          conditional.copy(
            branches = rewrittenBranches.map(_._1),
            default = rewrittenDefault.map(_._1)
          )(conditional.position)
        (
          rewrittenConditional,
          containsUpdates(rewrittenConditional, visibleCallables)
        )

      case next @ NextStatement(queries) =>
        val rewrittenQueries = queries.map(rewriteQuery(_, visibleCallables))
        val rewrittenNext = next.copy(queries = rewrittenQueries.map(_._1))(next.position)
        (rewrittenNext, containsUpdates(rewrittenNext, visibleCallables))

      case singleQuery: SingleQuery =>
        val rewrittenQuery = rewriteSingleQuery(singleQuery, visibleCallables)
        (rewrittenQuery, containsUpdates(rewrittenQuery, visibleCallables))
    }

  private def rewriteSingleQuery(singleQuery: SingleQuery, visibleCallables: VisibleCallables): Query =
    singleQuery.endoRewrite(topDown(
      Rewriter.lift {
        case subqueryCall @ ImportingWithSubqueryCall(innerQuery, _, _) =>
          subqueryCall.copy(
            innerQuery = rewriteQuery(innerQuery, visibleCallables)._1
          )(subqueryCall.position)
        case subqueryCall @ ScopeClauseSubqueryCall(innerQuery, _, _, _, _, _) =>
          subqueryCall.copy(
            innerQuery = rewriteQuery(innerQuery, visibleCallables)._1
          )(subqueryCall.position)
        case subqueryExpression: FullSubqueryExpression =>
          subqueryExpression.withQuery(rewriteQuery(subqueryExpression.query, visibleCallables)._1)
        case call: ResolvedLocalCall if visibleCallables.updatingProcedures(call.procedureName) =>
          call.withBodyContainsUpdates(true)
      },
      stopper = {
        case q: Query if q != singleQuery => true
        case _                            => false
      }
    ))

  private def rewritePartQuery(
    query: PartQuery,
    visibleCallables: VisibleCallables
  ): (PartQuery, Boolean) = {
    val (rewrittenQuery, containsUpdates) = rewriteQuery(query, visibleCallables)
    (rewrittenQuery.asInstanceOf[PartQuery], containsUpdates)
  }

  private def rewriteConditionalBranch(
    branch: ConditionalQueryBranch,
    visibleCallables: VisibleCallables
  ): (ConditionalQueryBranch, Boolean) = {
    val (rewrittenQuery, containsUpdates) = rewritePartQuery(branch.query, visibleCallables)
    val rewrittenPredicate = branch.predicate.map(rewriteExpression(_, visibleCallables))
    val rewrittenBranch =
      branch.copy(
        predicate = rewrittenPredicate.map(_._1),
        query = rewrittenQuery
      )(branch.position)
    (
      rewrittenBranch,
      containsUpdates || rewrittenPredicate.exists(_._2)
    )
  }

  private def rewriteExpression(
    expression: Expression,
    visibleCallables: VisibleCallables
  ): (Expression, Boolean) = {
    val rewrittenExpression = expression.endoRewrite(topDown(Rewriter.lift {
      case subqueryExpression: FullSubqueryExpression =>
        subqueryExpression.withQuery(rewriteQuery(subqueryExpression.query, visibleCallables)._1)
    }))
    (rewrittenExpression, containsUpdates(rewrittenExpression, visibleCallables))
  }

  private def containsUpdates(query: Query, visibleCallables: VisibleCallables): Boolean =
    query.containsUpdates || containsUpdatesInQueryTree(query, visibleCallables)

  private def containsUpdates(expression: Expression, visibleCallables: VisibleCallables): Boolean =
    containsUpdatesInExpressionTree(expression, visibleCallables)

  private def containsUpdatesInQueryTree(query: Query, visibleCallables: VisibleCallables): Boolean =
    query.folder.treeFold(false) {
      case _: LocalCallableDefinition =>
        (acc: Boolean) => SkipChildren(acc)

      case subqueryExpression: FullSubqueryExpression =>
        (acc: Boolean) => SkipChildren(acc || containsUpdates(subqueryExpression.query, visibleCallables))

      case functionInvocation: FunctionInvocation =>
        (acc: Boolean) =>
          if (
            acc || functionInvocation.maybeLocalFunction.exists(f =>
              visibleCallables.updatingFunctions(f.functionName)
            )
          )
            SkipChildren(true)
          else
            TraverseChildren(acc)

      case _ =>
        (acc: Boolean) =>
          if (acc) SkipChildren(acc)
          else TraverseChildren(acc)
    }

  private def containsUpdatesInExpressionTree(expression: Expression, visibleCallables: VisibleCallables): Boolean =
    expression.folder.treeFold(false) {
      case subqueryExpression: FullSubqueryExpression =>
        (acc: Boolean) => SkipChildren(acc || containsUpdates(subqueryExpression.query, visibleCallables))

      case functionInvocation: FunctionInvocation =>
        (acc: Boolean) =>
          if (
            acc || functionInvocation.maybeLocalFunction.exists(f =>
              visibleCallables.updatingFunctions(f.functionName)
            )
          )
            SkipChildren(true)
          else
            TraverseChildren(acc)

      case _ =>
        (acc: Boolean) =>
          if (acc) SkipChildren(acc)
          else TraverseChildren(acc)
    }

  override def phase = AST_REWRITE

  override def preConditions: Set[StepSequencer.Condition] =
    Set(BaseContains[Statement](), LocalProceduresPartiallyResolved, CallInvocationsResolved)

  override def postConditions: Set[StepSequencer.Condition] = Set(LocalProceduresFullyResolved)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(UpToDateScopes)

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this
}
