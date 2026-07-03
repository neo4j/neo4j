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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.DeprecatedSyntaxReplaced
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.LocalFunctionsResolved
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.LocalProceduresPartiallyResolved
import org.neo4j.cypher.internal.notification.LocalFunctionShadowsNonLocal
import org.neo4j.cypher.internal.notification.LocalProcedureShadowsNonLocal
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.GQLAliasFunctionNameRewritten
import org.neo4j.cypher.internal.rewriting.conditions.ProcedureCallWrappedAndExpanded
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterWithParent
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUpWithParent

import scala.util.Success
import scala.util.Try

/**
 * Resolves non-local [[UnresolvedCall]]s into resolved procedure calls and [[FunctionInvocation]]
 * into [[ResolvedFunctionInvocation]] if needed using a [[ScopedProcedureSignatureResolver]].
 * Local procedure calls are handled earlier by ResolveLocalProceduresStep1/2. Subclasses pick
 * the policy for unresolved non-local calls — [[StrictResolveCallables]] throws,
 * [[TryResolveCallables]] leaves them as-is.
 * Emits notification for local callable definitions whose names resolve with the given resolver,
 * i.e. shadow non-local callable.
 */
sealed abstract class ResolveCallables extends Phase[BaseContext, BaseState, BaseState] {
  self: Product =>

  def resolver: ScopedProcedureSignatureResolver

  override def phase = AST_REWRITE

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val instrumentedResolver = new InstrumentedProcedureSignatureResolver(resolver)
    // emit notification for local callable definitions whose names resolve with the given resolver, i.e. shadow non-local callable
    localCallableShadowNotification(from, context, instrumentedResolver)

    // actual procedure and function resolution
    val rewrittenStatement = from.statement().endoRewrite(rewriter(from, context, instrumentedResolver))

    from.withStatement(rewrittenStatement)
      // normalizeWithAndReturnClauses aliases return columns, but only now do we have return columns for procedure calls
      // so now we can assign them in the state.
      .withReturnColumns(rewrittenStatement.returnColumns.map(_.name))
      .withProcedureSignatureVersion(instrumentedResolver.signatureVersionIfResolved)
  }

  private def localCallableShadowNotification(
    from: BaseState,
    context: BaseContext,
    resolver: ScopedProcedureSignatureResolver
  ): Unit = {
    from.statement().folder(context.cancellationChecker).treeForeach {
      case lpd @ LocalProcedureDefinition(name, _, _, _) =>
        val dummyUnresolvedCall = UnresolvedCall(name)(lpd.position)
        Try(resolveProcedure(from, resolver, dummyUnresolvedCall)) match {
          case Success(_) => context.notificationLogger.log(LocalProcedureShadowsNonLocal(name.position, name.fullName))
          case _          => ()
        }
      case lfd @ LocalFunctionDefinition(name, inputSignature, _, _) =>
        val dummyUnresolvedFunctionInvocation =
          FunctionInvocation(
            name,
            distinct = false,
            inputSignature.map(_ => Null()(lfd.position.zeroLength)).toIndexedSeq
          )(lfd.position)
        resolveFunction(resolver, dummyUnresolvedFunctionInvocation) match {
          case ResolvedFunctionInvocation(_, Some(_), _, _) =>
            context.notificationLogger.log(LocalFunctionShadowsNonLocal(name.position, name.fullName))
          case _ => ()
        }
    }
  }

  def rewriter(from: BaseState, context: BaseContext, resolver: ScopedProcedureSignatureResolver): Rewriter =
    resolverProcedureCall(from, context, resolver) andThen fakeStandaloneCallDeclarations

  def rewriter(from: BaseState, context: BaseContext): Rewriter = rewriter(from, context, resolver)

  // rewriter that amends unresolved procedure calls with procedure signature information
  private def resolverProcedureCall(
    from: BaseState,
    context: BaseContext,
    resolver: ScopedProcedureSignatureResolver
  ): Rewriter =
    bottomUpWithParent(
      RewriterWithParent.lift {
        case (unresolved: UnresolvedCall, _) =>
          resolveProcedure(from, resolver, unresolved)

        case (function: FunctionInvocation, Some(_: GraphFunctionReference)) =>
          function

        case (function: FunctionInvocation, _)
          if function.scopedNeedsToBeResolved(QueryLanguage.toCypherVersion(resolver.queryLanguage)) =>
          resolveFunction(resolver, function)
      },
      cancellation = context.cancellationChecker
    )

  def resolveProcedure(
    from: BaseState,
    resolver: ScopedProcedureSignatureResolver,
    unresolved: UnresolvedCall
  ): CallClause = {
    val resolved = ResolvedNonLocalCall(resolver.procedureSignature)(unresolved)
    // We coerce here to ensure that the semantic check run after this rewriter assigns a type
    // to the coercion expressions
    val coerced: CallClause = resolved.coerceArguments
    coerced
  }

  def resolveFunction(resolver: ScopedProcedureSignatureResolver, unresolved: FunctionInvocation): Expression = {
    val otherVersion = QueryLanguage.toCypherVersion(QueryLanguage.otherVersion(resolver.queryLanguage))
    val resolved = ResolvedFunctionInvocation.fromUnresolved(
      signatureLookup = resolver.functionSignature,
      otherVersionLookup = resolver.functionSignatureInOtherVersion,
      otherVersion = otherVersion
    )(unresolved)
    // We coerce here to ensure that the semantic check run after this rewriter assigns a type
    // to the coercion expression
    val coerced = resolved.coerceArguments
    coerced
  }

  // Current procedure calling syntax allows simplified short-hand syntax for queries
  // that only consist of a standalone procedure call. In all other cases attempts to
  // use the simplified syntax lead to errors during semantic checking.
  //
  // This rewriter rewrites standalone calls in simplified syntax to calls in standard
  // syntax to prevent them from being rejected during semantic checking.
  private val fakeStandaloneCallDeclarations = Rewriter.lift {
    case q @ SingleQuery(Seq(resolved: ResolvedNonLocalCall)) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(clauses = newResolved +: projection.toSeq)(q.position)

    case q @ SingleQuery(Seq(graph: GraphSelection, resolved: ResolvedNonLocalCall)) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(clauses = Seq(graph, newResolved) ++ projection)(q.position)
  }

  private def getResolvedAndProjection(resolved: ResolvedNonLocalCall): (ResolvedNonLocalCall, Option[Return]) = {
    val newResolved = resolved.withFakedFullDeclarations

    // Add the equivalent of a return for each item yielded by the procedure
    val projection =
      Option(newResolved.callResults)
        .filter(_.nonEmpty)
        .map { callResults =>
          Return(
            distinct = false,
            returnItems = ReturnItems(
              FreeProjection,
              items = callResults.map(item =>
                AliasedReturnItem(item.variable.copyId, item.variable.copyId)(resolved.position)
              )
            )(resolved.position),
            None,
            None,
            None,
            None
          )(resolved.position)
        }

    (newResolved, projection)
  }
}

/**
 * StepSequencer entry point and parse-pipeline factory for callable resolution. Delegates to the
 * [[ResolveCallables]] phase carried on [[ParsingConfig.resolveCallables]].
 */
object ResolveCallables
    extends StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] =
    Set(
      LocalFunctionsResolved,
      LocalProceduresPartiallyResolved,
      ProcedureCallWrappedAndExpanded,
      GQLAliasFunctionNameRewritten,
      DeprecatedSyntaxReplaced
    )

  override def postConditions: Set[StepSequencer.Condition] =
    Set(CallInvocationsResolved, FunctionInvocationsResolved)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    config.resolveCallables
}

/**
 * Rewrites unresolved calls into resolved calls. Throws if a procedure or function is not found.
 */
case class StrictResolveCallables(resolver: ScopedProcedureSignatureResolver) extends ResolveCallables {

  override def postConditions: Set[StepSequencer.Condition] =
    Set(CallInvocationsResolved, FunctionInvocationsResolved)
}

object StrictResolveCallables {

  /** No procedure registry available; any procedure/function call resolution will throw. Intended for tests. */
  val NoResolver: StrictResolveCallables = StrictResolveCallables(ScopedProcedureSignatureResolver.NoResolver)
}

/**
 * Rewrites unresolved calls into resolved calls, or leaves them unresolved if not found.
 *
 * Used in fabricParsing to best-effort resolve procedures/functions against the local coordinator's
 * registry before query fragmentation. This allows QueryType to classify fragments as Read/Write.
 * Procedures unknown to the local registry remain unresolved (QueryType.ReadPlusUnresolved) and
 * are resolved later by StrictResolveCallables on individual fragments.
 */
case class TryResolveCallables(resolver: ScopedProcedureSignatureResolver) extends ResolveCallables {

  override def postConditions: Set[StepSequencer.Condition] = Set()

  override def resolveProcedure(
    from: BaseState,
    resolver: ScopedProcedureSignatureResolver,
    unresolved: UnresolvedCall
  ): CallClause =
    Try(super.resolveProcedure(from, resolver, unresolved)).getOrElse(unresolved)

  override def resolveFunction(
    resolver: ScopedProcedureSignatureResolver,
    unresolved: FunctionInvocation
  ): Expression = {
    super.resolveFunction(resolver, unresolved) match {
      case resolved @ ResolvedFunctionInvocation(_, Some(_), _, _) => resolved
      case _                                                       => unresolved
    }
  }
}

class InstrumentedProcedureSignatureResolver(resolver: ScopedProcedureSignatureResolver)
    extends ScopedProcedureSignatureResolver {

  private var resolved = false

  def procedureSignature(name: ProcedureName): ProcedureSignature = {
    resolved = true
    resolver.procedureSignature(name)
  }

  def functionSignature(name: FunctionName): Option[UserFunctionSignature] = {
    resolved = true
    resolver.functionSignature(name)
  }

  def functionSignatureInOtherVersion(name: FunctionName): Option[UserFunctionSignature] =
    resolver.functionSignatureInOtherVersion(name)

  def signatureVersionIfResolved: Option[Long] =
    if (resolved) Some(resolver.procedureSignatureVersion) else None

  override def procedureSignatureVersion: Long = resolver.procedureSignatureVersion

  override def queryLanguage: QueryLanguage = resolver.queryLanguage
}
