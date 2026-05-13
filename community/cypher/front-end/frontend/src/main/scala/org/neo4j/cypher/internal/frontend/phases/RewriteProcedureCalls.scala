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
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.LocalCallables
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp

import scala.util.Try

trait RewriteProcedureCalls {

  def process(from: BaseState, context: BaseContext, resolver: ScopedProcedureSignatureResolver): BaseState = {
    val instrumentedResolver = new InstrumentedProcedureSignatureResolver(resolver)
    val rewrittenStatement = from.statement().endoRewrite(rewriter(from, context, instrumentedResolver))

    from.withStatement(rewrittenStatement)
      // normalizeWithAndReturnClauses aliases return columns, but only now do we have return columns for procedure calls
      // so now we can assign them in the state.
      .withReturnColumns(rewrittenStatement.returnColumns.map(_.name))
      .withProcedureSignatureVersion(instrumentedResolver.signatureVersionIfResolved)
  }

  def rewriter(from: BaseState, context: BaseContext, resolver: ScopedProcedureSignatureResolver): Rewriter =
    resolverProcedureCall(from, context, resolver) andThen fakeStandaloneCallDeclarations

  // rewriter that amends unresolved procedure calls with procedure signature information
  private def resolverProcedureCall(
    from: BaseState,
    context: BaseContext,
    resolver: ScopedProcedureSignatureResolver
  ): Rewriter =
    bottomUp(Rewriter.lift {
      case unresolved: UnresolvedCall =>
        resolveProcedure(from, context, resolver, unresolved)

      case function: FunctionInvocation
        if function.scopedNeedsToBeResolved(QueryLanguage.toCypherVersion(resolver.queryLanguage)) =>
        resolveFunction(resolver, function)
    })

  def resolveProcedure(
    from: BaseState,
    context: BaseContext,
    resolver: ScopedProcedureSignatureResolver,
    unresolved: UnresolvedCall
  ): CallClause = {
    // try resolve to local procedure
    val procedureName = unresolved.procedureName
    val definitions = from.localDefinitions().localProcedureDefinitions
    val locallyResolved = {
      if (context.semanticFeatures contains LocalCallables)
        from.scopeState().recordedScopes(unresolved).incoming.localCallables.collectFirst(Function.unlift {
          case sig if sig.name.fullNameEqual(procedureName) =>
            definitions.get(procedureName).map { definition =>
              ResolvedLocalCall(unresolved, definition, definition.inferredOutputSignature(from.semantics()))
            }
          case _ => None
        })
      else None
    }
    val resolved = locallyResolved.getOrElse(
      // otherwise resolve to non-local procedure
      ResolvedNonLocalCall(resolver.procedureSignature)(unresolved)
    )
    // We coerce here to ensure that the semantic check run after this rewriter assigns a type
    // to the coercion expressions
    val coerced = resolved.coerceArguments
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
 * Rewrites unresolved calls into resolved calls. Throws if a procedure or function is not found.
 */
case class StrictRewriteProcedureCalls(resolver: ScopedProcedureSignatureResolver)
    extends Phase[BaseContext, BaseState, BaseState] with RewriteProcedureCalls {

  override def phase = AST_REWRITE

  override def process(from: BaseState, context: BaseContext): BaseState = process(from, context, resolver)

  override def postConditions: Set[StepSequencer.Condition] =
    Set(CallInvocationsResolved, FunctionInvocationsResolved)
}

/**
 * Rewrites unresolved calls into resolved calls, or leaves them unresolved if not found.
 *
 * Used in fabricParsing to best-effort resolve procedures/functions against the local coordinator's
 * registry before query fragmentation. This allows QueryType to classify fragments as Read/Write.
 * Procedures unknown to the local registry remain unresolved (QueryType.ReadPlusUnresolved) and
 * are resolved later by StrictRewriteProcedureCalls on individual fragments.
 */
case class TryRewriteProcedureCalls(resolver: ScopedProcedureSignatureResolver)
    extends Phase[BaseContext, BaseState, BaseState] with RewriteProcedureCalls {

  override def phase = AST_REWRITE

  override def process(from: BaseState, context: BaseContext): BaseState = process(from, context, resolver)

  override def postConditions: Set[StepSequencer.Condition] = Set()

  override def resolveProcedure(
    from: BaseState,
    context: BaseContext,
    resolver: ScopedProcedureSignatureResolver,
    unresolved: UnresolvedCall
  ): CallClause =
    Try(super.resolveProcedure(from, context, resolver, unresolved)).getOrElse(unresolved)

  override def resolveFunction(
    resolver: ScopedProcedureSignatureResolver,
    unresolved: FunctionInvocation
  ): Expression = {
    super.resolveFunction(resolver, unresolved) match {
      case resolved @ ResolvedFunctionInvocation(_, Some(_), _, _) => resolved
      case _                                                       => unresolved
    }
  }

  def rewriter(from: BaseState, context: BaseContext): Rewriter = rewriter(from, context, resolver)
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
