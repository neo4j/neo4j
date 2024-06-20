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
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp

import scala.util.Try

trait RewriteProcedureCalls {

  def process(from: BaseState, resolver: ScopedProcedureSignatureResolver): BaseState = {
    val instrumentedResolver = new InstrumentedProcedureSignatureResolver(resolver)
    val rewrittenStatement = from.statement().endoRewrite(rewriter(instrumentedResolver))

    from.withStatement(rewrittenStatement)
      // normalizeWithAndReturnClauses aliases return columns, but only now do we have return columns for procedure calls
      // so now we can assign them in the state.
      .withReturnColumns(rewrittenStatement.returnColumns.map(_.name))
      .withProcedureSignatureVersion(instrumentedResolver.signatureVersionIfResolved)
  }

  def rewriter(resolver: ScopedProcedureSignatureResolver): Rewriter =
    resolverProcedureCall(resolver) andThen fakeStandaloneCallDeclarations

  // rewriter that amends unresolved procedure calls with procedure signature information
  private def resolverProcedureCall(resolver: ScopedProcedureSignatureResolver): Rewriter =
    bottomUp(Rewriter.lift {
      case unresolved: UnresolvedCall =>
        resolveProcedure(resolver, unresolved)

      case function: FunctionInvocation if function.needsToBeResolved =>
        resolveFunction(resolver, function)
    })

  def resolveProcedure(resolver: ScopedProcedureSignatureResolver, unresolved: UnresolvedCall): CallClause = {
    val resolved = ResolvedCall(resolver.procedureSignature)(unresolved)
    // We coerce here to ensure that the semantic check run after this rewriter assigns a type
    // to the coercion expressions
    val coerced = resolved.coerceArguments
    coerced
  }

  def resolveFunction(resolver: ScopedProcedureSignatureResolver, unresolved: FunctionInvocation): Expression = {
    val resolved = ResolvedFunctionInvocation(resolver.functionSignature)(unresolved)
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
    case q @ SingleQuery(Seq(resolved: ResolvedCall)) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(clauses = newResolved +: projection.toSeq)(q.position)

    case q @ SingleQuery(Seq(graph: GraphSelection, resolved: ResolvedCall)) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(clauses = Seq(graph, newResolved) ++ projection)(q.position)
  }

  private def getResolvedAndProjection(resolved: ResolvedCall): (ResolvedCall, Option[Return]) = {
    val newResolved = resolved.withFakedFullDeclarations

    // Add the equivalent of a return for each item yielded by the procedure
    val projection =
      Option(newResolved.callResults)
        .filter(_.nonEmpty)
        .map { callResults =>
          Return(
            distinct = false,
            returnItems = ReturnItems(
              includeExisting = false,
              items = callResults.map(item =>
                AliasedReturnItem(item.variable.copyId, item.variable.copyId)(resolved.position)
              )
            )(resolved.position),
            None,
            None,
            None
          )(resolved.position)
        }

    (newResolved, projection)
  }
}

/**
 * Rewrites unresolved calls into resolved calls, or leaves them unresolved if not found.
 */
case class TryRewriteProcedureCalls(resolver: ScopedProcedureSignatureResolver)
    extends Phase[BaseContext, BaseState, BaseState] with RewriteProcedureCalls {

  override def phase = AST_REWRITE

  override def process(from: BaseState, context: BaseContext): BaseState = process(from, resolver)

  override def postConditions: Set[StepSequencer.Condition] = Set()

  override def resolveProcedure(resolver: ScopedProcedureSignatureResolver, unresolved: UnresolvedCall): CallClause =
    Try(super.resolveProcedure(resolver, unresolved)).getOrElse(unresolved)

  override def resolveFunction(
    resolver: ScopedProcedureSignatureResolver,
    unresolved: FunctionInvocation
  ): Expression = {
    super.resolveFunction(resolver, unresolved) match {
      case resolved @ ResolvedFunctionInvocation(_, Some(_), _) => resolved
      case _                                                    => unresolved
    }
  }

  val rewriter: Rewriter = rewriter(resolver)
}

class InstrumentedProcedureSignatureResolver(resolver: ScopedProcedureSignatureResolver)
    extends ScopedProcedureSignatureResolver {
  private var hasAttemptedToResolve = false

  def procedureSignature(name: QualifiedName): ProcedureSignature = {
    hasAttemptedToResolve = true
    resolver.procedureSignature(name)
  }

  def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    hasAttemptedToResolve = true
    resolver.functionSignature(name)
  }

  def signatureVersionIfResolved: Option[Long] =
    if (hasAttemptedToResolve) Some(resolver.procedureSignatureVersion) else None

  override def procedureSignatureVersion: Long = resolver.procedureSignatureVersion
}
