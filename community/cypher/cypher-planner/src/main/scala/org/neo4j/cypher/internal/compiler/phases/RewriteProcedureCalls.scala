/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.StatementCondition
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp

import scala.util.Try

trait RewriteProcedureCalls {

  def process(from: BaseState, resolver: ProcedureSignatureResolver): BaseState = {
    val rewrittenStatement = from.statement().endoRewrite(rewriter(resolver))
    from.withStatement(rewrittenStatement)
      // normalizeWithAndReturnClauses aliases return columns, but only now do we have return columns for procedure calls
      // so now we can assign them in the state.
      .withReturnColumns(rewrittenStatement.returnColumns.map(_.name))
  }

  def rewriter(resolver: ProcedureSignatureResolver): Rewriter =
    resolverProcedureCall(resolver) andThen fakeStandaloneCallDeclarations

  // rewriter that amends unresolved procedure calls with procedure signature information
  private def resolverProcedureCall(resolver: ProcedureSignatureResolver): Rewriter =
    bottomUp(Rewriter.lift {
      case unresolved: UnresolvedCall =>
        resolveProcedure(resolver, unresolved)

      case function: FunctionInvocation if function.needsToBeResolved =>
        resolveFunction(resolver, function)
    })

  def resolveProcedure(resolver: ProcedureSignatureResolver, unresolved: UnresolvedCall): CallClause = {
    val resolved = ResolvedCall(resolver.procedureSignature)(unresolved)
    // We coerce here to ensure that the semantic check run after this rewriter assigns a type
    // to the coercion expressions
    val coerced = resolved.coerceArguments
    coerced
  }

  def resolveFunction(resolver: ProcedureSignatureResolver, unresolved: FunctionInvocation): Expression = {
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
    case q @ Query(None, part @ SingleQuery(Seq(resolved: ResolvedCall))) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(part = part.copy(clauses = newResolved +: projection.toSeq)(part.position))(q.position)

    case q @ Query(None, part @ SingleQuery(Seq(graph: GraphSelection, resolved: ResolvedCall))) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(part = part.copy(clauses = Seq(graph, newResolved) ++ projection)(part.position))(q.position)
  }

  private def getResolvedAndProjection(resolved: ResolvedCall): (ResolvedCall, Option[Return]) = {
    val newResolved = resolved.withFakedFullDeclarations

    //Add the equivalent of a return for each item yielded by the procedure
    val projection =
      Option(newResolved.callResults)
        .filter(_.nonEmpty)
        .map { callResults =>
          Return(
            distinct = false,
            returnItems = ReturnItems(
              includeExisting = false,
              items = callResults.map(item => AliasedReturnItem(
                item.variable.copyId,
                item.variable.copyId)(resolved.position, isAutoAliased = true))
            )(resolved.position),
            None, None, None
          )(resolved.position)
        }

    (newResolved, projection)
  }
}

/**
 * Given a way to lookup procedure signatures, this phase rewrites unresolved calls into resolved calls.
 */
case object RewriteProcedureCalls extends Phase[PlannerContext, BaseState, BaseState] with RewriteProcedureCalls {

  override def phase = AST_REWRITE


  override def process(from: BaseState, context: PlannerContext): BaseState = process(from, context.planContext)

  override def postConditions: Set[StepSequencer.Condition] = Set(StatementCondition(containsNoNodesOfType[UnresolvedCall]))

}

/**
 * Rewrites unresolved calls into resolved calls, or leaves them unresolved if not found.
 */
case class TryRewriteProcedureCalls(resolver: ProcedureSignatureResolver) extends Phase[BaseContext, BaseState, BaseState] with RewriteProcedureCalls {

  override def phase = AST_REWRITE

  override def process(from: BaseState, context: BaseContext): BaseState = process(from, resolver)

  override def postConditions: Set[StepSequencer.Condition] = Set()

  override def resolveProcedure(resolver: ProcedureSignatureResolver, unresolved: UnresolvedCall): CallClause =
    Try(super.resolveProcedure(resolver, unresolved)).getOrElse(unresolved)

  override def resolveFunction(resolver: ProcedureSignatureResolver, unresolved: FunctionInvocation): Expression = {
    super.resolveFunction(resolver, unresolved) match {
      case resolved @ ResolvedFunctionInvocation(_, Some(_), _) => resolved
      case _                                                    => unresolved
    }
  }

  val rewriter: Rewriter = rewriter(resolver)
}
