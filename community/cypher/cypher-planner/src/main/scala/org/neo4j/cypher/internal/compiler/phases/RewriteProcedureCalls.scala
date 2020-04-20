/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.logical.plans.{ResolvedCall, ResolvedFunctionInvocation}
import org.neo4j.cypher.internal.planner.spi.{ProcedureSignatureResolver}
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.FunctionInvocation
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseState, Condition, Phase, StatementCondition}
import org.neo4j.cypher.internal.v4_0.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, bottomUp}

// Given a way to lookup procedure signatures, this phase rewrites unresolved calls into resolved calls
case object RewriteProcedureCalls extends Phase[PlannerContext, BaseState, BaseState] {

  // Current procedure calling syntax allows simplified short-hand syntax for queries
  // that only consist of a standalone procedure call. In all other cases attempts to
  // use the simplified syntax lead to errors during semantic checking.
  //
  // This rewriter rewrites standalone calls in simplified syntax to calls in standard
  // syntax to prevent them from being rejected during semantic checking.
  private val fakeStandaloneCallDeclarations = Rewriter.lift {
    case q@Query(None, part@SingleQuery(Seq(resolved: ResolvedCall))) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(part = part.copy(clauses = Seq(newResolved, projection))(part.position))(q.position)

    case q@Query(None, part@SingleQuery(Seq(graph: GraphSelection, resolved: ResolvedCall))) =>
      val (newResolved, projection) = getResolvedAndProjection(resolved)
      q.copy(part = part.copy(clauses = Seq(graph, newResolved, projection))(part.position))(q.position)
  }

  private def getResolvedAndProjection(resolved: ResolvedCall) = {
    val newResolved = resolved.withFakedFullDeclarations
    //Add the equivalent of a return for each item yielded by the procedure
    val aliases = newResolved.callResults.map(item => AliasedReturnItem(item.variable, item.variable)(resolved.position))
    val projection = Return(distinct = false, ReturnItems(includeExisting = false, aliases)(resolved.position),
      None, None, None)(resolved.position)
    (newResolved, projection)
  }

  def resolverProcedureCall(context: ProcedureSignatureResolver) = bottomUp(Rewriter.lift {
    case unresolved: UnresolvedCall =>
      val resolved = ResolvedCall(context.procedureSignature)(unresolved)
      // We coerce here to ensure that the semantic check run after this rewriter assigns a type
      // to the coercion expressions
      val coerced = resolved.coerceArguments
      coerced

    case function: FunctionInvocation if function.needsToBeResolved =>
      val resolved = ResolvedFunctionInvocation(context.functionSignature)(function)

      // We coerce here to ensure that the semantic check run after this rewriter assigns a type
      // to the coercion expression
      val coerced = resolved.coerceArguments
      coerced
  })

  // rewriter that amends unresolved procedure calls with procedure signature information
  def rewriter(context: ProcedureSignatureResolver): AnyRef => AnyRef =
    resolverProcedureCall(context) andThen fakeStandaloneCallDeclarations

  override def phase = AST_REWRITE

  override def description = "resolve procedure calls"

  override def process(from: BaseState, context: PlannerContext): BaseState = {
    val rewrittenStatement = from.statement().endoRewrite(rewriter(context.planContext))
    from.withStatement(rewrittenStatement)
      // normalizeWithAndReturnClauses aliases return columns, but only now do we have return columns for procedure calls
      // so now we can assign them in the state.
      .withReturnColumns(rewrittenStatement.returnColumns.map(_.name))
  }

  override def postConditions: Set[Condition] = Set(StatementCondition(containsNoNodesOfType[UnresolvedCall]))
}
