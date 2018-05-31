/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.phases

import org.opencypher.v9_0.util.{Rewriter, bottomUp}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.ast.conditions.containsNoNodesOfType
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.opencypher.v9_0.frontend.phases.{BaseState, Condition, Phase, StatementCondition}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanContext
import org.neo4j.cypher.internal.v3_5.logical.plans.{ResolvedCall, ResolvedFunctionInvocation}
import org.opencypher.v9_0.expressions.FunctionInvocation

// Given a way to lookup procedure signatures, this phase rewrites unresolved calls into resolved calls
case object RewriteProcedureCalls extends Phase[PlannerContext, BaseState, BaseState] {

  // Current procedure calling syntax allows simplified short-hand syntax for queries
  // that only consist of a standalone procedure call. In all other cases attempts to
  // use the simplified syntax lead to errors during semantic checking.
  //
  // This rewriter rewrites standalone calls in simplified syntax to calls in standard
  // syntax to prevent them from being rejected during semantic checking.
  private val fakeStandaloneCallDeclarations = Rewriter.lift {
    case q@Query(None, part@SingleQuery(Seq(resolved@ResolvedCall(_, _, _, _, _)))) if !resolved.fullyDeclared =>
      val result = q.copy(part = part.copy(clauses = Seq(resolved.withFakedFullDeclarations))(part.position))(q.position)
      result
  }

  def resolverProcedureCall(context: PlanContext) = bottomUp(Rewriter.lift {
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
  def rewriter(context: PlanContext): AnyRef => AnyRef =
    resolverProcedureCall(context) andThen fakeStandaloneCallDeclarations

  override def phase = AST_REWRITE

  override def description = "resolve procedure calls"

  override def process(from: BaseState, context: PlannerContext): BaseState = {
    val rewrittenStatement = from.statement().endoRewrite(rewriter(context.planContext))
    from.withStatement(rewrittenStatement)
  }

  override def postConditions: Set[Condition] = Set(StatementCondition(containsNoNodesOfType[UnresolvedCall]))
}
