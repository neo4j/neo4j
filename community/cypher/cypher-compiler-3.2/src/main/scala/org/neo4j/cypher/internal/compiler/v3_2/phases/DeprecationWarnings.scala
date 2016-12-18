/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS
import org.neo4j.cypher.internal.compiler.v3_2.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters.replaceAliasedFunctionInvocations.aliases
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilationState.{State2, State4}
import org.neo4j.cypher.internal.compiler.v3_2.spi.ProcedureSignature
import org.neo4j.cypher.internal.frontend.v3_2.InternalException
import org.neo4j.cypher.internal.frontend.v3_2.ast.{FunctionInvocation, FunctionName, Statement, UnresolvedCall}
import org.neo4j.cypher.internal.frontend.v3_2.notification.{DeprecatedFunctionNotification, DeprecatedProcedureNotification, InternalNotification}


object SyntaxDeprecationWarnings extends VisitorPhase[State2] {
  override def visit(value: State2, context: Context): Unit = {
    val warnings = findDeprecations(value.statement)

    warnings.foreach(context.notificationLogger.log)
  }

  private def findDeprecations(statement: Statement): Set[InternalNotification] =
    statement.treeFold(Set.empty[InternalNotification]) {
      case f@FunctionInvocation(_, FunctionName(name), _, _) if aliases.get(name).nonEmpty =>
        (seq) => (seq + DeprecatedFunctionNotification(f.position, name, aliases(name)), None)
    }

  override def phase = DEPRECATION_WARNINGS

  override def description = "find deprecated Cypher constructs or procs and generate warnings for them"
}

object ProcedureDeprecationWarnings extends VisitorPhase[State4] {
  override def visit(value: State4, context: Context): Unit = {
    val warnings = findDeprecations(value.statement)

    warnings.foreach(context.notificationLogger.log)
  }

  private def findDeprecations(statement: Statement): Set[InternalNotification] =
    statement.treeFold(Set.empty[InternalNotification]) {
      case f@ResolvedCall(ProcedureSignature(name, _, _, Some(deprecatedBy), _, _), _, _, _, _) =>
        (seq) => (seq + DeprecatedProcedureNotification(f.position, name.toString, deprecatedBy), None)
      case _:UnresolvedCall =>
        throw new InternalException("Expected procedures to have been resolved already")
    }

  override def phase = DEPRECATION_WARNINGS

  override def description = "find deprecated Cypher constructs or procs and generate warnings for them"
}
