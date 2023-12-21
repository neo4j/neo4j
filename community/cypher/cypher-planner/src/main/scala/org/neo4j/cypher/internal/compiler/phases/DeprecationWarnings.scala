/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.VisitorPhase
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.exceptions.InternalException
import org.neo4j.notifications.DeprecatedFieldNotification
import org.neo4j.notifications.DeprecatedProcedureNotification
import org.neo4j.notifications.ProcedureWarningNotification

/**
 * Find calls to deprecated procedures and functions and generate warnings for them.
 */
case object ProcedureAndFunctionDeprecationWarnings extends VisitorPhase[BaseContext, BaseState] {

  override def visit(value: BaseState, context: BaseContext): Unit = {
    val warnings = findDeprecations(value.statement())

    warnings.foreach(context.notificationLogger.log)
  }

  private def findDeprecations(statement: Statement): Set[InternalNotification] =
    statement.folder.treeFold(Set.empty[InternalNotification]) {
      case f @ ResolvedCall(
          ProcedureSignature(name, _, _, Some(deprecatedBy), _, _, _, _, _, _, _, _),
          _,
          _,
          _,
          _,
          _
        ) =>
        seq => SkipChildren(seq + DeprecatedProcedureNotification(f.position, name.toString, deprecatedBy))
      case f @ ResolvedFunctionInvocation(
          _,
          Some(UserFunctionSignature(name, _, _, Some(deprecatedBy), _, _, _, _, _)),
          _
        ) =>
        seq => SkipChildren(seq + DeprecatedFunctionNotification(f.position, name.toString, deprecatedBy))
      case _: UnresolvedCall =>
        throw new InternalException("Expected procedures to have been resolved already")
    }

  override def phase = DEPRECATION_WARNINGS

}

/**
 * Find calls to procedures with warnings.
 */
case object ProcedureWarnings extends VisitorPhase[BaseContext, BaseState] {

  override def visit(value: BaseState, context: BaseContext): Unit = {
    val warnings = findWarnings(value.statement())

    warnings.foreach(context.notificationLogger.log)
  }

  private def findWarnings(statement: Statement): Set[InternalNotification] =
    statement.folder.treeFold(Set.empty[InternalNotification]) {
      case f @ ResolvedCall(ProcedureSignature(name, _, _, _, _, _, Some(warning), _, _, _, _, _), _, _, _, _, _) =>
        seq => SkipChildren(seq + ProcedureWarningNotification(f.position, name.toString, warning))
      case ResolvedCall(ProcedureSignature(name, _, Some(output), None, _, _, _, _, _, _, _, _), _, results, _, _, _)
        if output.exists(_.deprecated) =>
        set => SkipChildren(set ++ usedDeprecatedFields(name.toString, results, output))
      case _: UnresolvedCall =>
        throw new InternalException("Expected procedures to have been resolved already")
    }

  private def usedDeprecatedFields(procedure: String, used: Seq[ProcedureResultItem], available: Seq[FieldSignature]) =
    used.filter(r => available.exists(o => o.name == r.outputName && o.deprecated)).map(r =>
      DeprecatedFieldNotification(r.position, procedure, r.outputName)
    )

  override def phase = DEPRECATION_WARNINGS

}
