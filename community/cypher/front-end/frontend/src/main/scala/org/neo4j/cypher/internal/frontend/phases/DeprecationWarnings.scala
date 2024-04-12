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

import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.exceptions.InternalException
import org.neo4j.notifications.DeprecatedFunctionFieldNotification
import org.neo4j.notifications.DeprecatedProcedureFieldNotification
import org.neo4j.notifications.DeprecatedProcedureNotification
import org.neo4j.notifications.DeprecatedProcedureReturnFieldNotification
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
          ProcedureSignature(name, inputFields, _, Some(DeprecationInfo(true, deprecatedBy)), _, _, _, _, _, _, _, _),
          _,
          _,
          _,
          _,
          _
        ) =>
        seq =>
          SkipChildren(
            seq ++ inputFields.filter(_.deprecated).map(inputField =>
              DeprecatedProcedureFieldNotification(f.position, name.toString, inputField.name)
            ).toSet
              + DeprecatedProcedureNotification(f.position, name.toString, deprecatedBy)
          )
      case f @ ResolvedCall(
          ProcedureSignature(name, inputFields, _, _, _, _, _, _, _, _, _, _),
          _,
          _,
          _,
          _,
          _
        ) if inputFields.exists(_.deprecated) =>
        seq =>
          SkipChildren(
            seq ++ inputFields.filter(_.deprecated).map(inputField =>
              DeprecatedProcedureFieldNotification(f.position, name.toString, inputField.name)
            ).toSet
          )
      case f @ ResolvedFunctionInvocation(
          _,
          Some(UserFunctionSignature(name, inputFields, _, Some(DeprecationInfo(true, deprecatedBy)), _, _, _, _, _)),
          _
        ) =>
        seq =>
          SkipChildren(seq ++ inputFields.filter(_.deprecated).map(inputField =>
            DeprecatedFunctionFieldNotification(f.position, name.toString, inputField.name)
          ).toSet + DeprecatedFunctionNotification(
            f.position,
            name.toString,
            deprecatedBy
          ))
      case f @ ResolvedFunctionInvocation(
          name,
          Some(UserFunctionSignature(_, inputFields, _, _, _, _, _, _, _)),
          _
        ) if inputFields.exists(_.deprecated) =>
        seq =>
          SkipChildren(seq ++ inputFields.filter(_.deprecated).map(inputField =>
            DeprecatedFunctionFieldNotification(f.position, name.toString, inputField.name)
          ).toSet)
      case f: FunctionInvocation =>
        val deprecationWarnings: Seq[DeprecatedFunctionNotification] = f.function.signatures.filter {
          case FunctionTypeSignature(_, _, _, _, _, argumentTypes, _, deprecated, _, _, _, _) =>
            deprecated && argumentTypes.length == f.arguments.length
          case _ => false
        }.map(_.asInstanceOf[FunctionTypeSignature]).map(fts =>
          DeprecatedFunctionNotification(
            f.position,
            f.function.name,
            fts.deprecatedBy
          )
        )
        seq => SkipChildren(seq ++ deprecationWarnings.toSet)
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
      case ResolvedCall(ProcedureSignature(name, _, Some(output), _, _, _, _, _, _, _, _, _), _, results, _, _, _)
        if output.exists(_.deprecated) =>
        set => SkipChildren(set ++ usedDeprecatedFields(name.toString, results, output))
      case _: UnresolvedCall =>
        throw new InternalException("Expected procedures to have been resolved already")
    }

  private def usedDeprecatedFields(procedure: String, used: Seq[ProcedureResultItem], available: Seq[FieldSignature]) =
    used.filter(r => available.exists(o => o.name == r.outputName && o.deprecated)).map(r =>
      DeprecatedProcedureReturnFieldNotification(r.position, procedure, r.outputName)
    )

  override def phase = DEPRECATION_WARNINGS

}
