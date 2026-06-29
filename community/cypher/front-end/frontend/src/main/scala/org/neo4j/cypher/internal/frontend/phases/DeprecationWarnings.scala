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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.notification.DeprecatedFunctionFieldNotification
import org.neo4j.cypher.internal.notification.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.notification.DeprecatedProcedureFieldNotification
import org.neo4j.cypher.internal.notification.DeprecatedProcedureNotification
import org.neo4j.cypher.internal.notification.DeprecatedProcedureReturnFieldNotification
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.ProcedureWarningNotification
import org.neo4j.cypher.internal.notification.RedundantOptionalProcedure
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

case object ProcedureAndFunctionDeprecationsThrown extends Condition
case object ProcedureWarningsThrown extends Condition

/**
 * Find calls to deprecated procedures and functions and generate warnings for them.
 */
case object ProcedureAndFunctionDeprecationWarnings extends VisitorPhase[BaseContext, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(CallInvocationsResolved, FunctionInvocationsResolved)
  override def postConditions: Set[StepSequencer.Condition] = Set(ProcedureAndFunctionDeprecationsThrown)
  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def visit(value: BaseState, context: BaseContext): Unit = {
    val warnings = findDeprecations(value.statement())

    warnings.foreach(context.notificationLogger.log)
  }

  private def findDeprecations(statement: Statement): Set[InternalNotification] =
    statement.folder.treeFold(Set.empty[InternalNotification]) {
      case f @ ResolvedNonLocalCall(
          ProcedureSignature(name, inputFields, maybeOutput, maybeDeprecatedInfo, _, _, _, _, _, _, _, _),
          _,
          results,
          _,
          _,
          _,
          _
        ) => seq =>
          TraverseChildren(
            seq // Deprecated input fields
              ++ inputFields.filter(_.deprecated).map(inputField =>
                DeprecatedProcedureFieldNotification(f.position, name.fullName, inputField.name)
              ).toSet
              // Deprecated Procedure
              ++ (maybeDeprecatedInfo match {
                case _ @Some(DeprecationInfo(true, deprecatedBy)) =>
                  Set(DeprecatedProcedureNotification(f.position, name.fullName, deprecatedBy))
                case _ => Set.empty
              })
              // Deprecated output fields
              ++ (maybeOutput match {
                case _ @Some(output) if output.exists(_.deprecated) =>
                  results.filter(r => output.exists(o => o.name == r.outputName && o.deprecated)).map(r =>
                    DeprecatedProcedureReturnFieldNotification(r.position, name.fullName, r.outputName)
                  )
                case _ => Set.empty
              })
          )
      case f @ ResolvedFunctionInvocation(
          _,
          Some(UserFunctionSignature(name, inputFields, _, maybeDeprecatedInfo, _, _, _, _, _)),
          _,
          _
        ) =>
        seq =>
          TraverseChildren(seq
          // Deprecated input fields
            ++ inputFields.filter(_.deprecated).map(inputField =>
              DeprecatedFunctionFieldNotification(f.position, name.fullName, inputField.name)
            ).toSet
            // Deprecated Function
            ++ (maybeDeprecatedInfo match {
              case _ @Some(DeprecationInfo(true, deprecatedBy)) =>
                Set(DeprecatedFunctionNotification(f.position, name.fullName, deprecatedBy))
              case _ => Set.empty
            }))
      case f: FunctionInvocation =>
        // Deprecated Built-In Function
        val deprecationWarnings: Seq[DeprecatedFunctionNotification] = f.function.signatures.filter {
          case FunctionTypeSignature(_, _, _, _, _, argumentTypes, _, deprecated, _, _, _, _, _, _, _) =>
            deprecated && argumentTypes.length == f.arguments.length
        }.map(fts =>
          DeprecatedFunctionNotification(
            f.position,
            f.function.name,
            fts.deprecatedBy
          )
        )
        seq => TraverseChildren(seq ++ deprecationWarnings.toSet)
      // If passing through the fabric path all procedures might not have been resolved yet.
      // In this case they will fail later during strict resolution.
      case _: UnresolvedCall => seq => SkipChildren(seq)
    }

  override def phase = DEPRECATION_WARNINGS

}

/**
 * Find calls to procedures with warnings.
 */
case object ProcedureWarnings extends VisitorPhase[BaseContext, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set(CallInvocationsResolved, FunctionInvocationsResolved)
  override def postConditions: Set[StepSequencer.Condition] = Set(ProcedureWarningsThrown)
  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def visit(value: BaseState, context: BaseContext): Unit = {
    val warnings = findWarnings(value.statement())

    warnings.foreach(context.notificationLogger.log)
  }

  private def findWarnings(statement: Statement): Set[InternalNotification] =
    statement.folder.treeFold(Set.empty[InternalNotification]) {
      case f @ ResolvedNonLocalCall(
          ProcedureSignature(name, _, _, _, _, _, maybeWarning, _, _, _, _, _),
          _,
          result,
          _,
          _,
          _,
          _
        ) =>
        seq =>
          TraverseChildren(
            seq // A warning message the procedure wants us to generate
              ++ (maybeWarning match {
                case _ @Some(warning) => Set(ProcedureWarningNotification(f.position, name.fullName, warning))
                case _                => Set.empty
              }) // Redundant usage of optional (on void proc)
              ++ (if (f.optional && result.isEmpty) {
                    Set(RedundantOptionalProcedure(f.position, name.fullName))
                  } else {
                    Set.empty
                  })
          )
      // If passing through the fabric path all procedures might not have been resolved yet.
      // In this case they will fail later during strict resolution.
      case _: UnresolvedCall => seq => SkipChildren(seq)
    }

  override def phase = DEPRECATION_WARNINGS
}
