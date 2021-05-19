/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS
import org.neo4j.cypher.internal.rewriting.Deprecation
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp

case object DeprecatedSyntaxReplaced extends Condition

/**
 * Find deprecated Cypher constructs and generate warnings for them.
 *
 * Replace deprecated syntax.
 */
case class SyntaxDeprecationWarningsAndReplacements(deprecations: Deprecations) extends Phase[BaseContext, BaseState, BaseState] {

  override def process(state: BaseState, context: BaseContext): BaseState = {
    // collect notifications and replacements
    case class Acc(notifications: Set[InternalNotification], replacements: Map[ASTNode, ASTNode]) {
      def +(deprecation: Deprecation): Acc = Acc(
        notifications ++ deprecation.notification,
        replacements ++ deprecation.replacement
      )
    }

    val foundWithoutContext = state.statement().fold(Acc(Set.empty, Map.empty)) {
      deprecations.find.andThen(deprecation => acc => acc + deprecation)
    }

    val Acc(notifications, replacements) = deprecations.findWithContext(state.statement(), state.maybeSemanticTable).foldLeft(foundWithoutContext) {
      case (acc, deprecation) => acc + deprecation
    }

    // issue notifications
    notifications.foreach(context.notificationLogger.log)

    // apply replacements
    val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case astNode: ASTNode => replacements.getOrElse(astNode, astNode)
    })
    val newStatement = state.statement().endoRewrite(rewriter)
    state.withStatement(newStatement)
  }

  override def postConditions: Set[StepSequencer.Condition] = Set(DeprecatedSyntaxReplaced)

  override def phase = DEPRECATION_WARNINGS
}
