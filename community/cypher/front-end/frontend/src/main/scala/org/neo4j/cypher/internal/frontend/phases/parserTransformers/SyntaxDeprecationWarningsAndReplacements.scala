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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.Deprecation
import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.SemanticDeprecations
import org.neo4j.cypher.internal.rewriting.SyntacticDeprecations
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp

case object DeprecatedSyntaxReplaced extends Condition
case object DeprecatedSemanticsReplaced extends Condition

/**
 * Find deprecated Cypher constructs, generate warnings for them, and replace deprecated syntax with currently accepted syntax.
 */
case class SyntaxDeprecationWarningsAndReplacements(deprecations: Deprecations)
    extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step with ParsePipelineTransformerFactory {

  override def process(state: BaseState, context: BaseContext): BaseState = {
    val allDeprecations = deprecations match {
      case syntacticDeprecations: SyntacticDeprecations =>
        val foundWithoutContext = state.statement().folder.fold(Set.empty[Deprecation]) {
          syntacticDeprecations.find(context.cypherVersion).andThen(deprecation => acc => acc + deprecation)
        }
        val foundWithContext = syntacticDeprecations.findWithContext(state.statement())
        foundWithoutContext ++ foundWithContext
      case semanticDeprecations: SemanticDeprecations =>
        val semanticTable = state.maybeSemanticTable.getOrElse(
          throw new IllegalStateException(
            s"Got semantic deprecations ${semanticDeprecations.getClass.getSimpleName} but no SemanticTable"
          )
        )

        state.statement().folder.fold(Set.empty[Deprecation]) {
          semanticDeprecations.find(context.cypherVersion, semanticTable).andThen(deprecation =>
            acc => acc + deprecation
          )
        }
    }

    val notifications = allDeprecations.flatMap(_.notification)
    val replacements = allDeprecations.flatMap(_.replacement).toMap

    // issue notifications
    notifications.foreach(context.notificationLogger.log)

    // apply replacements
    val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case astNode: ASTNode => replacements.getOrElse(Ref(astNode), astNode)
    })
    val newStatement = state.statement().endoRewrite(rewriter)
    state.withStatement(newStatement)
  }

  override def preConditions: Set[Condition] = deprecations match {
    case _: SyntacticDeprecations => Set(
        BaseContains[Statement]()
      )
    case _: SemanticDeprecations => Set(
        BaseContains[Statement](),
        BaseContains[SemanticTable]()
      ) ++ SemanticInfoAvailable
  }

  override def invalidatedConditions: Set[Condition] = SemanticInfoAvailable + UpToDateScopes

  override def postConditions: Set[StepSequencer.Condition] = deprecations match {
    case _: SyntacticDeprecations => Set(DeprecatedSyntaxReplaced)
    case _: SemanticDeprecations  => Set(DeprecatedSemanticsReplaced)
  }

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def phase = DEPRECATION_WARNINGS
}
