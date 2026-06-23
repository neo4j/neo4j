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
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveLiteral
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.METADATA_COLLECTION
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.rewriting.conditions.CallInvocationsResolved
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.FoldingBehavior
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.LiteralOffset
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

case object ObfuscationMetadataCollected extends Condition

/**
 * Collect two config-independent views of the literals to redact (see [[ObfuscationMetadata]]): the sensitive-only
 * view (passwords, sensitive arguments/parameters) and the all-literals view.
 *
 * This phase runs more than once per query and merges its results:
 *  - An early pass, directly after parsing and BEFORE any rewrites, catches every literal while offsets are
 *    still those of the original query text — rewrites (e.g. merging predicates) can move or merge literals,
 *    which would corrupt or lose all-literals offsets.
 *  - A later pass, after procedure/function resolution, catches the SensitiveParameter/sensitive-argument
 *    markers placed by SensitiveParameterRewriter, which do not exist yet during the early pass. The
 *    [[preConditions]] below describe THIS pass.
 */
case object ObfuscationMetadataCollection
    extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def phase: CompilationPhaseTracer.CompilationPhase = METADATA_COLLECTION

  override def preConditions: Set[StepSequencer.Condition] =
    Set(CallInvocationsResolved, FunctionInvocationsResolved)

  override def postConditions: Set[StepSequencer.Condition] = Set(ObfuscationMetadataCollected)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val extractedParameters = from.maybeExtractedParams.getOrElse(Map.empty)
    val extractedParamNames = extractedParameters.keySet.map(_.name)
    val parameters = from.statement().folder.findAllByClass[Parameter]

    val Offsets(sensitiveOffsets, allOffsets) = collectLiteralOffsets(from.statement(), extractedParameters)
    val sensitiveParams = collectSensitiveParameterNames(parameters, extractedParamNames)
    val metadata = ObfuscationMetadata(sensitiveOffsets, allOffsets, sensitiveParams)

    from.withObfuscationMetadata(from.maybeObfuscationMetadata.fold(metadata)(_.merge(metadata)))
  }

  private case class Offsets(sensitive: Vector[LiteralOffset], all: Vector[LiteralOffset])

  private def collectLiteralOffsets(
    statement: Statement,
    extractedParameters: Map[AutoExtractedParameter, Expression]
  ): Offsets = {
    val partial: PartialFunction[Any, Offsets => FoldingBehavior[Offsets]] = {
      case literal: SensitiveLiteral if literal.literalLength > 0 => { case Offsets(sensitive, all) =>
        val offset = LiteralOffset(
          literal.position.offset,
          literal.position.line,
          Some(literal.literalLength)
        )
        SkipChildren(Offsets(sensitive :+ offset, all :+ offset))
      }
      case literal: Literal => { case acc @ Offsets(sensitive, all) =>
        val sensitiveLiteral = literal.asSensitiveLiteral
        if (sensitiveLiteral.literalLength > 0) {
          val offset = LiteralOffset(
            sensitiveLiteral.position.offset,
            sensitiveLiteral.position.line,
            Some(sensitiveLiteral.literalLength)
          )
          SkipChildren(Offsets(sensitive, all :+ offset))
        } else {
          TraverseChildren(acc)
        }
      }
      case p: AutoExtractedParameter with SensitiveAutoParameter => { case Offsets(sensitive, all) =>
        extractedParameters.get(p) match {
          case Some(originalExp) =>
            // contributes when the original holds plain Literals (e.g. a sensitive procedure/function argument).
            val offsets = originalExp.folder.findAllByClass[Literal]
              .map(_.asSensitiveLiteral)
              .collect {
                case l if l.literalLength > 0 =>
                  LiteralOffset(l.position.offset, l.position.line, Some(l.literalLength))
              }
              .toVector
            SkipChildren(Offsets(sensitive ++ offsets, all ++ offsets))
          case None =>
            // Original literal not recovered: mark the position with unknown length so it is still redacted.
            val offset = LiteralOffset(p.position.offset, p.position.line, None)
            SkipChildren(Offsets(sensitive :+ offset, all :+ offset))
        }
      }
    }

    val fromStatement =
      statement.folder.treeFold(Offsets(Vector.empty, Vector.empty))(partial)
    extractedParameters.folder.treeFold(fromStatement)(partial)
  }

  private def collectSensitiveParameterNames(
    queryParams: Seq[Parameter],
    extractedParamNames: Set[String]
  ): Set[String] =
    queryParams.folder.findAllByClass[SensitiveParameter].map(_.name).toSet -- extractedParamNames

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this
}
