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
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.FoldingBehavior
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.LiteralOffset
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.StepSequencer

/**
 * Collect sensitive literals and parameters.
 */
case object ObfuscationMetadataCollection extends Phase[BaseContext, BaseState, BaseState] {

  override def phase: CompilationPhaseTracer.CompilationPhase = METADATA_COLLECTION

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val extractedParamNames = from.maybeExtractedParams.map(_.keySet.map(_.name)).getOrElse(Set.empty)
    val parameters = from.statement().folder.findAllByClass[Parameter]

    val offsets =
      collectSensitiveLiteralOffsets(from.statement(), from.maybeExtractedParams.getOrElse(Map.empty))
    val sensitiveParams = collectSensitiveParameterNames(parameters, extractedParamNames)

    from.withObfuscationMetadata(ObfuscationMetadata(offsets, sensitiveParams))
  }

  private def collectSensitiveLiteralOffsets(
    statement: Statement,
    extractedParameters: Map[AutoExtractedParameter, Expression]
  ): Vector[LiteralOffset] = {

    val partial: PartialFunction[Any, Vector[LiteralOffset] => FoldingBehavior[Vector[LiteralOffset]]] = {
      case literal: SensitiveLiteral =>
        (acc: Vector[LiteralOffset]) =>
          SkipChildren(acc :+ LiteralOffset(
            literal.position.offset,
            literal.position.line,
            Some(literal.literalLength)
          ))
      case p: AutoExtractedParameter with SensitiveAutoParameter =>
        (acc: Vector[LiteralOffset]) =>
          extractedParameters.get(p) match {
            case Some(originalExp) =>
              val literalOffsets = originalExp.folder.findAllByClass[Literal]
                .map(_.asSensitiveLiteral)
                .map(l => LiteralOffset(l.position.offset, l.position.line, Some(l.literalLength)))
              SkipChildren(acc ++ literalOffsets)
            case None =>
              // Note, this can lead to query obfuscator failing and the query not being logged
              SkipChildren(acc :+ LiteralOffset(p.position.offset, p.position.line, None))
          }
    }

    val fromStatement = statement.folder.treeFold(Vector.empty[LiteralOffset])(partial)
    val fromStatementAndExtracted = extractedParameters.folder.treeFold(fromStatement)(partial)
    fromStatementAndExtracted.distinct.sortBy(_.start(0))
  }

  private def collectSensitiveParameterNames(
    queryParams: Seq[Parameter],
    extractedParamNames: Set[String]
  ): Set[String] =
    queryParams.folder.findAllByClass[SensitiveParameter].map(_.name).toSet -- extractedParamNames
}
