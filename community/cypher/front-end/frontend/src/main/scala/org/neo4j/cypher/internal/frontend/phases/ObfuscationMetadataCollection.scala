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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveLiteral
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.METADATA_COLLECTION
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
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
    val extractedParamNames = from.maybeExtractedParams.map(_.keys.toSet).getOrElse(Set.empty)
    val parameters = from.statement().folder.findAllByClass[Parameter]

    val offsets = collectSensitiveLiteralOffsets(from.statement(), extractedParamNames)
    val sensitiveParams = collectSensitiveParameterNames(parameters, extractedParamNames)

    from.withObfuscationMetadata(ObfuscationMetadata(offsets, sensitiveParams))
  }

  private def collectSensitiveLiteralOffsets(
    statement: Statement,
    extractedParamNames: Set[String]
  ): Vector[LiteralOffset] =
    statement.folder.treeFold(Vector.empty[LiteralOffset]) {
      case literal: SensitiveLiteral =>
        acc => SkipChildren(acc :+ LiteralOffset(literal.position.offset, literal.position.line, literal.literalLength))
      case p: SensitiveAutoParameter if extractedParamNames.contains(p.name) =>
        acc => SkipChildren(acc :+ LiteralOffset(p.position.offset, p.position.line, None))

    }.distinct.sortBy(_.start(0))

  private def collectSensitiveParameterNames(
    queryParams: Seq[Parameter],
    extractedParamNames: Set[String]
  ): Set[String] =
    queryParams.folder.findAllByClass[SensitiveParameter].map(_.name).toSet -- extractedParamNames
}
