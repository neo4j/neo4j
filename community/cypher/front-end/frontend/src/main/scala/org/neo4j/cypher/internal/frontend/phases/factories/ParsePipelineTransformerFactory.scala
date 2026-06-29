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
package org.neo4j.cypher.internal.frontend.phases.factories

import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.IfChangedSetSemantics
import org.neo4j.cypher.internal.frontend.phases.ResolveCallables
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.rewriting.rewriters.Forced
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.Never
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

case class ParsingConfig(
  resolveCallables: ResolveCallables,
  extractLiterals: ExtractLiteral = ExtractLiteral.ALWAYS,
  /* TODO: This is not part of configuration - Move to BaseState */
  parameterTypeMapping: Map[String, ParameterTypeInfo] = Map.empty,
  resolveSimpleDynamicExpressions: Boolean = false,
  enabledVirtualGraph: Boolean = false
) {

  def literalExtractionStrategy: LiteralExtractionStrategy = extractLiterals match {
    case ExtractLiteral.ALWAYS          => Forced
    case ExtractLiteral.NEVER           => Never
    case ExtractLiteral.IF_NO_PARAMETER => IfNoParameter
  }
}

trait ParsePipelineTransformerFactory {

  def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState]

  def getCheckedTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = {
    val transformer = getTransformer(config)
    if (transformer.invalidatedConditions.intersect(SemanticAnalysis.postConditions).nonEmpty)
      IfChangedSetSemantics.using(transformer)
    else transformer
  }

}
