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
package org.neo4j.cypher.internal.frontend.helpers

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.CypherType

case class InputDataStreamTestInitialState(
  queryText: String,
  plannerName: PlannerName,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  initialFields: Map[String, CypherType] = Map.empty,
  maybeProcedureSignatureVersion: Option[Long] = None,
  maybeStatement: Option[ast.Statement] = None,
  maybeSemantics: Option[SemanticState] = None,
  maybeExtractedParams: Option[Map[AutoExtractedParameter, Expression]] = None,
  maybeSemanticTable: Option[SemanticTable] = None,
  accumulatedConditions: Set[StepSequencer.Condition] = Set.empty,
  maybeReturnColumns: Option[Seq[String]] = None,
  maybeObfuscationMetadata: Option[ObfuscationMetadata] = None
) extends BaseState {

  override def withStatement(s: ast.Statement): InputDataStreamTestInitialState = {
    // the unmodified parser is part of the pipeline and it will try to set the result of parsing 'RETURN 1'
    // we simply ignore statements that do not contain InputDataStream AST node
    if (s.folder.findAllByClass[ast.InputDataStream].isEmpty) {
      copy()
    } else {
      copy(maybeStatement = Some(s))
    }
  }

  override def withSemanticTable(s: SemanticTable): InputDataStreamTestInitialState = copy(maybeSemanticTable = Some(s))

  override def withSemanticState(s: SemanticState): InputDataStreamTestInitialState = copy(maybeSemantics = Some(s))

  override def withParams(p: Map[AutoExtractedParameter, Expression]): InputDataStreamTestInitialState =
    copy(maybeExtractedParams = Some(p))

  override def withReturnColumns(cols: Seq[String]): InputDataStreamTestInitialState =
    copy(maybeReturnColumns = Some(cols))

  override def withObfuscationMetadata(o: ObfuscationMetadata): InputDataStreamTestInitialState =
    copy(maybeObfuscationMetadata = Some(o))

  override def withProcedureSignatureVersion(signatureVersion: Option[Long]): InputDataStreamTestInitialState =
    copy(maybeProcedureSignatureVersion = signatureVersion)
}
