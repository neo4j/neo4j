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
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.LocalDefinitionsDirectory
import org.neo4j.cypher.internal.frontend.phases.PipelineDebugInfo
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ObfuscationMetadata

//noinspection TypeAnnotation
case class TestState(
  override val maybeStatement: Option[ast.Statement],
  override val maybeScopeState: Option[ScopeState] = None,
  override val maybeLocalDefinitions: Option[LocalDefinitionsDirectory] = None
) extends BaseState {
  override def queryText: String = statement().toString

  override object plannerName extends PlannerName {
    override def name: String = "Test"

    override def version: String = "3.4"

    override def toTextOutput: String = name
  }

  override def maybeReturnColumns: Option[Seq[String]] = None

  override def maybeProcedureSignatureVersion: Option[Long] = None

  override def maybeSemantics: Option[SemanticState] = None

  override def maybeExtractedParams: Option[Map[AutoExtractedParameter, Expression]] = None

  override def maybeResolvedParams: Option[Set[String]] = None

  override def maybeSemanticTable: Option[SemanticTable] = None

  override def maybeObfuscationMetadata: Option[ObfuscationMetadata] = None

  override def maybeDebugInfo: Option[PipelineDebugInfo] = None

  override def accumulatedConditions = Set.empty

  override def semanticsUpToDate: Boolean = true

  override def withProcedureSignatureVersion(signatureVersion: Option[Long]): TestState = fail("not implemented")

  override def withStatement(s: ast.Statement): TestState = copy(maybeStatement = Some(s))

  override def withReturnColumns(cols: Seq[String]): TestState = fail("not implemented")

  override def withScopeState(s: ScopeState): TestState = copy(maybeScopeState = Some(s))

  override def withLocalDefinitions(localDefinitionsDirectory: LocalDefinitionsDirectory): TestState =
    copy(maybeLocalDefinitions = Some(localDefinitionsDirectory))

  override def withSemanticTable(s: SemanticTable): TestState = fail("not implemented")

  override def withSemanticState(s: SemanticState): TestState = fail("not implemented")

  override def withParams(p: Map[AutoExtractedParameter, Expression]): TestState = fail("not implemented")

  override protected def withResolvedParams(p: Set[String]): TestState = fail("not implemented")

  override def withObfuscationMetadata(o: ObfuscationMetadata): TestState = fail("not implemented")

  override def withDebugInfo(d: PipelineDebugInfo): TestState = fail("not implemented")

  override val anonymousVariableNameGenerator: AnonymousVariableNameGenerator = new AnonymousVariableNameGenerator()

  override def withSemanticsUpToDate(b: Boolean): TestState = fail("not implemented")
}
