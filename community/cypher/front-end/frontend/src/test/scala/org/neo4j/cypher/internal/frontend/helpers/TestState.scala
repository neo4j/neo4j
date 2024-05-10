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

//noinspection TypeAnnotation
case class TestState(override val maybeStatement: Option[ast.Statement]) extends BaseState {
  override def queryText: String = statement().toString

  override object plannerName extends PlannerName {
    override def name: String = "Test"

    override def version: String = "3.4"

    override def toTextOutput: String = name
  }

  override def maybeReturnColumns: Option[Seq[String]] = None

  override def maybeProcedureSignatureVersion: Option[Long] = None
  override def maybeSemantics = None

  override def maybeExtractedParams = None

  override def maybeSemanticTable = None

  override def maybeObfuscationMetadata: Option[ObfuscationMetadata] = None

  override def accumulatedConditions = Set.empty

  override def withProcedureSignatureVersion(signatureVersion: Option[Long]): BaseState = fail("not implemented")
  override def withStatement(s: ast.Statement) = copy(Some(s))

  override def withReturnColumns(cols: Seq[String]): BaseState = fail("not implemented")

  override def withSemanticTable(s: SemanticTable) = fail("not implemented")

  override def withSemanticState(s: SemanticState) = fail("not implemented")

  override def withParams(p: Map[AutoExtractedParameter, Expression]) = fail("not implemented")

  override def withObfuscationMetadata(o: ObfuscationMetadata) = fail("not implemented")

  override val anonymousVariableNameGenerator: AnonymousVariableNameGenerator = new AnonymousVariableNameGenerator()
}
