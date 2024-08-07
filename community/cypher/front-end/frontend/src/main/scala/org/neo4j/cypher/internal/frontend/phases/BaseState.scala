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
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.StepSequencer

trait BaseState {
  def queryText: String
  def plannerName: PlannerName
  def maybeStatement: Option[Statement]
  def maybeReturnColumns: Option[Seq[String]]
  def maybeSemantics: Option[SemanticState]
  def maybeExtractedParams: Option[Map[String, Any]]
  def maybeSemanticTable: Option[SemanticTable]
  def maybeObfuscationMetadata: Option[ObfuscationMetadata]
  def anonymousVariableNameGenerator: AnonymousVariableNameGenerator

  def accumulatedConditions: Set[StepSequencer.Condition]


  def statement(): Statement = maybeStatement getOrElse fail("Statement")
  def returnColumns(): Seq[String] = maybeReturnColumns getOrElse fail("Return columns")
  def semantics(): SemanticState = maybeSemantics getOrElse fail("Semantics")
  def extractedParams(): Map[String, Any] = maybeExtractedParams getOrElse fail("Extracted parameters")
  def semanticTable(): SemanticTable = maybeSemanticTable getOrElse fail("Semantic table")
  def obfuscationMetadata(): ObfuscationMetadata = maybeObfuscationMetadata getOrElse fail("Obfuscation metadata")

  protected def fail(what: String) = {
    throw new IllegalStateException(s"$what not yet initialised")
  }

  def withStatement(s: Statement): BaseState
  def withReturnColumns(cols: Seq[String]): BaseState
  def withSemanticTable(s: SemanticTable): BaseState
  def withSemanticState(s: SemanticState): BaseState
  def withParams(p: Map[String, Any]): BaseState
  def withObfuscationMetadata(o: ObfuscationMetadata): BaseState
}

case class InitialState(queryText: String,
  plannerName: PlannerName,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  maybeStatement: Option[Statement] = None,
  maybeSemantics: Option[SemanticState] = None,
  maybeExtractedParams: Option[Map[String, Any]] = None,
  maybeSemanticTable: Option[SemanticTable] = None,
  accumulatedConditions: Set[StepSequencer.Condition] = Set.empty,
  maybeReturnColumns: Option[Seq[String]] = None,
  maybeObfuscationMetadata: Option[ObfuscationMetadata] = None) extends BaseState {

  override def withStatement(s: Statement): InitialState = copy(maybeStatement = Some(s))

  override def withReturnColumns(cols: Seq[String]): InitialState = copy(maybeReturnColumns = Some(cols))

  override def withSemanticTable(s: SemanticTable): InitialState = copy(maybeSemanticTable = Some(s))

  override def withSemanticState(s: SemanticState): InitialState = copy(maybeSemantics = Some(s))

  override def withParams(p: Map[String, Any]): InitialState = copy(maybeExtractedParams = Some(p))

  override def withObfuscationMetadata(o: ObfuscationMetadata): InitialState = copy(maybeObfuscationMetadata = Some(o))
}
