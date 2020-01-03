/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.frontend.phases

import org.neo4j.cypher.internal.v4_0.ast.{Query, Statement}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticState, SemanticTable}
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.internal.v4_0.util.InputPosition

trait BaseState {
  def queryText: String
  def startPosition: Option[InputPosition]
  def plannerName: PlannerName
  def initialFields: Map[String, CypherType]
  def maybeStatement: Option[Statement]
  def maybeReturnColumns: Option[Seq[String]]
  def maybeSemantics: Option[SemanticState]
  def maybeExtractedParams: Option[Map[String, Any]]
  def maybeSemanticTable: Option[SemanticTable]

  def accumulatedConditions: Set[Condition]


  def statement(): Statement = maybeStatement getOrElse fail("Statement")
  def returnColumns(): Seq[String] = maybeReturnColumns getOrElse fail("Return columns")
  def semantics(): SemanticState = maybeSemantics getOrElse fail("Semantics")
  def extractedParams(): Map[String, Any] = maybeExtractedParams getOrElse fail("Extracted parameters")
  def semanticTable(): SemanticTable = maybeSemanticTable getOrElse fail("Semantic table")

  protected def fail(what: String) = {
    throw new IllegalStateException(s"$what not yet initialised")
  }

  def withStatement(s: Statement): BaseState
  def withReturnColumns(cols: Seq[String]): BaseState
  def withSemanticTable(s: SemanticTable): BaseState
  def withSemanticState(s: SemanticState): BaseState
  def withParams(p: Map[String, Any]): BaseState
}

case class InitialState(queryText: String,
  startPosition: Option[InputPosition],
  plannerName: PlannerName,
  initialFields: Map[String, CypherType] = Map.empty,
  maybeStatement: Option[Statement] = None,
  maybeSemantics: Option[SemanticState] = None,
  maybeExtractedParams: Option[Map[String, Any]] = None,
  maybeSemanticTable: Option[SemanticTable] = None,
  accumulatedConditions: Set[Condition] = Set.empty,
  maybeReturnColumns: Option[Seq[String]] = None) extends BaseState {

  override def withStatement(s: Statement): InitialState = copy(maybeStatement = Some(s))

  override def withReturnColumns(cols: Seq[String]): InitialState = copy(maybeReturnColumns = Some(cols))

  override def withSemanticTable(s: SemanticTable): InitialState = copy(maybeSemanticTable = Some(s))

  override def withSemanticState(s: SemanticState): InitialState = copy(maybeSemantics = Some(s))

  override def withParams(p: Map[String, Any]): InitialState = copy(maybeExtractedParams = Some(p))
}
