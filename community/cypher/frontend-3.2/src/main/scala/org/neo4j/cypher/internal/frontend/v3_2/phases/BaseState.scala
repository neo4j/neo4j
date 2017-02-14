/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, InternalException, PlannerName, SemanticState, SemanticTable}

trait BaseState {
  def queryText: String
  def startPosition: Option[InputPosition]
  def plannerName: PlannerName
  def maybeStatement: Option[Statement]
  def maybeSemantics: Option[SemanticState]
  def maybeExtractedParams: Option[Map[String, Any]]
  def maybeSemanticTable: Option[SemanticTable]

  def accumulatedConditions: Set[Condition]

  def isPeriodicCommit(): Boolean = statement() match {
    case Query(Some(_), _) => true
    case _ => false
  }

  def statement(): Statement = maybeStatement getOrElse fail("Statement")
  def semantics(): SemanticState = maybeSemantics getOrElse fail("Semantics")
  def extractedParams(): Map[String, Any] = maybeExtractedParams getOrElse fail("Extracted parameters")
  def semanticTable(): SemanticTable = maybeSemanticTable getOrElse fail("Semantic table")

  protected def fail(what: String) = {
    throw new InternalException(s"$what not yet initialised")
  }

  def withStatement(s: Statement): BaseState
  def withSemanticTable(s: SemanticTable): BaseState
}

case class BaseStateImpl(queryText: String,
                         startPosition: Option[InputPosition],
                         plannerName: PlannerName,
                         maybeStatement: Option[Statement] = None,
                         maybeSemantics: Option[SemanticState] = None,
                         maybeExtractedParams: Option[Map[String, Any]] = None,
                         maybeSemanticTable: Option[SemanticTable] = None,
                         accumulatedConditions: Set[Condition] = Set.empty) extends BaseState {
  override def withStatement(s: Statement): BaseState = copy(maybeStatement = Some(s))

  override def withSemanticTable(s: SemanticTable): BaseState = copy(maybeSemanticTable = Some(s))
}
