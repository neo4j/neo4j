/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.phases

import org.neo4j.cypher.internal.compiler.v3_2.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v3_2.planner.UnionQuery
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, InternalException, SemanticState, SemanticTable}

case class CompilationState(queryText: String,
                            startPosition: Option[InputPosition],
                            plannerName: String,
                            maybeStatement: Option[Statement] = None,
                            maybeSemantics: Option[SemanticState] = None,
                            maybeExtractedParams: Option[Map[String, Any]] = None,
                            maybeSemanticTable: Option[SemanticTable] = None,
                            maybeExecutionPlan: Option[ExecutionPlan] = None,
                            maybeUnionQuery: Option[UnionQuery] = None,
                            accumulatedConditions: Set[Condition] = Set.empty) {

  def isPeriodicCommit: Boolean = statement match {
    case Query(Some(_), _) => true
    case _ => false
  }

  def statement = maybeStatement getOrElse fail("Statement")
  def semantics = maybeSemantics getOrElse fail("Semantics")
  def extractedParams = maybeExtractedParams getOrElse fail("Extracted parameters")
  def semanticTable = maybeSemanticTable getOrElse fail("Semantic table")
  def executionPlan = maybeExecutionPlan getOrElse fail("Execution plan")
  def unionQuery = maybeUnionQuery getOrElse fail("Union query")

  private def fail(what: String) = {
    throw new InternalException(s"$what not yet initialised")
  }
}
