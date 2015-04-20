/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.ast.Statement

class SemanticChecker(semanticCheckMonitor: SemanticCheckMonitor) {
  def check(queryText: String, statement: Statement, notificationLogger: InternalNotificationLogger = devNullLogger,
            mkException: (String, InputPosition) => CypherException): SemanticState = {

    semanticCheckMonitor.startSemanticCheck(queryText)
    val SemanticCheckResult(semanticState, semanticErrors) = statement.semanticCheck(SemanticState.clean.withNotificationLogger(notificationLogger))

    val scopeTreeIssues = ScopeTreeVerifier.verify(semanticState.scopeTree)
    if (scopeTreeIssues.nonEmpty)
      throw new InternalException(scopeTreeIssues.mkString(s"\n"))

    if (semanticErrors.nonEmpty) {
      semanticCheckMonitor.finishSemanticCheckError(queryText, semanticErrors)
    } else {
      semanticCheckMonitor.finishSemanticCheckSuccess(queryText)
    }
    semanticErrors.map { error => throw mkException(error.msg, error.position) }

    semanticState
  }
}
