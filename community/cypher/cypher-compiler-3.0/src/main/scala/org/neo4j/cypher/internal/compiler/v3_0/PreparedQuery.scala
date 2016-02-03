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
package org.neo4j.cypher.internal.compiler.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.commands.StatementConverters._
import org.neo4j.cypher.internal.compiler.v3_0.commands.AbstractQuery
import org.neo4j.cypher.internal.compiler.v3_0.tracing.rewriters.RewriterCondition
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Query, Statement}
import org.neo4j.cypher.internal.frontend.v3_0.{InputPosition, Rewriter, Scope, SemanticTable}

sealed trait PreparedQuery {
  def statement: Statement
  def queryText: String
  def extractedParams: Map[String, Any]

  def notificationLogger: InternalNotificationLogger
  def plannerName: String

  def abstractQuery: AbstractQuery = statement.asQuery(notificationLogger, plannerName).setQueryText(queryText)

  def isPeriodicCommit = statement match {
    case Query(Some(_), _) => true
    case _ => false
  }
}

case class PreparedQuerySyntax(statement: Statement,
                               queryText: String,
                               extractedParams: Map[String, Any])(val notificationLogger: InternalNotificationLogger,
                                                                  val plannerName: String = "",
                                                                  val conditions: Set[RewriterCondition])

  extends PreparedQuery {

  def rewrite(rewriter: Rewriter): PreparedQuerySyntax =
    copy(statement = statement.endoRewrite(rewriter))(notificationLogger, plannerName, conditions)

  def withSemantics(semanticTable: SemanticTable,
                    scopeTree: Scope) =
    PreparedQuerySemantics(statement, queryText, extractedParams, semanticTable, scopeTree)(notificationLogger, plannerName, conditions)
}

case class PreparedQuerySemantics(statement: Statement,
                                  queryText: String,
                                  extractedParams: Map[String, Any],
                                  semanticTable: SemanticTable,
                                  scopeTree: Scope)(val notificationLogger: InternalNotificationLogger,
                                                    val plannerName: String = "",
                                                    val conditions: Set[RewriterCondition] = Set.empty)

  extends PreparedQuery {

  // TODO: Get rid of this variant
  def rewrite(rewriter: Rewriter): PreparedQuerySemantics =
    rewrite(rewriter, identity)

  def rewrite(rewriter: Rewriter, tableTransformer: SemanticTable => SemanticTable): PreparedQuerySemantics =
    copy(
      statement = statement.endoRewrite(rewriter),
      semanticTable = tableTransformer(semanticTable)
    )(
      notificationLogger,
      plannerName,
      conditions
    )
}
