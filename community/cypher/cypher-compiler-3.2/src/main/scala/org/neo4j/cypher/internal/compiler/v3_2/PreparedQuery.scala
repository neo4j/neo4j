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
package org.neo4j.cypher.internal.compiler.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.commands.StatementConverters._
import org.neo4j.cypher.internal.compiler.v3_2.commands.AbstractQuery
import org.neo4j.cypher.internal.compiler.v3_2.tracing.rewriters.RewriterCondition
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, Rewriter, Scope, SemanticTable}

// A Cypher query goes through various stages of pre-processing before planning
//
// A prepared query captures all information that has been derived so far as part
// of this processing
//
// Currently there are two types of prepared queries:
//
// - PreparedQuerySyntax (can be constructed without db access)
// - PreparedQuerySemantics (construction may requires db access for resolving procedure signatures)
//
sealed trait PreparedQuery {
  type SELF <: PreparedQuery

  def statement: Statement
  def queryText: String
  def extractedParams: Map[String, Any]

  def plannerName: String

  def abstractQuery(notificationLogger: InternalNotificationLogger): AbstractQuery =
    statement.asQuery(notificationLogger, plannerName).setQueryText(queryText)

  def rewrite(rewriter: Rewriter): SELF

  def isPeriodicCommit = statement match {
    case Query(Some(_), _) => true
    case _ => false
  }
}

// Result of syntactical analysis of a Cypher query
//
// The contained statement is known to be syntactically correct, have correctly bound names
// (i.e. does not contain variables that hasn't been declared in an accessible scope before)
// and has passed limited type checking (without correct procedure signatures)
//
case class PreparedQuerySyntax(statement: Statement,
                               queryText: String,
                               offset: Option[InputPosition],
                               extractedParams: Map[String, Any])(val plannerName: String = "",
                                                                  val conditions: Set[RewriterCondition] = Set.empty)

  extends PreparedQuery {

  override type SELF = PreparedQuerySyntax

  override def rewrite(rewriter: Rewriter): PreparedQuerySyntax =
    copy(statement = statement.endoRewrite(rewriter))(plannerName, conditions)

  def withSemantics(semanticTable: SemanticTable,
                    scopeTree: Scope) =
    PreparedQuerySemantics(statement, queryText, offset, extractedParams, semanticTable, scopeTree)(plannerName, conditions)
}

// Result of semantic analysis of a Cypher query
//
// The contained statement has passed all syntactic checks as well as full type checking
// (taking into account correct procedure signatures).
//
case class PreparedQuerySemantics(statement: Statement,
                                  queryText: String,
                                  offset: Option[InputPosition],
                                  extractedParams: Map[String, Any],
                                  semanticTable: SemanticTable,
                                  scopeTree: Scope)(val plannerName: String = "",
                                                    val conditions: Set[RewriterCondition] = Set.empty)

  extends PreparedQuery {

  override type SELF = PreparedQuerySemantics

  override def rewrite(rewriter: Rewriter): PreparedQuerySemantics =
    rewrite(rewriter, identity)

  def rewrite(rewriter: Rewriter, tableTransformer: SemanticTable => SemanticTable): PreparedQuerySemantics =
    copy(
      statement = statement.endoRewrite(rewriter),
      semanticTable = tableTransformer(semanticTable)
    )(
      plannerName,
      conditions
    )
}
