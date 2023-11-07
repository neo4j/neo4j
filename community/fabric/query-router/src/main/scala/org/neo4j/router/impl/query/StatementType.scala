/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.router.impl.query

import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UpdateClause
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.fabric.util.Folded.FoldableOps
import org.neo4j.fabric.util.Folded.Stop
import org.neo4j.router.impl.query.StatementType.CommandType.AdministrationCommand
import org.neo4j.router.impl.query.StatementType.CommandType.CommandType
import org.neo4j.router.impl.query.StatementType.CommandType.QueryCommand
import org.neo4j.router.impl.query.StatementType.CommandType.SchemaCommand

import scala.util.Try

case class StatementType(commandType: CommandType, containsUpdatingClause: Option[Boolean]) {

  def isQueryCommand: Boolean =
    commandType.eq(QueryCommand)

  def isSchemaCommand: Boolean =
    commandType.eq(SchemaCommand)

  def isReadQuery: Boolean =
    isQueryCommand && containsUpdatingClause.contains(false)

  def isWrite: Boolean =
    containsUpdatingClause.getOrElse(false)

  override def toString: String = {
    commandType match {
      case AdministrationCommand       => "Administration command"
      case SchemaCommand               => "Schema modification"
      case QueryCommand if isReadQuery => "Read query"
      case QueryCommand if isWrite     => "Write query"
      case _                           => "Read query (with unresolved procedures)"
    }
  }
}

object StatementType {

  object CommandType extends Enumeration {
    type CommandType = Value
    val AdministrationCommand, SchemaCommand, QueryCommand = Value
  }

  // Java access helpers
  val AdministrationCommand: CommandType = CommandType.AdministrationCommand
  val SchemaCommand: CommandType = CommandType.SchemaCommand
  val QueryCommand: CommandType = CommandType.QueryCommand

  def of(commandType: CommandType): StatementType = StatementType(commandType, None)

  def of(commandType: CommandType, containsUpdates: Boolean): StatementType =
    StatementType(commandType, Some(containsUpdates))

  /*
   * Creates a StatementType given a statement and a procedure signature resolver.
   * The command type is decided directly from the statement, but we need to do some recursion
   * in order to figure out if the statement contains any writes.
   */
  def of(statement: Statement, resolver: ProcedureSignatureResolver): StatementType = {
    val maybeContainsUpdates = containsUpdates(statement, callClause => containsUpdates(callClause, resolver))

    statement match {
      case _: Query                 => StatementType(QueryCommand, maybeContainsUpdates)
      case _: SchemaCommand         => StatementType(SchemaCommand, maybeContainsUpdates)
      case _: AdministrationCommand => StatementType(AdministrationCommand, maybeContainsUpdates)
    }
  }

  /*
   * Recursive function which returns:if any of the clauses is an updating clause.
   *
   * - Some(TRUE)  if `ast` contains any updating clause
   * - Some(FALSE) if `ast` does neither have any updating clause nor any unresolved procedure call(s).
   * - None        if `ast` does not have any updating clause, but does have unresolved procedure call(s).
   */
  private def containsUpdates(ast: ASTNode, callClauseHandler: CallClause => Option[Boolean]): Option[Boolean] =
    ast.folded[Option[Boolean]](Some(false))(merge) {
      case _: UpdateClause          => Stop(Some(true))
      case c: CallClause            => Stop(callClauseHandler.apply(c))
      case _: SchemaCommand         => Stop(Some(true))
      case a: AdministrationCommand => Stop(if (a.isReadOnly) Some(false) else Some(true))
    }

  private def containsUpdates(ast: CallClause): Option[Boolean] = ast match {
    case _: UnresolvedCall                      => None
    case c: ResolvedCall if c.containsNoUpdates => Some(false)
    case _                                      => Some(true)
  }

  private def containsUpdates(ast: CallClause, resolver: ProcedureSignatureResolver): Option[Boolean] = ast match {
    case unresolved: UnresolvedCall => containsUpdates(tryResolve(unresolved, resolver))
    case c                          => containsUpdates(c)
  }

  private def tryResolve(unresolved: UnresolvedCall, resolver: ProcedureSignatureResolver): CallClause =
    Try(ResolvedCall(resolver.procedureSignature)(unresolved)).getOrElse(unresolved)

  private def merge: (Option[Boolean], Option[Boolean]) => Option[Boolean] = {
    // If any of the two options are true (contains update clause) => then return true (contains updating clause)
    case (maybeIsWrite1, maybeIsWrite2) if maybeIsWrite1.getOrElse(false) || maybeIsWrite2.getOrElse(false) =>
      Some(true)
    // If any of them does not yet know if they contain updates => return None
    case (maybeIsWrite1, maybeIsWrite2) if maybeIsWrite1.isEmpty || maybeIsWrite2.isEmpty => None
    // Both of them are read queries
    case _ => Some(false)

  }
}
