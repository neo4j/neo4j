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
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.fabric.util.Folded.FoldableOps
import org.neo4j.fabric.util.Folded.Stop
import org.neo4j.router.impl.query.StatementType.CommandOrQueryType.AdminCommand
import org.neo4j.router.impl.query.StatementType.CommandOrQueryType.CommandOrQueryType
import org.neo4j.router.impl.query.StatementType.CommandOrQueryType.Query
import org.neo4j.router.impl.query.StatementType.CommandOrQueryType.SchemaCommand
import org.neo4j.router.impl.query.StatementType.Mode.MaybeWrite
import org.neo4j.router.impl.query.StatementType.Mode.Mode
import org.neo4j.router.impl.query.StatementType.Mode.Read
import org.neo4j.router.impl.query.StatementType.Mode.Write

import scala.util.Try

case class StatementType(statementType: CommandOrQueryType, private val mode: Mode) {

  def isQuery: Boolean =
    statementType.eq(Query)

  def isSchemaCommand: Boolean =
    statementType.eq(SchemaCommand)

  def isReadQuery: Boolean =
    isQuery && mode.eq(Read)

  def isWrite: Boolean =
    mode.eq(Write)

  override def toString: String = {
    statementType match {
      case AdminCommand         => "Administration command"
      case SchemaCommand        => "Schema modification"
      case Query if isReadQuery => "Read query"
      case Query if isWrite     => "Write query"
      case _                    => "Read query (with unresolved procedures)"
    }
  }
}

object StatementType {

  object CommandOrQueryType extends Enumeration {
    type CommandOrQueryType = Value
    val AdminCommand, SchemaCommand, Query = Value
  }

  object Mode extends Enumeration {
    type Mode = Value
    val Read, Write, MaybeWrite = Value
  }

  // Java access helpers
  val AdminCommand: CommandOrQueryType = CommandOrQueryType.AdminCommand
  private val SchemaCommand: CommandOrQueryType = CommandOrQueryType.SchemaCommand
  val Query: CommandOrQueryType = CommandOrQueryType.Query

  def of(commandType: CommandOrQueryType): StatementType = StatementType(commandType, MaybeWrite)

  def of(commandType: CommandOrQueryType, containsUpdates: Boolean): StatementType =
    StatementType(commandType, if (containsUpdates) Write else Read)

  /*
   * Creates a StatementType given a statement and a procedure signature resolver.
   * The command type is decided directly from the statement, but we need to do some recursion
   * in order to figure out if the statement contains any writes.
   */
  def of(statement: Statement, resolver: ProcedureSignatureResolver): StatementType = {
    val maybeContainsUpdates = containsUpdates(statement, callClause => containsUpdates(callClause, resolver))

    statement match {
      case _: Query                 => StatementType(Query, maybeContainsUpdates)
      case _: SchemaCommand         => StatementType(SchemaCommand, maybeContainsUpdates)
      case _: AdministrationCommand => StatementType(AdminCommand, maybeContainsUpdates)
    }
  }

  /*
   * Recursive function which returns:if any of the clauses is an updating clause.
   *
   * - Some(TRUE)  if `ast` contains any updating clause
   * - Some(FALSE) if `ast` does neither have any updating clause nor any unresolved procedure call(s).
   * - None        if `ast` does not have any updating clause, but does have unresolved procedure call(s).
   */
  private def containsUpdates(ast: ASTNode, callClauseHandler: CallClause => Mode): Mode =
    ast.folded(Read)(merge) {
      case _: UpdateClause          => Stop(Write)
      case c: CallClause            => Stop(callClauseHandler.apply(c))
      case _: SchemaCommand         => Stop(Write)
      case a: AdministrationCommand => Stop(if (a.isReadOnly) Read else Write)
    }

  private def containsUpdates(ast: CallClause): Mode = ast match {
    case _: UnresolvedCall                      => MaybeWrite
    case c: ResolvedCall if c.containsNoUpdates => Read
    case _                                      => Write
  }

  private def containsUpdates(ast: CallClause, resolver: ProcedureSignatureResolver): Mode = ast match {
    case unresolved: UnresolvedCall => containsUpdates(tryResolve(unresolved, resolver))
    case c                          => containsUpdates(c)
  }

  private def tryResolve(unresolved: UnresolvedCall, resolver: ProcedureSignatureResolver): CallClause =
    Try(ResolvedCall(resolver.procedureSignature)(unresolved)).getOrElse(unresolved)

  private def merge: (Mode, Mode) => Mode = {
    case (Write, _)   => Write
    case (_, Write)   => Write
    case (Read, Read) => Read
    case _            => MaybeWrite
  }
}
