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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.cypher.internal.ast.ShowCurrentUserClause.homeColumn
import org.neo4j.cypher.internal.ast.ShowCurrentUserClause.pwdChangeReqColumn
import org.neo4j.cypher.internal.ast.ShowCurrentUserClause.rolesColumn
import org.neo4j.cypher.internal.ast.ShowCurrentUserClause.suspendedColumn
import org.neo4j.cypher.internal.ast.ShowCurrentUserClause.tagsColumn
import org.neo4j.cypher.internal.ast.ShowCurrentUserClause.userColumn
import org.neo4j.cypher.internal.logical.plans.CommandDefaultColumn
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.ShowCurrentUserCommunityCommand.getUserColumnValues
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_HOME_DB_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_SUSPENDED_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_TAGS_PROPERTY
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Using

case class ShowCurrentUserCommunityCommand(
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn]
) extends Command(defaultColumns, yieldColumns) {

  override def originalNameRows(
    state: QueryState,
    baseRow: CypherRow
  ): ClosingIterator[Map[String, AnyValue]] = {
    val ctx = state.query

    // Must run on system database
    mustRunOnSystemCheck(ctx, "SHOW CURRENT USER")

    val securityContext = ctx.transactionalContext.securityContext
    val username = securityContext.subject.executingUser

    if (username == "") {
      // securityContext.subject.username() is "" if the securityContext is AUTH_DISABLED (both for community and enterprise)
      // so test for this and return 0 rows in this case
      ClosingIterator.empty
    } else {

      // Only read from the system database once and only if we need to
      val (maybeChangeReq, tags) = {
        val relevantColumns =
          requestedColumnsNames.filter { column =>
            column.equals(pwdChangeReqColumn) ||
            column.equals(tagsColumn)
          }

        if (relevantColumns.nonEmpty) {
          val (maybeChangeReq, tags, _, _) = getUserColumnValues(username, relevantColumns, ctx)
          (maybeChangeReq, tags)
        } else (None, List.empty)
      }

      val resultRow: Map[String, AnyValue] = requestedColumnsNames.map {
        case `userColumn`  => userColumn -> Values.utf8Value(username)
        case `rolesColumn` => rolesColumn -> Values.NO_VALUE
        case `pwdChangeReqColumn` =>
          pwdChangeReqColumn -> maybeChangeReq.map(Values.booleanValue).getOrElse(Values.NO_VALUE)
        case `suspendedColumn` => suspendedColumn -> Values.NO_VALUE
        case `homeColumn`      => homeColumn -> Values.NO_VALUE
        case `tagsColumn`      => tagsColumn -> VirtualValues.list(tags.map(Values.stringValue): _*)
        case unknown           =>
          // This match should cover all existing columns, but we get scala warnings
          // on non-exhaustive match due to it being string values
          throw InternalException.internalError(
            this.getClass.getSimpleName,
            s"Unknown column for show current user. Missing case for column: $unknown.",
            s"Missing case for column: $unknown"
          )
      }.toMap[String, AnyValue]

      ClosingIterator.apply(resultRow)
    }
  }
}

object ShowCurrentUserCommunityCommand {

  /** Fetch all requested values we need to read from the system database for.
   * Columns that may be requested: passwordChangeRequired, tags, suspended, home
   * This assumes we only ask for things that are relevant to fetch, and that we are allowed to see.
   *
   * @param username         The name of the current user
   * @param requestedColumns Which of the relevant columns are requested to be returned.
   * @param ctx              The query context to find the transaction to read from the system database
   * @return                 A tuple containing (passwordChangeRequired, tags, suspended, home).
   *                         A `None` for the optional values means it should return null.
   */
  def getUserColumnValues(
    username: String,
    requestedColumns: List[String],
    ctx: QueryContext
  ): (Option[Boolean], List[String], Boolean, Option[String]) = {

    Using.Manager { use =>
      // We open a new system transaction to be able to read from the system database as we don't have the current transaction,
      // and it would be in StaticAccessMode.ACCESS anyway and need privilege elevation for the duration of this method.
      val tx = use(ctx.systemGraph.beginTx())
      val users = use(tx.findNodes(USER_LABEL))

      val maybeCurrentUser = users.asScala.find(userNode => {
        try {
          val user = userNode.getProperty(USER_NAME_PROPERTY).asInstanceOf[String]
          user.equals(username)
        } catch {
          case _: NotFoundException =>
            // user was deleted concurrently with reading properties from it,
            // leave out of result and continue
            false
        }
      })

      maybeCurrentUser.map(userNode => {
        val changeReq = if (requestedColumns.contains(pwdChangeReqColumn)) {
          val maybeChangeReq = userNode.getProperty(USER_CREDENTIALS_EXPIRED_PROPERTY, null)
          if (maybeChangeReq != null) Some(maybeChangeReq.asInstanceOf[Boolean]) else None
        } else None

        val suspended =
          if (requestedColumns.contains(suspendedColumn)) {
            userNode.getProperty(USER_SUSPENDED_PROPERTY, false).asInstanceOf[Boolean]
          } else false

        val home = if (requestedColumns.contains(homeColumn)) {
          val maybeHome = userNode.getProperty(USER_HOME_DB_PROPERTY, null)
          if (maybeHome != null) Some(maybeHome.asInstanceOf[String]) else None
        } else None

        val tags = if (requestedColumns.contains(tagsColumn)) {
          val maybeTags = userNode.getProperty(USER_TAGS_PROPERTY, null)
          maybeTags match {
            case tags: Array[String] => List.from(tags)
            // Single string should not exist but the Cypher code had it
            // (converting single values into list of single object)
            case tag: String => List(tag)
            case _           => List.empty
          }
        } else List.empty

        (changeReq, tags, suspended, home)

      }).getOrElse((None, List.empty, false, None))
    }.getOrElse((None, List.empty, false, None))
  }

}
