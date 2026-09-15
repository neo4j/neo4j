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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.ParameterProvider
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans.CommandDefaultColumn
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.admin.topology.DatabaseDetailsMapper
import org.neo4j.cypher.internal.runtime.admin.topology.ShowDatabaseServiceContext
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.StringValue

case class ShowDatabaseCommand(
  dbScope: DatabaseScope,
  defaultColumns: List[CommandDefaultColumn],
  yieldColumns: List[CommandYieldColumn],
  cypherVersion: CypherVersion
) extends Command(defaultColumns, yieldColumns) {

  override protected def originalNameRows(
    state: QueryState,
    baseRow: CypherRow
  ): ClosingIterator[Map[String, AnyValue]] = {

    // Must run on system database
    mustRunOnSystemCheck(
      state.query,
      dbScope match {
        case _: SingleNamedDatabaseScope => "SHOW DATABASE"
        case _: DefaultDatabaseScope     => "SHOW DEFAULT DATABASE"
        case _: HomeDatabaseScope        => "SHOW HOME DATABASE"
        case _                           => "SHOW DATABASES"
      }
    )

    val showService = state.query.getShowDatabaseService
    val context = ShowDatabaseServiceContext(
      state.query.transactionalContext.securityContext,
      cypherVersion,
      requestedColumnsNames.toSet
    )
    val resultRows: Iterator[Map[String, AnyValue]] = (dbScope match {
      case SingleNamedDatabaseScope(database) =>
        val results = showService.getSingleNamedDatabase(
          database,
          SlotBasedParameterProvider(state),
          context,
          ignoreNullInput = cypherVersion.isEqualOrAfter(CypherVersion.Cypher25)
        )
        results
      case DefaultDatabaseScope() => showService.getDefaultDatabase(context)
      case HomeDatabaseScope()    => showService.getHomeDatabase(context)
      case _                      => showService.getAllDatabases(context)
    }).map(DatabaseDetailsMapper.toMap)
      // Maybe fixme, we only need to sort here if there is no ORDER BY in the command
      .sortBy(_.apply(ShowDatabase.NAME_COL).asInstanceOf[StringValue].stringValue())
      .iterator
    ClosingIterator.apply(resultRows)
  }
}

case class SlotBasedParameterProvider(queryState: QueryState) extends ParameterProvider {

  override val get: PartialFunction[Expression, AnyValue] = {
    case p: ParameterFromSlot => queryState.params.apply(p.offset)
  }

  override val getName: PartialFunction[Expression, String] = {
    case p: ParameterFromSlot => p.name
  }
}
