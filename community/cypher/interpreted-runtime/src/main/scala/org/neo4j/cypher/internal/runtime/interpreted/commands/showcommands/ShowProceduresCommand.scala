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

import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.adminColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.argumentDescriptionColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.deprecatedByColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.descriptionColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.isDeprecatedColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.modeColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.nameColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.optionColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.returnDescriptionColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.rolesBoostedExecutionColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.rolesExecutionColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.signatureColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.worksOnSystemColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.api.CypherScope
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util

import scala.jdk.CollectionConverters.IteratorHasAsScala

// SHOW PROCEDURE[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
case class ShowProceduresCommand(
  executableBy: Option[ExecutableBy],
  columns: List[ShowColumn],
  yieldColumns: List[CommandResultItem],
  isCommunity: Boolean
) extends Command(columns, yieldColumns) {

  private val rolesColumnRequested =
    requestedColumnsNames.contains(rolesExecutionColumn) || requestedColumnsNames.contains(rolesBoostedExecutionColumn)

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    lazy val systemGraph = state.query.systemGraph

    val privileges =
      if (!isCommunity && (rolesColumnRequested || executableBy.isDefined))
        ShowProcFuncCommandHelper.getPrivileges(systemGraph, "PROCEDURE")
      else ShowProcFuncCommandHelper.Privileges(List.empty, List.empty, List.empty, List.empty)

    val txContext = state.query.transactionalContext
    val securityContext = txContext.securityContext
    val (userRoles, alwaysExecutable) =
      if (!isCommunity) {
        ShowProcFuncCommandHelper.getRolesForExecutableByUser(
          securityContext,
          txContext.securityAuthorizationHandler,
          systemGraph,
          executableBy,
          "SHOW PROCEDURES"
        )
      } else {
        (Set.empty[String], true)
      }
    val allowShowRoles: Boolean =
      if (!isCommunity && rolesColumnRequested)
        securityContext.allowsAdminAction(
          new AdminActionOnResource(SHOW_ROLE, DatabaseScope.ALL, Segment.ALL)
        ).allowsAccess()
      else
        false

    val allProcedures = txContext.procedures.proceduresGetAll(CypherScope.CYPHER_5).iterator.asScala
    val sortedProcedures = allProcedures.filter(proc => !proc.internal).toList.sortBy(a => a.name.toString)

    val rows = sortedProcedures.map { proc =>
      val (executeRoles, boostedExecuteRoles, allowedExecute) =
        if (!isCommunity && (rolesColumnRequested || executableBy.isDefined))
          ShowProcFuncCommandHelper.roles(proc.name.toString, proc.admin(), privileges, userRoles)
        else (Set.empty[String], Set.empty[String], isCommunity)

      executableBy match {
        case Some(_) =>
          getResultMap(proc, alwaysExecutable || allowedExecute, executeRoles, boostedExecuteRoles, allowShowRoles)
        case None => getResultMap(proc, executeRoles, boostedExecuteRoles, allowShowRoles)
      }
    }.filter(m => m.nonEmpty)

    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows)
    ClosingIterator.apply(updatedRows.iterator)
  }

  private def getResultMap(
    proc: ProcedureSignature,
    allowedExecute: Boolean,
    executeRoles: Set[String],
    boostedExecuteRoles: Set[String],
    allowShowRoles: Boolean
  ): Map[String, AnyValue] =
    if (allowedExecute) getResultMap(proc, executeRoles, boostedExecuteRoles, allowShowRoles)
    else Map.empty[String, AnyValue]

  private def getResultMap(
    proc: ProcedureSignature,
    executeRoles: Set[String],
    boostedExecuteRoles: Set[String],
    allowShowRoles: Boolean
  ): Map[String, AnyValue] = {
    val (rolesList, boostedRolesList) =
      if (rolesColumnRequested && allowShowRoles)
        ShowProcFuncCommandHelper.roleValues(executeRoles, boostedExecuteRoles)
      else (Values.NO_VALUE, Values.NO_VALUE)

    requestedColumnsNames.map {
      // Name of the procedure, for example "my.proc"
      case `nameColumn` => nameColumn -> Values.stringValue(proc.name().toString)
      // Procedure description or empty string
      case `descriptionColumn` => descriptionColumn -> Values.stringValue(proc.description().orElse(""))
      // Procedure mode: READ, WRITE, SCHEMA, DBMS, DEFAULT
      case `modeColumn` => modeColumn -> Values.stringValue(proc.mode().toString)
      // Tells if the procedure can be run on system database
      case `worksOnSystemColumn` => worksOnSystemColumn -> Values.booleanValue(proc.systemProcedure())
      // Procedure signature
      case `signatureColumn` => signatureColumn -> Values.stringValue(proc.toString)
      // Lists of arguments, as map of strings with name, type, default and description
      case `argumentDescriptionColumn` => argumentDescriptionColumn -> fieldDescriptions(proc.inputSignature())
      // Lists of returned values, as map of strings with name, type and description
      case `returnDescriptionColumn` => returnDescriptionColumn -> fieldDescriptions(proc.outputSignature())
      // Tells if it is a procedure annotated with `admin`
      case `adminColumn` => adminColumn -> Values.booleanValue(proc.admin())
      // List of roles that can execute the procedure
      case `rolesExecutionColumn` => rolesExecutionColumn -> rolesList
      // List of roles that can execute the procedure with boosted privileges
      case `rolesBoostedExecutionColumn` => rolesBoostedExecutionColumn -> boostedRolesList
      // Tells if the procedure is deprecated
      case `isDeprecatedColumn` => isDeprecatedColumn -> Values.booleanValue(proc.isDeprecated)
      case `deprecatedByColumn` => deprecatedByColumn -> Values.stringOrNoValue(proc.deprecated().orElse(null))
      // Additional output, for example if the procedure is deprecated
      case `optionColumn` => optionColumn -> getOptionValue(proc)
      case unknown        =>
        // This match should cover all existing columns but we get scala warnings
        // on non-exhaustive match due to it being string values
        throw new IllegalStateException(s"Missing case for column: $unknown")
    }.toMap[String, AnyValue]
  }

  private def fieldDescriptions(fields: util.List[FieldSignature]): ListValue =
    ShowProcFuncCommandHelper.fieldDescriptions(ShowProcFuncCommandHelper.getSignatureValues(fields))

  private def getOptionValue(proc: ProcedureSignature): MapValue = {
    val keys = Array("deprecated")
    val values: Array[AnyValue] = Array(Values.booleanValue(proc.isDeprecated))

    VirtualValues.map(keys, values)
  }
}
