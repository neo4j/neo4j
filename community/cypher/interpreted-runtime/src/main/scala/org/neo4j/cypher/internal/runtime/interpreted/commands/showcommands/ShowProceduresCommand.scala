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

import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util

import scala.jdk.CollectionConverters.SetHasAsScala

// SHOW PROCEDURE[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
case class ShowProceduresCommand(
  executableBy: Option[ExecutableBy],
  verbose: Boolean,
  columns: List[ShowColumn],
  isCommunity: Boolean
) extends Command(columns) {

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    lazy val systemGraph = state.query.systemGraph

    val privileges =
      if (!isCommunity && (verbose || executableBy.isDefined))
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
      if (!isCommunity && verbose)
        securityContext.allowsAdminAction(
          new AdminActionOnResource(SHOW_ROLE, DatabaseScope.ALL, Segment.ALL)
        ).allowsAccess()
      else
        false

    val allProcedures = txContext.procedures.proceduresGetAll().asScala.filter(proc => !proc.internal).toList
    val sortedProcedures = allProcedures.sortBy(a => a.name.toString)

    val rows = sortedProcedures.map { proc =>
      val (executeRoles, boostedExecuteRoles, allowedExecute) =
        if (!isCommunity && (verbose || executableBy.isDefined))
          ShowProcFuncCommandHelper.roles(proc.name.toString, proc.admin(), privileges, userRoles)
        else (Set.empty[String], Set.empty[String], isCommunity)

      executableBy match {
        case Some(_) =>
          getResultMap(proc, alwaysExecutable || allowedExecute, executeRoles, boostedExecuteRoles, allowShowRoles)
        case None => getResultMap(proc, executeRoles, boostedExecuteRoles, allowShowRoles)
      }
    }.filter(m => m.nonEmpty)

    ClosingIterator.apply(rows.iterator)
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
    val name = proc.name().toString
    val mode = proc.mode().toString
    val system = proc.systemProcedure()
    val maybeDescr = proc.description()
    val descrValue = Values.stringValue(maybeDescr.orElse(""))

    val briefResult = Map(
      // Name of the procedure, for example "my.proc"
      "name" -> Values.stringValue(name),
      // Procedure description or empty string
      "description" -> descrValue,
      // Procedure mode: READ, WRITE, SCHEMA, DBMS, DEFAULT
      "mode" -> Values.stringValue(mode),
      // Tells if the procedure can be run on system database
      "worksOnSystem" -> Values.booleanValue(system)
    )
    if (verbose) {
      val (rolesList, boostedRolesList) =
        if (allowShowRoles) ShowProcFuncCommandHelper.roleValues(executeRoles, boostedExecuteRoles)
        else (Values.NO_VALUE, Values.NO_VALUE)

      briefResult ++ Map(
        // Procedure signature
        "signature" -> Values.stringValue(proc.toString),
        // Lists of arguments, as map of strings with name, type, default and description
        "argumentDescription" -> fieldDescriptions(proc.inputSignature()),
        // Lists of returned values, as map of strings with name, type and description
        "returnDescription" -> fieldDescriptions(proc.outputSignature()),
        // Tells if it is a procedure annotated with `admin`
        "admin" -> Values.booleanValue(proc.admin()),
        // List of roles that can execute the procedure
        "rolesExecution" -> rolesList,
        // List of roles that can execute the procedure with boosted privileges
        "rolesBoostedExecution" -> boostedRolesList,
        // Tells if the procedure is deprecated
        "isDeprecated" -> Values.booleanValue(proc.deprecated().isPresent),
        // Additional output, for example if the procedure is deprecated
        "option" -> getOptionValue(proc)
      )
    } else {
      briefResult
    }
  }

  private def fieldDescriptions(fields: util.List[FieldSignature]): ListValue =
    ShowProcFuncCommandHelper.fieldDescriptions(ShowProcFuncCommandHelper.getSignatureValues(fields))

  private def getOptionValue(proc: ProcedureSignature): MapValue = {
    val keys = Array("deprecated")
    val values: Array[AnyValue] = Array(Values.booleanValue(proc.deprecated().isPresent))

    VirtualValues.map(keys, values)
  }
}
