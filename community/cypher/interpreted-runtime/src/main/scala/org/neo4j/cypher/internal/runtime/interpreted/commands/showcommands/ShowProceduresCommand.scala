/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause.CurrentUser
import org.neo4j.cypher.internal.ast.ShowProceduresClause.ExecutableBy
import org.neo4j.cypher.internal.ast.ShowProceduresClause.User
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_ROLE
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_USER
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.string.Globbing
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

// SHOW PROCEDURE[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
case class ShowProceduresCommand(executableBy: Option[ExecutableBy], verbose: Boolean, columns: Set[ShowColumn]) extends Command(columns) {
  override def originalNameRows(state: QueryState): ClosingIterator[Map[String, AnyValue]] = {
    val (privileges, systemGraph) =
      if (verbose || executableBy.isDefined) getProcedurePrivileges(state) // Always give Some(_: GraphDatabaseService)
      else (Privileges(List.empty, List.empty, List.empty, List.empty), None)

    val tx = state.query.transactionalContext.transaction
    val securityContext = tx.securityContext()
    val (userRoles, alwaysExecutable) = getRolesForUser(securityContext, systemGraph)
    val allowShowRoles = if (verbose) securityContext.allowsAdminAction(new AdminActionOnResource(SHOW_ROLE, DatabaseScope.ALL, Segment.ALL)) else false

    val allProcedures = tx.procedures().proceduresGetAll().asScala.filter(proc => !proc.internal).toList
    val sortedProcedures = allProcedures.sortBy(a => a.name.toString)

    val rows = sortedProcedures.map { proc =>
      val (executeRoles, boostedExecuteRoles, allowedExecute) =
        if (verbose || executableBy.isDefined) roles(proc.name.toString, proc.admin(), privileges, userRoles)
        else (Set.empty[String], Set.empty[String], false)

      executableBy match {
        case Some(_) => getResultMap(proc, alwaysExecutable || allowedExecute, executeRoles, boostedExecuteRoles, allowShowRoles)
        case None => getResultMap(proc, executeRoles, boostedExecuteRoles, allowShowRoles)
      }
    }.filter(m => m.nonEmpty)

    ClosingIterator.apply(rows.iterator)
  }

  private def getRolesForUser(securityContext: SecurityContext, systemGraph: Option[GraphDatabaseService]): (Set[String], Boolean) = executableBy match {
    case Some(CurrentUser) if securityContext.subject().equals(AuthSubject.AUTH_DISABLED) => (Set.empty[String], true)
    case Some(User(name)) if !securityContext.subject().hasUsername(name) =>
      // EXECUTABLE BY not_current_user
      val allowedShowUser = securityContext.allowsAdminAction(new AdminActionOnResource(SHOW_USER, DatabaseScope.ALL, Segment.ALL))
      if (!allowedShowUser) {
        val violationMessage: String = "Permission denied for SHOW PROCEDURES, requires SHOW USER privilege. " +
          "Try executing SHOW USER PRIVILEGES to determine the missing or denied privileges. " +
          "In case of missing privileges, they need to be granted (See GRANT). In case of denied privileges, they need to be revoked (See REVOKE) and granted."
        throw new AuthorizationViolationException(violationMessage)
      }
      val stx = systemGraph.get.beginTx() // Will be Some(_: GraphDatabaseService) since executableBy.isDefined
      val rolesResult = stx.execute(s"SHOW USERS YIELD user, roles WHERE user = '$name' RETURN roles").columnAs[util.List[String]]("roles")
      val rolesSet = if (rolesResult.hasNext) {
        // usernames are unique so we only get one result back
        rolesResult.next().asScala.toSet
      } else {
        // non-existing users have no roles
        Set.empty[String]
      }
      stx.commit()
      (rolesSet, false)

    case Some(_) => (securityContext.roles().asScala.toSet, false)
    case None    => (Set.empty[String], false)
  }

  private def getResultMap(proc: ProcedureSignature,
                           allowedExecute: Boolean,
                           executeRoles: Set[String],
                           boostedExecuteRoles: Set[String],
                           allowShowRoles: Boolean): Map[String, AnyValue] =
    if (allowedExecute) getResultMap(proc, executeRoles, boostedExecuteRoles, allowShowRoles)
    else Map.empty[String, AnyValue]

  private def getResultMap(proc: ProcedureSignature,
                           executeRoles: Set[String],
                           boostedExecuteRoles: Set[String],
                           allowShowRoles: Boolean): Map[String, AnyValue] = {
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
      "worksOnSystem" -> Values.booleanValue(system),
    )
    if (verbose) {
      val (rolesList, boostedRolesList) = if (allowShowRoles) roleValues(executeRoles, boostedExecuteRoles) else (Values.NO_VALUE, Values.NO_VALUE)

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
        // Additional output, for example if the procedure is deprecated
        "option" -> getOptionValue(proc)
      )
    } else {
      briefResult
    }
  }

  private def fieldDescriptions(fields: util.List[FieldSignature]): ListValue = {
    val fieldMaps: List[AnyValue] = fields.asScala.toList.map(f => {
      val keys = Array("name", "type", "description")
      val values: Array[AnyValue] = Array(Values.stringValue(f.name()), Values.stringValue(f.neo4jType().toString), Values.stringValue(f.toString))
      val default: util.Optional[String] = f.defaultValue().map(d => d.toString)

      if (default.isPresent) {
        VirtualValues.map(
          keys :+ "default",
          values :+ Values.stringValue(default.get())
        )
      } else {
        VirtualValues.map(keys, values)
      }
    })
    VirtualValues.fromList(fieldMaps.asJava)
  }

  private def getOptionValue(proc: ProcedureSignature): MapValue = {
    val keys = Array("deprecated")
    val values: Array[AnyValue] = Array(Values.booleanValue(proc.deprecated().isPresent))

    VirtualValues.map(keys, values)
  }

  private def getProcedurePrivileges(state: QueryState): (Privileges, Option[GraphDatabaseService]) = {
    val sg = state.query.graph().getDependencyResolver.resolveDependency(classOf[DatabaseManagementService]).database("system")
    val stx = sg.beginTx()
    val execQuery = "SHOW ALL PRIVILEGES YIELD * WHERE action='execute' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles"
    val executePrivilegesRes = stx.execute(execQuery, Map[String, Object]("seg" -> "PROCEDURE").asJava)
    val executePrivileges = executePrivilegesRes.asScala.foldLeft(List[Map[String, AnyRef]]())((acc, map) => acc :+ map.asScala.toMap)

    // Covers `EXECUTE BOOSTED` and config privileges
    val boostQuery = "SHOW ALL PRIVILEGES YIELD * WHERE action STARTS WITH 'execute_boosted' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles"
    val boostedExecutePrivilegesRes = stx.execute(boostQuery, Map[String, Object]("seg" -> "PROCEDURE").asJava)
    val boostedExecutePrivileges = boostedExecutePrivilegesRes.asScala.foldLeft(List[Map[String, AnyRef]]())((acc, map) => acc :+ map.asScala.toMap)

    // `EXECUTE ADMIN` is `EXECUTE BOOSTED` for all @Admin procedures, don't need the segment
    val adminQuery = "SHOW ALL PRIVILEGES YIELD * WHERE action='execute_admin' RETURN access, collect(role) as roles"
    val adminPrivileges = stx.execute(adminQuery).asScala.foldLeft(List[Map[String, AnyRef]]())((acc, map) => acc :+ map.asScala.toMap)

    // `ALL ON DBMS` and `admin` applies to all procedures, don't need the segment
    val allDbmsQuery = "SHOW ALL PRIVILEGES YIELD * WHERE action IN ['admin', 'dbms_actions'] RETURN access, collect(role) as roles"
    val allDbmsPrivileges = stx.execute(allDbmsQuery).asScala.foldLeft(List[Map[String, AnyRef]]())((acc, map) => acc :+ map.asScala.toMap)
    stx.commit()

    (Privileges(executePrivileges, boostedExecutePrivileges, allDbmsPrivileges, adminPrivileges), Some(sg))
  }

  private def roles(procName: String,
                    isAdmin: Boolean,
                    privileges: Privileges,
                    userRoles: Set[String]): (Set[String], Set[String], Boolean) = {
    /* Allows executing non-admin procedure if:
     * - no DENY EXECUTE
     * - GRANT EXECUTE or allowed execute as boosted
     *
     * Allows executing admin procedure if:
     * - no DENY
     * - allowed execute as boosted or execute admin
     *
     * Allows executing boosted procedure if:
     * - no DENY BOOSTED
     * - GRANT BOOSTED
     *
     * All on dbms privilege includes both execute and execute boosted
     */
    val (grantedExecuteRoles, grantedBoostedRoles, deniedExecuteRoles, deniedBoostedRoles) = if (isAdmin) {
      val grantedExecute = privileges.grantedAdminExecuteRoles ++ privileges.grantedBoostedExecuteRoles(procName) ++ privileges.grantedAllOnDbmsRoles
      val deniedBoosted = privileges.deniedAdminExecuteRoles ++ privileges.deniedBoostedExecuteRoles(procName) ++ privileges.deniedAllOnDbmsRoles
      val deniedExecute = privileges.deniedExecuteRoles(procName) ++ deniedBoosted
      (grantedExecute, grantedExecute, deniedExecute, deniedBoosted)
    } else {
      val grantedExecute = privileges.grantedExecuteRoles(procName) ++ privileges.grantedAllOnDbmsRoles
      val grantedBoosted = privileges.grantedBoostedExecuteRoles(procName) ++ privileges.grantedAllOnDbmsRoles
      val deniedExecute = privileges.deniedExecuteRoles(procName) ++ privileges.deniedAllOnDbmsRoles
      val deniedBoosted = privileges.deniedBoostedExecuteRoles(procName) ++ privileges.deniedAllOnDbmsRoles
      (grantedExecute, grantedBoosted, deniedExecute, deniedBoosted)
    }

    val allowedBoostedRoles = grantedBoostedRoles -- deniedBoostedRoles
    val allowedExecuteRoles = grantedExecuteRoles ++ allowedBoostedRoles -- deniedExecuteRoles

    /* Test if the user is allowed executing (from mix of roles):
     * Explicit:
     * - no DENY EXECUTE
     * - GRANT EXECUTE
     *
     * Implicit:
     * - no DENY EXECUTE
     * - no DENY BOOSTED
     * - GRANT BOOSTED
     */
    val allowedExplicit = userRoles.exists(r => grantedExecuteRoles.contains(r)) && userRoles.forall(r => !deniedExecuteRoles.contains(r))
    val allowedImplicit = userRoles.exists(r => grantedBoostedRoles.contains(r)) && userRoles.forall(r => !deniedBoostedRoles.contains(r) && !deniedExecuteRoles.contains(r))
    val allowedExecute = allowedExplicit || allowedImplicit

    (allowedExecuteRoles, allowedBoostedRoles, allowedExecute)
  }

  private def roleValues(allowedExecuteRoles: Set[String], allowedBoostedRoles: Set[String]): (ListValue, ListValue) = {
    val allowedExecute: List[AnyValue] = allowedExecuteRoles.toList.sorted.map(Values.stringValue)
    val allowedBoosted: List[AnyValue] = allowedBoostedRoles.toList.sorted.map(Values.stringValue)
    (VirtualValues.fromList(allowedExecute.asJava), VirtualValues.fromList(allowedBoosted.asJava))
  }

  private case class Privileges(executePrivileges: List[Map[String, AnyRef]], boostedExecutePrivileges: List[Map[String, AnyRef]],
                                allDbmsPrivileges: List[Map[String, AnyRef]], adminExecutePrivileges: List[Map[String, AnyRef]]) {
    private def granted(m: Map[String, AnyRef]) = m("access").equals("GRANTED")
    private def denied(m: Map[String, AnyRef]) = m("access").equals("DENIED")
    private val grantedExecutePrivileges: List[Map[String, AnyRef]] = executePrivileges.filter(granted)
    private val deniedExecutePrivileges: List[Map[String, AnyRef]] = executePrivileges.filter(denied)
    private val grantedBoostedExecutePrivileges: List[Map[String, AnyRef]] = boostedExecutePrivileges.filter(granted)
    private val deniedBoostedExecutePrivileges: List[Map[String, AnyRef]] = boostedExecutePrivileges.filter(denied)

    private def getMatching(name: String, privilege: Map[String, AnyRef]): List[String] = {
      val segment = privilege("segment").asInstanceOf[String]
      val glob = segment.substring(segment.indexOf("(") + 1, segment.indexOf(")"))
      val matchPredicate = Globbing.globPredicate(glob)
      val matching: Boolean = matchPredicate.test(name)

      if (matching) privilege("roles").asInstanceOf[util.List[String]].asScala.toList else List.empty
    }

    // Get the relevant roles for the procedures
    def grantedExecuteRoles(name:String): Set[String] = grantedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet
    def deniedExecuteRoles(name:String): Set[String] = deniedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet
    def grantedBoostedExecuteRoles(name:String): Set[String] = grantedBoostedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet
    def deniedBoostedExecuteRoles(name:String): Set[String] = deniedBoostedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet
    val grantedAllOnDbmsRoles: Set[String] = allDbmsPrivileges.filter(granted).flatMap(m => m("roles").asInstanceOf[util.List[String]].asScala.toList).toSet
    val deniedAllOnDbmsRoles: Set[String] = allDbmsPrivileges.filter(denied).flatMap(m => m("roles").asInstanceOf[util.List[String]].asScala.toList).toSet
    val grantedAdminExecuteRoles: Set[String] = adminExecutePrivileges.filter(granted).flatMap(m => m("roles").asInstanceOf[util.List[String]].asScala.toList).toSet
    val deniedAdminExecuteRoles: Set[String] = adminExecutePrivileges.filter(denied).flatMap(m => m("roles").asInstanceOf[util.List[String]].asScala.toList).toSet
  }
}
