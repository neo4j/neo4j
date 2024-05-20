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

import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.ExecutableBy
import org.neo4j.cypher.internal.ast.User
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_USER
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.kernel.impl.query.FunctionInformation.InputInformation
import org.neo4j.string.Globbing
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues

import java.util

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsScala

object ShowProcFuncCommandHelper {

  def getRolesForExecutableByUser(
    securityContext: SecurityContext,
    securityHandler: SecurityAuthorizationHandler,
    systemGraph: => GraphDatabaseService,
    executableBy: Option[ExecutableBy],
    command: String
  ): (Set[String], Boolean) = executableBy match {
    case Some(CurrentUser) if securityContext.subject().equals(AuthSubject.AUTH_DISABLED) => (Set.empty[String], true)
    case Some(User(name)) if !securityContext.subject().hasUsername(name)                 =>
      // EXECUTABLE BY not_current_user
      val allowedShowUser =
        securityContext.allowsAdminAction(new AdminActionOnResource(SHOW_USER, DatabaseScope.ALL, Segment.ALL))
      if (!allowedShowUser.allowsAccess()) {
        val violationMessage: String =
          if (allowedShowUser == PermissionState.EXPLICIT_DENY) {
            s"Permission denied for $command, requires SHOW USER privilege. " +
              "Try executing SHOW USER PRIVILEGES to determine the denied privileges. " +
              "In case of denied privileges, they need to be revoked (See REVOKE) and granted."
          } else {
            s"Permission not granted for $command, requires SHOW USER privilege. " +
              "Try executing SHOW USER PRIVILEGES to determine the missing privileges. " +
              "In case of missing privileges, they need to be granted (See GRANT)."
          }
        throw securityHandler.logAndGetAuthorizationException(securityContext, violationMessage)
      }
      val stx = systemGraph.beginTx()
      val rolesResult = stx.execute(
        "SHOW USERS YIELD user, roles WHERE user = $name RETURN roles",
        Map[String, Object]("name" -> name).asJava
      ).columnAs[util.List[String]]("roles")
      val rolesSet =
        if (rolesResult.hasNext) {
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

  def getSignatureValues(fields: util.List[FieldSignature]): List[InputInformation] = fields.asScala.toList.map(f => {
    val defaultValue = f.defaultValue()
    val default =
      if (defaultValue.isPresent) java.util.Optional.of[String](defaultValue.get.toString)
      else java.util.Optional.empty[String]()

    new InputInformation(
      f.name,
      f.neo4jType.toString,
      if (f.getDescription == null || f.getDescription.isEmpty) f.toString else f.getDescription,
      f.isDeprecated,
      default
    )
  })

  def fieldDescriptions(fields: List[InputInformation]): ListValue = {
    val fieldMaps: List[AnyValue] = fields.map(f => {
      val keys = Array("name", "type", "description", "isDeprecated")
      val values: Array[AnyValue] = Array(
        Values.stringValue(f.name),
        Values.stringValue(f.`type`),
        Values.stringValue(f.description),
        Values.booleanValue(f.isDeprecated)
      )
      val default = f.defaultValue()

      if (default.isPresent) {
        VirtualValues.map(
          keys :+ "default",
          values :+ Values.stringValue(default.get)
        )
      } else {
        VirtualValues.map(keys, values)
      }
    })
    VirtualValues.fromList(fieldMaps.asJava)
  }

  def getPrivileges(systemGraph: GraphDatabaseService, executeSegment: String): Privileges = {
    val stx = systemGraph.beginTx()
    val execQuery =
      "SHOW ALL PRIVILEGES YIELD * WHERE action='execute' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles"
    val executePrivilegesRes = stx.execute(execQuery, Map[String, Object]("seg" -> executeSegment).asJava)
    val executePrivileges = executePrivilegesRes.asScala.map(_.asScala.toMap).toList

    // Covers `EXECUTE BOOSTED` and config privileges
    val boostQuery =
      "SHOW ALL PRIVILEGES YIELD * WHERE action STARTS WITH 'execute_boosted' AND segment STARTS WITH $seg RETURN access, segment, collect(role) as roles"
    val boostedExecutePrivilegesRes = stx.execute(boostQuery, Map[String, Object]("seg" -> executeSegment).asJava)
    val boostedExecutePrivileges = boostedExecutePrivilegesRes.asScala.map(_.asScala.toMap).toList

    // `EXECUTE ADMIN` is `EXECUTE` and `EXECUTE BOOSTED` for all @Admin procedures, don't need the segment
    val adminPrivileges =
      if (executeSegment.equals("PROCEDURE")) {
        val adminQuery =
          "SHOW ALL PRIVILEGES YIELD * WHERE action='execute_admin' RETURN access, collect(role) as roles"
        stx.execute(adminQuery).asScala.map(_.asScala.toMap).toList
      } else List.empty

    // `ALL ON DBMS` and `admin` applies to all functions/procedures, don't need the segment
    val allDbmsQuery =
      "SHOW ALL PRIVILEGES YIELD * WHERE action IN ['admin', 'dbms_actions'] RETURN access, collect(role) as roles"
    val allDbmsPrivileges = stx.execute(allDbmsQuery).asScala.map(_.asScala.toMap).toList
    stx.commit()

    Privileges(executePrivileges, boostedExecutePrivileges, allDbmsPrivileges, adminPrivileges)
  }

  def roles(
    name: String,
    isAdmin: Boolean,
    privileges: Privileges,
    userRoles: Set[String]
  ): (Set[String], Set[String], Boolean) = {
    /* Allows executing function/non-admin procedure if:
     * - no DENY EXECUTE
     * - GRANT EXECUTE
     *
     * Allows executing admin procedure if:
     * - no DENY
     * - allowed execute admin or execute and execute as boosted
     *
     * Allows executing boosted function/procedure if:
     * - no DENY BOOSTED
     * - GRANT BOOSTED
     *
     * Allows execute admin if:
     * - no DENY EXECUTE ADMIN
     * - GRANT EXECUTE ADMIN
     */
    if (isAdmin) {
      val explicitlyGrantedBoosted = privileges.grantedBoostedExecuteRoles(name)
      val explicitlyGrantedExecute = privileges.grantedExecuteRoles(name)

      val grantedExecuteAndBoosted =
        privileges.grantedAdminExecuteRoles ++ explicitlyGrantedBoosted.intersect(explicitlyGrantedExecute)
      val grantedBoosted = privileges.grantedAdminExecuteRoles ++ explicitlyGrantedBoosted
      val deniedBoosted = privileges.deniedAdminExecuteRoles ++ privileges.deniedBoostedExecuteRoles(name)
      val deniedExecute = privileges.deniedExecuteRoles(name) ++ deniedBoosted

      val allowedBoostedRoles = grantedBoosted -- deniedBoosted
      val allowedExecuteRoles = grantedExecuteAndBoosted -- deniedExecute

      /* Test if the user is allowed executing admin procedure (from mix of roles):
       * - no DENY
       * - GRANT EXECUTE and GRANT EXECUTE BOOSTED
       */
      val allowedExecute =
        // no DENY
        userRoles.forall(r => !deniedExecute.contains(r)) && {
          // Single role allow executing
          userRoles.exists(r => allowedExecuteRoles.contains(r)) || {
            // Combination of roles allow executing
            userRoles.exists(explicitlyGrantedExecute.contains) && userRoles.exists(grantedBoosted.contains)
          }
        }

      (allowedExecuteRoles, allowedBoostedRoles, allowedExecute)
    } else {
      val grantedExecute = privileges.grantedExecuteRoles(name)
      val grantedBoosted = privileges.grantedBoostedExecuteRoles(name)
      val deniedExecute = privileges.deniedExecuteRoles(name)
      val deniedBoosted = privileges.deniedBoostedExecuteRoles(name)

      val allowedBoostedRoles = grantedBoosted -- deniedBoosted
      val allowedExecuteRoles = grantedExecute -- deniedExecute

      /* Test if the user is allowed executing (from mix of roles):
       * - no DENY EXECUTE
       * - GRANT EXECUTE
       */
      val allowedExecute =
        userRoles.exists(r => grantedExecute.contains(r)) && userRoles.forall(r => !deniedExecute.contains(r))

      (allowedExecuteRoles, allowedBoostedRoles, allowedExecute)
    }
  }

  def roleValues(allowedExecuteRoles: Set[String], allowedBoostedRoles: Set[String]): (ListValue, ListValue) = {
    val allowedExecute: List[AnyValue] = allowedExecuteRoles.toList.sorted.map(Values.stringValue)
    val allowedBoosted: List[AnyValue] = allowedBoostedRoles.toList.sorted.map(Values.stringValue)
    (VirtualValues.fromList(allowedExecute.asJava), VirtualValues.fromList(allowedBoosted.asJava))
  }

  case class Privileges(
    executePrivileges: List[Map[String, AnyRef]],
    boostedExecutePrivileges: List[Map[String, AnyRef]],
    allDbmsPrivileges: List[Map[String, AnyRef]],
    adminExecutePrivileges: List[Map[String, AnyRef]]
  ) {
    private def granted(m: Map[String, AnyRef]) = m("access").equals("GRANTED")
    private def denied(m: Map[String, AnyRef]) = m("access").equals("DENIED")
    private val grantedExecutePrivileges: List[Map[String, AnyRef]] = executePrivileges.filter(granted)
    private val deniedExecutePrivileges: List[Map[String, AnyRef]] = executePrivileges.filter(denied)
    private val grantedBoostedExecutePrivileges: List[Map[String, AnyRef]] = boostedExecutePrivileges.filter(granted)
    private val deniedBoostedExecutePrivileges: List[Map[String, AnyRef]] = boostedExecutePrivileges.filter(denied)

    private val grantedAllOnDbmsRoles: Set[String] =
      allDbmsPrivileges.filter(granted).flatMap(m => m("roles").asInstanceOf[util.List[String]].asScala.toList).toSet

    private val deniedAllOnDbmsRoles: Set[String] =
      allDbmsPrivileges.filter(denied).flatMap(m => m("roles").asInstanceOf[util.List[String]].asScala.toList).toSet

    private def getMatching(name: String, privilege: Map[String, AnyRef]): List[String] = {
      val segment = privilege("segment").asInstanceOf[String]
      val glob = segment.substring(segment.indexOf("(") + 1, segment.indexOf(")"))
      val matchPredicate = Globbing.globPredicate(glob)
      val matching: Boolean = matchPredicate.test(name)

      if (matching) privilege("roles").asInstanceOf[util.List[String]].asScala.toList else List.empty
    }

    // Get the relevant roles for the function/procedure
    // All on dbms privilege includes both execute and execute boosted so it gets added to all
    // Execute admin is only relevant for procedures with @Admin and are therefore handled separately
    def grantedExecuteRoles(name: String): Set[String] =
      grantedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet ++ grantedAllOnDbmsRoles

    def deniedExecuteRoles(name: String): Set[String] =
      deniedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet ++ deniedAllOnDbmsRoles

    def grantedBoostedExecuteRoles(name: String): Set[String] =
      grantedBoostedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet ++ grantedAllOnDbmsRoles

    def deniedBoostedExecuteRoles(name: String): Set[String] =
      deniedBoostedExecutePrivileges.flatMap(m => getMatching(name, m)).toSet ++ deniedAllOnDbmsRoles

    val grantedAdminExecuteRoles: Set[String] = adminExecutePrivileges.filter(granted).flatMap(m =>
      m("roles").asInstanceOf[util.List[String]].asScala.toList
    ).toSet

    val deniedAdminExecuteRoles: Set[String] = adminExecutePrivileges.filter(denied).flatMap(m =>
      m("roles").asInstanceOf[util.List[String]].asScala.toList
    ).toSet
  }
}
