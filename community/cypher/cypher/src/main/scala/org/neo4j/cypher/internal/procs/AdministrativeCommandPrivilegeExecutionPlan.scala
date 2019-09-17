/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.{ExecutionPlan, RuntimeName, SystemCommandRuntimeName}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.internal.kernel.api.security.AdminActionOnResource.DatabaseScope
import org.neo4j.internal.kernel.api.security.{AdminActionOnResource, SecurityContext, AdminAction => KernelAdminAction}
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

case class AdministrativeCommandPrivilegeExecutionPlan(securityContext: SecurityContext, action: AdminAction, database: DatabaseScope) extends ExecutionPlan {
  override def run(ctx: QueryContext,
                   doProfile: Boolean,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   ignore: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult = {
    if (securityContext.allowsAdminAction(new AdminActionOnResource(AdminActionMapper.asKernelAction(action), database))) {
      NoRuntimeResult(subscriber)
    } else {
      throw new AuthorizationViolationException(PERMISSION_DENIED)
    }
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

object AdminActionMapper {
  def asKernelAction(action: AdminAction): KernelAdminAction = action match {
    case StartDatabaseAction => KernelAdminAction.START_DATABASE
    case StopDatabaseAction => KernelAdminAction.STOP_DATABASE
    case CreateDatabaseAction => KernelAdminAction.CREATE_DATABASE
    case DropDatabaseAction => KernelAdminAction.DROP_DATABASE

    case ShowUserAction => KernelAdminAction.SHOW_USER
    case CreateUserAction => KernelAdminAction.CREATE_USER
    case AlterUserAction => KernelAdminAction.ALTER_USER
    case DropUserAction => KernelAdminAction.DROP_USER

    case ShowRoleAction => KernelAdminAction.SHOW_ROLE
    case CreateRoleAction => KernelAdminAction.CREATE_ROLE
    case DropRoleAction => KernelAdminAction.DROP_ROLE
    case GrantRoleAction => KernelAdminAction.GRANT_ROLE
    case RevokeRoleAction => KernelAdminAction.REVOKE_ROLE

    case ShowPrivilegeAction => KernelAdminAction.SHOW_PRIVILEGE
    case GrantPrivilegeAction => KernelAdminAction.GRANT_PRIVILEGE
    case RevokePrivilegeAction => KernelAdminAction.REVOKE_PRIVILEGE
    case DenyPrivilegeAction => KernelAdminAction.DENY_PRIVILEGE
  }

  def asCypherAdminAction(action: KernelAdminAction): AdminAction = action match {
    case KernelAdminAction.START_DATABASE => StartDatabaseAction
    case KernelAdminAction.STOP_DATABASE => StopDatabaseAction
    case KernelAdminAction.CREATE_DATABASE => CreateDatabaseAction
    case KernelAdminAction.DROP_DATABASE => DropDatabaseAction

    case KernelAdminAction.SHOW_USER => ShowUserAction
    case KernelAdminAction.CREATE_USER => CreateUserAction
    case KernelAdminAction.ALTER_USER => AlterUserAction
    case KernelAdminAction.DROP_USER => DropUserAction

    case KernelAdminAction.SHOW_ROLE => ShowRoleAction
    case KernelAdminAction.CREATE_ROLE => CreateRoleAction
    case KernelAdminAction.DROP_ROLE => DropRoleAction
    case KernelAdminAction.GRANT_ROLE => GrantRoleAction
    case KernelAdminAction.REVOKE_ROLE => RevokeRoleAction

    case KernelAdminAction.SHOW_PRIVILEGE => ShowPrivilegeAction
    case KernelAdminAction.GRANT_PRIVILEGE => GrantPrivilegeAction
    case KernelAdminAction.REVOKE_PRIVILEGE => RevokePrivilegeAction
    case KernelAdminAction.DENY_PRIVILEGE => DenyPrivilegeAction
  }
}
