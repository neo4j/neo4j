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
import org.neo4j.internal.kernel.api.security.{AdminActionOnResource, SecurityContext, PrivilegeAction => KernelPrivilegeAction}
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
  def asKernelAction(action: AdminAction): KernelPrivilegeAction = action match {
    case AccessDatabaseAction => KernelPrivilegeAction.ACCESS

    case StartDatabaseAction => KernelPrivilegeAction.START_DATABASE
    case StopDatabaseAction => KernelPrivilegeAction.STOP_DATABASE
    case CreateDatabaseAction => KernelPrivilegeAction.CREATE_DATABASE
    case DropDatabaseAction => KernelPrivilegeAction.DROP_DATABASE

    case CreateIndexAction => KernelPrivilegeAction.CREATE_INDEX
    case DropIndexAction => KernelPrivilegeAction.DROP_INDEX
    case CreateConstraintAction => KernelPrivilegeAction.CREATE_CONSTRAINT
    case DropConstraintAction => KernelPrivilegeAction.DROP_CONSTRAINT

    case CreateNodeLabelAction => KernelPrivilegeAction.CREATE_LABEL
    case CreateRelationshipTypeAction => KernelPrivilegeAction.CREATE_RELTYPE
    case CreatePropertyKeyAction => KernelPrivilegeAction.CREATE_PROPERTYKEY

    case ShowUserAction => KernelPrivilegeAction.SHOW_USER
    case CreateUserAction => KernelPrivilegeAction.CREATE_USER
    case AlterUserAction => KernelPrivilegeAction.ALTER_USER
    case DropUserAction => KernelPrivilegeAction.DROP_USER

    case ShowRoleAction => KernelPrivilegeAction.SHOW_ROLE
    case CreateRoleAction => KernelPrivilegeAction.CREATE_ROLE
    case DropRoleAction => KernelPrivilegeAction.DROP_ROLE
    case GrantRoleAction => KernelPrivilegeAction.GRANT_ROLE
    case RevokeRoleAction => KernelPrivilegeAction.REVOKE_ROLE

    case ShowPrivilegeAction => KernelPrivilegeAction.SHOW_PRIVILEGE
    case GrantPrivilegeAction => KernelPrivilegeAction.GRANT_PRIVILEGE
    case RevokePrivilegeAction => KernelPrivilegeAction.REVOKE_PRIVILEGE
    case DenyPrivilegeAction => KernelPrivilegeAction.DENY_PRIVILEGE
  }

  def asCypherAdminAction(action: KernelPrivilegeAction): AdminAction = action match {
    case KernelPrivilegeAction.START_DATABASE => StartDatabaseAction
    case KernelPrivilegeAction.STOP_DATABASE => StopDatabaseAction
    case KernelPrivilegeAction.CREATE_DATABASE => CreateDatabaseAction
    case KernelPrivilegeAction.DROP_DATABASE => DropDatabaseAction

    case KernelPrivilegeAction.CREATE_INDEX => CreateIndexAction
    case KernelPrivilegeAction.DROP_INDEX => DropIndexAction
    case KernelPrivilegeAction.CREATE_CONSTRAINT => CreateConstraintAction
    case KernelPrivilegeAction.DROP_CONSTRAINT => DropConstraintAction

    case KernelPrivilegeAction.CREATE_LABEL => CreateNodeLabelAction
    case KernelPrivilegeAction.CREATE_RELTYPE => CreateRelationshipTypeAction
    case KernelPrivilegeAction.CREATE_PROPERTYKEY => CreatePropertyKeyAction

    case KernelPrivilegeAction.SHOW_USER => ShowUserAction
    case KernelPrivilegeAction.CREATE_USER => CreateUserAction
    case KernelPrivilegeAction.ALTER_USER => AlterUserAction
    case KernelPrivilegeAction.DROP_USER => DropUserAction

    case KernelPrivilegeAction.SHOW_ROLE => ShowRoleAction
    case KernelPrivilegeAction.CREATE_ROLE => CreateRoleAction
    case KernelPrivilegeAction.DROP_ROLE => DropRoleAction
    case KernelPrivilegeAction.GRANT_ROLE => GrantRoleAction
    case KernelPrivilegeAction.REVOKE_ROLE => RevokeRoleAction

    case KernelPrivilegeAction.SHOW_PRIVILEGE => ShowPrivilegeAction
    case KernelPrivilegeAction.GRANT_PRIVILEGE => GrantPrivilegeAction
    case KernelPrivilegeAction.REVOKE_PRIVILEGE => RevokePrivilegeAction
    case KernelPrivilegeAction.DENY_PRIVILEGE => DenyPrivilegeAction
  }
}
