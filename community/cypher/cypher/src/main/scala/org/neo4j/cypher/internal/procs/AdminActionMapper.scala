/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.internal.kernel.api.security.{PrivilegeAction => KernelPrivilegeAction}

object AdminActionMapper {
  def asKernelAction(action: AdminAction): KernelPrivilegeAction = action match {
    case AccessDatabaseAction => KernelPrivilegeAction.ACCESS

    case StartDatabaseAction => KernelPrivilegeAction.START_DATABASE
    case StopDatabaseAction => KernelPrivilegeAction.STOP_DATABASE
    case CreateDatabaseAction => KernelPrivilegeAction.CREATE_DATABASE
    case DropDatabaseAction => KernelPrivilegeAction.DROP_DATABASE

    case IndexManagementAction => KernelPrivilegeAction.INDEX
    case CreateIndexAction => KernelPrivilegeAction.CREATE_INDEX
    case DropIndexAction => KernelPrivilegeAction.DROP_INDEX
    case ConstraintManagementAction => KernelPrivilegeAction.CONSTRAINT
    case SchemaManagementAction => KernelPrivilegeAction.SCHEMA
    case CreateConstraintAction => KernelPrivilegeAction.CREATE_CONSTRAINT
    case DropConstraintAction => KernelPrivilegeAction.DROP_CONSTRAINT

    case TokenManagementAction => KernelPrivilegeAction.TOKEN
    case CreateNodeLabelAction => KernelPrivilegeAction.CREATE_LABEL
    case CreateRelationshipTypeAction => KernelPrivilegeAction.CREATE_RELTYPE
    case CreatePropertyKeyAction => KernelPrivilegeAction.CREATE_PROPERTYKEY

    case ShowUserAction => KernelPrivilegeAction.SHOW_USER
    case CreateUserAction => KernelPrivilegeAction.CREATE_USER
    case AlterUserAction => KernelPrivilegeAction.ALTER_USER
    case DropUserAction => KernelPrivilegeAction.DROP_USER

    case AllRoleActions => KernelPrivilegeAction.ROLE_MANAGEMENT
    case ShowRoleAction => KernelPrivilegeAction.SHOW_ROLE
    case CreateRoleAction => KernelPrivilegeAction.CREATE_ROLE
    case DropRoleAction => KernelPrivilegeAction.DROP_ROLE
    case AssignRoleAction => KernelPrivilegeAction.ASSIGN_ROLE
    case RemoveRoleAction => KernelPrivilegeAction.REMOVE_ROLE

    case ShowPrivilegeAction => KernelPrivilegeAction.SHOW_PRIVILEGE
    case GrantPrivilegeAction => KernelPrivilegeAction.GRANT_PRIVILEGE
    case RevokePrivilegeAction => KernelPrivilegeAction.REVOKE_PRIVILEGE
    case DenyPrivilegeAction => KernelPrivilegeAction.DENY_PRIVILEGE

    case AllDatabaseAction => KernelPrivilegeAction.DATABASE_ACTIONS

    case AllAdminAction => KernelPrivilegeAction.ADMIN
  }

  def asCypherAdminAction(action: KernelPrivilegeAction): AdminAction = action match {
    case KernelPrivilegeAction.START_DATABASE => StartDatabaseAction
    case KernelPrivilegeAction.STOP_DATABASE => StopDatabaseAction
    case KernelPrivilegeAction.CREATE_DATABASE => CreateDatabaseAction
    case KernelPrivilegeAction.DROP_DATABASE => DropDatabaseAction

    case KernelPrivilegeAction.INDEX => IndexManagementAction
    case KernelPrivilegeAction.CREATE_INDEX => CreateIndexAction
    case KernelPrivilegeAction.DROP_INDEX => DropIndexAction
    case KernelPrivilegeAction.CONSTRAINT => ConstraintManagementAction
    case KernelPrivilegeAction.SCHEMA => SchemaManagementAction
    case KernelPrivilegeAction.CREATE_CONSTRAINT => CreateConstraintAction
    case KernelPrivilegeAction.DROP_CONSTRAINT => DropConstraintAction

    case KernelPrivilegeAction.TOKEN => TokenManagementAction
    case KernelPrivilegeAction.CREATE_LABEL => CreateNodeLabelAction
    case KernelPrivilegeAction.CREATE_RELTYPE => CreateRelationshipTypeAction
    case KernelPrivilegeAction.CREATE_PROPERTYKEY => CreatePropertyKeyAction

    case KernelPrivilegeAction.SHOW_USER => ShowUserAction
    case KernelPrivilegeAction.CREATE_USER => CreateUserAction
    case KernelPrivilegeAction.ALTER_USER => AlterUserAction
    case KernelPrivilegeAction.DROP_USER => DropUserAction

    case KernelPrivilegeAction.ROLE_MANAGEMENT => AllRoleActions
    case KernelPrivilegeAction.SHOW_ROLE => ShowRoleAction
    case KernelPrivilegeAction.CREATE_ROLE => CreateRoleAction
    case KernelPrivilegeAction.DROP_ROLE => DropRoleAction
    case KernelPrivilegeAction.ASSIGN_ROLE => AssignRoleAction
    case KernelPrivilegeAction.REMOVE_ROLE => RemoveRoleAction

    case KernelPrivilegeAction.SHOW_PRIVILEGE => ShowPrivilegeAction
    case KernelPrivilegeAction.GRANT_PRIVILEGE => GrantPrivilegeAction
    case KernelPrivilegeAction.REVOKE_PRIVILEGE => RevokePrivilegeAction
    case KernelPrivilegeAction.DENY_PRIVILEGE => DenyPrivilegeAction

    case KernelPrivilegeAction.DATABASE_ACTIONS => AllDatabaseAction

    case KernelPrivilegeAction.ADMIN => AllAdminAction
  }
}
