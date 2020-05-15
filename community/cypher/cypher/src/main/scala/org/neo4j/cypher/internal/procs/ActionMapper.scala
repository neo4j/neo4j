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

import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.AdminAction
import org.neo4j.cypher.internal.ast.AllAdminAction
import org.neo4j.cypher.internal.ast.AllConstraintActions
import org.neo4j.cypher.internal.ast.AllDatabaseAction
import org.neo4j.cypher.internal.ast.AllDatabaseManagementActions
import org.neo4j.cypher.internal.ast.AllDbmsAction
import org.neo4j.cypher.internal.ast.AllGraphAction
import org.neo4j.cypher.internal.ast.AllIndexActions
import org.neo4j.cypher.internal.ast.AllPrivilegeActions
import org.neo4j.cypher.internal.ast.AllRoleActions
import org.neo4j.cypher.internal.ast.AllTokenActions
import org.neo4j.cypher.internal.ast.AllTransactionActions
import org.neo4j.cypher.internal.ast.AllUserActions
import org.neo4j.cypher.internal.ast.AlterUserAction
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.DropConstraintAction
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.RemovePrivilegeAction
import org.neo4j.cypher.internal.ast.RemoveRoleAction
import org.neo4j.cypher.internal.ast.SetLabelAction
import org.neo4j.cypher.internal.ast.SetPasswordsAction
import org.neo4j.cypher.internal.ast.SetPropertyAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.internal.kernel.api.security

object ActionMapper {
  def asKernelAction(action: AdminAction): security.PrivilegeAction = action match {
    case AccessDatabaseAction => security.PrivilegeAction.ACCESS

    case AllIndexActions => security.PrivilegeAction.INDEX
    case CreateIndexAction => security.PrivilegeAction.CREATE_INDEX
    case DropIndexAction => security.PrivilegeAction.DROP_INDEX
    case AllConstraintActions => security.PrivilegeAction.CONSTRAINT
    case CreateConstraintAction => security.PrivilegeAction.CREATE_CONSTRAINT
    case DropConstraintAction => security.PrivilegeAction.DROP_CONSTRAINT

    case AllTokenActions => security.PrivilegeAction.TOKEN
    case CreateNodeLabelAction => security.PrivilegeAction.CREATE_LABEL
    case CreateRelationshipTypeAction => security.PrivilegeAction.CREATE_RELTYPE
    case CreatePropertyKeyAction => security.PrivilegeAction.CREATE_PROPERTYKEY

    case WriteAction => security.PrivilegeAction.WRITE

    case CreateElementAction => security.PrivilegeAction.CREATE_ELEMENT
    case DeleteElementAction => security.PrivilegeAction.DELETE_ELEMENT
    case SetLabelAction => security.PrivilegeAction.SET_LABEL
    case RemoveLabelAction => security.PrivilegeAction.REMOVE_LABEL
    case SetPropertyAction => security.PrivilegeAction.SET_PROPERTY
    case MergeAdminAction => security.PrivilegeAction.MERGE

    case AllGraphAction => security.PrivilegeAction.GRAPH_ACTIONS

    case AllDatabaseAction => security.PrivilegeAction.DATABASE_ACTIONS

    case TerminateTransactionAction => security.PrivilegeAction.TERMINATE_TRANSACTION
    case ShowTransactionAction => security.PrivilegeAction.SHOW_TRANSACTION
    case AllTransactionActions => security.PrivilegeAction.TRANSACTION_MANAGEMENT

    case StartDatabaseAction => security.PrivilegeAction.START_DATABASE
    case StopDatabaseAction => security.PrivilegeAction.STOP_DATABASE

    case AllUserActions => security.PrivilegeAction.USER_MANAGEMENT
    case ShowUserAction => security.PrivilegeAction.SHOW_USER
    case CreateUserAction => security.PrivilegeAction.CREATE_USER
    case SetUserStatusAction => security.PrivilegeAction.SET_USER_STATUS
    case SetPasswordsAction => security.PrivilegeAction.SET_PASSWORDS
    case AlterUserAction => security.PrivilegeAction.ALTER_USER
    case DropUserAction => security.PrivilegeAction.DROP_USER

    case AllRoleActions => security.PrivilegeAction.ROLE_MANAGEMENT
    case ShowRoleAction => security.PrivilegeAction.SHOW_ROLE
    case CreateRoleAction => security.PrivilegeAction.CREATE_ROLE
    case DropRoleAction => security.PrivilegeAction.DROP_ROLE
    case AssignRoleAction => security.PrivilegeAction.ASSIGN_ROLE
    case RemoveRoleAction => security.PrivilegeAction.REMOVE_ROLE

    case AllDatabaseManagementActions => security.PrivilegeAction.DATABASE_MANAGEMENT
    case CreateDatabaseAction => security.PrivilegeAction.CREATE_DATABASE
    case DropDatabaseAction => security.PrivilegeAction.DROP_DATABASE

    case AllPrivilegeActions => security.PrivilegeAction.PRIVILEGE_MANAGEMENT
    case ShowPrivilegeAction => security.PrivilegeAction.SHOW_PRIVILEGE
    case AssignPrivilegeAction => security.PrivilegeAction.ASSIGN_PRIVILEGE
    case RemovePrivilegeAction => security.PrivilegeAction.REMOVE_PRIVILEGE

    case AllDbmsAction => security.PrivilegeAction.DBMS_ACTIONS
    case AllAdminAction => security.PrivilegeAction.ADMIN
  }
}
