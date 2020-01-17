/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticCheckResult._
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticAnalysisTooling, SemanticCheck, SemanticCheckResult, SemanticError, SemanticFeature, SemanticState}
import org.neo4j.cypher.internal.v4_0.expressions.{LogicalVariable, Parameter, Variable}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.symbols._


sealed trait CatalogDDL extends Statement with SemanticAnalysisTooling {

  def name: String

  override def returnColumns: List[LogicalVariable] = List.empty
}

sealed trait MultiDatabaseAdministrationCommand extends CatalogDDL {
  def reservedRoleName: String = "PUBLIC"

  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleDatabases, position)
}

sealed trait MultiGraphDDL extends CatalogDDL {
  //TODO Refine to split between multigraph and views
  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleGraphs, position)
}

trait IfExistsDo
final case class IfExistsReplace() extends IfExistsDo
final case class IfExistsDoNothing() extends IfExistsDo
final case class IfExistsThrowError() extends IfExistsDo
final case class IfExistsInvalidSyntax() extends IfExistsDo

final case class ShowUsers()(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name: String = "SHOW USERS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateUser(userName: String,
                            initialStringPassword: Option[String],
                            initialParameterPassword: Option[Parameter],
                            requirePasswordChange: Boolean,
                            suspended: Option[Boolean],
                            ifExistsDo: IfExistsDo)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {
  assert(initialStringPassword.isDefined || initialParameterPassword.isDefined)
  assert(!(initialStringPassword.isDefined && initialParameterPassword.isDefined))

  override def name: String = ifExistsDo match {
    case _: IfExistsReplace => "CREATE OR REPLACE USER"
    case _ => "CREATE USER"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case _: IfExistsInvalidSyntax => SemanticError(s"Failed to create the specified user '$userName': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  }
}

final case class DropUser(userName: String, ifExists: Boolean)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "DROP USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterUser(userName: String,
                           initialStringPassword: Option[String],
                           initialParameterPassword: Option[Parameter],
                           requirePasswordChange: Option[Boolean],
                           suspended: Option[Boolean])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {
  assert(initialStringPassword.isDefined || initialParameterPassword.isDefined || requirePasswordChange.isDefined || suspended.isDefined)
  assert(!(initialStringPassword.isDefined && initialParameterPassword.isDefined))

  override def name = "ALTER USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class SetOwnPassword(newStringPassword: Option[String],
                                newParameterPassword: Option[Parameter],
                                currentStringPassword: Option[String],
                                currentParameterPassword: Option[Parameter])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {
  assert(newStringPassword.isDefined || newParameterPassword.isDefined)
  assert(!(newStringPassword.isDefined && newParameterPassword.isDefined))
  assert(currentStringPassword.isDefined || currentParameterPassword.isDefined)
  assert(!(currentStringPassword.isDefined && currentParameterPassword.isDefined))

  override def name = "ALTER CURRENT USER SET PASSWORD"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowRoles(withUsers: Boolean, showAll: Boolean)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name: String = if (showAll) "SHOW ALL ROLES" else "SHOW POPULATED ROLES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateRole(roleName: String, from: Option[String], ifExistsDo: IfExistsDo)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name: String = ifExistsDo match {
    case _: IfExistsReplace => "CREATE OR REPLACE ROLE"
    case _ => "CREATE ROLE"
  }

  override def semanticCheck: SemanticCheck = {
    if (reservedRoleName.equals(roleName)) {
      SemanticError(s"Failed to create the specified role '$roleName': '$roleName' is a reserved role name and cannot be created.", position)
    } else {
      ifExistsDo match {
        case _: IfExistsInvalidSyntax => SemanticError(s"Failed to create the specified role '$roleName': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
        case _ =>
          super.semanticCheck chain
            SemanticState.recordCurrentScope(this)
      }
    }
  }
}

final case class DropRole(roleName: String, ifExists: Boolean)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "DROP ROLE"

  override def semanticCheck: SemanticCheck = {
    if (reservedRoleName.equals(roleName)) {
      SemanticError(s"Failed to drop the specified role '$roleName': '$roleName' is a reserved role and cannot be dropped.", position)
    } else {
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
    }
  }
}

final case class GrantRolesToUsers(roleNames: Seq[String], userNames: Seq[String])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "GRANT ROLE"

  override def semanticCheck: SemanticCheck = {
    if (roleNames.contains(reservedRoleName)) {
      SemanticError(s"Failed to grant the specified role '$reservedRoleName': '$reservedRoleName' is a reserved role and cannot be granted.", position)
    } else {
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
    }
  }
}

final case class RevokeRolesFromUsers(roleNames: Seq[String], userNames: Seq[String])(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "REVOKE ROLE"

  override def semanticCheck: SemanticCheck =
    if (roleNames.contains(reservedRoleName)) {
      SemanticError(s"Failed to revoke the specified role '$reservedRoleName': '$reservedRoleName' is a reserved role and cannot be revoked.", position)
    } else {
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
    }
}

abstract class PrivilegeType(val name: String)

final case class DatabasePrivilege(action: DatabaseAction)(val position: InputPosition) extends PrivilegeType(action.name)

final case class DbmsPrivilege(action: AdminAction)(val position: InputPosition) extends PrivilegeType(action.name)

final case class TraversePrivilege()(val position: InputPosition) extends PrivilegeType("TRAVERSE")

final case class ReadPrivilege()(val position: InputPosition) extends PrivilegeType("READ")

final case class MatchPrivilege()(val position: InputPosition) extends PrivilegeType("MATCH")

final case class WritePrivilege()(val position: InputPosition) extends PrivilegeType("WRITE")

abstract class RevokeType(val name: String, val relType: String)

final case class RevokeGrantType()(val position: InputPosition) extends RevokeType("GRANT", "GRANTED")

final case class RevokeDenyType()(val position: InputPosition) extends RevokeType("DENY", "DENIED")

final case class RevokeBothType()(val position: InputPosition) extends RevokeType("", "")

sealed trait ActionResource {
  def simplify: Seq[ActionResource] = Seq(this)
}

final case class PropertyResource(property: String)(val position: InputPosition) extends ActionResource

final case class PropertiesResource(properties: Seq[String])(val position: InputPosition) extends ActionResource {
  override def simplify: Seq[ActionResource] = properties.map(PropertyResource(_)(position))
}

final case class DatabaseResource()(val position: InputPosition) extends ActionResource

final case class AllResource()(val position: InputPosition) extends ActionResource

final case class NoResource()(val position: InputPosition) extends ActionResource

sealed trait PrivilegeQualifier {
  def simplify: Seq[PrivilegeQualifier] = Seq(this)
}

final case class LabelQualifier(label: String)(val position: InputPosition) extends PrivilegeQualifier

final case class LabelsQualifier(labels: Seq[String])(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = labels.map(LabelQualifier(_)(position))
}

final case class RelationshipQualifier(reltype: String)(val position: InputPosition) extends PrivilegeQualifier

final case class RelationshipsQualifier(reltypes: Seq[String])(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = reltypes.map(RelationshipQualifier(_)(position))
}

final case class ElementsQualifier(values: Seq[String])(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = values.map(LabelQualifier(_)(position)) ++ values.map(RelationshipQualifier(_)(position))
}

final case class AllQualifier()(val position: InputPosition) extends PrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = Seq(LabelAllQualifier()(position), RelationshipAllQualifier()(position))
}

final case class LabelAllQualifier()(val position: InputPosition) extends PrivilegeQualifier

final case class RelationshipAllQualifier()(val position: InputPosition) extends PrivilegeQualifier

sealed trait GraphScope

final case class NamedGraphScope(database: String)(val position: InputPosition) extends GraphScope

final case class AllGraphsScope()(val position: InputPosition) extends GraphScope

sealed trait ShowPrivilegeScope

final case class ShowRolePrivileges(role: String)(val position: InputPosition) extends ShowPrivilegeScope

final case class ShowUserPrivileges(user: String)(val position: InputPosition) extends ShowPrivilegeScope

final case class ShowAllPrivileges()(val position: InputPosition) extends ShowPrivilegeScope

sealed trait AdminAction {
  def name: String = "<unknown>"
}

abstract class DatabaseAction(override val name: String = "<unknown>") extends AdminAction

case object AccessDatabaseAction extends DatabaseAction("ACCESS")
case object StartDatabaseAction extends DatabaseAction("START")
case object StopDatabaseAction extends DatabaseAction("STOP")
case object CreateDatabaseAction extends DatabaseAction("CREATE DATABASE")
case object DropDatabaseAction extends DatabaseAction("DROP DATABASE")
case object CreateIndexAction extends DatabaseAction("CREATE INDEX")
case object DropIndexAction extends DatabaseAction("DROP INDEX")
case object IndexManagementAction extends DatabaseAction("INDEX MANAGEMENT")
case object CreateConstraintAction extends DatabaseAction("CREATE CONSTRAINT")
case object DropConstraintAction extends DatabaseAction("DROP CONSTRAINT")
case object ConstraintManagementAction extends DatabaseAction("CONSTRAINT MANAGEMENT")
case object SchemaManagementAction extends DatabaseAction("SCHEMA MANAGEMENT")
case object CreateNodeLabelAction extends DatabaseAction("CREATE NEW NODE LABEL")
case object CreateRelationshipTypeAction extends DatabaseAction("CREATE NEW RELATIONSHIP TYPE")
case object CreatePropertyKeyAction extends DatabaseAction("CREATE NEW PROPERTY NAME")
case object TokenManagementAction extends DatabaseAction("NAME MANAGEMENT")
case object AllDatabaseAction extends DatabaseAction("ALL DATABASE PRIVILEGES")

abstract class UserManagementAction(override val name: String = "<unknown>") extends AdminAction

case object ShowUserAction extends UserManagementAction("SHOW USERS")
case object CreateUserAction extends UserManagementAction("CREATE USER")
case object AlterUserAction extends UserManagementAction("ALTER USER")
case object DropUserAction extends UserManagementAction("DROP USER")

abstract class RoleManagementAction(override val name: String = "<unknown>") extends AdminAction

case object AllRoleActions extends RoleManagementAction("ROLE MANAGEMENT")
case object ShowRoleAction extends RoleManagementAction("SHOW ROLE")
case object CreateRoleAction extends RoleManagementAction("CREATE ROLE")
case object DropRoleAction extends RoleManagementAction("DROP ROLE")
case object AssignRoleAction extends RoleManagementAction("ASSIGN ROLE")
case object RemoveRoleAction extends RoleManagementAction("REMOVE ROLE")

abstract class PrivilegeManagementAction(override val name: String = "<unknown>") extends AdminAction

case object ShowPrivilegeAction extends PrivilegeManagementAction("SHOW PRIVILEGES")
case object GrantPrivilegeAction extends PrivilegeManagementAction("GRANT PRIVILEGE")
case object RevokePrivilegeAction extends PrivilegeManagementAction("REVOKE PRIVILEGE")
case object DenyPrivilegeAction extends PrivilegeManagementAction("DENY PRIVILEGE")

abstract class DbmsAdminAction(override val name: String = "<unknown>") extends AdminAction

case object AllAdminAction extends DbmsAdminAction("ALL ADMIN PRIVILEGES")

object GrantPrivilege {
  def dbmsAction(action: AdminAction, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(DbmsPrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleNames)
  def databaseAction(action: DatabaseAction, scope: GraphScope, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(DatabasePrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), scope, AllQualifier()(InputPosition.NONE), roleNames)
  def traverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames)
  def read(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def asMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def write(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => GrantPrivilege =
    GrantPrivilege(WritePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames)
}

object DenyPrivilege {
  def dbmsAction(action: AdminAction, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(DbmsPrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleNames)
  def databaseAction(action: DatabaseAction, scope: GraphScope, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(DatabasePrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), scope, AllQualifier()(InputPosition.NONE), roleNames)
  def traverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames)
  def read(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def asMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames)
  def write(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => DenyPrivilege =
    DenyPrivilege(WritePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames)
}

object RevokePrivilege {
  // Revoke of grant
  def grantedDbmsAction(action: AdminAction, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(DbmsPrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleNames, RevokeGrantType()(InputPosition.NONE))
  def databaseGrantedAction(action: DatabaseAction, scope: GraphScope, roleNames: Seq[String]): InputPosition => RevokePrivilege  =
    RevokePrivilege(DatabasePrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), scope, AllQualifier()(InputPosition.NONE), roleNames, RevokeGrantType()(InputPosition.NONE))
  def grantedTraverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))
  def grantedRead(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))
  def grantedAsMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))
  def grantedWrite(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(WritePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeGrantType()(InputPosition.NONE))

  // Revoke of deny
  def deniedDbmsAction(action: AdminAction, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(DbmsPrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleNames, RevokeDenyType()(InputPosition.NONE))
  def databaseDeniedAction(action: DatabaseAction, scope: GraphScope, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(DatabasePrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), scope, AllQualifier()(InputPosition.NONE), roleNames, RevokeDenyType()(InputPosition.NONE))
  def deniedTraverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))
  def deniedRead(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))
  def deniedAsMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))
  def deniedWrite(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(WritePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeDenyType()(InputPosition.NONE))

  // Revoke
  def dbmsAction(action: AdminAction, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(DbmsPrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), AllGraphsScope()(InputPosition.NONE), AllQualifier()(InputPosition.NONE), roleNames, RevokeBothType()(InputPosition.NONE))
  def databaseAction(action: DatabaseAction, scope: GraphScope, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(DatabasePrivilege(action)(InputPosition.NONE), DatabaseResource()(InputPosition.NONE), scope, AllQualifier()(InputPosition.NONE), roleNames, RevokeBothType()(InputPosition.NONE))
  def traverse(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(TraversePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeBothType()(InputPosition.NONE))
  def read(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(ReadPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames,RevokeBothType()(InputPosition.NONE))
  def asMatch(resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(MatchPrivilege()(InputPosition.NONE), resource, scope, qualifier, roleNames, RevokeBothType()(InputPosition.NONE))
  def write(scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String]): InputPosition => RevokePrivilege =
    RevokePrivilege(WritePrivilege()(InputPosition.NONE), AllResource()(InputPosition.NONE), scope, qualifier, roleNames, RevokeBothType()(InputPosition.NONE))
}

sealed abstract class PrivilegeCommand(privilege: PrivilegeType, qualifier: PrivilegeQualifier, position: InputPosition)
  extends MultiDatabaseAdministrationCommand {

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      writeQualifierCheck chain
      SemanticState.recordCurrentScope(this)

  private def writeQualifierCheck: SemanticCheck =
    if (privilege.isInstanceOf[WritePrivilege] && !qualifier.isInstanceOf[AllQualifier])
      SemanticError("The use of ELEMENT, NODE or RELATIONSHIP with the WRITE privilege is not supported in this version.", position)
    else
      SemanticCheckResult.success
}

final case class GrantPrivilege(privilege: PrivilegeType, resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String])
                               (val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name = s"GRANT ${privilege.name}"
}

final case class DenyPrivilege(privilege: PrivilegeType, resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String])
                                (val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name = s"DENY ${privilege.name}"
}

final case class RevokePrivilege(privilege: PrivilegeType, resource: ActionResource, scope: GraphScope, qualifier: PrivilegeQualifier, roleNames: Seq[String],
                                 revokeType: RevokeType)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name: String = {
    if (revokeType.name.nonEmpty) {
      s"REVOKE ${revokeType.name} ${privilege.name}"
    } else {
      s"REVOKE ${privilege.name}"
    }
  }

  override def semanticCheck: SemanticCheck = privilege match {
    case _: MatchPrivilege =>
      if (revokeType.name.nonEmpty) {
        SemanticError(s"$name is not a valid command, use REVOKE ${revokeType.name} READ and REVOKE ${revokeType.name} TRAVERSE instead.", position)
      } else {
        SemanticError(s"$name is not a valid command, use REVOKE READ and REVOKE TRAVERSE instead.", position)
      }
    case _ => super.semanticCheck
  }
}

final case class ShowPrivileges(scope: ShowPrivilegeScope)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW PRIVILEGE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowDatabases()(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW DATABASES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowDefaultDatabase()(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW DEFAULT DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "SHOW DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateDatabase(dbName: String, ifExistsDo: IfExistsDo)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name: String = ifExistsDo match {
    case _: IfExistsReplace => "CREATE OR REPLACE DATABASE"
    case _ => "CREATE DATABASE"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case _: IfExistsInvalidSyntax => SemanticError(s"Failed to create the specified database '$dbName': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  }
}

final case class DropDatabase(dbName: String, ifExists: Boolean)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "DROP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StartDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "START DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StopDatabase(dbName: String)(val position: InputPosition) extends MultiDatabaseAdministrationCommand {

  override def name = "STOP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

object CreateGraph {
  def apply(graphName: CatalogName, query: Query)(position: InputPosition): CreateGraph =
    CreateGraph(graphName, query.part)(position)
}

final case class CreateGraph(graphName: CatalogName, query: QueryPart)
  (val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG CREATE GRAPH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this) chain
      query.semanticCheck
}

final case class DropGraph(graphName: CatalogName)(val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG DROP GRAPH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

object CreateView {
  def apply(graphName: CatalogName, params: Seq[Parameter], query: Query, innerQString: String)(position: InputPosition): CreateView =
    CreateView(graphName, params, query.part, innerQString)(position)
}

final case class CreateView(graphName: CatalogName, params: Seq[Parameter], query: QueryPart, innerQString: String)
  (val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG CREATE VIEW/QUERY"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this) chain
      recordGraphParameters chain
      query.semanticCheck

  private def recordGraphParameters(state: SemanticState): SemanticCheckResult = {
    params.foldLeft(success(state): SemanticCheckResult) { case (SemanticCheckResult(s, errors), p) =>
      s.declareVariable(Variable(s"$$${p.name}")(position), CTGraphRef) match {
        case Right(updatedState) => success(updatedState)
        case Left(semanticError) => SemanticCheckResult(s, errors :+ semanticError)
      }
    }
  }

}

final case class DropView(graphName: CatalogName)(val position: InputPosition) extends MultiGraphDDL {

  override def name = "CATALOG DROP VIEW/QUERY"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}
