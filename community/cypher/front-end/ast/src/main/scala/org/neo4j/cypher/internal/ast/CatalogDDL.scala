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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.success
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTGraphRef
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec

sealed trait CatalogDDL extends Statement with SemanticAnalysisTooling {

  def name: String
}

sealed trait AdministrationCommand extends CatalogDDL {
  // We parse USE to give a nice error message, but it's not considered to be a part of the AST
  private var useGraphVar: Option[UseGraph] = None
  def useGraph: Option[UseGraph] = useGraphVar
  def withGraph(useGraph: Option[UseGraph]): AdministrationCommand = {
    this.useGraphVar = useGraph
    this
  }

  def isReadOnly: Boolean

  override def semanticCheck: SemanticCheck =
      requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleDatabases, position) chain
      when(useGraphVar.isDefined)(SemanticError(s"The `USE` clause is not supported for Administration Commands.", position))
}

sealed trait ReadAdministrationCommand extends AdministrationCommand {
  val isReadOnly: Boolean = true

  private[ast] val defaultColumnSet: List[(String, CypherType)]
  def returnColumnNames: List[String] = calculateResultColumns(yields).map(_._1)

  def where: Option[Where] = None
  def yields: Option[Return] = None
  def returns: Option[Return] = None

  private def calculateResultColumns(yields: Option[Return]): List[(String, CypherType)] = yields match {
    case Some(resultColumns) =>
      resultColumns.returnItems.items.map(ri => {
        val columnType = defaultColumnSet.find(c => c._1 == ri.name).map(_._2).getOrElse(CTString)
        val aliasName = ri.alias.get.name
        (aliasName, columnType)
      }).toList
    case None => defaultColumnSet
  }

  private def createSymbol(variable: (String, CypherType), offset: Int): semantics.Symbol =
    semantics.Symbol(variable._1, Set(new InputPosition(offset, 1, 1)), TypeSpec.exact(variable._2))

  override def returnColumns: List[LogicalVariable] = returnColumnNames.map(name => Variable(name)(position))

  private def skipCheck(skip: Skip): SemanticCheck = {
    val skipValue = Integer.parseInt(skip.expression.asInstanceOf[SignedDecimalIntegerLiteral].stringVal)
    if (skipValue < 1) SemanticError(s"Invalid input. '$skipValue' is not a valid value. Must be a non-negative integer.", skip.expression.position)
    else None
  }

  private def limitCheck(limit: Limit): SemanticCheck = {
    val limitValue = Integer.parseInt(limit.expression.asInstanceOf[SignedDecimalIntegerLiteral].stringVal)
    if (limitValue < 1) SemanticError(s"Invalid input. '$limitValue' is not a valid value. Must be a non-negative integer.", limit.expression.position)
    else None
  }

  private def checkForDML(where: Where): SemanticCheck = {
    val invalid: Option[Expression] = where.expression.treeFind[Expression] { case _: ExistsSubClause => true }
    invalid.map(exp => SemanticError("The EXISTS clause is not valid on SHOW commands.", exp.position))
  }

  override def semanticCheck: SemanticCheck =
    super.semanticCheck
      .chain(withScopedState(
        // YIELD requires all variables defined by the command in scope
        declareVariables(defaultColumnSet.zipWithIndex.map { case (name, index) => createSymbol(name, index) })
        .chain(yields.semanticCheck)
        .chain(yields.flatMap(_.skip).map(skipCheck).getOrElse(None))
        .chain(yields.flatMap(_.limit).map(limitCheck).getOrElse(None))
      )
      .chain(withScopedState(
        // WHERE and ORDER BY should operate on the return columns scoped by the YIELD
        declareVariables(calculateResultColumns(yields).zipWithIndex.map { case (name, index) => createSymbol(name, index) })
          .chain(where.semanticCheck)
          .chain(where.map(checkForDML).getOrElse(None))
          .chain(yields.flatMap(_.orderBy).semanticCheck))
      ))
}

sealed trait WriteAdministrationCommand extends AdministrationCommand {
  val isReadOnly: Boolean = false
  override def returnColumns: List[LogicalVariable] = List.empty
}

sealed trait MultiGraphDDL extends CatalogDDL {
  override def returnColumns: List[LogicalVariable] = List.empty

  //TODO Refine to split between multigraph and views
  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleGraphs, position)
}

sealed trait IfExistsDo

case object IfExistsReplace extends IfExistsDo

case object IfExistsDoNothing extends IfExistsDo

case object IfExistsThrowError extends IfExistsDo

case object IfExistsInvalidSyntax extends IfExistsDo

final case class ShowUsers(override  val yields: Option[Return], override val where: Option[Where],
                           override val returns: Option[Return])(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = "SHOW USERS"

  override val defaultColumnSet: List[(String, CypherType)] = List(("user", CTString), ("roles", CTList(CTString)),
    ("passwordChangeRequired", CTBoolean), ("suspended", CTBoolean))

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

trait EitherAsString {
  def eitherAsString(either: Either[String, Parameter]): String = either match {
    case Left(u) => u
    case Right(p) => s"$$${p.name}"
  }
}

final case class CreateUser(userName: Either[String, Parameter],
                            initialPassword: Expression,
                            requirePasswordChange: Boolean,
                            suspended: Option[Boolean],
                            ifExistsDo: IfExistsDo)(val position: InputPosition) extends WriteAdministrationCommand with EitherAsString {
  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE USER"
    case _ => "CREATE USER"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax => SemanticError(s"Failed to create the specified user '$userAsString': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  }

  private val userAsString: String = eitherAsString(userName)
}

final case class DropUser(userName: Either[String, Parameter], ifExists: Boolean)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "DROP USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class AlterUser(userName: Either[String, Parameter],
                           initialPassword: Option[Expression],
                           requirePasswordChange: Option[Boolean],
                           suspended: Option[Boolean])(val position: InputPosition) extends WriteAdministrationCommand {
  assert(initialPassword.isDefined || requirePasswordChange.isDefined || suspended.isDefined)

  override def name = "ALTER USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class SetOwnPassword(newPassword: Expression, currentPassword: Expression)
                               (val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "ALTER CURRENT USER SET PASSWORD"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowRoles(withUsers: Boolean, showAll: Boolean, override val yields: Option[Return], override val where: Option[Where],
                           override val returns: Option[Return])(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = if (showAll) "SHOW ALL ROLES" else "SHOW POPULATED ROLES"

  override val defaultColumnSet: List[(String, CypherType)] = if (withUsers) List(("role", CTString), ("member", CTString)) else List(("role", CTString))

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateRole(roleName: Either[String, Parameter], from: Option[Either[String, Parameter]], ifExistsDo: IfExistsDo)
                           (val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE ROLE"
    case _ => "CREATE ROLE"
  }

  override def semanticCheck: SemanticCheck =
    ifExistsDo match {
      case IfExistsInvalidSyntax =>
        val name = Prettifier.escapeName(roleName)
        SemanticError(s"Failed to create the specified role '$name': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
      case _ =>
        super.semanticCheck chain
          SemanticState.recordCurrentScope(this)
    }
}

final case class DropRole(roleName: Either[String, Parameter], ifExists: Boolean)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "DROP ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class GrantRolesToUsers(roleNames: Seq[Either[String, Parameter]], userNames: Seq[Either[String, Parameter]])
                                  (val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "GRANT ROLE"

  override def semanticCheck: SemanticCheck = {
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
  }
}

final case class RevokeRolesFromUsers(roleNames: Seq[Either[String, Parameter]], userNames: Seq[Either[String, Parameter]])
                                     (val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "REVOKE ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

abstract class PrivilegeType(val name: String)

final case class DatabasePrivilege(action: DatabaseAction)(val position: InputPosition) extends PrivilegeType(action.name)

final case class DbmsPrivilege(action: AdminAction)(val position: InputPosition) extends PrivilegeType(action.name)

final case class GraphPrivilege(action: GraphAction)(val position: InputPosition) extends PrivilegeType(action.name)

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

final case class AllPropertyResource()(val position: InputPosition) extends ActionResource

final case class LabelResource(label: String)(val position: InputPosition) extends ActionResource

final case class LabelsResource(labels: Seq[String])(val position: InputPosition) extends ActionResource {
  override def simplify: Seq[ActionResource] = labels.map(LabelResource(_)(position))
}

final case class AllLabelResource()(val position: InputPosition) extends ActionResource

final case class NoResource()(val position: InputPosition) extends ActionResource

final case class DatabaseResource()(val position: InputPosition) extends ActionResource

sealed trait PrivilegeQualifier extends Rewritable {
  def simplify: Seq[PrivilegeQualifier] = Seq(this)

  override def dup(children: Seq[AnyRef]): PrivilegeQualifier.this.type = this
}

sealed trait GraphPrivilegeQualifier extends PrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): GraphPrivilegeQualifier.this.type = this
}

final case class LabelQualifier(label: String)(val position: InputPosition) extends GraphPrivilegeQualifier

final case class RelationshipQualifier(reltype: String)(val position: InputPosition) extends GraphPrivilegeQualifier

final case class ElementQualifier(value: String)(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def simplify: Seq[GraphPrivilegeQualifier] = Seq(LabelQualifier(value)(position), RelationshipQualifier(value)(position))
}

final case class ElementsAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier {
  override def simplify: Seq[PrivilegeQualifier] = Seq(LabelAllQualifier()(position), RelationshipAllQualifier()(position))
}

final case class AllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier

final case class LabelAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier

final case class RelationshipAllQualifier()(val position: InputPosition) extends GraphPrivilegeQualifier

sealed trait DatabasePrivilegeQualifier extends PrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): DatabasePrivilegeQualifier.this.type = this
}

final case class AllDatabasesQualifier()(val position: InputPosition) extends DatabasePrivilegeQualifier

final case class UserAllQualifier()(val position: InputPosition) extends DatabasePrivilegeQualifier

final case class UserQualifier(username: Either[String, Parameter])(val position: InputPosition) extends DatabasePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): UserQualifier.this.type =
    this.copy(children.head.asInstanceOf[Either[String, Parameter]])(position).asInstanceOf[this.type]
}

sealed trait GraphOrDatabaseScope extends Rewritable {
  override def dup(children: Seq[AnyRef]): GraphOrDatabaseScope.this.type = this
}

sealed trait GraphScope extends GraphOrDatabaseScope

final case class NamedGraphScope(graph: Either[String, Parameter])(val position: InputPosition) extends GraphScope {
  override def dup(children: Seq[AnyRef]): NamedGraphScope.this.type =
    this.copy(children.head.asInstanceOf[Either[String, Parameter]])(position).asInstanceOf[this.type]
}

final case class AllGraphsScope()(val position: InputPosition) extends GraphScope

final case class DefaultGraphScope()(val position: InputPosition) extends GraphScope

sealed trait DatabaseScope extends GraphOrDatabaseScope {
  val showCommandName: String
}

final case class NamedDatabaseScope(database: Either[String, Parameter])(val position: InputPosition) extends DatabaseScope {
  override def dup(children: Seq[AnyRef]): NamedDatabaseScope.this.type =
    this.copy(children.head.asInstanceOf[Either[String, Parameter]])(position).asInstanceOf[this.type]

  override val showCommandName: String = "ShowDatabase"
}

final case class AllDatabasesScope()(val position: InputPosition) extends DatabaseScope {
  override val showCommandName: String = "ShowDatabases"
}

final case class DefaultDatabaseScope()(val position: InputPosition) extends DatabaseScope {
  override val showCommandName: String = "ShowDefaultDatabase"
}

sealed trait ShowPrivilegeScope extends Rewritable {
  override def dup(children: Seq[AnyRef]): ShowPrivilegeScope.this.type = this
}

final case class ShowRolesPrivileges(roles: List[Either[String, Parameter]])(val position: InputPosition) extends ShowPrivilegeScope {
  override def dup(children: Seq[AnyRef]): ShowRolesPrivileges.this.type =
    this.copy(children.head.asInstanceOf[List[Either[String, Parameter]]])(position).asInstanceOf[this.type]
}

final case class ShowUserPrivileges(user: Option[Either[String, Parameter]])(val position: InputPosition) extends ShowPrivilegeScope {
  override def dup(children: Seq[AnyRef]): ShowUserPrivileges.this.type =
    this.copy(children.head.asInstanceOf[Option[Either[String, Parameter]]])(position).asInstanceOf[this.type]
}

final case class ShowUsersPrivileges(users: List[Either[String, Parameter]])(val position: InputPosition) extends ShowPrivilegeScope {
  override def dup(children: Seq[AnyRef]): ShowUsersPrivileges.this.type =
    this.copy(children.head.asInstanceOf[List[Either[String, Parameter]]])(position).asInstanceOf[this.type]
}

final case class ShowAllPrivileges()(val position: InputPosition) extends ShowPrivilegeScope

sealed trait AdminAction {
  def name: String = "<unknown>"
}

abstract class DatabaseAction(override val name: String) extends AdminAction

case object StartDatabaseAction extends DatabaseAction("START")

case object StopDatabaseAction extends DatabaseAction("STOP")

case object AllDatabaseAction extends DatabaseAction("ALL DATABASE PRIVILEGES")

case object AccessDatabaseAction extends DatabaseAction("ACCESS")

abstract class IndexManagementAction(override val name: String) extends DatabaseAction(name)

case object AllIndexActions extends IndexManagementAction("INDEX MANAGEMENT")

case object CreateIndexAction extends IndexManagementAction("CREATE INDEX")

case object DropIndexAction extends IndexManagementAction("DROP INDEX")

abstract class ConstraintManagementAction(override val name: String) extends DatabaseAction(name)

case object AllConstraintActions extends ConstraintManagementAction("CONSTRAINT MANAGEMENT")

case object CreateConstraintAction extends ConstraintManagementAction("CREATE CONSTRAINT")

case object DropConstraintAction extends ConstraintManagementAction("DROP CONSTRAINT")

abstract class NameManagementAction(override val name: String) extends DatabaseAction(name)

case object AllTokenActions extends NameManagementAction("NAME MANAGEMENT")

case object CreateNodeLabelAction extends NameManagementAction("CREATE NEW NODE LABEL")

case object CreateRelationshipTypeAction extends NameManagementAction("CREATE NEW RELATIONSHIP TYPE")

case object CreatePropertyKeyAction extends NameManagementAction("CREATE NEW PROPERTY NAME")

abstract class TransactionManagementAction(override val name: String) extends DatabaseAction(name)

case object AllTransactionActions extends TransactionManagementAction("TRANSACTION MANAGEMENT")

case object ShowTransactionAction extends TransactionManagementAction("SHOW TRANSACTION")

case object TerminateTransactionAction extends TransactionManagementAction("TERMINATE TRANSACTION")

abstract class DbmsAdminAction(override val name: String) extends AdminAction

case object AllAdminAction extends DbmsAdminAction("ALL ADMIN PRIVILEGES")

case object AllDbmsAction extends DbmsAdminAction("ALL DBMS PRIVILEGES")

abstract class UserManagementAction(override val name: String) extends DbmsAdminAction(name)

case object AllUserActions extends UserManagementAction("USER MANAGEMENT")

case object ShowUserAction extends UserManagementAction("SHOW USER")

case object CreateUserAction extends UserManagementAction("CREATE USER")

case object SetUserStatusAction extends UserManagementAction("SET USER STATUS")

case object SetPasswordsAction extends UserManagementAction("SET PASSWORDS")

case object AlterUserAction extends UserManagementAction("ALTER USER")

case object DropUserAction extends UserManagementAction("DROP USER")

abstract class RoleManagementAction(override val name: String) extends DbmsAdminAction(name)

case object AllRoleActions extends RoleManagementAction("ROLE MANAGEMENT")

case object ShowRoleAction extends RoleManagementAction("SHOW ROLE")

case object CreateRoleAction extends RoleManagementAction("CREATE ROLE")

case object DropRoleAction extends RoleManagementAction("DROP ROLE")

case object AssignRoleAction extends RoleManagementAction("ASSIGN ROLE")

case object RemoveRoleAction extends RoleManagementAction("REMOVE ROLE")

abstract class DatabaseManagementAction(override val name: String) extends DbmsAdminAction(name)

case object AllDatabaseManagementActions extends DatabaseManagementAction("DATABASE MANAGEMENT")

case object CreateDatabaseAction extends DatabaseManagementAction("CREATE DATABASE")

case object DropDatabaseAction extends DatabaseManagementAction("DROP DATABASE")

abstract class PrivilegeManagementAction(override val name: String) extends DbmsAdminAction(name)

case object AllPrivilegeActions extends PrivilegeManagementAction("PRIVILEGE MANAGEMENT")

case object ShowPrivilegeAction extends PrivilegeManagementAction("SHOW PRIVILEGE")

case object AssignPrivilegeAction extends PrivilegeManagementAction("ASSIGN PRIVILEGE")

case object RemovePrivilegeAction extends PrivilegeManagementAction("REMOVE PRIVILEGE")

abstract class GraphAction(override val name: String, val planName: String) extends AdminAction

case object ReadAction extends GraphAction("READ", "Read")

case object MatchAction extends GraphAction("MATCH", "Match")

case object MergeAdminAction extends GraphAction("MERGE", "Merge")

case object TraverseAction extends GraphAction("TRAVERSE", "Traverse")

case object CreateElementAction extends GraphAction("CREATE", "CreateElement")

case object DeleteElementAction extends GraphAction("DELETE", "DeleteElement")

case object SetLabelAction extends GraphAction("SET LABEL", "SetLabel")

case object RemoveLabelAction extends GraphAction("REMOVE LABEL", "RemoveLabel")

case object WriteAction extends GraphAction("WRITE", "Write")

case object SetPropertyAction extends GraphAction("SET PROPERTY", "SetProperty")

case object AllGraphAction extends GraphAction("ALL GRAPH PRIVILEGES", "AllGraphPrivileges")

object GrantPrivilege {

  def dbmsAction(action: AdminAction, roleNames: Seq[Either[String, Parameter]]): InputPosition => GrantPrivilege =
    GrantPrivilege(DbmsPrivilege(action)(InputPosition.NONE), None, List(AllGraphsScope()(InputPosition.NONE)), List(AllQualifier()(InputPosition.NONE)), roleNames)

  def databaseAction[T <: DatabasePrivilegeQualifier](action: DatabaseAction,
                     scope: List[DatabaseScope],
                     roleNames: Seq[Either[String, Parameter]],
                     qualifier: List[T] = List(AllDatabasesQualifier()(InputPosition.NONE))): InputPosition => GrantPrivilege =
    GrantPrivilege(DatabasePrivilege(action)(InputPosition.NONE), None, scope, qualifier, roleNames)

  def graphAction[T <: GraphPrivilegeQualifier](action: GraphAction,
                  resource: Option[ActionResource],
                  scope: List[GraphScope],
                  qualifier: List[T],
                  roleNames: Seq[Either[String, Parameter]]): InputPosition => GrantPrivilege =
    GrantPrivilege(GraphPrivilege(action)(InputPosition.NONE), resource, scope, qualifier, roleNames)
}

object DenyPrivilege {
  def dbmsAction(action: AdminAction, roleNames: Seq[Either[String, Parameter]]): InputPosition => DenyPrivilege =
    DenyPrivilege(DbmsPrivilege(action)(InputPosition.NONE), None, List(AllGraphsScope()(InputPosition.NONE)), List(AllQualifier()(InputPosition.NONE)), roleNames)

  def databaseAction[T <: DatabasePrivilegeQualifier](action: DatabaseAction,
                     scope: List[DatabaseScope],
                     roleNames: Seq[Either[String, Parameter]],
                     qualifier: List[T] = List(AllDatabasesQualifier()(InputPosition.NONE))): InputPosition => DenyPrivilege =
    DenyPrivilege(DatabasePrivilege(action)(InputPosition.NONE), None, scope, qualifier, roleNames)

  def graphAction[T <: GraphPrivilegeQualifier](action: GraphAction,
                  resource: Option[ActionResource],
                  scope: List[GraphScope],
                  qualifier: List[T],
                  roleNames: Seq[Either[String, Parameter]]): InputPosition => DenyPrivilege =
    DenyPrivilege(GraphPrivilege(action)(InputPosition.NONE), resource, scope, qualifier, roleNames)
}

object RevokePrivilege {
  def dbmsAction(action: AdminAction,
                 roleNames: Seq[Either[String, Parameter]],
                 revokeType: RevokeType): InputPosition => RevokePrivilege =
    RevokePrivilege(DbmsPrivilege(action)(InputPosition.NONE), None, List(AllGraphsScope()(InputPosition.NONE)), List(AllQualifier()(InputPosition.NONE)), roleNames, revokeType)

  def databaseAction[T <: DatabasePrivilegeQualifier](action: DatabaseAction,
                     scope: List[DatabaseScope],
                     roleNames: Seq[Either[String, Parameter]],
                     revokeType: RevokeType,
                     qualifier: List[T] = List(AllDatabasesQualifier()(InputPosition.NONE))): InputPosition => RevokePrivilege =
    RevokePrivilege(DatabasePrivilege(action)(InputPosition.NONE), None, scope, qualifier, roleNames, revokeType)

  def graphAction[T <: GraphPrivilegeQualifier](action: GraphAction,
                  resource: Option[ActionResource],
                  scope: List[GraphScope],
                  qualifier: List[T],
                  roleNames: Seq[Either[String, Parameter]],
                  revokeType: RevokeType): InputPosition => RevokePrivilege =
    RevokePrivilege(GraphPrivilege(action)(InputPosition.NONE), resource, scope, qualifier, roleNames, revokeType)
}

sealed abstract class PrivilegeCommand(privilege: PrivilegeType, qualifier: List[PrivilegeQualifier], position: InputPosition)
  extends WriteAdministrationCommand {

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class GrantPrivilege(privilege: PrivilegeType,
                                resource: Option[ActionResource],
                                scope: List[GraphOrDatabaseScope],
                                qualifier: List[PrivilegeQualifier],
                                roleNames: Seq[Either[String, Parameter]])
                               (val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {
  override def name = s"GRANT ${privilege.name}"
}

final case class DenyPrivilege(privilege: PrivilegeType,
                               resource: Option[ActionResource],
                               scope: List[GraphOrDatabaseScope],
                               qualifier: List[PrivilegeQualifier],
                               roleNames: Seq[Either[String, Parameter]])
                              (val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name = s"DENY ${privilege.name}"

  override def semanticCheck: SemanticCheck = {
    privilege match {
      case GraphPrivilege(MergeAdminAction) => SemanticError(s"`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
      case _ => super.semanticCheck
    }
  }
}

final case class RevokePrivilege(privilege: PrivilegeType,
                                 resource: Option[ActionResource],
                                 scope: List[GraphOrDatabaseScope],
                                 qualifier: List[PrivilegeQualifier],
                                 roleNames: Seq[Either[String, Parameter]],
                                 revokeType: RevokeType)(val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name: String = {
    if (revokeType.name.nonEmpty) {
      s"REVOKE ${revokeType.name} ${privilege.name}"
    } else {
      s"REVOKE ${privilege.name}"
    }
  }

  override def semanticCheck: SemanticCheck = {
    (privilege, revokeType) match {
      case (GraphPrivilege(MergeAdminAction), RevokeDenyType())  => SemanticError(s"`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
      case _ => super.semanticCheck
    }
  }

}

final case class ShowPrivileges(scope: ShowPrivilegeScope, override  val yields: Option[Return], override val where: Option[Where], override val returns: Option[Return])(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE"

  override val defaultColumnSet: List[(String, CypherType)] = List(("access", CTString), ("action", CTString),
    ("resource", CTString), ("graph", CTString), ("segment", CTString), ("role", CTString)) ++ (scope match {
    case _: ShowUserPrivileges | _: ShowUsersPrivileges => List(("user", CTString))
    case _ => List.empty
  })

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class ShowDatabase(scope: DatabaseScope, override val yields: Option[Return], override val where: Option[Where],
                              override val returns: Option[Return])(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = scope match {
    case _: NamedDatabaseScope => "SHOW DATABASE"
    case _: AllDatabasesScope => "SHOW DATABASES"
    case _: DefaultDatabaseScope => "SHOW DEFAULT DATABASE"
  }

  override val defaultColumnSet: List[(String, CypherType)] = List(("name", CTString), ("address", CTString), ("role", CTString),
    ("requestedStatus", CTString), ("currentStatus", CTString), ("error", CTString)) ++ (scope match {
    case _: DefaultDatabaseScope => List.empty
    case _ => List(("default", CTBoolean))
  })

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class CreateDatabase(dbName: Either[String, Parameter], ifExistsDo: IfExistsDo)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE DATABASE"
    case _ => "CREATE DATABASE"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax =>
      val name = Prettifier.escapeName(dbName)
      SemanticError(s"Failed to create the specified database '$name': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  }
}

sealed abstract class DropDatabaseAdditionalAction(val name: String)

case object DumpData extends DropDatabaseAdditionalAction("DUMP DATA")

case object DestroyData extends DropDatabaseAdditionalAction("DESTROY DATA")

final case class DropDatabase(dbName: Either[String, Parameter],
                              ifExists: Boolean,
                              additionalAction: DropDatabaseAdditionalAction)
                             (val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "DROP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StartDatabase(dbName: Either[String, Parameter])(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "START DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StopDatabase(dbName: Either[String, Parameter])(val position: InputPosition) extends WriteAdministrationCommand {

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
