/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType

sealed trait AdministrationCommand extends Statement with SemanticAnalysisTooling {

  def name: String

  // We parse USE to give a nice error message, but it's not considered to be a part of the AST
  private var useGraphVar: Option[UseGraph] = None
  def useGraph: Option[UseGraph] = useGraphVar
  def withGraph(useGraph: Option[UseGraph]): AdministrationCommand = {
    this.useGraphVar = useGraph
    this
  }

  def isReadOnly: Boolean

  override def containsUpdates: Boolean = !isReadOnly

  override def semanticCheck: SemanticCheck =
      requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleDatabases, position) chain
      when(useGraphVar.isDefined)(SemanticError(s"The `USE` clause is not required for Administration Commands. Retry your query omitting the `USE` clause and it will be routed automatically.", position))
}

sealed trait ReadAdministrationCommand extends AdministrationCommand {

  val isReadOnly: Boolean = true

  private[ast] val defaultColumnSet: List[ShowColumn]

  def returnColumnNames: List[String] = (yields, returns) match {
    case (_, Some(r)) => r.returnItems.items.map(ri => ri.alias.get.name).toList
    case (Some(resultColumns), None) =>
      resultColumns.returnItems.items.map(ri => ri.alias.get.name).toList
    case (None, None) => defaultColumnNames
  }

  def defaultColumnNames: List[String] = defaultColumnSet.map(_.name)

  def yieldOrWhere: YieldOrWhere = None
  def yields: Option[Yield] = yieldOrWhere.flatMap(yw => yw.left.toOption.map { case (y, _) => y })
  def returns: Option[Return] = yieldOrWhere.flatMap(yw => yw.left.toOption.flatMap { case (_, r) => r })
  def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ReadAdministrationCommand

  override def returnColumns: List[LogicalVariable] = returnColumnNames.map(name => Variable(name)(position))

  override def semanticCheck: SemanticCheck = initialState => {

    def checkForExistsSubquery(where: Where): SemanticCheck = state => {
      val invalid: Option[Expression] = where.expression.treeFind[Expression] { case _: ExistsSubClause => true }
      invalid.map(exp => error("The EXISTS clause is not valid on SHOW commands.", exp.position)(state))
        .getOrElse(SemanticCheckResult.success(state))
    }

    def checkForReturnPattern: SemanticCheck = state => {
      val maybePatternExpression = state.typeTable.collectFirst { case (expression, _) if expression.isInstanceOf[PatternExpression] => expression }
      val maybePatternComprehension = state.typeTable.collectFirst { case (expression, _) if expression.isInstanceOf[PatternComprehension] => expression }

      (maybePatternExpression, maybePatternComprehension) match {
        case (Some(patternExpression), _) =>
          error("You cannot include a pattern expression in the RETURN of administration SHOW commands", patternExpression.position)(state)
        case (_, Some(patternComprehension)) =>
          error("You cannot include a pattern comprehension in the RETURN of administration SHOW commands", patternComprehension.position)(state)
        case _ =>
          SemanticCheckResult.success(state)
      }
    }

    def checkProjection(r: ProjectionClause, prevErrors: Seq[SemanticErrorDef]): SemanticCheck = state => {
      val closingResult = (r.semanticCheck chain r.where.map(checkForExistsSubquery).getOrElse(None) chain checkForReturnPattern)(state)
      val continuationResult = r.semanticCheckContinuation(closingResult.state.currentScope.scope)(closingResult.state)
      semantics.SemanticCheckResult(continuationResult.state, prevErrors ++ closingResult.errors ++ continuationResult.errors)
    }

    def initialCheckResult = super.semanticCheck
      .chain(state => SemanticCheckResult.success(state.newChildScope))
      .chain(
        // Create variables for the columns generated by the command
        semanticCheckFold(defaultColumnSet)(sc => declareVariable(sc.variable, sc.cypherType))
      )(initialState)

    Seq(yields, returns).foldLeft(initialCheckResult){ (checkResult, maybeClause) => maybeClause match {
      case None => checkResult
      case Some(r: ProjectionClause) => checkProjection(r, checkResult.errors).chain(recordCurrentScope(r))(checkResult.state)
    }}
  }
}

case class ShowColumn(variable: LogicalVariable, cypherType: CypherType, name: String)

object ShowColumn {
  def apply(name: String, cypherType: CypherType = CTString)(position: InputPosition): ShowColumn = ShowColumn(Variable(name)(position), cypherType, name)
}

sealed trait WriteAdministrationCommand extends AdministrationCommand {
  val isReadOnly: Boolean = false
  override def returnColumns: List[LogicalVariable] = List.empty
}

final case class HasCatalog(source: AdministrationCommand) extends AdministrationCommand {
  override def name: String = s"CATALOG ${source.name}"
  override def isReadOnly: Boolean = source.isReadOnly
  override def returnColumns: List[LogicalVariable] = source.returnColumns
  override def position: InputPosition = source.position
}

sealed trait IfExistsDo

case object IfExistsReplace extends IfExistsDo

case object IfExistsDoNothing extends IfExistsDo

case object IfExistsThrowError extends IfExistsDo

case object IfExistsInvalidSyntax extends IfExistsDo

final case class ShowUsers(override val yieldOrWhere: YieldOrWhere, override val defaultColumnSet: List[ShowColumn])(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = "SHOW USERS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowUsers = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowUsers {
  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowUsers =
    ShowUsers(yieldOrWhere, List(
      ShowColumn("user")(position),
      ShowColumn("roles", CTList(CTString))(position),
      ShowColumn("passwordChangeRequired", CTBoolean)(position),
      ShowColumn("suspended", CTBoolean)(position),
      ShowColumn("home")(position)))(position)
}

final case class ShowCurrentUser(override val yieldOrWhere: YieldOrWhere, override val defaultColumnSet: List[ShowColumn])(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = "SHOW CURRENT USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowCurrentUser = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowCurrentUser {
  def apply(yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowCurrentUser =
    ShowCurrentUser(yieldOrWhere, List(
      ShowColumn("user")(position),
      ShowColumn("roles", CTList(CTString))(position),
      ShowColumn("passwordChangeRequired", CTBoolean)(position),
      ShowColumn("suspended", CTBoolean)(position),
      ShowColumn("home")(position)))(position)
}

trait EitherAsString {
  def eitherAsString(either: Either[String, Parameter]): String = either match {
    case Left(u) => u
    case Right(p) => s"$$${p.name}"
  }
}

final case class CreateUser(userName: Either[String, Parameter],
                            isEncryptedPassword: Boolean,
                            initialPassword: Expression,
                            userOptions: UserOptions,
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
                           isEncryptedPassword: Option[Boolean],
                           initialPassword: Option[Expression],
                           userOptions: UserOptions,
                           ifExists: Boolean)(val position: InputPosition) extends WriteAdministrationCommand {
  assert(initialPassword.isDefined || userOptions.requirePasswordChange.isDefined || userOptions.suspended.isDefined || userOptions.homeDatabase.isDefined)
  if ( userOptions.homeDatabase.isDefined && userOptions.homeDatabase.get == null ) {
    assert(initialPassword.isEmpty && userOptions.requirePasswordChange.isEmpty && userOptions.suspended.isEmpty)
  }

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

sealed trait HomeDatabaseAction
case object RemoveHomeDatabaseAction extends HomeDatabaseAction
final case class SetHomeDatabaseAction(name: Either[String, Parameter]) extends HomeDatabaseAction

final case class UserOptions(requirePasswordChange: Option[Boolean], suspended: Option[Boolean], homeDatabase: Option[HomeDatabaseAction])

final case class ShowRoles(withUsers: Boolean, showAll: Boolean, override val yieldOrWhere: YieldOrWhere, override val defaultColumnSet: List[ShowColumn])(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = if (showAll) "SHOW ALL ROLES" else "SHOW POPULATED ROLES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowRoles = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowRoles {
  def apply(withUsers: Boolean, showAll: Boolean,  yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowRoles = {
    val defaultColumnSet = if (withUsers) List(ShowColumn(Variable("role")(position), CTString, "role"), ShowColumn(Variable("member")(position), CTString, "member"))
      else List(ShowColumn(Variable("role")(position), CTString, "role"))
    ShowRoles(withUsers, showAll, yieldOrWhere, defaultColumnSet)(position)
  }
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

final case class RenameRole(fromRoleName: Either[String, Parameter], toRoleName: Either[String, Parameter], ifExists: Boolean)
                           (val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME ROLE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class RenameUser(fromUserName: Either[String, Parameter], toUserName: Either[String, Parameter], ifExists: Boolean)
                           (val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME USER"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
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

final case class DatabasePrivilege(action: DatabaseAction, scope: List[DatabaseScope])(val position: InputPosition) extends PrivilegeType(action.name)

final case class DbmsPrivilege(action: DbmsAction)(val position: InputPosition) extends PrivilegeType(action.name)

final case class GraphPrivilege(action: GraphAction, scope: List[GraphScope])(val position: InputPosition) extends PrivilegeType(action.name)

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

sealed trait ExecutePrivilegeQualifier extends PrivilegeQualifier

sealed trait ProcedurePrivilegeQualifier extends ExecutePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): ProcedurePrivilegeQualifier.this.type = this
}

final case class ProcedureQualifier(nameSpace: Namespace, procedureName: ProcedureName)(val position: InputPosition) extends ProcedurePrivilegeQualifier {
  override def simplify: Seq[ProcedurePrivilegeQualifier] = (nameSpace, procedureName) match {
    case (Namespace(Nil), ProcedureName("*")) => Seq(ProcedureAllQualifier()(position))
    case _ => Seq(this)
  }
}

final case class ProcedureAllQualifier()(val position: InputPosition) extends ProcedurePrivilegeQualifier

sealed trait FunctionPrivilegeQualifier extends ExecutePrivilegeQualifier {
  override def dup(children: Seq[AnyRef]): FunctionPrivilegeQualifier.this.type = this
}

final case class FunctionQualifier(nameSpace: Namespace, functionName: FunctionName)(val position: InputPosition) extends FunctionPrivilegeQualifier {
  override def simplify: Seq[FunctionPrivilegeQualifier] = (nameSpace, functionName) match {
    case (Namespace(Nil), FunctionName("*")) => Seq(FunctionAllQualifier()(position))
    case _ => Seq(this)
  }
}

final case class FunctionAllQualifier()(val position: InputPosition) extends FunctionPrivilegeQualifier

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

sealed trait DefaultScope

sealed trait GraphScope extends GraphOrDatabaseScope

final case class NamedGraphScope(graph: Either[String, Parameter])(val position: InputPosition) extends GraphScope {
  override def dup(children: Seq[AnyRef]): NamedGraphScope.this.type =
    this.copy(children.head.asInstanceOf[Either[String, Parameter]])(position).asInstanceOf[this.type]
}

final case class AllGraphsScope()(val position: InputPosition) extends GraphScope

final case class DefaultGraphScope()(val position: InputPosition) extends GraphScope with DefaultScope

final case class HomeGraphScope()(val position: InputPosition) extends GraphScope with DefaultScope

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

final case class DefaultDatabaseScope()(val position: InputPosition) extends DatabaseScope with DefaultScope {
  override val showCommandName: String = "ShowDefaultDatabase"
}

final case class HomeDatabaseScope()(val position: InputPosition) extends DatabaseScope with DefaultScope {
  override val showCommandName: String = "ShowHomeDatabase"
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

sealed trait AdministrationAction {
  def name: String = "<unknown>"
}

abstract class DatabaseAction(override val name: String) extends AdministrationAction

case object StartDatabaseAction extends DatabaseAction("START")

case object StopDatabaseAction extends DatabaseAction("STOP")

case object AllDatabaseAction extends DatabaseAction("ALL DATABASE PRIVILEGES")

case object AccessDatabaseAction extends DatabaseAction("ACCESS")

abstract class IndexManagementAction(override val name: String) extends DatabaseAction(name)

case object AllIndexActions extends IndexManagementAction("INDEX MANAGEMENT")

case object CreateIndexAction extends IndexManagementAction("CREATE INDEX")

case object DropIndexAction extends IndexManagementAction("DROP INDEX")

case object ShowIndexAction extends IndexManagementAction("SHOW INDEX")

abstract class ConstraintManagementAction(override val name: String) extends DatabaseAction(name)

case object AllConstraintActions extends ConstraintManagementAction("CONSTRAINT MANAGEMENT")

case object CreateConstraintAction extends ConstraintManagementAction("CREATE CONSTRAINT")

case object DropConstraintAction extends ConstraintManagementAction("DROP CONSTRAINT")

case object ShowConstraintAction extends ConstraintManagementAction("SHOW CONSTRAINT")

abstract class NameManagementAction(override val name: String) extends DatabaseAction(name)

case object AllTokenActions extends NameManagementAction("NAME MANAGEMENT")

case object CreateNodeLabelAction extends NameManagementAction("CREATE NEW NODE LABEL")

case object CreateRelationshipTypeAction extends NameManagementAction("CREATE NEW RELATIONSHIP TYPE")

case object CreatePropertyKeyAction extends NameManagementAction("CREATE NEW PROPERTY NAME")

abstract class TransactionManagementAction(override val name: String) extends DatabaseAction(name)

case object AllTransactionActions extends TransactionManagementAction("TRANSACTION MANAGEMENT")

case object ShowTransactionAction extends TransactionManagementAction("SHOW TRANSACTION")

case object TerminateTransactionAction extends TransactionManagementAction("TERMINATE TRANSACTION")

abstract class DbmsAction(override val name: String) extends AdministrationAction

case object AllDbmsAction extends DbmsAction("ALL DBMS PRIVILEGES")

case object ExecuteProcedureAction extends DbmsAction("EXECUTE PROCEDURE")

case object ExecuteBoostedProcedureAction extends DbmsAction("EXECUTE BOOSTED PROCEDURE")

case object ExecuteAdminProcedureAction extends DbmsAction("EXECUTE ADMIN PROCEDURES")

case object ExecuteFunctionAction extends DbmsAction("EXECUTE USER DEFINED FUNCTION")

case object ExecuteBoostedFunctionAction extends DbmsAction("EXECUTE BOOSTED USER DEFINED FUNCTION")

abstract class UserManagementAction(override val name: String) extends DbmsAction(name)

case object AllUserActions extends UserManagementAction("USER MANAGEMENT")

case object ShowUserAction extends UserManagementAction("SHOW USER")

case object CreateUserAction extends UserManagementAction("CREATE USER")

case object RenameUserAction extends RoleManagementAction("RENAME USER")

case object SetUserStatusAction extends UserManagementAction("SET USER STATUS")

case object SetPasswordsAction extends UserManagementAction("SET PASSWORDS")

case object SetUserHomeDatabaseAction extends UserManagementAction("SET USER HOME DATABASE")

case object AlterUserAction extends UserManagementAction("ALTER USER")

case object DropUserAction extends UserManagementAction("DROP USER")

abstract class RoleManagementAction(override val name: String) extends DbmsAction(name)

case object AllRoleActions extends RoleManagementAction("ROLE MANAGEMENT")

case object ShowRoleAction extends RoleManagementAction("SHOW ROLE")

case object CreateRoleAction extends RoleManagementAction("CREATE ROLE")

case object RenameRoleAction extends RoleManagementAction("RENAME ROLE")

case object DropRoleAction extends RoleManagementAction("DROP ROLE")

case object AssignRoleAction extends RoleManagementAction("ASSIGN ROLE")

case object RemoveRoleAction extends RoleManagementAction("REMOVE ROLE")

abstract class DatabaseManagementAction(override val name: String) extends DbmsAction(name)

case object AllDatabaseManagementActions extends DatabaseManagementAction("DATABASE MANAGEMENT")

case object CreateDatabaseAction extends DatabaseManagementAction("CREATE DATABASE")

case object DropDatabaseAction extends DatabaseManagementAction("DROP DATABASE")

abstract class PrivilegeManagementAction(override val name: String) extends DbmsAction(name)

case object AllPrivilegeActions extends PrivilegeManagementAction("PRIVILEGE MANAGEMENT")

case object ShowPrivilegeAction extends PrivilegeManagementAction("SHOW PRIVILEGE")

case object AssignPrivilegeAction extends PrivilegeManagementAction("ASSIGN PRIVILEGE")

case object RemovePrivilegeAction extends PrivilegeManagementAction("REMOVE PRIVILEGE")

abstract class GraphAction(override val name: String, val planName: String) extends AdministrationAction

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

  def dbmsAction(action: DbmsAction,
                 roleNames: Seq[Either[String, Parameter]],
                 qualifier: List[PrivilegeQualifier] = List(AllQualifier()(InputPosition.NONE))
                ): InputPosition => GrantPrivilege =
    GrantPrivilege(DbmsPrivilege(action)(InputPosition.NONE), None, qualifier, roleNames)

  def databaseAction(action: DatabaseAction,
                     scope: List[DatabaseScope],
                     roleNames: Seq[Either[String, Parameter]],
                     qualifier: List[DatabasePrivilegeQualifier] = List(AllDatabasesQualifier()(InputPosition.NONE))): InputPosition => GrantPrivilege =
    GrantPrivilege(DatabasePrivilege(action, scope)(InputPosition.NONE), None, qualifier, roleNames)

  def graphAction[T <: GraphPrivilegeQualifier](action: GraphAction,
                  resource: Option[ActionResource],
                  scope: List[GraphScope],
                  qualifier: List[T],
                  roleNames: Seq[Either[String, Parameter]]): InputPosition => GrantPrivilege =
    GrantPrivilege(GraphPrivilege(action, scope)(InputPosition.NONE), resource, qualifier, roleNames)
}

object DenyPrivilege {
  def dbmsAction(action: DbmsAction,
                 roleNames: Seq[Either[String, Parameter]],
                 qualifier: List[PrivilegeQualifier] = List(AllQualifier()(InputPosition.NONE))
                ): InputPosition => DenyPrivilege =
    DenyPrivilege(DbmsPrivilege(action)(InputPosition.NONE), None, qualifier, roleNames)

  def databaseAction(action: DatabaseAction,
                     scope: List[DatabaseScope],
                     roleNames: Seq[Either[String, Parameter]],
                     qualifier: List[DatabasePrivilegeQualifier] = List(AllDatabasesQualifier()(InputPosition.NONE))): InputPosition => DenyPrivilege =
    DenyPrivilege(DatabasePrivilege(action, scope)(InputPosition.NONE), None, qualifier, roleNames)

  def graphAction[T <: GraphPrivilegeQualifier](action: GraphAction,
                  resource: Option[ActionResource],
                  scope: List[GraphScope],
                  qualifier: List[T],
                  roleNames: Seq[Either[String, Parameter]]): InputPosition => DenyPrivilege =
    DenyPrivilege(GraphPrivilege(action, scope)(InputPosition.NONE), resource, qualifier, roleNames)
}

object RevokePrivilege {
  def dbmsAction(action: DbmsAction,
                 roleNames: Seq[Either[String, Parameter]],
                 revokeType: RevokeType,
                 qualifier: List[PrivilegeQualifier] = List(AllQualifier()(InputPosition.NONE))
                ): InputPosition => RevokePrivilege =
    RevokePrivilege(DbmsPrivilege(action)(InputPosition.NONE), None, qualifier, roleNames, revokeType)

  def databaseAction(action: DatabaseAction,
                     scope: List[DatabaseScope],
                     roleNames: Seq[Either[String, Parameter]],
                     revokeType: RevokeType,
                     qualifier: List[DatabasePrivilegeQualifier] = List(AllDatabasesQualifier()(InputPosition.NONE))): InputPosition => RevokePrivilege =
    RevokePrivilege(DatabasePrivilege(action, scope)(InputPosition.NONE), None, qualifier, roleNames, revokeType)

  def graphAction[T <: GraphPrivilegeQualifier](action: GraphAction,
                  resource: Option[ActionResource],
                  scope: List[GraphScope],
                  qualifier: List[T],
                  roleNames: Seq[Either[String, Parameter]],
                  revokeType: RevokeType): InputPosition => RevokePrivilege =
    RevokePrivilege(GraphPrivilege(action, scope)(InputPosition.NONE), resource, qualifier, roleNames, revokeType)
}

sealed abstract class PrivilegeCommand(privilege: PrivilegeType, qualifier: List[PrivilegeQualifier], position: InputPosition)
  extends WriteAdministrationCommand {

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class GrantPrivilege(privilege: PrivilegeType,
                                resource: Option[ActionResource],
                                qualifier: List[PrivilegeQualifier],
                                roleNames: Seq[Either[String, Parameter]])
                               (val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {
  override def name = s"GRANT ${privilege.name}"
}

final case class DenyPrivilege(privilege: PrivilegeType,
                               resource: Option[ActionResource],
                               qualifier: List[PrivilegeQualifier],
                               roleNames: Seq[Either[String, Parameter]])
                              (val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name = s"DENY ${privilege.name}"

  override def semanticCheck: SemanticCheck = {
    privilege match {
      case GraphPrivilege(MergeAdminAction, _) => SemanticError(s"`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
      case _ => super.semanticCheck
    }
  }
}

final case class RevokePrivilege(privilege: PrivilegeType,
                                 resource: Option[ActionResource],
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
      case (GraphPrivilege(MergeAdminAction, _), RevokeDenyType())  => SemanticError(s"`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
      case _ => super.semanticCheck
    }
  }

}

final case class ShowPrivileges(scope: ShowPrivilegeScope,
                                override val yieldOrWhere: YieldOrWhere,
                                override val defaultColumnSet: List[ShowColumn])(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowPrivileges = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowPrivileges{
  def apply(scope: ShowPrivilegeScope, yieldOrWhere: YieldOrWhere)(position:InputPosition): ShowPrivileges = {
    val columns = List(ShowColumn("access")(position), ShowColumn("action")(position), ShowColumn("resource")(position),
      ShowColumn("graph")(position), ShowColumn("segment")(position), ShowColumn("role")(position)) ++ (scope match {
          case _: ShowUserPrivileges | _: ShowUsersPrivileges => List(ShowColumn("user")(position))
          case _ => List.empty
        })
    ShowPrivileges(scope, yieldOrWhere, columns)(position)
  }
}

final case class ShowPrivilegeCommands(scope: ShowPrivilegeScope,
                                       asRevoke: Boolean,
                                       override val yieldOrWhere: YieldOrWhere,
                                       override val defaultColumnSet: List[ShowColumn])(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE COMMANDS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowPrivilegeCommands = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowPrivilegeCommands{
  def apply(scope: ShowPrivilegeScope, asRevoke: Boolean, yieldOrWhere: YieldOrWhere)(position:InputPosition): ShowPrivilegeCommands = {
    val columns = List(ShowColumn("command")(position))
    ShowPrivilegeCommands(scope, asRevoke, yieldOrWhere, columns)(position)
  }
}

final case class ShowDatabase(scope: DatabaseScope, override val yieldOrWhere: YieldOrWhere, override val defaultColumnSet: List[ShowColumn])(val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = scope match {
    case _: NamedDatabaseScope => "SHOW DATABASE"
    case _: AllDatabasesScope => "SHOW DATABASES"
    case _: DefaultDatabaseScope => "SHOW DEFAULT DATABASE"
    case _: HomeDatabaseScope => "SHOW HOME DATABASE"
  }

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowDatabase = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowDatabase{
  def apply(scope: DatabaseScope, yieldOrWhere: YieldOrWhere)(position:InputPosition): ShowDatabase = {
    val columns = List(
      ShowColumn("name")(position), ShowColumn("address")(position), ShowColumn("role")(position), ShowColumn("requestedStatus")(position),
      ShowColumn("currentStatus")(position), ShowColumn("error")(position)) ++ (scope match {
      case _: DefaultDatabaseScope => List.empty
      case _: HomeDatabaseScope => List.empty
      case _ => List(ShowColumn("default", CTBoolean)(position), ShowColumn("home", CTBoolean)(position))})
    ShowDatabase(scope, yieldOrWhere, columns)(position)
  }
}

sealed trait WaitableAdministrationCommand extends WriteAdministrationCommand {
  val waitUntilComplete: WaitUntilComplete

  override def returnColumns: List[LogicalVariable] = waitUntilComplete match {
    case NoWait => List.empty
    case _ => List("address","state", "message", "success").map(Variable(_)(position))
  }
}

final case class CreateDatabase(dbName: Either[String, Parameter],
                                ifExistsDo: IfExistsDo,
                                options: Options,
                                waitUntilComplete: WaitUntilComplete)(val position: InputPosition)
  extends WaitableAdministrationCommand {

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
                              additionalAction: DropDatabaseAdditionalAction,
                              waitUntilComplete: WaitUntilComplete)
                             (val position: InputPosition) extends WaitableAdministrationCommand {

  override def name = "DROP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StartDatabase(dbName: Either[String, Parameter], waitUntilComplete: WaitUntilComplete)(val position: InputPosition) extends WaitableAdministrationCommand {

  override def name = "START DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StopDatabase(dbName: Either[String, Parameter], waitUntilComplete: WaitUntilComplete)(val position: InputPosition) extends WaitableAdministrationCommand {

  override def name = "STOP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

sealed trait WaitUntilComplete {
  val DEFAULT_TIMEOUT = 300L
  val name: String
  def timeout: Long = DEFAULT_TIMEOUT
}

case object NoWait extends WaitUntilComplete {
  override val name: String = ""
}
case object IndefiniteWait extends WaitUntilComplete {
  override val name: String = " WAIT"
}
case class TimeoutAfter(timoutSeconds: Long) extends WaitUntilComplete {
  override val name: String = s" WAIT $timoutSeconds SECONDS"
  override def timeout: Long = timoutSeconds
}
