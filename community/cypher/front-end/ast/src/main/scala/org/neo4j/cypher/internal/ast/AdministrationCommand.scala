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
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString

sealed trait AdministrationCommand extends StatementWithGraph with SemanticAnalysisTooling {

  def name: String

  // We parse USE to give a nice error message, but it's not considered to be a part of the AST
  private var useGraphVar: Option[UseGraph] = None
  def useGraph: Option[UseGraph] = useGraphVar
  override def withGraph(useGraph: Option[UseGraph]): AdministrationCommand = {
    this.useGraphVar = useGraph
    this
  }

  def isReadOnly: Boolean

  override def containsUpdates: Boolean = !isReadOnly

  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.MultipleDatabases, position) chain
      when(useGraphVar.isDefined)(error(s"The `USE` clause is not required for Administration Commands. Retry your query omitting the `USE` clause and it will be routed automatically.", position))
}

sealed trait ReadAdministrationCommand extends AdministrationCommand {

  val isReadOnly: Boolean = true

  private[ast] val defaultColumnSet: List[ShowColumn]

  def returnColumnNames: List[String] = (yields, returns) match {
    case (_, Some(r))                => r.returnItems.items.map(ri => ri.alias.get.name).toList
    case (Some(resultColumns), None) => resultColumns.returnItems.items.map(ri => ri.alias.get.name).toList
    case (None, None)                => defaultColumnNames
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

    Seq(yields, returns).foldLeft(initialCheckResult) { (checkResult, maybeClause) => maybeClause match {
      case None => checkResult
      case Some(r: ProjectionClause) => checkProjection(r, checkResult.errors).chain(recordCurrentScope(r))(checkResult.state)
    }}
  }
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

trait EitherAsString {
  def eitherAsString(either: Either[String, Parameter]): String = either match {
    case Left(u)  => u
    case Right(p) => s"$$${p.name}"
  }
}

// User commands

final case class ShowUsers(override val yieldOrWhere: YieldOrWhere, override val defaultColumnSet: List[ShowColumn])
                          (val position: InputPosition) extends ReadAdministrationCommand {

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

final case class ShowCurrentUser(override val yieldOrWhere: YieldOrWhere, override val defaultColumnSet: List[ShowColumn])
                                (val position: InputPosition) extends ReadAdministrationCommand {

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
    case IfExistsInvalidSyntax => error(s"Failed to create the specified user '$userAsString': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
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

final case class RenameUser(fromUserName: Either[String, Parameter], toUserName: Either[String, Parameter], ifExists: Boolean)
                           (val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME USER"

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
  if (userOptions.homeDatabase.isDefined && userOptions.homeDatabase.get == null) {
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

// Role commands

final case class ShowRoles(withUsers: Boolean, showAll: Boolean, override val yieldOrWhere: YieldOrWhere, override val defaultColumnSet: List[ShowColumn])
                          (val position: InputPosition) extends ReadAdministrationCommand {

  override def name: String = if (showAll) "SHOW ALL ROLES" else "SHOW POPULATED ROLES"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowRoles = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowRoles {
  def apply(withUsers: Boolean, showAll: Boolean, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowRoles = {
    val defaultColumnSet =
      if (withUsers) List(ShowColumn(Variable("role")(position), CTString, "role"), ShowColumn(Variable("member")(position), CTString, "member"))
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
        error(s"Failed to create the specified role '$name': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
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

final case class RenameRole(fromRoleName: Either[String, Parameter], toRoleName: Either[String, Parameter], ifExists: Boolean)
                           (val position: InputPosition) extends WriteAdministrationCommand {

  override def name: String = "RENAME ROLE"

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

// Privilege commands

final case class ShowPrivileges(scope: ShowPrivilegeScope,
                                override val yieldOrWhere: YieldOrWhere,
                                override val defaultColumnSet: List[ShowColumn])(val position: InputPosition) extends ReadAdministrationCommand {
  override def name = "SHOW PRIVILEGE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowPrivileges = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowPrivileges {
  def apply(scope: ShowPrivilegeScope, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowPrivileges = {
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

object ShowPrivilegeCommands {
  def apply(scope: ShowPrivilegeScope, asRevoke: Boolean, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowPrivilegeCommands = {
    val columns = List(ShowColumn("command")(position))
    ShowPrivilegeCommands(scope, asRevoke, yieldOrWhere, columns)(position)
  }
}

//noinspection ScalaUnusedSymbol
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

final case class DenyPrivilege(privilege: PrivilegeType,
                               resource: Option[ActionResource],
                               qualifier: List[PrivilegeQualifier],
                               roleNames: Seq[Either[String, Parameter]])
                              (val position: InputPosition) extends PrivilegeCommand(privilege, qualifier, position) {

  override def name = s"DENY ${privilege.name}"

  override def semanticCheck: SemanticCheck = {
    privilege match {
      case GraphPrivilege(MergeAdminAction, _) => error(s"`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
      case _ => super.semanticCheck
    }
  }
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
      case (GraphPrivilege(MergeAdminAction, _), RevokeDenyType()) => error(s"`DENY MERGE` is not supported. Use `DENY SET PROPERTY` and `DENY CREATE` instead.", position)
      case _ => super.semanticCheck
    }
  }

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

// Database commands

final case class ShowDatabase(scope: DatabaseScope, override val yieldOrWhere: YieldOrWhere, defaultColumns: DefaultOrAllShowColumns)
                             (val position: InputPosition) extends ReadAdministrationCommand {
  override val defaultColumnSet: List[ShowColumn] = defaultColumns.columns

  override def name: String = scope match {
    case _: NamedDatabaseScope   => "SHOW DATABASE"
    case _: AllDatabasesScope    => "SHOW DATABASES"
    case _: DefaultDatabaseScope => "SHOW DEFAULT DATABASE"
    case _: HomeDatabaseScope    => "SHOW HOME DATABASE"
  }

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)

  override def withYieldOrWhere(newYieldOrWhere: YieldOrWhere): ShowDatabase = this.copy(yieldOrWhere = newYieldOrWhere)(position)
}

object ShowDatabase {
  def apply(scope: DatabaseScope, yieldOrWhere: YieldOrWhere)(position: InputPosition): ShowDatabase = {
    val showColumns = List(
      // (column, brief)
      (ShowColumn("name")(position), true),
      (ShowColumn("aliases", CTList(CTString))(position), true),
      (ShowColumn("access")(position), true),
      (ShowColumn("databaseID")(position), false),
      (ShowColumn("serverID")(position), false),
      (ShowColumn("address")(position), true),
      (ShowColumn("role")(position), true),
      (ShowColumn("requestedStatus")(position), true),
      (ShowColumn("currentStatus")(position), true),
      (ShowColumn("error")(position), true)
    ) ++ (scope match {
      case _: DefaultDatabaseScope => List.empty
      case _: HomeDatabaseScope => List.empty
      case _ => List((ShowColumn("default", CTBoolean)(position), true), (ShowColumn("home", CTBoolean)(position), true))
    }) ++ List(
      (ShowColumn("lastCommittedTxn", CTInteger)(position), false),
      (ShowColumn("replicationLag", CTInteger)(position), false)
    )
    val briefShowColumns = showColumns.filter(_._2).map(_._1)
    val allShowColumns = showColumns.map(_._1)

    val allColumns = yieldOrWhere match {
      case Some(Left(_)) => true
      case _ => false
    }
    val columns = DefaultOrAllShowColumns(allColumns, briefShowColumns, allShowColumns)
    ShowDatabase(scope, yieldOrWhere, columns)(position)
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
      error(s"Failed to create the specified database '$name': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  }
}

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

final case class AlterDatabase(dbName: Either[String, Parameter],
                              ifExists: Boolean,
                              access: Access)
                             (val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "ALTER DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StartDatabase(dbName: Either[String, Parameter], waitUntilComplete: WaitUntilComplete)
                              (val position: InputPosition) extends WaitableAdministrationCommand {

  override def name = "START DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class StopDatabase(dbName: Either[String, Parameter], waitUntilComplete: WaitUntilComplete)
                             (val position: InputPosition) extends WaitableAdministrationCommand {

  override def name = "STOP DATABASE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

sealed trait WaitableAdministrationCommand extends WriteAdministrationCommand {
  val waitUntilComplete: WaitUntilComplete

  override def returnColumns: List[LogicalVariable] = waitUntilComplete match {
    case NoWait => List.empty
    case _      => List("address", "state", "message", "success").map(Variable(_)(position))
  }
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

sealed trait Access
case object ReadOnlyAccess extends Access
case object ReadWriteAccess extends Access

sealed abstract class DropDatabaseAdditionalAction(val name: String)
case object DumpData extends DropDatabaseAdditionalAction("DUMP DATA")
case object DestroyData extends DropDatabaseAdditionalAction("DESTROY DATA")

final case class CreateDatabaseAlias(aliasName: Either[String, Parameter],
                                     targetName: Either[String, Parameter],
                                     ifExistsDo: IfExistsDo)(val position: InputPosition) extends WriteAdministrationCommand with EitherAsString {
  override def name: String = ifExistsDo match {
    case IfExistsReplace | IfExistsInvalidSyntax => "CREATE OR REPLACE ALIAS"
    case _ => "CREATE ALIAS"
  }

  override def semanticCheck: SemanticCheck = ifExistsDo match {
    case IfExistsInvalidSyntax => error(s"Failed to create the specified alias '${Prettifier.escapeName(aliasName)}': cannot have both `OR REPLACE` and `IF NOT EXISTS`.", position)
    case _ =>
      super.semanticCheck chain
        SemanticState.recordCurrentScope(this)
  }
}

final case class AlterDatabaseAlias(aliasName: Either[String, Parameter], targetName: Either[String, Parameter], ifExists: Boolean)
                                   (val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "ALTER ALIAS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class DropDatabaseAlias(aliasName: Either[String, Parameter], ifExists: Boolean)(val position: InputPosition) extends WriteAdministrationCommand {

  override def name = "DROP ALIAS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}
