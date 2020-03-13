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
package org.neo4j.cypher.internal.compiler

import org.neo4j.configuration.helpers.{DatabaseNameValidator, NormalizedDatabaseName}
import org.neo4j.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContext}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.{LogicalPlan, NameValidator, PrivilegePlan, QualifiedName, ResolvedCall, SecurityAdministrationLogicalPlan}
import org.neo4j.cypher.internal.planner.spi.AdministrationPlannerName
import org.neo4j.cypher.internal.v4_0.ast.{IndexManagementAction, _}
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.ast.semantics.{SemanticCheckResult, SemanticState}
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.string.UTF8

/**
  * This planner takes on queries that run at the DBMS level for multi-database administration
  */
case object MultiDatabaseAdministrationCommandPlanBuilder extends Phase[PlannerContext, BaseState, LogicalPlanState] {

  val prettifier = Prettifier(ExpressionStringifier())

  override def phase: CompilationPhase = PIPE_BUILDING

  override def description = "take on administrative queries that require no planning such as multi-database administration commands"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = {
    implicit val idGen: SequentialIdGen = new SequentialIdGen()
    def planRevokeDatabasePrivileges(source: Option[PrivilegePlan],
                                     roleNames: Seq[String],
                                     action: AdminAction,
                                     planAction: (Option[PrivilegePlan], String, AdminAction) => Option[PrivilegePlan]): Option[PrivilegePlan] = {
      planDatabasePrivileges(source, roleNames, action, planAction, planAction)
    }
    def planDatabasePrivileges(source: Option[PrivilegePlan],
                               roleNames: Seq[String],
                               action: AdminAction,
                               planAction: (Option[PrivilegePlan], String, AdminAction) => Option[PrivilegePlan],
                               planExtra: (Option[PrivilegePlan], String, AdminAction) => Option[PrivilegePlan] = (s, _, _) => s): Option[PrivilegePlan] = {
      def planActions(source: Option[PrivilegePlan],
                      action: AdminAction,
                      roleName: String,
                      g: AdminAction*): Option[PrivilegePlan] = g.foldLeft(planExtra(source, roleName, action)) {
        case (plan, act) => planAction(plan, roleName, act)
      }
      roleNames.foldLeft(source) {
        case (source, roleName) => action match {
          case IndexManagementAction =>
            planActions(source, action, roleName, CreateIndexAction, DropIndexAction)
          case ConstraintManagementAction =>
            planActions(source, action, roleName, CreateConstraintAction, DropConstraintAction)
          case SchemaManagementAction =>
            val constraints = planActions(source, ConstraintManagementAction, roleName, CreateConstraintAction, DropConstraintAction)
            val indexes = planActions(constraints, IndexManagementAction, roleName, CreateIndexAction, DropIndexAction)
            val schema = planActions(indexes, SchemaManagementAction, roleName)
            schema
          case TokenManagementAction =>
            planActions(source, action, roleName, CreateNodeLabelAction, CreateRelationshipTypeAction, CreatePropertyKeyAction)
          case AllDatabaseAction =>
            val dbx = planActions(source, AllDatabaseAction, roleName, AccessDatabaseAction, StartDatabaseAction, StopDatabaseAction)
            val constraints = planActions(dbx, ConstraintManagementAction, roleName, CreateConstraintAction, DropConstraintAction)
            val indexes = planActions(constraints, IndexManagementAction, roleName, CreateIndexAction, DropIndexAction)
            val schema = planActions(indexes, SchemaManagementAction, roleName)
            val tokens = planActions(schema, TokenManagementAction, roleName, CreateNodeLabelAction, CreateRelationshipTypeAction, CreatePropertyKeyAction)
            tokens
          case _ => planAction(source, roleName, action)
        }
      }
    }
    def planDbmsRevokePrivileges(source: Option[PrivilegePlan],
                                 roleNames: Seq[String],
                                 action: AdminAction,
                                 planAction: (Option[PrivilegePlan], String, AdminAction) => Option[PrivilegePlan]): Option[PrivilegePlan] = {
      def planActions(source: Option[PrivilegePlan], roleName: String, g: AdminAction*): Option[PrivilegePlan] = g.foldLeft(source) {
        case (plan, act) => planAction(plan, roleName, act)
      }
      roleNames.foldLeft(source) {
        case (source, roleName) => action match {
          case AllRoleActions => planActions(source, roleName, AllRoleActions, CreateRoleAction, DropRoleAction, AssignRoleAction, RemoveRoleAction, ShowRoleAction)
          case _ => planAction(source, roleName, action)
        }
      }
    }
    def planRevokes(source: Option[PrivilegePlan],
                   revokeType: RevokeType,
                   planRevoke: (Option[PrivilegePlan], String) => Some[PrivilegePlan]): Some[PrivilegePlan] = revokeType match {
      case t: RevokeBothType =>
        val revokeGrant = planRevoke(source, RevokeGrantType()(t.position).relType)
        planRevoke(revokeGrant, RevokeDenyType()(t.position).relType)
      case t => planRevoke(source, t.relType)
    }
    def validateNameAndGetSourceForCreateRole(roleName: String, ifExistsDo: IfExistsDo): Option[SecurityAdministrationLogicalPlan] = {
      NameValidator.assertValidRoleName(roleName)
      ifExistsDo match {
        case _: IfExistsReplace =>
          val admin = Some(plans.AssertDbmsAdmin(DropRoleAction, CreateRoleAction))
          val checkFrozenRole = Some(plans.CheckFrozenRole(admin, roleName))
          Some(plans.DropRole(checkFrozenRole, roleName))
        case _: IfExistsDoNothing => Some(plans.DoNothingIfExists(Some(plans.AssertDbmsAdmin(CreateRoleAction)), "Role", roleName))
        case _ => Some(plans.AssertDbmsAdmin(CreateRoleAction))
      }
    }
    def validateNameAndGetSourceForCreateUser(userName: String, ifExistsDo: IfExistsDo): Option[SecurityAdministrationLogicalPlan] = {
      NameValidator.assertValidUsername(userName)
      ifExistsDo match {
        case _: IfExistsReplace => Some(plans.DropUser(Some(plans.AssertDbmsAdmin(DropUserAction, CreateUserAction)), userName))
        case _: IfExistsDoNothing => Some(plans.DoNothingIfExists(Some(plans.AssertDbmsAdmin(CreateUserAction)), "User", userName))
        case _ => Some(plans.AssertDbmsAdmin(CreateUserAction))
      }
    }
    def normalizeGraphScope(graphScope: GraphScope): GraphScope = {
      graphScope match {
        case scope@NamedGraphScope(dbName) =>
          val normalizedName = new NormalizedDatabaseName(dbName).name()
          NamedGraphScope(normalizedName)(scope.position)
        case scope => scope
      }
    }

    val maybeLogicalPlan: Option[LogicalPlan] = from.statement() match {
      // SHOW USERS
      case _: ShowUsers =>
        Some(plans.ShowUsers(Some(plans.AssertDbmsAdmin(ShowUserAction))))

      // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] WITH PASSWORD password
      case c@CreateUser(userName, Some(initialStringPassword), initialParameterPassword, requirePasswordChange, suspended, ifExistsDo) =>
        val source = validateNameAndGetSourceForCreateUser(userName, ifExistsDo)
        Some(plans.LogSystemCommand(
          plans.CreateUser(source, userName, Some(UTF8.encode(initialStringPassword)), initialParameterPassword, requirePasswordChange, suspended),
          prettifier.asString(c)))

      // CREATE [OR REPLACE] USER foo [IF NOT EXISTS] WITH PASSWORD $password
      case c@CreateUser(userName, None, initialParameterPassword, requirePasswordChange, suspended, ifExistsDo) =>
        val source = validateNameAndGetSourceForCreateUser(userName, ifExistsDo)
        Some(plans.LogSystemCommand(
          plans.CreateUser(source, userName, None, initialParameterPassword, requirePasswordChange, suspended),
          prettifier.asString(c)))

      // DROP USER foo [IF EXISTS]
      case c@DropUser(userName, ifExists) =>
        val admin = Some(plans.AssertDbmsAdmin(DropUserAction))
        val source = if (ifExists) Some(plans.DoNothingIfNotExists(admin, "User", userName)) else Some(plans.EnsureNodeExists(admin, "User", userName))
        Some(plans.LogSystemCommand(
          plans.DropUser(source, userName),
          prettifier.asString(c)))

      // ALTER USER foo
      case c@AlterUser(userName, initialStringPassword, initialParameterPassword, requirePasswordChange, suspended) =>
        val initialPasswordString = initialStringPassword.map(UTF8.encode)
        val admin = plans.AssertDbmsAdmin(AlterUserAction)
        val assertionSubPlan =
          if(suspended.isDefined) plans.AssertNotCurrentUser(Some(admin), userName, s"Failed to alter the specified user '$userName': Changing your own activation status is not allowed.")
          else admin
        Some(plans.LogSystemCommand(
          plans.AlterUser(Some(assertionSubPlan), userName, initialPasswordString, initialParameterPassword, requirePasswordChange, suspended),
          prettifier.asString(c)))

      // ALTER CURRENT USER SET PASSWORD FROM currentPassword TO newPassword
      case c@SetOwnPassword(newStringPassword, newParameterPassword, currentStringPassword, currentParameterPassword) =>
        val newPasswordString = newStringPassword.map(UTF8.encode)
        val currentPasswordString = currentStringPassword.map(UTF8.encode)
        Some(plans.LogSystemCommand(
          plans.SetOwnPassword(newPasswordString, newParameterPassword, currentPasswordString, currentParameterPassword),
          prettifier.asString(c)))

      // SHOW [ ALL | POPULATED ] ROLES [ WITH USERS ]
      case ShowRoles(withUsers, showAll) =>
        Some(plans.ShowRoles(Some(plans.AssertDbmsAdmin(ShowRoleAction)), withUsers, showAll))

      // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS]
      case c@CreateRole(roleName, None, ifExistsDo) =>
        val source = validateNameAndGetSourceForCreateRole(roleName, ifExistsDo)
        Some(plans.LogSystemCommand(plans.CreateRole(source, roleName), prettifier.asString(c)))

      // CREATE [OR REPLACE] ROLE foo [IF NOT EXISTS] AS COPY OF bar
      case c@CreateRole(roleName, Some(fromName), ifExistsDo) =>
        val source = validateNameAndGetSourceForCreateRole(roleName, ifExistsDo)
        Some(plans.LogSystemCommand(plans.CopyRolePrivileges(
          Some(plans.CopyRolePrivileges(
            Some(plans.CreateRole(
              Some(plans.RequireRole(source, fromName)), roleName)
            ), roleName, fromName, "GRANTED")
          ), roleName, fromName, "DENIED"), prettifier.asString(c)))

      // DROP ROLE foo [IF EXISTS]
      case c@DropRole(roleName, ifExists) =>
        val admin = Some(plans.AssertDbmsAdmin(DropRoleAction))
        val checkFrozenRole = Some(plans.CheckFrozenRole(admin, roleName))
        val source = if (ifExists) Some(plans.DoNothingIfNotExists(checkFrozenRole, "Role", roleName)) else Some(plans.EnsureNodeExists(checkFrozenRole, "Role", roleName))
        Some(plans.LogSystemCommand(plans.DropRole(source, roleName), prettifier.asString(c)))

      // GRANT roles TO users
      case c@GrantRolesToUsers(roleNames, userNames) =>
        (for (userName <- userNames; roleName <- roleNames) yield {
          roleName -> userName
        }).foldLeft(Some(plans.AssertDbmsAdmin(AssignRoleAction).asInstanceOf[SecurityAdministrationLogicalPlan])) {
          case (source, (userName, roleName)) => Some(plans.GrantRoleToUser(source, userName, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE roles FROM users
      case c@RevokeRolesFromUsers(roleNames, userNames) =>
        (for (userName <- userNames; roleName <- roleNames) yield {
          roleName -> userName
        }).foldLeft(Some(plans.AssertDbmsAdmin(RemoveRoleAction).asInstanceOf[SecurityAdministrationLogicalPlan])) {
          case (source, (userName, roleName)) => Some(plans.RevokeRoleFromUser(source, userName, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT CREATE ROLE ON DBMS TO role
      case c@GrantPrivilege(DbmsPrivilege(action), _, _, _, roleNames) =>
        roleNames.foldLeft(Option(plans.AssertDbmsAdmin(GrantPrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, roleName) => Some(plans.GrantDbmsAction(source, action, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY CREATE ROLE ON DBMS TO role
      case c@DenyPrivilege(DbmsPrivilege(action), _, _, _, roleNames) =>
        roleNames.foldLeft(Option(plans.AssertDbmsAdmin(DenyPrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, roleName) => Some(plans.DenyDbmsAction(source, action, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE CREATE ROLE ON DBMS FROM role
      case c@RevokePrivilege(DbmsPrivilege(action), _, _, _, roleNames, revokeType) =>
        val source = roleNames.foldLeft(Some(plans.AssertDbmsAdmin(RevokePrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (previous, roleName) => Some(plans.AssertValidRevoke(previous, action, AllGraphsScope()(InputPosition.NONE), roleName, revokeType))
        }
        planDbmsRevokePrivileges(source, roleNames, action,
          (plan, role, act) => planRevokes(plan, revokeType, (s, r) => Some(plans.RevokeDbmsAction(s, act, role, r)))
        ).map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT ACCESS/START/STOP/TOKEN/SCHEMA ON DATABASE foo TO role
      case c@GrantPrivilege(DatabasePrivilege(action), _, database, _, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        planDatabasePrivileges(
          Option(plans.AssertDbmsAdmin(GrantPrivilegeAction).asInstanceOf[PrivilegePlan]), roleNames, action,
          (plan, role, act) => Some(plans.GrantDatabaseAction(plan, act, normalizedDatabase, role))
        ).map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY ACCESS/START/STOP/TOKEN/SCHEMA ON DATABASE foo TO role
      case c@DenyPrivilege(DatabasePrivilege(action), _, database, _, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        planDatabasePrivileges(
          Option(plans.AssertDbmsAdmin(DenyPrivilegeAction).asInstanceOf[PrivilegePlan]), roleNames, action,
          (plan, role, act) => Some(plans.DenyDatabaseAction(plan, act, normalizedDatabase, role))
        ).map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE ACCESS/START/STOP/TOKEN/SCHEMA ON DATABASE foo FROM role
      case c@RevokePrivilege(DatabasePrivilege(action), _, database, _, roleNames, revokeType) =>
        val normalizedDatabase = normalizeGraphScope(database)
        val source = roleNames.foldLeft(Some(plans.AssertDbmsAdmin(RevokePrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (previous, roleName) => Some(plans.AssertValidRevoke(previous, action, normalizedDatabase, roleName, revokeType))
        }
        planRevokeDatabasePrivileges(source, roleNames, action,
          (plan, role, act) => planRevokes(plan, revokeType, (s, r) => Some(plans.RevokeDatabaseAction(s, act, normalizedDatabase, role, r)))
        ).map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role
      case c@GrantPrivilege(TraversePrivilege(), _, database, segments, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        (for (roleName <- roleNames; segment <- segments.simplify) yield {
          roleName -> segment
        }).foldLeft(Some(plans.AssertDbmsAdmin(GrantPrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, (roleName, segment)) => Some(plans.GrantTraverse(source, normalizedDatabase, segment, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY TRAVERSE ON GRAPH foo ELEMENTS A (*) TO role
      case c@DenyPrivilege(TraversePrivilege(), _, database, segments, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        (for (roleName <- roleNames; segment <- segments.simplify) yield {
          roleName -> segment
        }).foldLeft(Some(plans.AssertDbmsAdmin(DenyPrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, (roleName, segment)) => Some(plans.DenyTraverse(source, normalizedDatabase, segment, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE TRAVERSE ON GRAPH foo ELEMENTS A (*) FROM role
      case c@RevokePrivilege(TraversePrivilege(), _, database, segments, roleNames, revokeType) =>
        val normalizedDatabase = normalizeGraphScope(database)
        (for (roleName <- roleNames; segment <- segments.simplify) yield {
          roleName -> segment
        }).foldLeft(Some(plans.AssertDbmsAdmin(RevokePrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, (roleName, segment)) => planRevokes(source, revokeType, (s, r) => Some(plans.RevokeTraverse(s, normalizedDatabase, segment, roleName, r)))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT WRITE ON GRAPH foo ELEMENTS * (*) TO role
      case c@GrantPrivilege(WritePrivilege(), _, database, segments, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        (for (roleName <- roleNames; segment <- segments.simplify) yield {
          roleName -> segment
        }).foldLeft(Some(plans.AssertDbmsAdmin(GrantPrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, (roleName, segment)) => Some(plans.GrantWrite(source, AllResource()(InputPosition.NONE), normalizedDatabase, segment, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY WRITE ON GRAPH foo ELEMENTS * (*) TO role
      case c@DenyPrivilege(WritePrivilege(), _, database, segments, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        (for (roleName <- roleNames; segment <- segments.simplify) yield {
          roleName -> segment
        }).foldLeft(Some(plans.AssertDbmsAdmin(DenyPrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, (roleName, segment)) => Some(plans.DenyWrite(source, AllResource()(InputPosition.NONE), normalizedDatabase, segment, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE WRITE ON GRAPH foo ELEMENTS * (*) FROM role
      case c@RevokePrivilege(WritePrivilege(), _, database, segments, roleNames, revokeType) =>
        val normalizedDatabase = normalizeGraphScope(database)
        (for (roleName <- roleNames; segment <- segments.simplify) yield {
          roleName -> segment
        }).foldLeft(Some(plans.AssertDbmsAdmin(RevokePrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, (roleName, segment)) => planRevokes(source, revokeType, (s, r) => Some(plans.RevokeWrite(s, AllResource()(InputPosition.NONE), normalizedDatabase, segment, roleName, r)))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // GRANT READ {prop} ON GRAPH foo ELEMENTS A (*) TO role
      // GRANT MATCH {prop} ON GRAPH foo ELEMENTS A (*) TO role
      case c@GrantPrivilege(privilege, resources, database, segments, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        val combos = for (roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          roleName -> (segment, resource)
        }
        val isAdmin = Some(plans.AssertDbmsAdmin(GrantPrivilegeAction).asInstanceOf[PrivilegePlan])
        val plan = privilege match {
          case ReadPrivilege() => isAdmin
          case MatchPrivilege() => combos.foldLeft(isAdmin) {
            case (source, (roleName, (segment, _))) => Some(plans.GrantTraverse(source, normalizedDatabase, segment, roleName))
          }
        }
        combos.foldLeft(plan) {
          case (source, (roleName, (segment, resource))) => Some(plans.GrantRead(source, resource, normalizedDatabase, segment, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // DENY READ {prop} ON GRAPH foo ELEMENTS A (*) TO role
      // DENY MATCH {prop} ON GRAPH foo ELEMENTS A (*) TO role
      case c@DenyPrivilege(privilege, resources, database, segments, roleNames) =>
        val normalizedDatabase = normalizeGraphScope(database)
        val combos = for (roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          roleName -> (segment, resource)
        }
        val isAdmin = Some(plans.AssertDbmsAdmin(DenyPrivilegeAction).asInstanceOf[PrivilegePlan])
        val plan = privilege match {
          case MatchPrivilege()  if resources.isInstanceOf[AllResource] => combos.foldLeft(isAdmin) {
            case (source, (roleName, (segment, _))) => Some(plans.DenyTraverse(source, normalizedDatabase, segment, roleName))
          }
          case _ => isAdmin
        }
        combos.foldLeft(plan) {
          case (source, (roleName, (segment, resource))) => Some(plans.DenyRead(source, resource, normalizedDatabase, segment, roleName))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // REVOKE READ {prop} ON GRAPH foo ELEMENTS A (*) FROM role
      // REVOKE MATCH {prop} ON GRAPH foo ELEMENTS A (*) FROM role
      case c@RevokePrivilege(_, resources, database, segments, roleNames, revokeType) =>
        val normalizedDatabase = normalizeGraphScope(database)
        (for (roleName <- roleNames; segment <- segments.simplify; resource <- resources.simplify) yield {
          roleName -> (segment, resource)
        }).foldLeft(Some(plans.AssertDbmsAdmin(RevokePrivilegeAction).asInstanceOf[PrivilegePlan])) {
          case (source, (roleName, (segment, resource))) => planRevokes(source, revokeType, (s,r) => Some(plans.RevokeRead(s, resource, normalizedDatabase, segment, roleName, r)))
        }.map(plan => plans.LogSystemCommand(plan, prettifier.asString(c)))

      // SHOW [ALL | USER user | ROLE role] PRIVILEGES
      case ShowPrivileges(scope) =>
        Some(plans.ShowPrivileges(Some(plans.AssertDbmsAdmin(ShowPrivilegeAction)), scope))

      // SHOW DATABASES
      case _: ShowDatabases =>
        Some(plans.ShowDatabases())

      // SHOW DEFAULT DATABASE
      case _: ShowDefaultDatabase =>
        Some(plans.ShowDefaultDatabase())

      // SHOW DATABASE foo
      case ShowDatabase(dbName) =>
        Some(plans.ShowDatabase(new NormalizedDatabaseName(dbName)))

      // CREATE [OR REPLACE] DATABASE foo [IF NOT EXISTS]
      case CreateDatabase(dbName, ifExistsDo) =>
        val normalizedName = new NormalizedDatabaseName(dbName)
        try {
          DatabaseNameValidator.validateExternalDatabaseName(normalizedName)
        } catch {
          case e: IllegalArgumentException => throw new InvalidArgumentException(e.getMessage)
        }
        val source = ifExistsDo match {
          case _: IfExistsReplace => Some(plans.DropDatabase(Some(plans.AssertDbmsAdmin(DropDatabaseAction, CreateDatabaseAction)), normalizedName))
          case _: IfExistsDoNothing => Some(plans.DoNothingIfExists(Some(plans.AssertDbmsAdmin(CreateDatabaseAction)), "Database", normalizedName.name()))
          case _ => Some(plans.AssertDbmsAdmin(CreateDatabaseAction))
        }
        Some(plans.EnsureValidNumberOfDatabases(Some(plans.CreateDatabase(source, normalizedName))))

      // DROP DATABASE foo [IF EXISTS]
      case DropDatabase(dbName, ifExists) =>
        val normalizedName = new NormalizedDatabaseName(dbName)
        val admin = Some(plans.AssertDbmsAdmin(DropDatabaseAction))
        val source = if (ifExists) Some(plans.DoNothingIfNotExists(admin, "Database", normalizedName.name())) else admin
        Some(plans.DropDatabase(Some(plans.EnsureValidNonSystemDatabase(source, normalizedName, "delete")), normalizedName))

      // START DATABASE foo
      case StartDatabase(dbName) =>
        val normalizedName = new NormalizedDatabaseName(dbName)
        Some(plans.StartDatabase(Some(plans.AssertDatabaseAdmin(StartDatabaseAction, normalizedName)), normalizedName))

      // STOP DATABASE foo
      case StopDatabase(dbName) =>
        val normalizedName = new NormalizedDatabaseName(dbName)
        Some(plans.StopDatabase(
          Some(plans.EnsureValidNonSystemDatabase(
            Some(plans.AssertDatabaseAdmin(StopDatabaseAction, normalizedName)), normalizedName, "stop")), normalizedName))

      // Global call: CALL foo.bar.baz("arg1", 2) // only if system procedure is allowed!
      case Query(None, SingleQuery(Seq(resolved@ResolvedCall(signature, _, _, _, _),Return(_,_,_,_,_,_)))) if signature.systemProcedure =>
        val checkCredentialsExpired = signature.name match {
          // TODO this is a hot fix to get browser to get password change required error
          // It should be changed so that only a few key procedures are allowed to be run with password change required
          case QualifiedName(Seq("db"), "indexes") => true
          case _ => false
        }
        val SemanticCheckResult(_, errors) = resolved.semanticCheck(SemanticState.clean)
        errors.foreach { error => throw context.cypherExceptionFactory.syntaxException(error.msg, error.position) }
        Some(plans.SystemProcedureCall(signature.name.toString, from.queryText, context.params, checkCredentialsExpired))

      case _ => None
    }

    val planState = LogicalPlanState(from)

    if (maybeLogicalPlan.isDefined)
      planState.copy(maybeLogicalPlan = maybeLogicalPlan, plannerName = AdministrationPlannerName)
    else planState
  }
}

case object UnsupportedSystemCommand extends Phase[PlannerContext, BaseState, LogicalPlanState] {
  override def phase: CompilationPhase = PIPE_BUILDING

  override def description: String = "Unsupported system command"

  override def postConditions: Set[Condition] = Set.empty

  override def process(from: BaseState, context: PlannerContext): LogicalPlanState = throw new RuntimeException(s"Not a recognised system command or procedure. " +
    s"This Cypher command can only be executed in a user database: ${from.queryText}")
}
