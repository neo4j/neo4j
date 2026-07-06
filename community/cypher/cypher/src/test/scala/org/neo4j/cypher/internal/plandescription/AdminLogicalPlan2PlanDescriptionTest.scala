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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.FileResource
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LoadAllDataAction
import org.neo4j.cypher.internal.ast.LoadAllQualifier
import org.neo4j.cypher.internal.ast.LoadCidrAction
import org.neo4j.cypher.internal.ast.LoadCidrQualifier
import org.neo4j.cypher.internal.ast.LoadUrlAction
import org.neo4j.cypher.internal.ast.LoadUrlQualifier
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NativeAuth
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.Node
import org.neo4j.cypher.internal.ast.OidcCredentialForwarding
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.PasswordChange
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.ProcedureAllQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.Relationship
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.RemoteAliasStoredCredentials
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.Restrict
import org.neo4j.cypher.internal.ast.ShowDatabasesClause
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.logical.plans.AllScope
import org.neo4j.cypher.internal.logical.plans.AllowedNonAdministrationCommands
import org.neo4j.cypher.internal.logical.plans.AlterAuthRule
import org.neo4j.cypher.internal.logical.plans.AlterDatabase
import org.neo4j.cypher.internal.logical.plans.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.AlterServer
import org.neo4j.cypher.internal.logical.plans.AlterShardedDatabase
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.AlterUsers
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDatabaseAction
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActions
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActionsOrSelf
import org.neo4j.cypher.internal.logical.plans.AssertManagementActionNotBlocked
import org.neo4j.cypher.internal.logical.plans.AssertNotBlockedDropAlias
import org.neo4j.cypher.internal.logical.plans.AssertNotBlockedRemoteAliasManagement
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.CopyRolePrivileges
import org.neo4j.cypher.internal.logical.plans.CreateAuthRule
import org.neo4j.cypher.internal.logical.plans.CreateDatabase
import org.neo4j.cypher.internal.logical.plans.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.CreateRole
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.DeallocateServer
import org.neo4j.cypher.internal.logical.plans.DenyDatabaseAction
import org.neo4j.cypher.internal.logical.plans.DenyDbmsAction
import org.neo4j.cypher.internal.logical.plans.DenyGraphAction
import org.neo4j.cypher.internal.logical.plans.DenyLoadAction
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfNotExists
import org.neo4j.cypher.internal.logical.plans.DropAuthRule
import org.neo4j.cypher.internal.logical.plans.DropDatabase
import org.neo4j.cypher.internal.logical.plans.DropDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.DropRole
import org.neo4j.cypher.internal.logical.plans.DropServer
import org.neo4j.cypher.internal.logical.plans.DropUser
import org.neo4j.cypher.internal.logical.plans.EnableServer
import org.neo4j.cypher.internal.logical.plans.EnsureNodeExists
import org.neo4j.cypher.internal.logical.plans.EnsureValidNonSystemDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureValidNumberOfDatabases
import org.neo4j.cypher.internal.logical.plans.GrantDatabaseAction
import org.neo4j.cypher.internal.logical.plans.GrantDbmsAction
import org.neo4j.cypher.internal.logical.plans.GrantGraphAction
import org.neo4j.cypher.internal.logical.plans.GrantLoadAction
import org.neo4j.cypher.internal.logical.plans.GrantRoleToAuthRule
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.HomeScope
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.NamedScope
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.RenameAuthRule
import org.neo4j.cypher.internal.logical.plans.RenameRole
import org.neo4j.cypher.internal.logical.plans.RenameServer
import org.neo4j.cypher.internal.logical.plans.RenameUser
import org.neo4j.cypher.internal.logical.plans.RequireRole
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeGraphAction
import org.neo4j.cypher.internal.logical.plans.RevokeLoadAction
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromAuthRule
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.SetOwnPassword
import org.neo4j.cypher.internal.logical.plans.ShowAliases
import org.neo4j.cypher.internal.logical.plans.ShowAuthRules
import org.neo4j.cypher.internal.logical.plans.ShowCurrentUser
import org.neo4j.cypher.internal.logical.plans.ShowDatabases
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowPrivilegeCommands
import org.neo4j.cypher.internal.logical.plans.ShowPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.ShowRoles
import org.neo4j.cypher.internal.logical.plans.ShowServers
import org.neo4j.cypher.internal.logical.plans.ShowSupportedPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.StandardDatabase
import org.neo4j.cypher.internal.logical.plans.StartDatabase
import org.neo4j.cypher.internal.logical.plans.StopDatabase
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.logical.plans.UserEntity
import org.neo4j.cypher.internal.logical.plans.WaitForCompletion
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.details
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTestBase.planDescription
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.values.virtual.VirtualValues

class AdminLogicalPlan2PlanDescriptionTest extends LogicalPlan2PlanDescriptionTestBase {

  private val privLhsLP = attach(AssertAllowedDbmsActions(ShowUserAction), 2.0, providedOrder = ProvidedOrder.empty)

  private val adminPlanDescription: PlanDescriptionImpl =
    planDescription(id, "AdministrationCommand", Seq.empty, Seq.empty, Set.empty)

  test("AllowedNonAdministrationCommands") {
    assertGood(
      attach(
        AllowedNonAdministrationCommands(
          SingleQuery(Seq(ShowProceduresClause(None, None, List.empty, yieldAll = false, None)(pos)))(pos),
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        AllowedNonAdministrationCommands(
          SingleQuery(Seq(ShowProceduresClause(None, None, List.empty, yieldAll = false, None)(pos)))(pos),
          Some(ShowProcedures(None, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty))
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowProcedures",
        Seq.empty,
        Seq(details("proceduresForUser(all), defaultColumns")),
        Set.empty
      )
    )

    val call = ResolvedNonLocalCall(
      ProcedureSignature(
        ProcedureName(Namespace(List("db", "index", "fulltext"))(pos), "listAvailableAnalyzers")(pos),
        IndexedSeq.empty,
        Some(Vector(
          FieldSignature(
            "analyzer",
            StringType(isNullable = true)(pos),
            description = "The name of the analyzer."
          ),
          FieldSignature(
            "description",
            StringType(isNullable = true)(pos),
            description = "The  description of the analyzer."
          ),
          FieldSignature(
            "stopWords",
            ListType(StringType(isNullable = true)(pos), isNullable = true)(pos),
            description = "The stopwords used by the analyzer to tokenize strings."
          )
        )),
        None,
        ProcedureReadOnlyAccess,
        Some("List the available analyzers that the full-text indexes can be configured with."),
        None,
        eager = false,
        109,
        systemProcedure = true
      ),
      Seq.empty,
      IndexedSeq(ProcedureResultItem(None, Variable("analyzer")(pos, isIsolated = false))(pos))
    )(pos)
    assertGood(
      attach(
        AllowedNonAdministrationCommands(
          SingleQuery(Seq(
            ShowFunctionsClause(AllFunctions, None, None, List.empty, yieldAll = false, None)(pos),
            call
          ))(pos),
          Some(
            ProduceResult.withNoCachedProperties(
              ProcedureCall(
                ShowFunctions(AllFunctions, None, List.empty, List.empty, yieldAll = false, Set.empty, Set.empty),
                call
              ),
              Seq(varFor("a"), varFor("b"), varFor("c\nd"))
            )
          )
        ),
        1.0
      ),
      planDescription(
        id,
        "ProduceResults",
        Seq(
          planDescription(
            id,
            "ProcedureCall",
            Seq(
              planDescription(
                id,
                "ShowFunctions",
                Seq.empty,
                Seq(details("allFunctions, functionsForUser(all), defaultColumns")),
                Set.empty
              )
            ),
            Seq(details("db.index.fulltext.listAvailableAnalyzers() :: (analyzer :: STRING)")),
            Set("analyzer")
          )
        ),
        Seq(details(Seq("a", "b", "`c d`"))),
        Set("analyzer")
      )
    )

    assertGood(
      attach(
        AllowedNonAdministrationCommands(
          SingleQuery(Seq(ShowDatabasesClause(
            AllDatabasesScope()(pos),
            None,
            List.empty,
            yieldAll = false,
            None,
            cypher5ColumnsOnly = false
          )(pos)))(pos),
          Some(ShowDatabases(
            AllDatabasesScope()(pos),
            List.empty,
            List.empty,
            yieldAll = true,
            Set.empty,
            Set.empty
          ))
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowDatabases",
        Seq.empty,
        Seq(details("allDatabases, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        AllowedNonAdministrationCommands(
          SingleQuery(Seq(ShowDatabasesClause(
            AllDatabasesScope()(pos),
            None,
            List.empty,
            yieldAll = false,
            None,
            cypher5ColumnsOnly = true
          )(pos)))(pos),
          None
        ),
        1.0
      ),
      adminPlanDescription
    )
  }

  test("System procedure calls") {
    assertGood(
      attach(
        SystemProcedureCall(
          ResolvedNonLocalCall(
            ProcedureSignature(
              ProcedureName(Namespace(List("db", "index", "fulltext"))(pos), "listAvailableAnalyzers")(pos),
              IndexedSeq.empty,
              Some(Vector(
                FieldSignature(
                  "analyzer",
                  StringType(isNullable = true)(pos),
                  description = "The name of the analyzer."
                ),
                FieldSignature(
                  "description",
                  StringType(isNullable = true)(pos),
                  description = "The  description of the analyzer."
                ),
                FieldSignature(
                  "stopWords",
                  ListType(StringType(isNullable = true)(pos), isNullable = true)(pos),
                  description = "The stopwords used by the analyzer to tokenize strings."
                )
              )),
              None,
              ProcedureReadOnlyAccess,
              Some("List the available analyzers that the full-text indexes can be configured with."),
              None,
              eager = false,
              109,
              systemProcedure = true
            ),
            Seq.empty,
            IndexedSeq(ProcedureResultItem(None, Variable("analyzer")(pos, isIsolated = false))(pos))
          )(pos),
          None,
          VirtualValues.EMPTY_MAP,
          checkCredentialsExpired = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ProcedureCall",
        Seq.empty,
        Seq(details("db.index.fulltext.listAvailableAnalyzers() :: (analyzer :: STRING)")),
        Set.empty
      )
    )
  }

  test("User commands") {
    assertGood(attach(ShowUsers(privLhsLP, withAuth = true, List(), None, None), 1.0), adminPlanDescription)

    assertGood(attach(ShowCurrentUser(List(), None, None), 1.0), adminPlanDescription)

    assertGood(
      attach(
        CreateUser(
          privLhsLP,
          util.Left("name"),
          suspended = None,
          defaultDatabase = None,
          nativeAuth =
            Some(NativeAuth(List(
              Password(varFor("password"), isEncrypted = false)(pos),
              PasswordChange(requireChange = false)(pos)
            ))(pos)),
          externalAuths = Seq.empty,
          tags = None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(RenameUser(privLhsLP, util.Left("user1"), Left("user2")), 1.0), adminPlanDescription)

    assertGood(attach(DropUser(privLhsLP, util.Left("name")), 1.0), adminPlanDescription)

    assertGood(
      attach(
        AlterUser(
          privLhsLP,
          util.Left("name"),
          suspended = Some(false),
          defaultDatabase = None,
          nativeAuth = Some(NativeAuth(List(PasswordChange(requireChange = true)(pos)))(pos)),
          Seq.empty,
          RemoveAuth(all = false, List(stringLiteral("provider"))),
          Seq.empty
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        AlterUsers(
          privLhsLP,
          Seq(util.Left("alice"), util.Left("bob")),
          ifExists = false,
          Seq.empty
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(SetOwnPassword(privLhsLP, stringLiteral("oldPassword"), stringLiteral("newPassword")), 1.0),
      adminPlanDescription
    )
  }

  test("Role commands") {
    assertGood(
      attach(
        ShowRoles(
          privLhsLP,
          withUsers = false,
          withAuthRules = false,
          showAll = true,
          asCommands = false,
          List(),
          None,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(DropRole(privLhsLP, util.Left("role")), 1.0), adminPlanDescription)

    assertGood(attach(CreateRole(privLhsLP, util.Left("role"), immutable = false), 1.0), adminPlanDescription)

    assertGood(attach(RenameRole(privLhsLP, util.Left("role1"), Left("role2")), 1.0), adminPlanDescription)

    assertGood(attach(RequireRole(privLhsLP, util.Left("role")), 1.0), adminPlanDescription)

    assertGood(
      attach(CopyRolePrivileges(privLhsLP, util.Left("role1"), util.Left("role2"), grantDeny = "DENIED"), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(GrantRoleToUser(privLhsLP, util.Left("role"), util.Left("user"), "GRANT ROLE role TO user"), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(RevokeRoleFromUser(privLhsLP, util.Left("role"), util.Left("user"), "REVOKE ROLE role FROM user"), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(GrantRoleToAuthRule(privLhsLP, util.Left("role"), util.Left("rule"), "GRANT ROLE role TO rule"), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(
        RevokeRoleFromAuthRule(privLhsLP, util.Left("role"), util.Left("rule"), "REVOKE ROLE role FROM rule"),
        1.0
      ),
      adminPlanDescription
    )

  }

  test("Auth rule commands") {
    assertGood(
      attach(ShowAuthRules(privLhsLP, asCommands = false, List(), None, None), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(CreateAuthRule(privLhsLP, util.Left("authRule"), literalBoolean(true), Some(true)), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(AlterAuthRule(privLhsLP, util.Left("authRule"), Some(literalBoolean(true)), Some(true)), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(RenameAuthRule(privLhsLP, util.Left("fromAuthRuleName"), util.Left("toAuthRuleName")), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(DropAuthRule(privLhsLP, util.Left("authRule")), 1.0),
      adminPlanDescription
    )
  }

  test("Privilege commands") {
    assertGood(
      attach(
        GrantDbmsAction(
          privLhsLP,
          ExecuteProcedureAction,
          ProcedureAllQualifier()(pos),
          util.Left("role1"),
          immutable = false,
          "GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        DenyDbmsAction(
          privLhsLP,
          ExecuteBoostedProcedureAction,
          ProcedureQualifier("apoc.sin")(pos),
          util.Left("role1"),
          immutable = false,
          "DENY ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        RevokeDbmsAction(
          privLhsLP,
          ExecuteAdminProcedureAction,
          ProcedureAllQualifier()(pos),
          util.Left("role1"),
          "GRANTED",
          immutableOnly = false,
          "REVOKE GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        GrantDatabaseAction(
          privLhsLP,
          CreateNodeLabelAction,
          NamedScope(NamespacedName("foo")(pos)),
          UserAllQualifier()(pos),
          util.Left("role1"),
          immutable = false,
          "GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        DenyDatabaseAction(
          privLhsLP,
          CreateNodeLabelAction,
          AllScope,
          UserQualifier(literalString("user1"))(pos),
          util.Left("role1"),
          immutable = false,
          "DENY ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        RevokeDatabaseAction(
          privLhsLP,
          CreateNodeLabelAction,
          AllScope,
          UserQualifier(literalString("user1"))(pos),
          util.Left("role1"),
          "GRANTED",
          immutableOnly = false,
          "REVOKE GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        GrantGraphAction(
          privLhsLP,
          TraverseAction,
          NoResource()(pos),
          HomeScope,
          LabelQualifier("Label1")(pos),
          util.Left("role1"),
          immutable = false,
          "GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        DenyGraphAction(
          privLhsLP,
          ReadAction,
          AllPropertyResource()(pos),
          HomeScope,
          PatternQualifier(
            Seq(LabelQualifier("Label1")(pos)),
            Some(varFor("n")),
            Equals(Property(varFor("n"), PropertyKeyName("prop1")(pos))(pos), Null.NULL)(pos),
            Node
          ),
          util.Left("role1"),
          immutable = false,
          "DENY ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        DenyGraphAction(
          privLhsLP,
          ReadAction,
          AllPropertyResource()(pos),
          HomeScope,
          PatternQualifier(
            Seq(RelationshipQualifier("R")(pos)),
            Some(varFor("n")),
            Equals(Property(varFor("n"), PropertyKeyName("prop1")(pos))(pos), Null.NULL)(pos),
            Relationship
          ),
          util.Left("role1"),
          immutable = false,
          "DENY ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        RevokeGraphAction(
          privLhsLP,
          WriteAction,
          NoResource()(pos),
          AllScope,
          ElementsAllQualifier()(pos),
          util.Left("role1"),
          "GRANTED",
          immutableOnly = false,
          "REVOKE GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        GrantLoadAction(
          privLhsLP,
          LoadAllDataAction,
          FileResource()(pos),
          LoadAllQualifier()(pos),
          util.Left("role1"),
          immutable = false,
          "GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        DenyLoadAction(
          privLhsLP,
          LoadCidrAction,
          FileResource()(pos),
          LoadCidrQualifier(Left("cidr"))(pos),
          util.Left("role1"),
          immutable = false,
          "DENY ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        RevokeLoadAction(
          privLhsLP,
          LoadUrlAction,
          FileResource()(pos),
          LoadUrlQualifier(Right(parameter("url", CTString)))(pos),
          util.Left("role1"),
          "GRANTED",
          immutableOnly = false,
          "REVOKE GRANT ..."
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        ShowSupportedPrivileges(
          List(),
          None,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        ShowPrivileges(
          Some(privLhsLP),
          ShowUsersPrivileges(List(literalString("user1"), literalString("user2")))(pos),
          List(),
          None,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        ShowPrivilegeCommands(
          Some(privLhsLP),
          ShowUsersPrivileges(List(literalString("user1"), literalString("user2")))(pos),
          asRevoke = false,
          List(),
          None,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )
  }

  test("Database commands") {
    assertGood(
      attach(
        CreateDatabase(privLhsLP, util.Left("db1"), NoOptions, IfExistsDoNothing, StandardDatabase, None, None),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(DropDatabase(privLhsLP, NamespacedName("db1")(pos), DumpData, forceComposite = false, Restrict), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(
        AlterDatabase(
          privLhsLP,
          NamespacedName("db1")(pos),
          Some(ReadOnlyAccess),
          None,
          NoOptions,
          None,
          Set.empty,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        AlterDatabase(
          privLhsLP,
          NamespacedName("db1")(pos),
          Some(ReadWriteAccess),
          None,
          NoOptions,
          None,
          Set.empty,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        AlterShardedDatabase(privLhsLP, NamespacedName("db1")(pos), Some(ReadWriteAccess), NoOptions, None, None),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(StartDatabase(privLhsLP, NamespacedName("db1")(pos)), 1.0), adminPlanDescription)

    assertGood(attach(StopDatabase(privLhsLP, NamespacedName("db1")(pos)), 1.0), adminPlanDescription)
  }

  test("Server commands") {
    assertGood(
      attach(EnableServer(privLhsLP, Left("s1"), NoOptions), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(AlterServer(privLhsLP, Left("s1"), NoOptions), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(RenameServer(privLhsLP, Left("s1"), Left("s2")), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(DropServer(privLhsLP, Left("s1")), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(ShowServers(privLhsLP, verbose = false, List.empty, None, None), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(DeallocateServer(privLhsLP, dryRun = false, Seq(Left("s1"))), 1.0),
      adminPlanDescription
    )
  }

  test("Alias commands") {
    assertGood(
      attach(
        CreateLocalDatabaseAlias(
          privLhsLP,
          NamespacedName("alias1")(pos),
          NamespacedName("db1")(pos),
          None,
          replace = false
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        CreateLocalDatabaseAlias(
          privLhsLP,
          NamespacedName("alias1")(pos),
          NamespacedName("db1")(pos),
          Some(util.Left(Map("a" -> stringLiteral("b")))),
          replace = false
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        CreateRemoteDatabaseAlias(
          privLhsLP,
          NamespacedName("alias1")(pos),
          NamespacedName("db1")(pos),
          replace = false,
          util.Left("url"),
          RemoteAliasStoredCredentials(
            varFor("user"),
            varFor("password")
          )(pos),
          None,
          None,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        CreateRemoteDatabaseAlias(
          privLhsLP,
          NamespacedName("alias1")(pos),
          NamespacedName("db1")(pos),
          replace = false,
          util.Left("url"),
          OidcCredentialForwarding()(pos),
          None,
          None,
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(DropDatabaseAlias(privLhsLP, NamespacedName("alias1")(pos)), 1.0), adminPlanDescription)

    assertGood(
      attach(
        AlterLocalDatabaseAlias(
          privLhsLP,
          NamespacedName("alias1")(pos),
          Some(NamespacedName("db2")(pos)),
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        AlterRemoteDatabaseAlias(
          privLhsLP,
          NamespacedName("alias1")(pos),
          Some(NamespacedName("db2")(pos)),
          Some(util.Left("url")),
          Some(util.Left("user")),
          Some(varFor("password")),
          None,
          Some(Left(Map("some" -> StringLiteral("prop")(pos.withInputLength(0))))),
          None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(ShowAliases(privLhsLP, None, verbose = false, List.empty, None, None), 1.0), adminPlanDescription)

    assertGood(
      attach(
        ShowAliases(privLhsLP, Some(NamespacedName("alias1")(pos)), verbose = false, List.empty, None, None),
        1.0
      ),
      adminPlanDescription
    )
  }

  test("Various admin helpers and assert plans") {
    assertGood(
      attach(EnsureValidNonSystemDatabase(privLhsLP, "ALTER DATABASE", NamespacedName("db1")(pos), "action1"), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(
        EnsureValidNumberOfDatabases(CreateDatabase(
          privLhsLP,
          util.Left("db1"),
          NoOptions,
          IfExistsDoNothing,
          StandardDatabase,
          None,
          None
        )),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(LogSystemCommand(privLhsLP, "command1"), 1.0), adminPlanDescription)

    assertGood(
      attach(DoNothingIfNotExists(privLhsLP, "DROP USER", UserEntity, util.Left("user1"), "delete"), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(DoNothingIfExists(privLhsLP, "DROP USER", UserEntity, util.Left("user1")), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(
        EnsureNodeExists(
          privLhsLP,
          "DROP USER",
          UserEntity,
          util.Left("user1"),
          labelDescription = "User",
          action = "delete"
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        AssertNotCurrentUser(
          privLhsLP,
          util.Left("user1"),
          "verb1",
          "validation message",
          ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N42).build()
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(AssertAllowedDbmsActionsOrSelf(util.Left("user1"), DropRoleAction), 1.0), adminPlanDescription)

    assertGood(
      attach(AssertAllowedDatabaseAction(StopDatabaseAction, NamespacedName("db1")(pos), None), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(AssertManagementActionNotBlocked("CREATE DATABASE", CreateDatabaseAction), 1.0),
      adminPlanDescription
    )

    assertGood(attach(AssertNotBlockedRemoteAliasManagement(), 1.0), adminPlanDescription)

    assertGood(attach(AssertNotBlockedDropAlias(NamespacedName("alias")(pos)), 1.0), adminPlanDescription)

    assertGood(
      attach(
        WaitForCompletion(
          StartDatabase(
            AssertAllowedDatabaseAction(StartDatabaseAction, NamespacedName("db1")(pos), Some(privLhsLP)),
            NamespacedName("db1")(pos)
          ),
          NamespacedName("db1")(pos),
          IndefiniteWait()(pos)
        ),
        1.0
      ),
      adminPlanDescription
    )
  }

}
