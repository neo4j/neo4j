/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.generator

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Access
import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.ActionResourceBase
import org.neo4j.cypher.internal.ast.AddTags
import org.neo4j.cypher.internal.ast.AdditiveProjection
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AdministrationCommand.NATIVE_AUTH
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AllAliasManagementActions
import org.neo4j.cypher.internal.ast.AllAuthRuleActions
import org.neo4j.cypher.internal.ast.AllConstraintActions
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllDatabaseAction
import org.neo4j.cypher.internal.ast.AllDatabaseManagementActions
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllDbmsAction
import org.neo4j.cypher.internal.ast.AllExistsConstraints
import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.AllGraphAction
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllIndexActions
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllPrivilegeActions
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AllQualifier
import org.neo4j.cypher.internal.ast.AllRoleActions
import org.neo4j.cypher.internal.ast.AllSecretManagementActions
import org.neo4j.cypher.internal.ast.AllTokenActions
import org.neo4j.cypher.internal.ast.AllTransactionActions
import org.neo4j.cypher.internal.ast.AllUserActions
import org.neo4j.cypher.internal.ast.AllUserMetadataActions
import org.neo4j.cypher.internal.ast.AlterAliasAction
import org.neo4j.cypher.internal.ast.AlterAuthRule
import org.neo4j.cypher.internal.ast.AlterAuthRuleAction
import org.neo4j.cypher.internal.ast.AlterCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AlterUserAction
import org.neo4j.cypher.internal.ast.AlterUsers
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.AstHint
import org.neo4j.cypher.internal.ast.Auth
import org.neo4j.cypher.internal.ast.AuthId
import org.neo4j.cypher.internal.ast.AuthRuleCondition
import org.neo4j.cypher.internal.ast.AuthRuleEnabled
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CascadeAliases
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CommaSeparatedNames
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.CommandClauseNames
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.CompositeDatabaseManagementActions
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateAliasAction
import org.neo4j.cypher.internal.ast.CreateAuthRule
import org.neo4j.cypher.internal.ast.CreateAuthRuleAction
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.CreateConstraint
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateReplicaDatabase
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabaseAndDbmsAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabasePrivilegeQualifier
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultWith
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropAliasAction
import org.neo4j.cypher.internal.ast.DropAuthRule
import org.neo4j.cypher.internal.ast.DropAuthRuleAction
import org.neo4j.cypher.internal.ast.DropCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.DropConstraintAction
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropServer
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.EdgeType
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByLabel
import org.neo4j.cypher.internal.ast.EdgeTypeReferenceByVariable
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.EmptyNodeTypeReference
import org.neo4j.cypher.internal.ast.EnableServer
import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.ExpandHintAll
import org.neo4j.cypher.internal.ast.ExpandHintInto
import org.neo4j.cypher.internal.ast.ExpandStep
import org.neo4j.cypher.internal.ast.ExplicitGroupingElements
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.ExpressionNames
import org.neo4j.cypher.internal.ast.FileResource
import org.neo4j.cypher.internal.ast.Finish
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToAuthRules
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphPrivilegeQualifier
import org.neo4j.cypher.internal.ast.GraphType
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.ExistenceConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.GraphTypeConstraintBody
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.KeyConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.PropertyTypeConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraint.UniquenessConstraint
import org.neo4j.cypher.internal.ast.GraphTypeConstraintDefinition
import org.neo4j.cypher.internal.ast.GraphTypeConstraintName
import org.neo4j.cypher.internal.ast.GraphTypeElementReference
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.GroupingAll
import org.neo4j.cypher.internal.ast.GroupingElements
import org.neo4j.cypher.internal.ast.GroupingNone
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.ImpersonateUserAction
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.Insert
import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsNotNormalized
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadActions
import org.neo4j.cypher.internal.ast.LoadAllDataAction
import org.neo4j.cypher.internal.ast.LoadAllQualifier
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.LoadCidrAction
import org.neo4j.cypher.internal.ast.LoadCidrQualifier
import org.neo4j.cypher.internal.ast.LoadPrivilege
import org.neo4j.cypher.internal.ast.LoadPrivilegeQualifier
import org.neo4j.cypher.internal.ast.LoadUrlAction
import org.neo4j.cypher.internal.ast.LoadUrlQualifier
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.LocalFieldSignature
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.MatchAction
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.NoNames
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.Node
import org.neo4j.cypher.internal.ast.NodeAllExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropExistsConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeType
import org.neo4j.cypher.internal.ast.NodeTypeReference
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByIdentifyingLabel
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByLabel
import org.neo4j.cypher.internal.ast.NodeTypeReferenceByVariable
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.OidcCredentialForwarding
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.OrderByOrPaginationWithType
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.ParsedAsLimit
import org.neo4j.cypher.internal.ast.ParsedAsOrderBy
import org.neo4j.cypher.internal.ast.ParsedAsSkip
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.Password
import org.neo4j.cypher.internal.ast.PasswordChange
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PrivilegeCommand
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.PropExistsConstraints
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.PropertyType
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineConstraintBody
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineKeyConstraint
import org.neo4j.cypher.internal.ast.PropertyType.PropertyInlineUniquenessConstraint
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadSecretsAction
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.RelAllExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropExistsConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.Relationship
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.RemoteAliasCredentials
import org.neo4j.cypher.internal.ast.RemoteAliasStoredCredentials
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveAllTags
import org.neo4j.cypher.internal.ast.RemoveAuth
import org.neo4j.cypher.internal.ast.RemoveDynamicPropertyItem
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RemoveItem
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePrivilegeAction
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.RemoveRoleAction
import org.neo4j.cypher.internal.ast.RemoveTags
import org.neo4j.cypher.internal.ast.RenameAuthRule
import org.neo4j.cypher.internal.ast.RenameAuthRuleAction
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameRoleAction
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.RenameUserAction
import org.neo4j.cypher.internal.ast.Restrict
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromAuthRules
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.RevokeType
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SecretQualifier
import org.neo4j.cypher.internal.ast.ServerManagementAction
import org.neo4j.cypher.internal.ast.SetAuthAction
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.SetDatabaseDefaultLanguageAction
import org.neo4j.cypher.internal.ast.SetDynamicPropertyItem
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetItem
import org.neo4j.cypher.internal.ast.SetLabelAction
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetOwnPassword
import org.neo4j.cypher.internal.ast.SetPasswordsAction
import org.neo4j.cypher.internal.ast.SetPropertyAction
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SetTags
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetUserMetadataAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.ShardDefinition
import org.neo4j.cypher.internal.ast.ShowAliasAction
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowAuthRuleAction
import org.neo4j.cypher.internal.ast.ShowAuthRules
import org.neo4j.cypher.internal.ast.ShowConstraintAction
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentGraphTypeClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabasesClause
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexAction
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowSecretsAction
import org.neo4j.cypher.internal.ast.ShowServerAction
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSettingAction
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUserMetadataAction
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleNamedDatabaseScope
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsBatchParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.TransactionManagementAction
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.ast.UserOptions
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.UserTagsAction
import org.neo4j.cypher.internal.ast.UsingExpandHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOrScan
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.ast.VectorValueConstructor
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.ast.WriteSecretsAction
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.YieldOrWhere
import org.neo4j.cypher.internal.ast.generator.AstGenerator.boolean
import org.neo4j.cypher.internal.ast.generator.AstGenerator.listSetOfSizeBetween
import org.neo4j.cypher.internal.ast.generator.AstGenerator.oneOrMore
import org.neo4j.cypher.internal.ast.generator.AstGenerator.tuple
import org.neo4j.cypher.internal.ast.generator.AstGenerator.twoOrMore
import org.neo4j.cypher.internal.ast.generator.AstGenerator.validString
import org.neo4j.cypher.internal.ast.generator.AstGenerator.zeroOrMore
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.AllReducePredicate
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.DynamicLabelExpression
import org.neo4j.cypher.internal.expressions.DynamicLabelOrRelTypeExpression
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.EntityType
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasALabelOrType
import org.neo4j.cypher.internal.expressions.HasAnyDynamicLabel
import org.neo4j.cypher.internal.expressions.HasAnyDynamicLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasAnyDynamicType
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasDynamicLabels
import org.neo4j.cypher.internal.expressions.HasDynamicLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasDynamicType
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.IterablePredicateExpression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.MapProjectionElement
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.MatchMode
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NFCNormalForm
import org.neo4j.cypher.internal.expressions.NFDNormalForm
import org.neo4j.cypher.internal.expressions.NFKCNormalForm
import org.neo4j.cypher.internal.expressions.NFKDNormalForm
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.NormalForm
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathLengthQuantifier
import org.neo4j.cypher.internal.expressions.PathMode
import org.neo4j.cypher.internal.expressions.PathMode.Acyclic
import org.neo4j.cypher.internal.expressions.PathMode.Trail
import org.neo4j.cypher.internal.expressions.PathMode.Walk
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPart.AllShortestPaths
import org.neo4j.cypher.internal.expressions.PatternPart.AnyPath
import org.neo4j.cypher.internal.expressions.PatternPart.AnyShortestPath
import org.neo4j.cypher.internal.expressions.PatternPart.Selector
import org.neo4j.cypher.internal.expressions.PatternPart.ShortestGroups
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyExists
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableSelector
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.DynamicLeaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.ProcedureOutput
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTFloat32
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTInteger16
import org.neo4j.cypher.internal.util.symbols.CTInteger32
import org.neo4j.cypher.internal.util.symbols.CTInteger8
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.PropertyValueCypher5Type
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.UUIDType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.util.UnicodeHelper
import org.reflections.Reflections
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.alphaLowerChar
import org.scalacheck.Gen.choose
import org.scalacheck.Gen.chooseNum
import org.scalacheck.Gen.const
import org.scalacheck.Gen.frequency
import org.scalacheck.Gen.listOfN
import org.scalacheck.Gen.lzy
import org.scalacheck.Gen.nonEmptyListOf
import org.scalacheck.Gen.oneOf
import org.scalacheck.Gen.option
import org.scalacheck.Gen.pick
import org.scalacheck.Gen.posNum
import org.scalacheck.Gen.sequence
import org.scalacheck.Gen.some
import org.scalacheck.util.Buildable

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.ArraySeq
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.Random

object AstGenerator {
  val OR_MORE_UPPER_BOUND = 3

  def zeroOrMore[T](gen: Gen[T]): Gen[List[T]] =
    choose(0, OR_MORE_UPPER_BOUND).flatMap(listOfN(_, gen))

  def zeroOrMore[T](seq: Seq[T]): Gen[Seq[T]] =
    choose(0, Math.min(OR_MORE_UPPER_BOUND, seq.size)).flatMap(pick(_, seq)).map(_.toSeq)

  def oneOrMore[T](gen: Gen[T]): Gen[List[T]] =
    choose(1, OR_MORE_UPPER_BOUND).flatMap(listOfN(_, gen))

  def oneOrMore[T](seq: Seq[T]): Gen[Seq[T]] =
    choose(1, Math.min(OR_MORE_UPPER_BOUND, seq.size)).flatMap(pick(_, seq)).map(_.toSeq)

  def twoOrMore[T](gen: Gen[T]): Gen[List[T]] =
    choose(2, OR_MORE_UPPER_BOUND).flatMap(listOfN(_, gen))

  def twoOrMore[T](seq: Seq[T]): Gen[Seq[T]] =
    choose(2, Math.min(OR_MORE_UPPER_BOUND, seq.size)).flatMap(pick(_, seq)).map(_.toSeq)

  def tuple[A, B](ga: Gen[A], gb: Gen[B]): Gen[(A, B)] = for {
    a <- ga
    b <- gb
  } yield (a, b)

  def boolean: Gen[Boolean] =
    Arbitrary.arbBool.arbitrary

  def char: Gen[Char] =
    Arbitrary.arbChar.arbitrary.suchThat(acceptedChar)

  // It is difficult to randomly generate a valid unicode string, so this rejects any string
  // that may contain a unicode looking sequence to avoid parser errors.
  def validString: Gen[String] = oneOf(
    alphaLowerChar.map(_.toString),
    nonEmptyListOf(char).map(_.mkString).suchThat { chars =>
      // Is isIdentifier really neeed, isn't that handled by the prettifier?
      !chars.contains("\\u") && UnicodeHelper.isIdentifierInAllVersions(chars)
    }
  )

  def acceptedChar(c: Char): Boolean = {
    val DEL_ERROR = '\ufdea'
    val INS_ERROR = '\ufdeb'
    val RESYNC = '\ufdec'
    val RESYNC_START = '\ufded'
    val RESYNC_END = '\ufdee'
    val RESYNC_EOI = '\ufdef'
    val EOI = '\uffff'

    c match {
      case DEL_ERROR    => false
      case INS_ERROR    => false
      case RESYNC       => false
      case RESYNC_START => false
      case RESYNC_END   => false
      case RESYNC_EOI   => false
      case EOI          => false
      case _            => true
    }
  }

  /*
   * Simulate a ListSet of size between minSize and maxSize, i.e. make a list of the correct size with unique values
   */
  def listSetOfSizeBetween[T](minSize: Int, maxSize: Int, elementGenerator: Gen[T]): Gen[List[T]] = {
    for {
      nbrElements <- choose(minSize, maxSize)
      elements <- setOfSize(nbrElements, Set.empty, elementGenerator)
    } yield elements.toList
  }

  def setOfSize[T](size: Int, setSoFar: Set[T], elementGenerator: Gen[T]): Gen[Set[T]] = {
    if (setSoFar.size == size) Gen.const(setSoFar)
    else {
      for {
        element <- elementGenerator
        newSet = setSoFar + element
        result <- setOfSize(size, newSet, elementGenerator)
      } yield result
    }
  }
}

/**
 * Random query generation
 * Implements instances of Gen[T] for all query ast nodes
 * Generated queries are syntactically (but not semantically) valid
 *
 * Limits AST depth with the size parameter to try to avoid stack overflows.
 */
//noinspection ScalaWeakerAccess
class AstGenerator(
  simpleStrings: Boolean = true,
  allowedVarNames: Option[Seq[String]] = None,
  allowedParamNames: Option[Seq[String]] = None,
  val whenAstDifferUseCypherVersion: CypherVersion = CypherVersion.Legacy.legacyVersion()
) {
  // HELPERS
  // ==========================================================================

  private val anonVarNameCount = new AtomicLong()

  protected val pos: InputPosition.Range = InputPosition.NONE

  private val usesCypher5 = whenAstDifferUseCypherVersion == CypherVersion.Cypher5

  def string: Gen[String] =
    if (simpleStrings) alphaLowerChar.map(_.toString)
    else validString

  def anonVariable(): Variable =
    Variable("anonVariableWithReallyUniqueName" + anonVarNameCount.getAndIncrement())(pos, false)

  // IDENTIFIERS
  // ==========================================================================

  def _identifier: Gen[String] =
    if (simpleStrings) alphaLowerChar.map(_.toString)
    else validString

  def _labelName: Gen[LabelName] =
    _identifier.map(LabelName(_)(pos))

  def _relTypeName: Gen[RelTypeName] =
    _identifier.map(RelTypeName(_)(pos))

  def _labelOrTypeName: Gen[LabelOrRelTypeName] =
    _identifier.map(LabelOrRelTypeName(_)(pos))

  def _propertyKeyName: Gen[PropertyKeyName] =
    _identifier.map(PropertyKeyName(_)(pos))

  // EXPRESSIONS
  // ==========================================================================

  // LEAFS
  // ----------------------------------

  def _nullLit: Gen[Null] =
    const(Null.NULL)

  def _stringLit: Gen[StringLiteral] =
    string.flatMap(StringLiteral(_)(pos.withInputLength(0)))

  def _sensitiveStringLiteral: Gen[SensitiveStringLiteral] =
    // Needs to be '******' since all sensitive strings get rendered as such
    // Would normally get rewritten as SensitiveAutoParameter which can be generated as parameter when needed
    const(SensitiveStringLiteral("******".getBytes(StandardCharsets.UTF_8))(pos.withInputLength(0)))

  def _booleanLit: Gen[BooleanLiteral] =
    oneOf(True()(pos), False()(pos))

  def _infinityLit: Gen[Literal] =
    const(Infinity()(pos))

  def _nanLit: Gen[Literal] =
    const(NaN()(pos))

  def _unsignedIntString(prefix: String, radix: Int): Gen[String] = for {
    num <- posNum[Int]
    str = Integer.toString(num, radix)
  } yield List(prefix, str).mkString

  def _signedIntString(prefix: String, radix: Int): Gen[String] = for {
    str <- _unsignedIntString(prefix, radix)
    neg <- boolean
    sig = if (neg) "-" else ""
  } yield List(sig, str).mkString

  def _pathLengthQuantifier: Gen[PathLengthQuantifier] =
    _unsignedIntString("", 10).map(PathLengthQuantifier(_)(pos))

  def _unsignedDecIntLit: Gen[UnsignedDecimalIntegerLiteral] =
    _unsignedIntString("", 10).map(UnsignedDecimalIntegerLiteral(_)(pos))

  def _signedDecIntLit: Gen[SignedDecimalIntegerLiteral] =
    _signedIntString("", 10).map(SignedDecimalIntegerLiteral(_)(pos))

  def _signedHexIntLit: Gen[SignedHexIntegerLiteral] =
    _signedIntString("0x", 16).map(SignedHexIntegerLiteral(_)(pos))

  def _signedOctIntLit: Gen[SignedOctalIntegerLiteral] =
    _signedIntString("0o", 8).map(SignedOctalIntegerLiteral(_)(pos))

  def _doubleLit: Gen[DecimalDoubleLiteral] =
    Arbitrary.arbDouble.arbitrary.map(_.toString).map(DecimalDoubleLiteral(_)(pos))

  def _parameter: Gen[Parameter] = {
    val nameGen = allowedParamNames match {
      case None        => _identifier
      case Some(Seq()) => const("").suchThat(_ => false)
      case Some(names) => oneOf(names)
    }
    for {
      name <- nameGen
    } yield ExplicitParameter(name, AnyType(isNullable = true)(pos))(pos)
  }

  def _stringParameter: Gen[Parameter] = _identifier.map(ExplicitParameter(_, CTString)(pos))

  def _mapParameter: Gen[Parameter] = _identifier.map(ExplicitParameter(_, CTMap)(pos))

  def _intParameter: Gen[Parameter] = _identifier.map(ExplicitParameter(_, CTInteger)(pos))

  def _sensitiveStringParameter: Gen[Parameter with SensitiveParameter] =
    _identifier.map(new ExplicitParameter(_, CTString)(pos) with SensitiveParameter)

  def _sensitiveAutoStringParameter: Gen[Parameter with SensitiveAutoParameter] =
    _identifier.map(new ExplicitParameter(_, CTString)(pos) with SensitiveAutoParameter)

  def _variable: Gen[Variable] = {
    val nameGen = allowedVarNames match {
      case None        => _identifier
      case Some(Seq()) => const("").suchThat(_ => false)
      case Some(names) => oneOf(names)
    }
    for {
      name <- nameGen
    } yield Variable(name)(pos, Variable.isIsolatedDefault)
  }

  // Predicates
  // ----------------------------------

  def _predicateComparisonPar(l: Expression, r: Expression): Gen[Expression] = oneOf(
    GreaterThanOrEqual(l, r)(pos),
    GreaterThan(l, r)(pos),
    LessThanOrEqual(l, r)(pos),
    LessThan(l, r)(pos),
    Equals(l, r)(pos),
    NotEquals(l, r)(pos),
    InvalidNotEquals(l, r)(pos)
  )

  def _predicateComparison: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    res <- _predicateComparisonPar(l, r)
  } yield res

  /*
   * A predicate comparison chain (e.g. a > b < c = d) must contain at least 2 comparisons and therefore at least
   * 3 expressions to use Ands. Since Ands is using a ListSet, duplicates will be removed in AST.
   *
   * Example 1:
   *   1. Original query: RETURN a > b = a > b
   *   2. Original AST (pseudo code): ... Ands(ListSet((a > b),(b = a))) ...
   *   3. Prettified query: RETURN a > b = a
   *   4. Prettified AST (pseudo code): ... Ands(ListSet((a > b),(b = a))) ...
   *
   * Example 2:
   *   1. Original query: RETURN a > a > a
   *   2. Original AST (pseudo code): ... Ands(ListSet((a > a))) ...
   *   3. Prettified query: RETURN a > a
   *   4. Prettified AST (pseudo code): ... (a > a) ...
   *
   * The AST generator is used by PrettifierPropertyTest to test the round-trip of step 2 - 4 above.
   * For both examples the queries in step 1 and 3 are different but equivalent.
   * For Example 1, the AST in step 2 and 4 is the same.
   * For Example 2, the AST in step 2 and 4 is different <= this will lead to an error in PrettifierPropertyTest.
   *
   * As we consider this difference in AST acceptable,
   * we will make sure to generate at least 3 unique expressions to avoid to end up in Example 2.
   */
  def _predicateComparisonChain: Gen[Expression] = for {
    exprs <- listSetOfSizeBetween(3, 5, _expression)
    pairs = exprs.sliding(2)
    gens = pairs.map(p => _predicateComparisonPar(p.head, p.last)).toList
    chain <- sequence(gens)(Buildable.buildableFactory)
  } yield Ands(chain)(pos)

  def _predicateUnary: Gen[Expression] = for {
    r <- _expression
    typeName <- _cypherTypeName
    normalForm <- _normalForm
    containsIs <- boolean
    dynamicLabelsAllowed <- boolean
    labelExpression <- _labelExpression(None, containsIs, dynamicLabelsAllowed)
    propertyKey <- _propertyKeyName
    baseUnary = Seq(
      Not(r)(pos),
      IsNull(r)(pos),
      IsNotNull(r)(pos),
      IsTyped(r, typeName)(pos, IsTyped.withDoubleColonOnlyDefault),
      IsNotTyped(r, typeName)(pos),
      IsNormalized(r, normalForm)(pos),
      IsNotNormalized(r, normalForm)(pos),
      LabelExpressionPredicate(r, labelExpression)(pos, isParenthesized = usesCypher5)
    )
    res <- if (usesCypher5) oneOf(baseUnary)
    else oneOf(oneOf(baseUnary), for { v <- _variable } yield PropertyExists(v, propertyKey)(pos))
  } yield res

  def _predicateBinary: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    res <- oneOf(
      And(l, r)(pos),
      Or(l, r)(pos),
      Xor(l, r)(pos),
      RegexMatch(l, r)(pos),
      In(l, r)(pos),
      StartsWith(l, r)(pos),
      EndsWith(l, r)(pos),
      Contains(l, r)(pos)
    )
  } yield res

  // Collections
  // ----------------------------------

  def _map: Gen[MapExpression] = for {
    items <- zeroOrMore(tuple(_propertyKeyName, _expression))
  } yield MapExpression(items)(pos)

  def _property: Gen[Property] = for {
    map <- _expression
    key <- _propertyKeyName
  } yield Property(map, key)(pos)

  def _mapProjectionElement: Gen[MapProjectionElement] =
    oneOf(
      for { key <- _propertyKeyName; exp <- _expression } yield LiteralEntry(key, exp)(pos),
      for { id <- _variable } yield VariableSelector(id)(pos),
      for { key <- _propertyKeyName } yield PropertySelector(key)(pos),
      const(AllPropertiesSelector()(pos))
    )

  def _mapProjection: Gen[MapProjection] = for {
    name <- _variable
    items <- oneOrMore(_mapProjectionElement)
  } yield MapProjection(name, items)(pos)

  def _list: Gen[ListLiteral] =
    _listOf(_expression)

  def _listOf(expressionGen: Gen[Expression]): Gen[ListLiteral] = for {
    parts <- zeroOrMore(expressionGen)
  } yield ListLiteral(parts)(pos)

  def _listSlice: Gen[ListSlice] = for {
    list <- _expression
    from <- option(_expression)
    to <- option(_expression)
  } yield ListSlice(list, from, to)(pos)

  def _containerIndex: Gen[ContainerIndex] = for {
    expr <- _expression
    idx <- _expression
  } yield ContainerIndex(expr, idx)(pos)

  def _filterScope: Gen[FilterScope] = for {
    variable <- _variable
    innerPredicate <- _expression
  } yield FilterScope(variable, Some(innerPredicate))(pos)

  def _extractScope: Gen[ExtractScope] = for {
    variable <- _variable
    innerPredicate <- option(_expression)
    extractExpression <- option(_expression)
  } yield ExtractScope(variable, innerPredicate, extractExpression)(pos)

  def _listComprehension: Gen[ListComprehension] = for {
    scope <- _extractScope
    expression <- _expression
  } yield ListComprehension(scope, expression)(pos)

  def _iterablePredicate: Gen[IterablePredicateExpression] = for {
    scope <- _filterScope
    expression <- _expression
    predicate <- oneOf(
      AllIterablePredicate(scope, expression)(pos),
      AnyIterablePredicate(scope, expression)(pos),
      NoneIterablePredicate(scope, expression)(pos),
      SingleIterablePredicate(scope, expression)(pos)
    )
  } yield predicate

  def _reduceScope: Gen[ReduceScope] = for {
    accumulator <- _variable
    variable <- _variable
    expression <- _expression
  } yield ReduceScope(accumulator, variable, expression)(pos)

  def _reduceExpr: Gen[ReduceExpression] = for {
    scope <- _reduceScope
    init <- _expression
    list <- _expression
  } yield ReduceExpression(scope, init, list)(pos)

  def _allReducePredicate: Gen[AllReducePredicate] = for {
    accumulator <- _variable
    reductionStepVariable <- _variable
    reductionStep <- _expression
    predicate <- _expression
    init <- _expression
    list <- _expression
  } yield {
    AllReducePredicate(
      accumulator = accumulator,
      init = init,
      reductionStepVariable = reductionStepVariable,
      list = list,
      reductionStep = reductionStep,
      predicate = predicate,
      pos = pos
    )
  }

  // Arithmetic
  // ----------------------------------

  def _arithmeticUnary: Gen[Expression] = for {
    r <- _expression
    exp <- oneOf(
      UnaryAdd(r)(pos),
      UnarySubtract(r)(pos)
    )
  } yield exp

  def _arithmeticBinary: Gen[Expression] = for {
    l <- _expression
    r <- _expression
    exp <- oneOf(
      Add(l, r)(pos),
      Multiply(l, r)(pos),
      Divide(l, r)(pos),
      Pow(l, r)(pos),
      Modulo(l, r)(pos),
      Subtract(l, r)(pos)
    )
  } yield exp

  def _case: Gen[CaseExpression] = oneOf(_simpleCase, _generalCase)

  def _simpleCase: Gen[CaseExpression] = for {
    expression <- _expression
    caseOperand <- option(expression)
    alternatives <- oneOrMore(tuple(Equals(expression, expression)(pos), _expression))
    default <- option(_expression)
  } yield CaseExpression(caseOperand, alternatives, default)(pos)

  def _generalCase: Gen[CaseExpression] = for {
    alternatives <- oneOrMore(tuple(_expression, _expression))
    default <- option(_expression)
  } yield CaseExpression(None, alternatives, default)(pos)

  // Functions
  // ----------------------------------

  def _namespace: Gen[Namespace] = for {
    parts <- zeroOrMore(_identifier)
  } yield Namespace(parts)(pos)

  def _functionInvocation: Gen[FunctionInvocation] = for {
    namespace <- _namespace
    functionName <- _identifier
    distinct <- boolean
    args <- zeroOrMore(_expression)
  } yield FunctionInvocation(FunctionName(namespace, functionName)(pos), distinct, args.toIndexedSeq)(pos)

  def _glob: Gen[String] = for {
    parts <- zeroOrMore(_identifier)
    name <- _identifier
  } yield parts.mkString(".") ++ name

  def _countStar: Gen[CountStar] =
    const(CountStar()(pos))

  def _vectorValueConstructor: Gen[VectorValueConstructor] = for {
    vectorCandidate <- _expression
    dimension <- _expression
    cypherType <- _vectorCandidateType
  } yield VectorValueConstructor(vectorCandidate, dimension, cypherType)(pos)

  // Patterns
  // ----------------------------------

  def _relationshipsPattern: Gen[RelationshipsPattern] = for {
    chain <- _relationshipChain(false)
  } yield RelationshipsPattern(chain)(pos)

  def _patternExpr: Gen[PatternExpression] = for {
    pattern <- _relationshipsPattern
  } yield PatternExpression(pattern)(None, None)

  def _shortestPaths(dynamicLabelsAllowed: Boolean): Gen[ShortestPathsPatternPart] = for {
    element <- _patternElement(dynamicLabelsAllowed)
    single <- boolean
  } yield ShortestPathsPatternPart(element, single)(pos)

  def _shortestPathExpr: Gen[ShortestPathExpression] = for {
    pattern <- _shortestPaths(false)
  } yield ShortestPathExpression(pattern)

  def _existsExpression: Gen[ExistsExpression] = for {
    query <- _query
    introducedVariables <- zeroOrMore(_variable)
    scopeDependencies <- zeroOrMore(_variable)
  } yield ExistsExpression(query)(pos, Some(introducedVariables.toSet), Some(scopeDependencies.toSet))

  def _collectExpression: Gen[CollectExpression] = for {
    query <- _query
    introducedVariables <- zeroOrMore(_variable)
    scopeDependencies <- zeroOrMore(_variable)
  } yield CollectExpression(query)(pos, Some(introducedVariables.toSet), Some(scopeDependencies.toSet))

  def _countExpression: Gen[CountExpression] = for {
    query <- _query
    introducedVariables <- zeroOrMore(_variable)
    scopeDependencies <- zeroOrMore(_variable)
  } yield CountExpression(query)(pos, Some(introducedVariables.toSet), Some(scopeDependencies.toSet))

  def _patternComprehension: Gen[PatternComprehension] = for {
    namedPath <- option(_variable)
    pattern <- _relationshipsPattern
    predicate <- option(_expression)
    projection <- _expression
    introducedVariables <- zeroOrMore(_variable)
    scopeDependencies <- zeroOrMore(_variable)
  } yield PatternComprehension(namedPath, pattern, predicate, projection)(
    pos,
    Some(introducedVariables.toSet),
    Some(scopeDependencies.toSet)
  )

  // Expression
  // ----------------------------------

  def _shallowExpression: Gen[Expression] = oneOf(
    lzy(_nullLit),
    lzy(_stringLit),
    lzy(_booleanLit),
    lzy(_signedDecIntLit),
    lzy(_signedHexIntLit),
    lzy(_signedOctIntLit),
    lzy(_doubleLit),
    lzy(_variable),
    lzy(_parameter),
    lzy(_infinityLit),
    lzy(_nanLit)
  )

  // This is now a frozen list
  private def _deepExpressionCypher5: Gen[Expression] = oneOf(
    lzy(_predicateComparison),
    lzy(_predicateUnary),
    lzy(_predicateBinary),
    lzy(_predicateComparisonChain),
    lzy(_iterablePredicate),
    lzy(_arithmeticUnary),
    lzy(_arithmeticBinary),
    lzy(_case),
    lzy(_functionInvocation),
    lzy(_countStar),
    lzy(_reduceExpr),
    lzy(_shortestPathExpr),
    lzy(_patternExpr),
    lzy(_map),
    lzy(_mapProjection),
    lzy(_property),
    lzy(_list),
    lzy(_listSlice),
    lzy(_listComprehension),
    lzy(_containerIndex),
    lzy(_existsExpression),
    lzy(_countExpression),
    lzy(_collectExpression),
    lzy(_patternComprehension)
  )

  // For newer expressions
  private def _deepExpression: Gen[Expression] = oneOf(
    lzy(_predicateComparison),
    lzy(_predicateUnary),
    lzy(_predicateBinary),
    lzy(_predicateComparisonChain),
    lzy(_iterablePredicate),
    lzy(_arithmeticUnary),
    lzy(_arithmeticBinary),
    lzy(_case),
    lzy(_functionInvocation),
    lzy(_countStar),
    lzy(_reduceExpr),
    lzy(_shortestPathExpr),
    lzy(_patternExpr),
    lzy(_map),
    lzy(_mapProjection),
    lzy(_property),
    lzy(_list),
    lzy(_listSlice),
    lzy(_listComprehension),
    lzy(_containerIndex),
    lzy(_existsExpression),
    lzy(_countExpression),
    lzy(_collectExpression),
    lzy(_patternComprehension),
    lzy(_vectorValueConstructor),
    lzy(_allReducePredicate)
  )

  def _expression: Gen[Expression] = Gen.sized { size =>
    if (size <= 0) {
      _shallowExpression
    } else {
      if (usesCypher5) {
        frequency(
          4 -> _shallowExpression,
          1 -> Gen.resize(size - 1, _deepExpressionCypher5)
        )
      } else {
        frequency(
          4 -> _shallowExpression,
          1 -> Gen.resize(size - 1, _deepExpression)
        )
      }
    }
  }

  def _labelCheckExpression(variable: Variable): Gen[Expression] = {
    def _hasLabels(): Gen[HasLabels] = for {
      labels <- oneOrMore(_labelName)
    } yield HasLabels(variable, labels)(pos)

    def _hasAnyLabel(): Gen[HasAnyLabel] = for {
      labels <- oneOrMore(_labelName)
    } yield HasAnyLabel(variable, labels)(pos)

    def _hasLabelsOrTypes(): Gen[HasLabelsOrTypes] = for {
      labelOrRelTypes <- oneOrMore(_labelOrTypeName)
    } yield HasLabelsOrTypes(variable, labelOrRelTypes)(pos)

    def _hasTypes(): Gen[HasTypes] = for {
      relTypes <- oneOrMore(_relTypeName)
    } yield HasTypes(variable, relTypes)(pos)

    def _hasDynamicLabelsOrTypes(): Gen[HasDynamicLabelsOrTypes] = for {
      labels <- oneOrMore(_stringLit)
    } yield HasDynamicLabelsOrTypes(variable, labels)(pos)

    def _hasAnyDynamicLabelsOrTypes(): Gen[HasAnyDynamicLabelsOrTypes] = for {
      labels <- oneOrMore(_stringLit)
    } yield HasAnyDynamicLabelsOrTypes(variable, labels)(pos)

    def _hasDynamicLabels(): Gen[HasDynamicLabels] = for {
      labels <- oneOrMore(_stringLit)
    } yield HasDynamicLabels(variable, labels)(pos)

    def _hasAnyDynamicLabel(): Gen[HasAnyDynamicLabel] = for {
      labels <- oneOrMore(_stringLit)
    } yield HasAnyDynamicLabel(variable, labels)(pos)

    def _hasDynamicType(): Gen[HasDynamicType] = for {
      relTypes <- oneOrMore(_stringLit)
    } yield HasDynamicType(variable, relTypes)(pos)

    def _hasAnyDynamicType(): Gen[HasAnyDynamicType] = for {
      relTypes <- oneOrMore(_stringLit)
    } yield HasAnyDynamicType(variable, relTypes)(pos)

    oneOf(
      _hasLabels(),
      _hasAnyLabel(),
      _hasLabelsOrTypes(),
      _hasTypes(),
      _hasDynamicLabels(),
      _hasAnyDynamicLabel(),
      _hasDynamicType(),
      _hasAnyDynamicType(),
      _hasDynamicLabelsOrTypes(),
      _hasAnyDynamicLabelsOrTypes(),
      lzy(HasALabelOrType(variable)(pos)),
      lzy(HasALabel(variable)(pos))
    )
  }

  def _labelExpression(
    entityType: Option[EntityType],
    containsIs: Boolean,
    dynamicLabelsAllowed: Boolean
  ): Gen[LabelExpression] = {

    def _labelExpressionConjunction(): Gen[Conjunctions] = for {
      lhs <- _labelExpression(entityType, containsIs, dynamicLabelsAllowed)
      rhs <- _labelExpression(entityType, containsIs, dynamicLabelsAllowed)
    } yield Conjunctions.flat(lhs, rhs, pos, containsIs)

    def _labelExpressionDisjunction(): Gen[Disjunctions] = for {
      lhs <- _labelExpression(entityType, containsIs, dynamicLabelsAllowed)
      rhs <- _labelExpression(entityType, containsIs, dynamicLabelsAllowed)
    } yield Disjunctions.flat(lhs, rhs, pos, containsIs)

    for {
      labelExpr <- frequency(
        5 -> oneOf[LabelExpression](
          lzy(Wildcard(containsIs)(pos)),
          lzy(entityType match {
            case Some(NODE_TYPE)         => _labelName.map(Leaf(_, containsIs))
            case Some(RELATIONSHIP_TYPE) => _relTypeName.map(Leaf(_, containsIs))
            case None                    => _labelOrTypeName.map(Leaf(_, containsIs))
          }),
          lzy(entityType match {
            case Some(NODE_TYPE) if dynamicLabelsAllowed => _dynamicLabelExpression.map(DynamicLeaf(_, containsIs))
            case Some(NODE_TYPE)                         => _labelName.map(Leaf(_, containsIs))
            case Some(RELATIONSHIP_TYPE) if dynamicLabelsAllowed =>
              _dynamicRelTypeExpression.map(DynamicLeaf(_, containsIs))
            case Some(RELATIONSHIP_TYPE)      => _relTypeName.map(Leaf(_, containsIs))
            case None if dynamicLabelsAllowed => _dynamicLabelOrRelTypeExpression.map(DynamicLeaf(_, containsIs))
            case None                         => _labelOrTypeName.map(Leaf(_, containsIs))
          })
        ),
        1 -> oneOf[LabelExpression](
          lzy(_labelExpressionConjunction()),
          lzy(_labelExpressionDisjunction()),
          lzy(_labelExpression(entityType, containsIs, dynamicLabelsAllowed).map(Negation(
            _,
            containsIs
          )(pos)))
        )
      )
    } yield labelExpr
  }

  def _dynamicLabelExpression: Gen[DynamicLabelExpression] = for {
    expr <- _expression
    all <- boolean
  } yield DynamicLabelExpression(expr, all)(pos)

  def _dynamicRelTypeExpression: Gen[DynamicRelTypeExpression] = for {
    expr <- _expression
    all <- boolean
  } yield DynamicRelTypeExpression(expr, all)(pos)

  def _dynamicLabelOrRelTypeExpression: Gen[DynamicLabelOrRelTypeExpression] = for {
    expr <- _expression
    all <- boolean
  } yield DynamicLabelOrRelTypeExpression(expr, all)(pos)

  // PATTERNS
  // ==========================================================================

  def _nodePattern(dynamicLabelsAllowed: Boolean): Gen[NodePattern] = for {
    variable <- option(_variable)
    containsIs <- boolean
    labelExpression <- option(_labelExpression(Some(NODE_TYPE), containsIs, dynamicLabelsAllowed))
    properties <- option(oneOf(_map, _parameter))
    predicate <- variable match {
      case Some(someVariable) =>
        option(oneOf(
          _expression,
          _labelCheckExpression(someVariable)
        )) // Only generate WHERE if we have a variable name.
      case None => const(None)
    }
  } yield NodePattern(variable, labelExpression, properties, predicate)(pos)

  def _range: Gen[Range] = for {
    lower <- option(_pathLengthQuantifier)
    upper <- option(_pathLengthQuantifier)
  } yield Range(lower, upper)(pos)

  def _semanticDirection: Gen[SemanticDirection] =
    oneOf(
      SemanticDirection.OUTGOING,
      SemanticDirection.INCOMING,
      SemanticDirection.BOTH
    )

  def _relationshipPattern(dynamicLabelsAllowed: Boolean): Gen[RelationshipPattern] = for {
    variable <- option(_variable)
    containsIs <- boolean
    labelExpression <- option(_labelExpression(Some(RELATIONSHIP_TYPE), containsIs, dynamicLabelsAllowed))
    length <- option(option(_range))
    properties <- option(oneOf(_map, _parameter))
    direction <- _semanticDirection
    predicate <- variable match {
      case Some(someVariable) =>
        option(oneOf(
          _expression,
          _labelCheckExpression(someVariable)
        )) // Only generate WHERE if we have a variable name.
      case None => const(None)
    }
  } yield RelationshipPattern(variable, labelExpression, length, properties, predicate, direction)(pos)

  def _relationshipChain(dynamicLabelsAllowed: Boolean): Gen[RelationshipChain] = for {
    element <- _pathPrimary(dynamicLabelsAllowed)
    relationship <- _relationshipPattern(dynamicLabelsAllowed)
    rightNode <- _nodePattern(dynamicLabelsAllowed)
  } yield RelationshipChain(element, relationship, rightNode)(pos)

  def _pathPrimary(dynamicLabelsAllowed: Boolean): Gen[SimplePattern] = oneOf(
    _nodePattern(dynamicLabelsAllowed),
    lzy(_relationshipChain(dynamicLabelsAllowed))
  )

  def _generalQuantifier: Gen[IntervalQuantifier] = for {
    lower <- option(_pathLengthQuantifier)
    upper <- option(_pathLengthQuantifier)
  } yield IntervalQuantifier(lower, upper)(pos)

  def _fixedQuantifier: Gen[FixedQuantifier] = for {
    value <- _pathLengthQuantifier
  } yield FixedQuantifier(value)(pos)

  def _quantifier: Gen[GraphPatternQuantifier] = oneOf(
    const(StarQuantifier()(pos)),
    const(PlusQuantifier()(pos)),
    _generalQuantifier,
    _fixedQuantifier
  )

  def _quantifiedPath(dynamicLabelsAllowed: Boolean): Gen[QuantifiedPath] = for {
    primary <- _pathFactor(dynamicLabelsAllowed)
    quantifier <- _quantifier
    where <- option(_expression)
  } yield QuantifiedPath(PatternPart(primary), quantifier, where)(pos)

  def _pathFactor(dynamicLabelsAllowed: Boolean): Gen[PathFactor] = oneOf(
    lzy(_quantifiedPath(dynamicLabelsAllowed)),
    lzy(_pathPrimary(dynamicLabelsAllowed))
  )

  def _pathConcatenation(dynamicLabelsAllowed: Boolean): Gen[PathConcatenation] = for {
    elements <- twoOrMore(_pathFactor(dynamicLabelsAllowed))
  } yield PathConcatenation(elements)(pos)

  def _patternElement(dynamicLabelsAllowed: Boolean): Gen[PatternElement] = oneOf(
    lzy(_pathFactor(dynamicLabelsAllowed)),
    lzy(_pathConcatenation(dynamicLabelsAllowed))
  )

  private def literalIntOrParam: Gen[Either[PathLengthQuantifier, Parameter]] = for {
    int <- lzy(_pathLengthQuantifier)
    param <- lzy(_parameter)
    count <- oneOf(Left(int), Right(param))
  } yield {
    count
  }

  private def literalIntOnly: Gen[Either[PathLengthQuantifier, Parameter]] = for {
    int <- lzy(_pathLengthQuantifier)
  } yield {
    Left(int)
  }

  def _selector: Gen[Selector] = for {
    count <- if (usesCypher5) literalIntOnly else literalIntOrParam
    selector <- oneOf(
      lzy(AnyPath(count)(pos)),
      lzy(AllPaths()(pos)),
      lzy(AnyShortestPath(count)(pos)),
      lzy(AllShortestPaths()(pos)),
      lzy(ShortestGroups(count)(pos))
    )
  } yield selector

  def _anonPatternPart: Gen[AnonymousPatternPart] = for {
    element <- _patternElement(true)
    single <- boolean
    part <- oneOf(
      PathPatternPart(element),
      ShortestPathsPatternPart(element, single)(pos)
    )
  } yield part

  def _namedPatternPart: Gen[NamedPatternPart] = for {
    variable <- _variable
    part <- _anonPatternPart
  } yield NamedPatternPart(variable, part)(pos)

  def _nonPrefixedPatternPart: Gen[NonPrefixedPatternPart] =
    oneOf(
      _anonPatternPart,
      _namedPatternPart
    )

  def _prefixedPatternPart: Gen[PrefixedPatternPart] =
    for {
      part <- _nonPrefixedPatternPart
      selector <- _selector
      pathMode <- _pathMode
    } yield PrefixedPatternPart(selector, pathMode, part)

  def _pathMode: Gen[PathMode] =
    if (usesCypher5)
      const(Walk(implicitlyCreated = true)(pos))
    else
      oneOf(
        Walk(implicitlyCreated = true)(pos),
        Walk()(pos),
        Trail()(pos),
        Acyclic()(pos)
      )

  def _patternForMatch: Gen[Pattern.ForMatch] = for {
    parts <- oneOrMore(_prefixedPatternPart)
  } yield Pattern.ForMatch(parts)(pos)

  def _patternForUpdate: Gen[Pattern.ForUpdate] = for {
    parts <- oneOrMore(_nonPrefixedPatternPart)
  } yield Pattern.ForUpdate(parts)(pos)

  def _patternForInsert: Gen[Pattern.ForUpdate] = for {
    parts <- oneOrMore(_insertPatternPart)
  } yield Pattern.ForUpdate(parts)(pos)

  def _insertPatternPart: Gen[AnonymousPatternPart] = for {
    element <- _insertPatternElement
    part <- PathPatternPart(element)
  } yield part

  def _insertPatternElement: Gen[SimplePattern] = oneOf(
    _insertNodePattern,
    lzy(_insertRelationshipChain)
  )

  def _insertNodePattern: Gen[NodePattern] = for {
    variable <- option(_variable)
    containsIs <- boolean
    labelExpression <- option(_insertNodeLabelExpression(containsIs))
    properties <- option(_map)
  } yield NodePattern(variable, labelExpression, properties, None)(pos)

  def _insertNodeLabelExpression(containsIs: Boolean): Gen[LabelExpression] = {

    def _insertLabelExpressionConjunction(): Gen[Conjunctions] = for {
      lhs <- _insertNodeLabelExpression(containsIs)
      rhs <- _insertNodeLabelExpression(containsIs)
    } yield Conjunctions.flat(lhs, rhs, pos, containsIs)

    for {
      labelExpr <- frequency(
        5 ->
          lzy(Some(NODE_TYPE) match {
            case Some(NODE_TYPE) => _labelName.map(Leaf(_, containsIs))
          }),
        1 -> lzy(_insertLabelExpressionConjunction())
      )
    } yield labelExpr
  }

  def _insertRelationshipChain: Gen[RelationshipChain] = for {
    element <- _insertPatternElement
    relationship <- _insertRelationshipPattern
    rightNode <- _insertNodePattern
  } yield RelationshipChain(element, relationship, rightNode)(pos)

  def _insertRelationshipPattern: Gen[RelationshipPattern] = for {
    variable <- option(_variable)
    containsIs <- boolean
    labelExpression <- _relTypeName.map(Leaf(_, containsIs))
    properties <- option(_map)
    direction <- _semanticDirection
  } yield RelationshipPattern(variable, Some(labelExpression), None, properties, None, direction)(pos)

  // CLAUSES
  // ==========================================================================

  def _aliasedReturnItem: Gen[ReturnItem] = for {
    expr <- _expression
    variable <- _variable
  } yield AliasedReturnItem(expr, variable)(pos)

  def _returnItem: Gen[ReturnItem] = for {
    expr <- _expression
    variable <- _variable
    item <- oneOf(
      UnaliasedReturnItem(expr, "")(pos),
      AliasedReturnItem(expr, variable)(pos)
    )
  } yield item

  def _groupingElements: Gen[GroupingElements] = for {
    expressions <- oneOrMore(_expression)
    elements <- oneOf(
      GroupingNone()(pos),
      GroupingAll()(pos),
      ExplicitGroupingElements(expressions)(pos)
    )
  } yield elements

  def _groupBy: Gen[GroupBy] = for {
    elements <- _groupingElements
  } yield GroupBy(elements)(pos)

  def _sortItem: Gen[SortItem] = for {
    expr <- _expression
    item <- oneOf(
      AscSortItem(expr)(pos),
      DescSortItem(expr)(pos)
    )
  } yield item

  def _orderBy: Gen[OrderBy] = for {
    items <- oneOrMore(_sortItem)
  } yield OrderBy(items)(pos)

  def _skip: Gen[Skip] =
    _expression.map(Skip(_)(pos))

  def _limit: Gen[Limit] =
    _expression.map(Limit(_)(pos))

  def _where: Gen[Where] =
    _expression.map(Where(_)(pos))

  def _search: Gen[Search] = for {
    variable <- _variable
    score <- option(_variable)
    indexName <- _stringLiteralOrParameter
    expr <- _expression
    where <- option(_where)
    limit <- _limit
  } yield Search(variable, score, indexName, expr, where, limit)(pos)

  def _with: Gen[With] = for {
    distinct <- boolean
    projectionType <- oneOf(AdditiveProjection, FreeProjection)
    retItems <- oneOrMore(_returnItem)
    groupBy <- if (usesCypher5) Gen.const(None) else option(_groupBy)
    orderBy <- option(_orderBy)
    skip <- option(_skip)
    limit <- option(_limit)
    where <- option(_where)
  } yield With(distinct, ReturnItems(projectionType, retItems)(pos), groupBy, orderBy, skip, limit, where)(pos)

  def _let: Gen[With] = for {
    retItems <- oneOrMore(_aliasedReturnItem)
  } yield With(
    distinct = false,
    ReturnItems(AdditiveProjection, retItems)(pos),
    None,
    None,
    None,
    None,
    None,
    ParsedAsLet
  )(pos)

  def _filter: Gen[With] =
    _where.map(where =>
      With(
        distinct = false,
        ReturnItems(AdditiveProjection, Seq.empty)(pos),
        None,
        None,
        None,
        None,
        Some(where),
        ParsedAsFilter
      )(pos)
    )

  def _orderByAndPageStatement: Gen[With] = for {
    orderBy <- option(_orderBy)
    skip <- option(_skip)
    limit <- option(_limit)
    withType <- orderBy.map(_ => ParsedAsOrderBy).orElse(skip.map(_ => ParsedAsSkip)).orElse(limit.map(_ =>
      ParsedAsLimit
    )).getOrElse(DefaultWith)
  } yield With(
    distinct = false,
    ReturnItems(AdditiveProjection, Seq.empty)(pos),
    None,
    orderBy,
    skip,
    limit,
    None,
    withType
  )(pos)

  def _return: Gen[Return] = for {
    distinct <- boolean
    projectionType <- oneOf(AdditiveProjection, FreeProjection)
    retItems <- oneOrMore(_returnItem)
    groupBy <- if (usesCypher5) Gen.const(None) else option(_groupBy)
    orderBy <- option(_orderBy)
    skip <- option(_skip)
    limit <- option(_limit)
  } yield Return(distinct, ReturnItems(projectionType, retItems)(pos), groupBy, orderBy, skip, limit)(pos)

  def _finish: Gen[Finish] = const(Finish()(pos))

  def _yield: Gen[Yield] = for {
    retItems <- oneOrMore(_yieldItem)
    orderBy <- option(_orderBy)
    skip <- option(_signedDecIntLit.map(Skip(_)(pos)))
    limit <- option(_signedDecIntLit.map(Limit(_)(pos)))
    where <- option(_where)
  } yield Yield(ReturnItems(FreeProjection, retItems)(pos), orderBy, skip, limit, where)(pos)

  def _yieldItem: Gen[ReturnItem] = for {
    var1 <- _variable
    item <- UnaliasedReturnItem(var1, "")(pos)
  } yield item

  def _match: Gen[Match] = for {
    optional <- boolean
    matchMode <- _matchMode
    pattern <- _patternForMatch
    hints <- zeroOrMore(_hint)
    where <- option(_where)
    search <- if (usesCypher5) const(None) else option(_search)
  } yield Match(optional, matchMode, pattern, hints, where, search)(pos)

  def _matchMode: Gen[MatchMode] = oneOf(MatchMode.RepeatableElements()(pos), MatchMode.DifferentRelationships()(pos))

  def _create: Gen[Create] = for {
    pattern <- _patternForUpdate
  } yield Create(pattern)(pos)

  def _insert: Gen[Insert] = for {
    pattern <- _patternForInsert
  } yield Insert(pattern)(pos)

  def _unwind: Gen[Unwind] = for {
    expression <- _expression
    variable <- _variable
    useForInSyntax <- if (usesCypher5) const(false) else frequency(2 -> const(false), 1 -> const(true))
  } yield Unwind(expression, variable)(pos, useForInSyntax = useForInSyntax)

  def _setItem: Gen[SetItem] = for {
    variable <- _variable
    labels <- oneOrMore(_labelName)
    dynamicLabels <- oneOrMore(_expression)
    property <- _property
    dynamicProperty <- _containerIndex
    expression <- _expression
    containsIs <- boolean
    item <- oneOf(
      SetLabelItem(variable, labels, dynamicLabels, containsIs)(pos),
      SetPropertyItem(property, expression)(pos),
      SetDynamicPropertyItem(dynamicProperty, expression)(pos),
      SetExactPropertiesFromMapItem(variable, expression)(pos),
      SetIncludingPropertiesFromMapItem(variable, expression)(pos)
    )
  } yield item

  def _removeItem: Gen[RemoveItem] = for {
    variable <- _variable
    labels <- oneOrMore(_labelName)
    dynamicLabels <- oneOrMore(_expression)
    containsIs <- boolean
    property <- _property
    dynamicProperty <- _containerIndex
    item <- oneOf(
      RemoveLabelItem(variable, labels, dynamicLabels, containsIs)(pos),
      RemovePropertyItem(property)(pos),
      RemoveDynamicPropertyItem(dynamicProperty)(pos)
    )
  } yield item

  def _set: Gen[SetClause] = for {
    items <- oneOrMore(_setItem)
  } yield SetClause(items)(pos)

  def _remove: Gen[Remove] = for {
    items <- oneOrMore(_removeItem)
  } yield Remove(items)(pos)

  def _delete: Gen[Delete] = for {
    expressions <- oneOrMore(_expression)
    forced <- boolean
  } yield Delete(expressions, forced)(pos)

  def _mergeAction: Gen[MergeAction] = for {
    set <- _set
    action <- oneOf(
      OnCreate(set)(pos),
      OnMatch(set)(pos)
    )
  } yield action

  def _merge: Gen[Merge] = for {
    pattern <- _nonPrefixedPatternPart
    actions <- oneOrMore(_mergeAction)
  } yield Merge(pattern, actions)(pos)

  def _procedureOutput: Gen[ProcedureOutput] = for {
    name <- _identifier
  } yield ProcedureOutput(name)(pos)

  def _procedureResultItem: Gen[ProcedureResultItem] = for {
    output <- option(_procedureOutput)
    variable <- _variable
  } yield ProcedureResultItem(output, variable)(pos)

  def _procedureResult: Gen[ProcedureResult] = for {
    items <- oneOrMore(_procedureResultItem)
    where <- option(_where)
  } yield ProcedureResult(items.toIndexedSeq, where)(pos)

  def _call: Gen[UnresolvedCall] = for {
    namespace <- _namespace
    procedureName <- _identifier
    declaredArguments <- option(zeroOrMore(_expression))
    declaredResult <- option(_procedureResult)
    yieldAll <- if (declaredResult.isDefined) const(false) else boolean // can't have both YIELD * and declare results
  } yield UnresolvedCall(ProcedureName(namespace, procedureName)(pos), declaredArguments, declaredResult, yieldAll)(pos)

  def _foreach: Gen[Foreach] = for {
    variable <- _variable
    expression <- _expression
    updates <- oneOrMore(_clause)
  } yield Foreach(variable, expression, updateWithClauseWhenReparsingWouldChangeAstAndPrettifying(updates))(pos)

  def _loadCsv: Gen[LoadCSV] = for {
    withHeaders <- boolean
    urlString <- _expression
    variable <- _variable
    fieldTerminator <- option(_stringLit)
  } yield LoadCSV(withHeaders, urlString, variable, fieldTerminator)(pos)

  // Hints
  // ----------------------------------

  def _usingIndexHint: Gen[UsingIndexHint] = for {
    variable <- _variable
    labelOrRelType <- _labelOrTypeName
    properties <- oneOrMore(_propertyKeyName)
    spec <- oneOf(SeekOnly, SeekOrScan)
    indexType <- oneOf(UsingAnyIndexType, UsingRangeIndexType, UsingTextIndexType, UsingPointIndexType)
  } yield UsingIndexHint(variable, labelOrRelType, properties, spec, indexType)(pos)

  def _usingJoinHint: Gen[UsingJoinHint] = for {
    variables <- oneOrMore(_variable)
  } yield UsingJoinHint(NonEmptyList.from(variables))(pos)

  def _usingScanHint: Gen[UsingScanHint] = for {
    variable <- _variable
    labelOrRelType <- _labelOrTypeName
  } yield UsingScanHint(variable, labelOrRelType)(pos)

  def _expandStep: Gen[ExpandStep] = oneOf(
    // Shape 1: endpoints only — FROM x TO y
    for {
      from <- _variable
      to <- _variable
      mode <- option(oneOf(ExpandHintAll, ExpandHintInto))
    } yield ExpandStep(Some(from), Some(to), None, mode)(pos),
    // Shape 2: endpoints + via — FROM x TO y VIA r
    for {
      from <- _variable
      to <- _variable
      via <- _variable
      mode <- option(oneOf(ExpandHintAll, ExpandHintInto))
    } yield ExpandStep(Some(from), Some(to), Some(via), mode)(pos),
    // Shape 3: via only — VIA r
    for {
      via <- _variable
      mode <- option(oneOf(ExpandHintAll, ExpandHintInto))
    } yield ExpandStep(None, None, Some(via), mode)(pos)
  )

  def _usingExpandHint: Gen[UsingExpandHint] = for {
    n <- choose(1, 3)
    steps <- listOfN(n, _expandStep)
  } yield UsingExpandHint(NonEmptyList.from(steps))(pos)

  def _hint: Gen[AstHint] = oneOf(
    _usingIndexHint,
    _usingJoinHint,
    _usingScanHint,
    _usingExpandHint
  )

  // Queries
  // ----------------------------------

  def _use: Gen[UseGraph] = for {
    names <- listOfN(1, _identifier)
    function <- _functionInvocation
    cypherVersionFlag <- oneOf(true, false)
    graphRef <- oneOf(
      GraphDirectReference(CatalogName(names, cypherVersionFlag))(pos),
      GraphFunctionReference(function, cypherVersionFlag)(pos)
    )
  } yield UseGraph(graphRef)(pos)

  def _importingWithSubqueryCall: Gen[ImportingWithSubqueryCall] = for {
    innerQuery <- _query
    params <- option(_inTransactionsParameters)
    optional <- boolean
  } yield ImportingWithSubqueryCall(innerQuery, params, optional)(pos)

  def _scopeClauseSubqueryCall: Gen[ScopeClauseSubqueryCall] = for {
    innerQuery <- _query
    _importVariables <- listOfN(1, _variable)
    (isImportingAll, importVariables) <- oneOf((true, Seq.empty), (false, _importVariables))
    params <- option(_inTransactionsParameters)
    optional <- boolean
  } yield ScopeClauseSubqueryCall(innerQuery, isImportingAll, importVariables, params, optional)(pos)

  def _inTransactionsParameters: Gen[InTransactionsParameters] =
    for {
      batchSize <- option(_expression)
      concurrency <- option(option(_expression))
      onErrorBehaviour <- option(oneOf[InTransactionsOnErrorBehaviour](
        Seq(
          OnErrorContinue,
          OnErrorBreak,
          OnErrorFail,
          OnErrorRetryThenContinue,
          OnErrorRetryThenBreak,
          OnErrorRetryThenFail
        )
      ))
      retryParams <- option(_expression)
      reportAs <- option(string)
      disjointByMode <- option(_inTransactionsDisjointByMode)
    } yield InTransactionsParameters(
      batchSize.map(InTransactionsBatchParameters(_)(pos)),
      concurrency.map(InTransactionsConcurrencyParameters(_)(pos)),
      onErrorBehaviour.map {
        case eb @ (OnErrorRetryThenContinue | OnErrorRetryThenBreak | OnErrorRetryThenFail) =>
          InTransactionsErrorParameters(
            eb,
            retryParameters = retryParams.map(t => InTransactionsRetryParameters(Some(t))(pos))
          )(pos)
        case eb => InTransactionsErrorParameters(eb, None)(pos)
      },
      reportAs.map(v => InTransactionsReportParameters(Variable(s"`$v`")(pos, Variable.isIsolatedDefault))(pos)),
      if (usesCypher5) None else disjointByMode.map(InTransactionsDisjointByParameters(_)(pos))
    )(pos)

  private def _inTransactionsDisjointByMode: Gen[InTransactionsDisjointByMode] =
    Gen.frequency(
      1 -> Gen.const(InTransactionsDisjointByMode.DisjointByAuto),
      1 -> Gen.const(InTransactionsDisjointByMode.DisjointByNone),
      3 -> nonEmptyListOf(_expression).map(exprs => InTransactionsDisjointByMode.DisjointByExpressions(exprs))
    )

  private def _anyClause: Gen[Clause] =
    if (usesCypher5) {
      oneOf(
        lzy(_use),
        lzy(_with),
        lzy(_orderByAndPageStatement),
        lzy(_return),
        lzy(_finish),
        lzy(_match),
        lzy(_create),
        lzy(_insert),
        lzy(_unwind),
        lzy(_set),
        lzy(_remove),
        lzy(_delete),
        lzy(_merge),
        lzy(_call),
        lzy(_loadCsv),
        lzy(_foreach),
        lzy(_importingWithSubqueryCall),
        lzy(_scopeClauseSubqueryCall)
      )
    } else {
      oneOf(
        lzy(_use),
        lzy(_with),
        lzy(_let),
        lzy(_filter),
        lzy(_orderByAndPageStatement),
        lzy(_return),
        lzy(_finish),
        lzy(_match),
        lzy(_create),
        lzy(_insert),
        lzy(_unwind),
        lzy(_set),
        lzy(_remove),
        lzy(_delete),
        lzy(_merge),
        lzy(_call),
        lzy(_loadCsv),
        lzy(_foreach),
        lzy(_importingWithSubqueryCall),
        lzy(_scopeClauseSubqueryCall),
        lzy(commandClause)
      )
    }

  private def _shallowClause: Gen[Clause] =
    if (usesCypher5) {
      oneOf(
        lzy(_use),
        lzy(_with),
        lzy(_orderByAndPageStatement),
        lzy(_return),
        lzy(_finish),
        lzy(_match),
        lzy(_create),
        lzy(_insert),
        lzy(_unwind),
        lzy(_set),
        lzy(_remove),
        lzy(_delete),
        lzy(_merge),
        lzy(_call),
        lzy(_loadCsv)
      )
    } else {
      oneOf(
        lzy(_use),
        lzy(_with),
        lzy(_let),
        lzy(_filter),
        lzy(_orderByAndPageStatement),
        lzy(_return),
        lzy(_finish),
        lzy(_match),
        lzy(_create),
        lzy(_insert),
        lzy(_unwind),
        lzy(_set),
        lzy(_remove),
        lzy(_delete),
        lzy(_merge),
        lzy(_call),
        lzy(_loadCsv),
        lzy(commandClause)
      )
    }

  def _clause: Gen[Clause] = Gen.sized { size =>
    if (size <= 0) {
      _shallowClause
    } else {
      Gen.resize(size - 1, _anyClause)
    }
  }

  private def updateWithClauseWhenReparsingWouldChangeAstAndPrettifying(clauses: List[Clause]): List[Clause] = {
    // Generating an ast with a With parsed as yield or a default With followed by any
    // With parsed as OrderBy, Skip or Limit will then be prettified and reparsed into a single With
    // where the order by/skip/limit is part of the default/yield With instead
    // which then fails the round trip check (as that get different indentations when prettified again)
    // For example:
    // SingleQuery(Seq(
    //   With(ReturnItems(AdditiveProjection, List.empty)(pos), DefaultWith)(pos),
    //   With(distinct = false, ReturnItems(AdditiveProjection, List.empty)(pos), Some(OrderBy(List(AscSortItem(True()(pos))(pos)))(pos)), None, None, None, ParsedAsOrderBy)(pos))
    // )(pos)
    // |WITH *
    // |ORDER BY true ASCENDING
    // becomes
    // SingleQuery(Seq(
    //   With(distinct = false, ReturnItems(AdditiveProjection, List.empty)(pos), Some(OrderBy(List(AscSortItem(True()(pos))(pos)))(pos)), None, None, None, DefaultWith)(pos))
    // )(pos)
    // |WITH *
    // |  ORDER BY true ASCENDING
    clauses.foldLeft[List[Clause]](List.empty) {
      case (init :+ (w1: With), w2: With)
        if w1.withType == DefaultWith && w2.withType.isInstanceOf[OrderByOrPaginationWithType] =>
        init :+ w1 :+ w2.copy(withType = DefaultWith)(w2.position)
      // The With as part of a CommandClause is always ParsedAsYield, so we only need to check if it exists
      case (init :+ (c: CommandClause), w: With)
        if c.yieldWith.nonEmpty && w.withType.isInstanceOf[OrderByOrPaginationWithType] =>
        init :+ c :+ w.copy(withType = DefaultWith)(w.position)
      case (cs, c) => cs :+ c
    }
  }

  def _singleQuery: Gen[SingleQuery] = for {
    s <- choose(1, 1)
    clauses <- listOfN(s, _clause)
  } yield SingleQuery(updateWithClauseWhenReparsingWouldChangeAstAndPrettifying(clauses))(pos)

  def _union: Gen[Union] = for {
    lhs <- _unionArgument
    rhs <- _singleQuery
    union <- oneOf(
      UnionDistinct(lhs, rhs)(pos),
      UnionAll(lhs, rhs)(pos)
    )
  } yield union

  def _unionArgument: Gen[Query] = {
    if (usesCypher5) frequency(
      5 -> lzy(_singleQuery),
      1 -> lzy(_union)
    )
    else frequency(
      5 -> lzy(_singleQuery),
      1 -> lzy(_union),
      1 -> lzy(_topLevelBraces)
    )
  }

  def _topLevelBraces: Gen[TopLevelBraces] = for {
    inner <- _query
    use <- option(_use)
  } yield TopLevelBraces(inner, use)(pos)

  def _when: Gen[ConditionalQueryWhen] = for {
    branches <- oneOrMore(_branch)
    default <- option[PartQuery](_partQuery)
  } yield ConditionalQueryWhen(branches, default.map(x => ConditionalQueryBranch(None, x)(pos)))(pos)

  def _branch: Gen[ConditionalQueryBranch] = for {
    predicate <- _expression
    query <- _partQuery
  } yield ConditionalQueryBranch(Some(predicate), query)(pos)

  def _partQuery: Gen[PartQuery] = for {
    partQuery <- oneOf(_singleQuery, _topLevelBraces)
  } yield partQuery

  def _next: Gen[NextStatement] = for {
    queries <- listOfN(2, _nextArgument)
  } yield NextStatement(queries)(pos)

  def _nextArgument: Gen[Query] =
    frequency(
      5 -> lzy(_singleQuery),
      1 -> lzy(_union),
      1 -> lzy(_topLevelBraces),
      1 -> lzy(_when)
    )

  def _queryWithLocalDefinition: Gen[QueryWithLocalDefinitions] = for {
    s <- choose(1, 1)
    localDefinitions <- listOfN(s, _localDefinition)
    query <- _queryWithLocalDefinitionArgument
  } yield QueryWithLocalDefinitions(localDefinitions, query)(pos)

  def _queryWithLocalDefinitionArgument: Gen[Query] =
    frequency(
      5 -> lzy(_singleQuery),
      1 -> lzy(_union),
      1 -> lzy(_topLevelBraces),
      1 -> lzy(_when),
      1 -> lzy(_next)
    )

  def _localDefinition: Gen[LocalCallableDefinition] = for {
    definition <- oneOf(
      _localProcedureDefinition,
      _localFunctionDefinition
    )
  } yield definition

  def _localProcedureDefinition: Gen[LocalProcedureDefinition] = for {
    namespace <- _namespace
    name <- _identifier
    numInputFields <- choose(0, 3)
    inputSignature <- listOfN(numInputFields, _optionalFieldSignature)
    numOutputFields <- choose(1, 3)
    outputSignature <- option(listOfN(numOutputFields, _mandatoryFieldSignature))
    body <- _query
  } yield LocalProcedureDefinition(ProcedureName(namespace, name)(pos), inputSignature, outputSignature, body)(pos)

  def _localFunctionDefinition: Gen[LocalFunctionDefinition] = for {
    namespace <- _namespace
    name <- _identifier
    numInputFields <- choose(0, 3)
    inputSignature <- listOfN(numInputFields, _optionalFieldSignature)
    outputSignature <- option(_cypherTypeName)
    body <- oneOf(
      _localFunctionQueryBody,
      _localFunctionExpressionBody
    )
  } yield LocalFunctionDefinition(FunctionName(namespace, name)(pos), inputSignature, outputSignature, body)(pos)

  def _localFunctionQueryBody: Gen[QueryBody] = for {
    body <- _query
  } yield QueryBody(body)(pos)

  def _localFunctionExpressionBody: Gen[ExpressionBody] = for {
    body <- _expression
  } yield ExpressionBody(body)(pos)

  def _mandatoryFieldSignature: Gen[LocalFieldSignature] = for {
    name <- _identifier
    typ <- option(_cypherTypeName)
  } yield LocalFieldSignature(name, typ, None)(pos)

  def _optionalFieldSignature: Gen[LocalFieldSignature] = for {
    name <- _identifier
    typ <- option(_cypherTypeName)
    default <- option(_expression)
  } yield LocalFieldSignature(name, typ, default)(pos)

  def _query: Gen[Query] = Gen.sized { size =>
    if (size <= 0) _singleQuery
    else Gen.resize(size - 1, _anyQuery)
  }

  private def _anyQuery: Gen[Query] = {
    if (usesCypher5) frequency(
      5 -> lzy(_singleQuery),
      1 -> lzy(_union)
    )
    else frequency(
      5 -> lzy(_singleQuery),
      1 -> lzy(_union),
      1 -> lzy(_topLevelBraces),
      1 -> lzy(_when),
      1 -> lzy(_next),
      1 -> lzy(_queryWithLocalDefinition)
    )
  }

  // Show commands
  // ----------------------------------

  def _indexType: Gen[ShowIndexType] = for {
    indexType <- oneOf(
      AllIndexes,
      RangeIndexes,
      FulltextIndexes,
      TextIndexes,
      PointIndexes,
      VectorIndexes,
      LookupIndexes
    )
  } yield indexType

  def _listOfLabels: Gen[List[LabelName]] = for {
    labels <- oneOrMore(_labelName)
  } yield labels

  def _listOfRelTypes: Gen[List[RelTypeName]] = for {
    types <- oneOrMore(_relTypeName)
  } yield types

  def _constraintType: Gen[ShowConstraintType] = for {
    returnCypher5Values <- const(usesCypher5)
    constraintType <- oneOf(
      AllConstraints,
      if (returnCypher5Values) UniqueConstraints.cypher5 else UniqueConstraints.cypher25,
      if (returnCypher5Values) NodeUniqueConstraints.cypher5 else NodeUniqueConstraints.cypher25,
      if (returnCypher5Values) RelUniqueConstraints.cypher5 else RelUniqueConstraints.cypher25,
      if (returnCypher5Values) PropExistsConstraints.cypher5 else PropExistsConstraints.cypher25,
      if (returnCypher5Values) NodePropExistsConstraints.cypher5 else NodePropExistsConstraints.cypher25,
      if (returnCypher5Values) RelPropExistsConstraints.cypher5 else RelPropExistsConstraints.cypher25,
      AllExistsConstraints,
      NodeAllExistsConstraints,
      RelAllExistsConstraints,
      KeyConstraints,
      NodeKeyConstraints,
      RelKeyConstraints,
      PropTypeConstraints,
      NodePropTypeConstraints,
      RelPropTypeConstraints
    )
  } yield constraintType

  def _showIndexes: Gen[Query] = for {
    indexType <- _indexType
    use <- option(_use)
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(
          ShowIndexesClause(
            indexType,
            Some(w),
            List.empty,
            yieldAll = false,
            None
          )(pos)
        )
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(
          ShowIndexesClause(
            indexType,
            None,
            yi,
            yieldAll = false,
            Some(w)
          )(pos)
        ) ++ r
      case _ if yieldAll =>
        Seq(
          ShowIndexesClause(
            indexType,
            None,
            List.empty,
            yieldAll = true,
            Some(getFullWithStarFromYield)
          )(pos)
        )
      case _ =>
        Seq(
          ShowIndexesClause(
            indexType,
            None,
            List.empty,
            yieldAll = false,
            None
          )(pos)
        )
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showConstraints: Gen[Query] = for {
    constraintType <- _constraintType
    use <- option(_use)
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowConstraintsClause(constraintType, Some(w), List.empty, yieldAll = false, None, usesCypher5)(pos))
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowConstraintsClause(constraintType, None, yi, yieldAll = false, Some(w), usesCypher5)(pos)) ++ r
      case _ if yieldAll =>
        Seq(
          ShowConstraintsClause(
            constraintType,
            None,
            List.empty,
            yieldAll = true,
            Some(getFullWithStarFromYield),
            usesCypher5
          )(pos)
        )
      case _ =>
        Seq(ShowConstraintsClause(constraintType, None, List.empty, yieldAll = false, None, usesCypher5)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showCurrentGraphType: Gen[Query] = for {
    use <- option(_use)
    asGraph <- boolean
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowCurrentGraphTypeClause(asGraph, Some(w), List.empty, yieldAll = false, None)(pos))
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowCurrentGraphTypeClause(asGraph, None, yi, yieldAll = false, Some(w))(pos)) ++ r
      case _ if yieldAll =>
        Seq(ShowCurrentGraphTypeClause(asGraph, None, List.empty, yieldAll = true, Some(getFullWithStarFromYield))(pos))
      case _ =>
        Seq(ShowCurrentGraphTypeClause(asGraph, None, List.empty, yieldAll = false, None)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showProcedures: Gen[Query] = for {
    name <- _identifier
    exec <- option(oneOf(CurrentUser, User(name)(pos)))
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
    use <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowProceduresClause(exec, Some(w), List.empty, yieldAll = false, None)(pos))
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowProceduresClause(exec, None, yi, yieldAll = false, Some(w))(pos)) ++ r
      case _ if yieldAll =>
        Seq(ShowProceduresClause(exec, None, List.empty, yieldAll = true, Some(getFullWithStarFromYield))(pos))
      case _ =>
        Seq(ShowProceduresClause(exec, None, List.empty, yieldAll = false, None)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showFunctions: Gen[Query] = for {
    name <- _identifier
    funcType <- oneOf(AllFunctions, BuiltInFunctions, UserDefinedFunctions)
    exec <- option(oneOf(CurrentUser, User(name)(pos)))
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
    use <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowFunctionsClause(funcType, exec, Some(w), List.empty, yieldAll = false, None)(pos))
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowFunctionsClause(funcType, exec, None, yi, yieldAll = false, Some(w))(pos)) ++ r
      case _ if yieldAll =>
        Seq(ShowFunctionsClause(funcType, exec, None, List.empty, yieldAll = true, Some(getFullWithStarFromYield))(pos))
      case _ =>
        Seq(ShowFunctionsClause(funcType, exec, None, List.empty, yieldAll = false, None)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showTransactions: Gen[Query] = for {
    ids <- namesOrNameExpression
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
    use <- option(_use)
  } yield {
    val returnCypher5Types = usesCypher5
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowTransactionsClause(ids, Some(w), List.empty, yieldAll = false, None, returnCypher5Types)(pos))
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowTransactionsClause(ids, None, yi, yieldAll = false, Some(w), returnCypher5Types)(pos)) ++ r
      case _ if yieldAll =>
        Seq(
          ShowTransactionsClause(
            ids,
            None,
            List.empty,
            yieldAll = true,
            Some(getFullWithStarFromYield),
            returnCypher5Types
          )(pos)
        )
      case _ =>
        Seq(ShowTransactionsClause(ids, None, List.empty, yieldAll = false, None, returnCypher5Types)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _terminateTransactions: Gen[Query] = for {
    ids <- namesOrNameExpressionNonEmpty
    yields <- option(_yield)
    yieldAll <- boolean
    returns <- option(_return)
    use <- option(_use)
  } yield {
    val terminateClauses = (yields, returns) match {
      case (Some(y), r) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(TerminateTransactionsClause(ids, yi, yieldAll = false, Some(w), None)(pos)) ++ r
      case _ if yieldAll =>
        Seq(TerminateTransactionsClause(ids, List.empty, yieldAll = true, Some(getFullWithStarFromYield), None)(pos))
      case _ => Seq(TerminateTransactionsClause(ids, List.empty, yieldAll = false, None, None)(pos))
    }
    val fullClauses = use.map(u => u +: terminateClauses).getOrElse(terminateClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showSettings: Gen[Query] = for {
    names <- namesOrNameExpression
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
    use <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) => Seq(ShowSettingsClause(names, Some(w), List.empty, yieldAll = false, None)(pos))
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowSettingsClause(names, None, yi, yieldAll = false, Some(w))(pos)) ++ r
      case _ if yieldAll =>
        Seq(ShowSettingsClause(names, None, List.empty, yieldAll = true, Some(getFullWithStarFromYield))(pos))
      case _ => Seq(ShowSettingsClause(names, None, List.empty, yieldAll = false, None)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _combinedCommands: Gen[Query] = for {
    show <- showAsPartOfCombined
    terminate <- terminateAsPartOfCombined
    additionalShow <- zeroOrMore(showAsPartOfCombined)
    additionalTerminate <- zeroOrMore(terminateAsPartOfCombined)
    showFirst <- boolean
    returns <- _return
    use <- option(_use)
  } yield {

    val clauses =
      if (additionalShow.isEmpty && additionalTerminate.isEmpty) {
        // no additional clauses so take the two base ones
        if (showFirst) Seq(show, terminate) else Seq(terminate, show)
      } else if (additionalTerminate.isEmpty) {
        // Only additional show, make show only command
        // add base show to ensure at least 2 clauses
        // (can be a mix of different show commands)
        show +: additionalShow
      } else if (additionalShow.isEmpty) {
        // Only additional terminate, make terminate only command
        // add base terminate to ensure at least 2 clauses
        terminate +: additionalTerminate
      } else {
        // multiple additional clauses, add all together and mix the order they appear in
        val all = Seq(show, terminate) ++ additionalShow ++ additionalTerminate
        Random.shuffle(all)
      }

    val clausesWithReturn = clauses :+ returns
    val fullClauses = use.map(u => u +: clausesWithReturn).getOrElse(clausesWithReturn)
    SingleQuery(fullClauses)(pos)
  }

  private def showAsPartOfCombined: Gen[Clause] = for {
    ids <- namesOrNameExpression
    constraintType <- _constraintType
    indexType <- _indexType
    funcType <- oneOf(AllFunctions, BuiltInFunctions, UserDefinedFunctions)
    name <- _identifier
    exec <- option(oneOf(CurrentUser, User(name)(pos)))
    dbName <- _databaseName
    scope <- oneOf(
      SingleNamedDatabaseScope(dbName)(pos),
      AllDatabasesScope()(pos),
      DefaultDatabaseScope()(pos),
      HomeDatabaseScope()(pos)
    )
    asGraph <- boolean
    yields <- _yield
    yieldAll <- boolean
    clauseCypher5 <- const((item: List[CommandResultItem], all: Boolean, w: With) =>
      ShowTransactionsClause(ids, None, item, all, Some(w), returnCypher5Types = true)(pos)
    )
    clauseCypher25orAbove <- oneOf(
      (item: List[CommandResultItem], all: Boolean, w: With) =>
        ShowTransactionsClause(ids, None, item, all, Some(w), returnCypher5Types = false)(pos),
      (item: List[CommandResultItem], all: Boolean, w: With) =>
        ShowFunctionsClause(funcType, exec, None, item, all, Some(w))(pos),
      (item: List[CommandResultItem], all: Boolean, w: With) =>
        ShowProceduresClause(exec, None, item, all, Some(w))(pos),
      (item: List[CommandResultItem], all: Boolean, w: With) => ShowSettingsClause(ids, None, item, all, Some(w))(pos),
      (item: List[CommandResultItem], all: Boolean, w: With) =>
        ShowConstraintsClause(constraintType, None, item, all, Some(w), returnCypher5Columns = false)(pos),
      (item: List[CommandResultItem], all: Boolean, w: With) =>
        ShowIndexesClause(indexType, None, item, all, Some(w))(pos),
      (item: List[CommandResultItem], all: Boolean, w: With) =>
        ShowCurrentGraphTypeClause(asGraph, None, item, all, Some(w))(pos),
      (item: List[CommandResultItem], all: Boolean, w: With) =>
        ShowDatabasesClause(scope, None, item, all, Some(w), cypher5ColumnsOnly = false)(pos)
    )
    clause = if (usesCypher5) clauseCypher5 else clauseCypher25orAbove
  } yield {
    val (withClause, items) = turnYieldToWith(yields)
    if (yieldAll) clause(List.empty, true, getFullWithStarFromYield)
    else clause(items, false, withClause)
  }

  private def terminateAsPartOfCombined: Gen[Clause] = for {
    ids <- namesOrNameExpressionNonEmpty
    yields <- _yield
    yieldAll <- boolean
  } yield {
    val (withClause, items) = turnYieldToWith(yields)
    if (yieldAll)
      TerminateTransactionsClause(ids, List.empty, yieldAll = true, Some(getFullWithStarFromYield), None)(pos)
    else TerminateTransactionsClause(ids, items, yieldAll = false, Some(withClause), None)(pos)
  }

  private def commandClause: Gen[Clause] = for {
    show <- showAsPartOfCombined
    terminate <- terminateAsPartOfCombined
    clause <- oneOf(show, terminate)
  } yield clause

  /* names for show commands:
   * - can be an expression or a list of strings
   * - a singular string is parsed as string expression
   * - two or more names give a name list
   */
  private def namesOrNameExpression: Gen[CommandClauseNames] = for {
    nonEmptyIds <- namesOrNameExpressionNonEmpty
    ids <- oneOf(nonEmptyIds, NoNames)
  } yield {
    ids
  }

  private def namesOrNameExpressionNonEmpty: Gen[CommandClauseNames] = for {
    multiIdStringList <- twoOrMore(_stringLit)
    multiIdList <- const(ListLiteral(multiIdStringList)(pos))
    expr <- _expression
    ids <- oneOf(CommaSeparatedNames(multiIdList), ExpressionNames(expr))
  } yield {
    ids
  }

  private def turnYieldToWith(yieldClause: Yield): (With, List[CommandResultItem]) = {
    val returnItems = yieldClause.returnItems
    val yieldItems =
      returnItems.items.map(r => {
        val variable = r.expression.asInstanceOf[Variable]
        val aliasedVariable = r.alias.getOrElse(variable)
        CommandResultItem(variable.name, aliasedVariable)(pos)
      }).toList
    val itemOrder = if (returnItems.items.nonEmpty) Some(returnItems.items.map(_.name).toList) else None
    val (orderBy, where) = CommandClause.updateAliasedVariablesFromYieldInOrderByAndWhere(yieldClause)
    val withClause = With(
      distinct = false,
      ReturnItems(AdditiveProjection, Seq(), itemOrder)(returnItems.position),
      None,
      orderBy,
      yieldClause.skip,
      yieldClause.limit,
      where,
      withType = ParsedAsYield
    )(yieldClause.position)

    (withClause, yieldItems)
  }

  private def getFullWithStarFromYield =
    With(
      distinct = false,
      ReturnItems(AdditiveProjection, Seq())(pos),
      None,
      None,
      None,
      None,
      None,
      withType = ParsedAsYield
    )(pos)

  def _showCommands: Gen[Query] = {
    if (usesCypher5) {
      oneOf(
        _showIndexes,
        _showConstraints,
        _showProcedures,
        _showFunctions,
        _showTransactions,
        _terminateTransactions,
        _showSettings,
        _showDatabases,
        _combinedCommands
      )
    } else {
      oneOf(
        _showIndexes,
        _showConstraints,
        _showCurrentGraphType,
        _showProcedures,
        _showFunctions,
        _showTransactions,
        _terminateTransactions,
        _showSettings,
        _showDatabases,
        _combinedCommands
      )
    }
  }

  // Schema commands
  // ----------------------------------

  // Index and constraint

  def _variableProperty: Gen[Property] = for {
    map <- _variable
    key <- _propertyKeyName
  } yield Property(map, key)(pos)

  def _listOfProperties: Gen[List[Property]] = for {
    props <- oneOrMore(_variableProperty)
  } yield props

  def _maybeEmptyListOfProperties: Gen[List[Property]] = for {
    props <- zeroOrMore(_variableProperty)
  } yield props

  def _cypherTypeName: Gen[CypherType] = for {
    _type <- oneOf(allCypherTypeNamesFromReflection)
  } yield _type

  def _vectorCandidateType: Gen[CypherType] = for {
    _type <- oneOf(
      CTInteger.withIsNullable(false),
      CTInteger32.withIsNullable(false),
      CTInteger16.withIsNullable(false),
      CTInteger8.withIsNullable(false),
      CTFloat.withIsNullable(false),
      CTFloat32.withIsNullable(false)
    )
  } yield _type

  def _normalForm: Gen[NormalForm] = for {
    _type <- oneOf(
      NFCNormalForm,
      NFDNormalForm,
      NFKCNormalForm,
      NFKDNormalForm
    )
  } yield _type

  private val allCypherTypeNamesFromReflection: Set[CypherType] = {
    val reflections = new Reflections("org.neo4j.cypher.internal.util.symbols")
    var innerTypes = reflections.getSubTypesOf[CypherType](classOf[CypherType]).asScala.toSet
      .flatMap((cls: Class[_ <: CypherType]) => {
        try {
          // NOTHING, NULL
          val constructor = cls.getDeclaredConstructor(classOf[InputPosition])
          Set(constructor.newInstance(pos))
        } catch {
          case _: NoSuchMethodException =>
            try {
              // List<...>
              // Gets handled separately afterwords
              cls.getDeclaredConstructor(classOf[CypherType], classOf[Boolean], classOf[InputPosition])
              Set()
            } catch {
              case _: NoSuchMethodException =>
                // remaining types
                try {
                  val constructor = cls.getDeclaredConstructor(classOf[Boolean], classOf[InputPosition])
                  Set(constructor.newInstance(true, pos), constructor.newInstance(false, pos))
                } catch {
                  // Closed Dynamic Unions, we will handle after
                  case _: NoSuchMethodException => Set()
                }
            }
        }
      })

    innerTypes = if (usesCypher5) {
      innerTypes.filter {
        case _: UUIDType          => false
        case _: VectorType        => false
        case _: PropertyValueType => false
        case _                    => true
      }
    } else {
      innerTypes.filter {
        case _: PropertyValueCypher5Type => false
        case _                           => true
      }
    }

    val supportedInnerVectorTypes = Seq(
      Some(IntegerType(isNullable = false)(pos)),
      Some(Integer32Type(isNullable = false)(pos)),
      Some(Integer16Type(isNullable = false)(pos)),
      Some(Integer8Type(isNullable = false)(pos)),
      Some(FloatType(isNullable = false)(pos)),
      Some(Float32Type(isNullable = false)(pos)),
      None
    )

    val vectorTypes: Set[CypherType] = if (!usesCypher5) supportedInnerVectorTypes.flatMap(inner => {
      Set(
        VectorType(inner, Some(1024), isNullable = true)(pos),
        VectorType(inner, Some(1024), isNullable = false)(pos),
        VectorType(inner, None, isNullable = true)(pos),
        VectorType(inner, None, isNullable = false)(pos)
      )
    }).toSet
    else Set.empty

    val supportedInnerTypes = innerTypes.filter(_.hasCypherParserSupport) ++ vectorTypes

    val listTypes = supportedInnerTypes.flatMap(inner => {
      Set(
        ListType(inner, isNullable = true)(pos),
        ListType(inner, isNullable = false)(pos)
      )
    })
    val nestedListTypes = listTypes.flatMap(inner => {
      Set(
        ListType(inner, isNullable = true)(pos),
        ListType(inner, isNullable = false)(pos)
      )
    })

    // Don't use all list types as it adds too many types to test
    val unionTypesWith2Types = supportedInnerTypes.flatMap(inner1 => {
      (supportedInnerTypes ++ listTypes.take(5)).flatMap(inner2 => {
        Set(
          ClosedDynamicUnionType(Set(inner1, inner2))(pos).simplify
        )
      })
    })

    val allTypes = supportedInnerTypes ++ listTypes ++ nestedListTypes ++ unionTypesWith2Types
    // Normalize to avoid duplicate types
    allTypes.map(CypherType.normalizeTypes)
  }

  def _createIndex: Gen[CreateIndex] = for {
    variable <- _variable
    labelName <- _labelName
    labels <- _listOfLabels
    relType <- _relTypeName
    types <- _listOfRelTypes
    props <- _listOfProperties
    additionalProps <- if (usesCypher5) const(List.empty) else _maybeEmptyListOfProperties
    name <- option(_stringLiteralOrParameter)
    ifExistsDo <- _ifExistsDo
    options <- _optionsMapAsEitherOrNone
    fromDefault <- boolean
    use <- option(_use)
    rangeNodeIndex =
      CreateIndex.createRangeNodeIndex(
        variable,
        labelName,
        props,
        name,
        ifExistsDo,
        options,
        fromDefault,
        use
      )(pos)
    rangeRelIndex =
      CreateIndex.createRangeRelationshipIndex(
        variable,
        relType,
        props,
        name,
        ifExistsDo,
        options,
        fromDefault,
        use
      )(
        pos
      )
    lookupNodeIndex = CreateIndex.createLookupIndex(
      variable,
      isNodeIndex = true,
      FunctionInvocation(FunctionName(Labels.name)(pos), distinct = false, IndexedSeq(variable))(pos),
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    lookupRelIndex = CreateIndex.createLookupIndex(
      variable,
      isNodeIndex = false,
      FunctionInvocation(FunctionName(Type.name)(pos), distinct = false, IndexedSeq(variable))(pos),
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    fulltextNodeIndex =
      CreateIndex.createFulltextNodeIndex(
        variable,
        labels,
        props,
        name,
        ifExistsDo,
        options,
        use
      )(pos)
    fulltextRelIndex =
      CreateIndex.createFulltextRelationshipIndex(
        variable,
        types,
        props,
        name,
        ifExistsDo,
        options,
        use
      )(pos)
    textNodeIndex = CreateIndex.createTextNodeIndex(
      variable,
      labelName,
      props,
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    textRelIndex =
      CreateIndex.createTextRelationshipIndex(
        variable,
        relType,
        props,
        name,
        ifExistsDo,
        options,
        use
      )(pos)
    pointNodeIndex = CreateIndex.createPointNodeIndex(
      variable,
      labelName,
      props,
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    pointRelIndex =
      CreateIndex.createPointRelationshipIndex(
        variable,
        relType,
        props,
        name,
        ifExistsDo,
        options,
        use
      )(pos)
    vectorNodeIndex =
      CreateIndex.createVectorNodeIndex(
        variable,
        if (usesCypher5) List(labelName) else labels,
        props,
        additionalProps,
        name,
        ifExistsDo,
        options,
        use
      )(pos)
    vectorRelIndex =
      CreateIndex.createVectorRelationshipIndex(
        variable,
        if (usesCypher5) List(relType) else types,
        props,
        additionalProps,
        name,
        ifExistsDo,
        options,
        use
      )(pos)
    command <- oneOf(
      rangeNodeIndex,
      rangeRelIndex,
      lookupNodeIndex,
      lookupRelIndex,
      fulltextNodeIndex,
      fulltextRelIndex,
      textNodeIndex,
      textRelIndex,
      pointNodeIndex,
      pointRelIndex,
      vectorNodeIndex,
      vectorRelIndex
    )
  } yield command

  def _dropIndex: Gen[DropIndexOnName] = for {
    name <- _stringLiteralOrParameter
    ifExists <- boolean
    use <- option(_use)
  } yield DropIndexOnName(name, ifExists, use)(pos)

  def _createConstraint: Gen[CreateConstraint] = for {
    variable <- _variable
    labelName <- _labelName
    relTypeName <- _relTypeName
    props <- _listOfProperties
    prop <- _variableProperty
    propType <- _cypherTypeName
    name <- option(_stringLiteralOrParameter)
    ifExistsDo <- _ifExistsDo
    options <- _optionsMapAsEitherOrNone
    use <- option(_use)
    nodeKey = CreateConstraint.createNodeKeyConstraint(
      variable,
      labelName,
      props,
      name,
      ifExistsDo,
      options,
      usesCypher5,
      use
    )(pos)
    relKey = CreateConstraint.createRelationshipKeyConstraint(
      variable,
      relTypeName,
      props,
      name,
      ifExistsDo,
      options,
      usesCypher5,
      use
    )(pos)
    nodeUniqueness = CreateConstraint.createNodePropertyUniquenessConstraint(
      variable,
      labelName,
      Seq(prop),
      name,
      ifExistsDo,
      options,
      usesCypher5,
      use
    )(pos)
    compositeUniqueness = CreateConstraint.createNodePropertyUniquenessConstraint(
      variable,
      labelName,
      props,
      name,
      ifExistsDo,
      options,
      usesCypher5,
      use
    )(pos)
    relUniqueness = CreateConstraint.createRelationshipPropertyUniquenessConstraint(
      variable,
      relTypeName,
      props,
      name,
      ifExistsDo,
      options,
      usesCypher5,
      use
    )(pos)
    nodeExistence = CreateConstraint.createNodePropertyExistenceConstraint(
      variable,
      labelName,
      prop,
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    relExistence = CreateConstraint.createRelationshipPropertyExistenceConstraint(
      variable,
      relTypeName,
      prop,
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    nodePropType = CreateConstraint.createNodePropertyTypeConstraint(
      variable,
      labelName,
      prop,
      propType,
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    relPropType = CreateConstraint.createRelationshipPropertyTypeConstraint(
      variable,
      relTypeName,
      prop,
      propType,
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    command <- oneOf(
      nodeKey,
      relKey,
      nodeUniqueness,
      compositeUniqueness,
      relUniqueness,
      nodeExistence,
      relExistence,
      nodePropType,
      relPropType
    )
  } yield command

  def _dropConstraint: Gen[DropConstraintOnName] = for {
    name <- _stringLiteralOrParameter
    ifExists <- boolean
    use <- option(_use)
  } yield DropConstraintOnName(name, ifExists, use)(pos)

  def _indexCommand: Gen[SchemaCommand] = oneOf(_createIndex, _dropIndex)

  def _constraintCommand: Gen[SchemaCommand] = oneOf(_createConstraint, _dropConstraint)

  // Graph Type

  def _inlineConstraintBody: Gen[PropertyInlineConstraintBody] = for {
    const <- oneOf(PropertyInlineKeyConstraint()(pos), PropertyInlineUniquenessConstraint()(pos))
  } yield const

  def _propertyType: Gen[PropertyType] = for {
    keyName <- _propertyKeyName
    propType <- _cypherTypeName
    constraint <- option(_inlineConstraintBody)
  } yield PropertyType(keyName, propType, constraint)(pos)

  def _edgeNode: Gen[NodeTypeReference] = for {
    ref <- _variable
    label <- _labelName
    optRef <- option(ref)
    labelOrVar <- oneOf(
      NodeTypeReferenceByVariable(ref)(pos),
      NodeTypeReferenceByLabel(label)(pos),
      NodeTypeReferenceByIdentifyingLabel(label, optRef)(pos),
      EmptyNodeTypeReference()(pos)
    )
  } yield labelOrVar

  def _edgeType: Gen[EdgeType] = for {
    src <- _edgeNode
    dest <- _edgeNode
    variable <- option(_variable)
    relType <- _relTypeName
    propTypes <- zeroOrMore(_propertyType).map(_.distinctBy(_.name))
    constraints <- variable.map(v => zeroOrMore(_graphTypeConstraintBody(v, inline = true))).getOrElse(const(Seq()))
    options <- _optionsMapAsEitherOrNone
    constraintsWithOptions = constraints.map((_, options)).toSet
  } yield EdgeType(src, variable, relType, propTypes.toSet, dest, constraintsWithOptions)(pos)

  def _nodeType: Gen[NodeType] = for {
    label <- _labelName
    variable <- option(_variable)
    additionalLabels <- zeroOrMore(_labelName)
    propTypes <- zeroOrMore(_propertyType).map(_.distinctBy(_.name))
    constraints <- variable.map(v => zeroOrMore(_graphTypeConstraintBody(v, inline = true))).getOrElse(const(Seq()))
    options <- _optionsMapAsEitherOrNone
    constraintsWithOptions = constraints.map((_, options)).toSet
  } yield NodeType(variable, label, additionalLabels.toSet, propTypes.toSet, constraintsWithOptions)(pos)

  def _graphTypeElementReference(variable: Variable): Gen[GraphTypeElementReference] = for {
    label <- _labelName
    nodeLabRef = NodeTypeReferenceByLabel(label, Some(variable))(pos)
    nodeLabIdentRef = NodeTypeReferenceByIdentifyingLabel(label, Some(variable))(pos)
    nodeVarRef = NodeTypeReferenceByVariable(variable)(pos)
    edgeVarRef = EdgeTypeReferenceByVariable(variable)(pos)
    relType <- _relTypeName
    relTypeRef = EdgeTypeReferenceByLabel(relType, Some(variable))(pos)
    relTypeIdentRef = EdgeTypeReferenceByIdentifyingLabel(relType, Some(variable))(pos)
    ref <- oneOf(nodeLabRef, nodeLabIdentRef, nodeVarRef, edgeVarRef, relTypeRef, relTypeIdentRef)
  } yield ref

  def _graphTypeConstraintProperty(alias: Variable): Gen[Property] = for {
    pkn <- _propertyKeyName
  } yield Property(alias, pkn)(pos)

  def _graphTypeConstraintBody(alias: Variable, inline: Boolean): Gen[GraphTypeConstraintBody] = for {
    props <- oneOrMore(_graphTypeConstraintProperty(alias))
    oneProp <- _graphTypeConstraintProperty(alias)
    ct <- _cypherTypeName
    key = KeyConstraint(ArraySeq.from(props))(pos)
    exists = ExistenceConstraint(ArraySeq(oneProp))(pos)
    unique = UniquenessConstraint(ArraySeq.from(props))(pos)
    typeConst = PropertyTypeConstraint(ArraySeq(oneProp), ct)(pos)
    body <- oneOf(key, exists, unique, typeConst)
    restrictedBody <- oneOf(key, unique)
  } yield if (inline) restrictedBody else body

  def _graphTypeConstraintDef: Gen[GraphTypeConstraintDefinition] = for {
    name <- option(_identifier)
    variable <- _variable
    ref <- _graphTypeElementReference(variable)
    options <- _optionsMapAsEitherOrNone
    body <- _graphTypeConstraintBody(variable, inline = false)
  } yield GraphTypeConstraintDefinition(name, ref, body, options)(pos)

  def _graphTypeConstraintName: Gen[GraphTypeConstraintName] = for {
    name <- _identifier
  } yield GraphTypeConstraintName(name)(pos)

  def _graphType(isDrop: Boolean): Gen[GraphType] = for {
    nodeTypes <- oneOrMore(_nodeType).map(_.distinctBy(_.identifyingLabel).toSet)
    edgeTypes <- oneOrMore(_edgeType).map(_.distinctBy(_.identifyingLabel).toSet)
    constraintDefs <- zeroOrMore(_graphTypeConstraintDef)
    constraintNames <- zeroOrMore(_graphTypeConstraintName)
    constraints = if (isDrop) constraintNames else constraintDefs
  } yield GraphType(nodeTypes ++ edgeTypes, constraints.toSet)(pos)

  def _alterCurrentGraphTypeCommand: Gen[AlterCurrentGraphType] = for {
    graphType <- _graphType(false)
    graphTypeWithConstraintNames <- _graphType(true)
    operation <- oneOf(
      AlterCurrentGraphType.Set,
      AlterCurrentGraphType.Add,
      AlterCurrentGraphType.Alter,
      AlterCurrentGraphType.Drop
    )
    gt = if (operation == AlterCurrentGraphType.Drop) graphTypeWithConstraintNames else graphType
  } yield AlterCurrentGraphType(gt, operation)(pos)

  // Top level schema command method

  def _schemaCommand: Gen[SchemaCommand] = {
    if (usesCypher5) {
      oneOf(_indexCommand, _constraintCommand)
    } else {
      oneOf(_indexCommand, _constraintCommand, _alterCurrentGraphTypeCommand)
    }
  }

  // Administration commands
  // ----------------------------------

  def _nameAsEither: Gen[Either[String, Parameter]] = for {
    name <- _identifier
    param <- _stringParameter
    finalName <- oneOf(Left(name), Right(param))
  } yield finalName

  def _stringLiteralOrParameter: Gen[Expression] = for {
    name <- _stringLit
    param <- _stringParameter
    finalName <- oneOf(name, param)
  } yield finalName

  def _databaseName: Gen[DatabaseName] = for {
    namespacedName <- _namespacedName
    param <- _stringParameter
    finalName <- oneOf(namespacedName, ParameterName(param)(pos))
  } yield finalName

  def _databaseNameNoNamespace: Gen[DatabaseName] = for {
    name <- listOfN(1, _identifier)
    param <- _stringParameter
    finalName <- if (usesCypher5) oneOf(NamespacedName.apply(name)(pos), ParameterName(param)(pos))
    else oneOf(NamespacedName(name, None)(pos), ParameterName(param)(pos))
  } yield finalName

  def _optionsMapAsEitherOrNone: Gen[Options] = for {
    map <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    param <- _mapParameter
    finalMap <- oneOf(OptionsMap(map)(pos), OptionsParam(param)(pos), NoOptions)
  } yield finalMap

  def _optionsForAlterDatabaseOrNone: Gen[Options] = for {
    map <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    finalMap <- oneOf(OptionsMap(map)(pos), NoOptions)
  } yield finalMap

  def _optionsToRemove(hasSetClause: Boolean): Gen[Set[String]] =
    if (hasSetClause) {
      // Must be empty
      Gen.containerOfN[Set, String](0, _identifier)
    } else {
      // Must have at least one entry
      Gen.chooseNum(1, 5).flatMap { n =>
        Gen.containerOfN[Set, String](n, _identifier)
      }
    }

  def _optionsMapAsEither: Gen[Options] = for {
    map <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    param <- _mapParameter
    finalMap <- oneOf(OptionsMap(map)(pos), OptionsParam(param)(pos))
  } yield finalMap

  def _optionalMapAsEither: Gen[Either[Map[String, Expression], Parameter]] = for {
    map <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    param <- _mapParameter
    finalMap <- oneOf(Left(map), Right(param))
  } yield finalMap

  def _listOfNameOfEither: Gen[List[Either[String, Parameter]]] = for {
    names <- oneOrMore(_nameAsEither)
  } yield names

  def _listOfStringLiteralOrParam: Gen[List[Expression]] = for {
    names <- oneOrMore(_stringLiteralOrParameter)
  } yield names

  def _password: Gen[Expression] =
    oneOf(_sensitiveStringParameter, _sensitiveAutoStringParameter, _sensitiveStringLiteral)

  def _remoteAliasStoredCredentials: Gen[RemoteAliasStoredCredentials] = for {
    username <- _stringLiteralOrParameter
    password <- _password
  } yield RemoteAliasStoredCredentials(username, password)(pos)

  def _oidcCredentialForwarding: Gen[OidcCredentialForwarding] = OidcCredentialForwarding()(pos)

  def _remoteAliasCredentials: Gen[RemoteAliasCredentials] =
    if (usesCypher5) _remoteAliasStoredCredentials
    else oneOf(_remoteAliasStoredCredentials, _oidcCredentialForwarding)

  def _ifExistsDo: Gen[IfExistsDo] =
    oneOf(IfExistsReplace, IfExistsDoNothing, IfExistsThrowError, IfExistsInvalidSyntax)

  def _namespacedName: Gen[NamespacedName] = for {
    name <- if (usesCypher5) {
      listOfN(1, _identifier)
    } else {
      listOfN(2, _identifier)
    }
    namespace <- _identifier
    maybeNamespace <- if (usesCypher5) {
      option(namespace)
    } else {
      const(None)
    }
  } yield NamespacedName(name, maybeNamespace)(pos)

  def _topology: Gen[Topology] = for {
    primaries <- option(_primaries)
    secondaries <-
      if (primaries.nonEmpty) {
        option(_secondaries)
      } else
        some(_secondaries)
  } yield Topology(primaries, secondaries)

  def _primaries: Gen[Either[Int, Parameter]] = for {
    intPrimaries <- chooseNum[Int](1, Integer.MAX_VALUE)
    paramPrimaries <- _intParameter
    primaries <- oneOf(Left(intPrimaries), Right(paramPrimaries))
  } yield primaries

  def _secondaries: Gen[Either[Int, Parameter]] = for {
    intSecondaries <- chooseNum[Int](0, Integer.MAX_VALUE)
    paramSecondaries <- _parameter
    secondaries <- oneOf(Left(intSecondaries), Right(paramSecondaries))
  } yield secondaries

  def _replicas: Gen[Either[Int, Parameter]] = for {
    intReplicas <- chooseNum[Int](1, Integer.MAX_VALUE)
    paramReplicas <- _intParameter
    replicas <- oneOf(Left(intReplicas), Right(paramReplicas))
  } yield replicas

  def _shardDef(isCreate: Boolean): Gen[ShardDefinition] = for {
    shardCount <- if (isCreate) chooseNum[Int](1, Integer.MAX_VALUE) else const(0)
    graphTopology <- option(_topology)
    replicas: Option[Either[Int, Parameter]] <- if (graphTopology.nonEmpty) option(_replicas) else some(_replicas)
  } yield ShardDefinition(shardCount, graphTopology, replicas)

  def _defaultLanguage: Gen[CypherVersion] = oneOf(CypherVersion.values)

  // User commands

  def _showUsers: Gen[ShowUsers] = for {
    yields <- _eitherYieldOrWhere
    withAuth <- boolean
  } yield ShowUsers(yields, withAuth)(pos)

  def _showCurrentUser: Gen[ShowCurrentUser] = for {
    yields <- _eitherYieldOrWhere
  } yield ShowCurrentUser(yields)(pos)

  def _eitherYieldOrWhere: Gen[YieldOrWhere] = for {
    yields <- _yield
    where <- _where
    returns <- option(_return)
    eyw <- oneOf(Seq(Left((yields, returns)), Right(where)))
    oeyw <- option(eyw)
  } yield oeyw

  def _createUser: Gen[CreateUser] = for {
    userName <- _stringLiteralOrParameter
    oldNativeAuth <- _nativeAuth(mandatoryPassword = true)
    newAuths <- _auths(mandatoryPassword = true, needsAuth = oldNativeAuth.isEmpty)
    suspended <- option(boolean)
    homeDatabase <- option(_setHomeDatabaseAction)
    ifExistsDo <- _ifExistsDo
    tags <- if (usesCypher5) const(None) else option(_tagsValue.map(SetTags(_)(pos)))
  } yield CreateUser(userName, UserOptions(suspended, homeDatabase), ifExistsDo, newAuths, oldNativeAuth, tags)(pos)

  def _renameUser: Gen[RenameUser] = for {
    fromUserName <- _stringLiteralOrParameter
    toUserName <- _stringLiteralOrParameter
    ifExists <- boolean
  } yield RenameUser(fromUserName, toUserName, ifExists)(pos)

  def _dropUser: Gen[DropUser] = for {
    userName <- _stringLiteralOrParameter
    ifExists <- boolean
  } yield DropUser(userName, ifExists)(pos)

  def _alterUser: Gen[AlterUser] = for {
    userName <- _stringLiteralOrParameter
    ifExists <- boolean
    oldNativeAuth <- _nativeAuth(mandatoryPassword = false)
    newAuths <- _auths(mandatoryPassword = false, needsAuth = false)
    removeAuth <- _removeAuth()
    suspended <- option(boolean)
    tags <- if (usesCypher5) const(Seq.empty[UserTagsAction]) else oneOf(const(Seq.empty), _alterUsersTags)
    // Need at least one SET or REMOVE clause
    homeDatabase <-
      if (oldNativeAuth.isEmpty && newAuths.isEmpty && removeAuth.isEmpty && suspended.isEmpty && tags.isEmpty)
        oneOf(some(_setHomeDatabaseAction), some(RemoveHomeDatabaseAction))
      else oneOf(option(_setHomeDatabaseAction), option(RemoveHomeDatabaseAction))
  } yield AlterUser(
    userName,
    UserOptions(suspended, homeDatabase),
    ifExists,
    newAuths,
    oldNativeAuth,
    removeAuth,
    tags
  )(pos)

  def _tagsValue: Gen[Expression] = oneOf(
    _stringLit,
    _stringParameter,
    _listOf(_stringLit)
  )

  def _alterUsersTags: Gen[Seq[UserTagsAction]] = oneOf(
    _tagsValue.map(expr => Seq(SetTags(expr)(pos))),
    _tagsValue.map(expr => Seq(AddTags(expr)(pos))),
    for {
      remove <- oneOf(
        _tagsValue.map(expr => RemoveTags(expr)(pos): UserTagsAction),
        const(RemoveAllTags()(pos): UserTagsAction)
      )
      maybeAdd <- option(_tagsValue.map(expr => AddTags(expr)(pos)))
    } yield Seq(remove) ++ maybeAdd
  )

  def _alterUsers: Gen[AlterUsers] = for {
    userNames <- oneOrMore(_stringLiteralOrParameter)
    ifExists <- boolean
    tags <- _alterUsersTags
  } yield AlterUsers(userNames, ifExists, tags)(pos)

  def _passwordClause: Gen[Password] = for {
    password <- _password
    encrypted <- boolean
  } yield Password(password, encrypted)(pos)

  def _passwordChangeClause: Gen[PasswordChange] = for {
    changeRequired <- boolean
  } yield PasswordChange(changeRequired)(pos)

  def _nativeAuth(mandatoryPassword: Boolean): Gen[Option[Auth]] = for {
    password <- if (mandatoryPassword) some(_passwordClause) else option(_passwordClause)
    changeRequired <- option(_passwordChangeClause)
    // want to be able to have no native auth even if the password is mandatory when there is one
    forceNone <- if (mandatoryPassword) boolean else const(false)
  } yield {
    if (forceNone) None
    else (password, changeRequired) match {
      case (Some(p), Some(c)) => Some(Auth(NATIVE_AUTH, List(p, c))(pos))
      case (Some(p), None)    => Some(Auth(NATIVE_AUTH, List(p))(pos))
      case (None, Some(c))    => Some(Auth(NATIVE_AUTH, List(c))(pos))
      case _                  => None
    }
  }

  def _authIds(): Gen[AuthId] = for {
    id <- _stringLiteralOrParameter
  } yield AuthId(id)(pos)

  def _externalAuth(): Gen[Auth] = for {
    provider <- string
    attr <- oneOrMore(_authIds())
  } yield Auth(provider, attr)(pos)

  def _auths(mandatoryPassword: Boolean, needsAuth: Boolean): Gen[List[Auth]] = for {
    nativeAuth <- _nativeAuth(mandatoryPassword)
    externalAuths <- if (needsAuth && nativeAuth.isEmpty) oneOrMore(_externalAuth()) else zeroOrMore(_externalAuth())
  } yield externalAuths ++ nativeAuth

  def _removeAuthExpr(): Gen[Expression] = for {
    s <- _stringLit
    l <- _listOf(_stringLit)
    p <- _parameter
    auth <- oneOf(s, l, p)
  } yield auth

  def _removeAuth(): Gen[RemoveAuth] = for {
    removeAll <- boolean
    // prettifier removes any explicit remove in presence of remove all
    auths <- if (!removeAll) oneOrMore(_removeAuthExpr()) else const(List.empty)
  } yield RemoveAuth(removeAll, auths)

  def _setHomeDatabaseAction: Gen[SetHomeDatabaseAction] = _databaseName.map(db => SetHomeDatabaseAction(db))

  def _setOwnPassword: Gen[SetOwnPassword] = for {
    newPassword <- _password
    oldPassword <- _password
  } yield SetOwnPassword(newPassword, oldPassword)(pos)

  def _userCommand: Gen[AdministrationCommand] =
    if (usesCypher5)
      oneOf(_showUsers, _showCurrentUser, _createUser, _renameUser, _dropUser, _alterUser, _setOwnPassword)
    else
      oneOf(_showUsers, _showCurrentUser, _createUser, _renameUser, _dropUser, _alterUser, _alterUsers, _setOwnPassword)

  // Auth rule commands

  def _showAuthRules: Gen[ShowAuthRules] = for {
    yields <- _eitherYieldOrWhere
    asCommands <- boolean
  } yield ShowAuthRules(yields, asCommands)(pos)

  def _createAuthRule: Gen[CreateAuthRule] = for {
    authRuleName <- _stringLiteralOrParameter
    ifExistsDo <- _ifExistsDo
    condition <- _authRuleCondition
    enabled <- option(_AuthRuleEnabled)
  } yield CreateAuthRule(authRuleName, ifExistsDo, List(condition) ++ enabled)(pos)

  def _alterAuthRule: Gen[AlterAuthRule] = for {
    authRuleName <- _stringLiteralOrParameter
    ifExists <- boolean
    condition <- option(_authRuleCondition)
    enabled <- if (condition.isEmpty) some(_AuthRuleEnabled) else option(_AuthRuleEnabled)
  } yield AlterAuthRule(authRuleName, ifExists, condition.toList ++ enabled)(pos)

  def _renameAuthRule: Gen[RenameAuthRule] = for {
    fromAuthRuleName <- _stringLiteralOrParameter
    toAuthRuleName <- _stringLiteralOrParameter
    ifExists <- boolean
  } yield RenameAuthRule(fromAuthRuleName, toAuthRuleName, ifExists)(pos)

  def _dropAuthRule: Gen[DropAuthRule] = for {
    authRuleName <- _stringLiteralOrParameter
    ifExists <- boolean
  } yield DropAuthRule(authRuleName, ifExists)(pos)

  def _authRuleCondition: Gen[AuthRuleCondition] = for {
    expression <- _expression
  } yield AuthRuleCondition(expression)(pos)

  def _AuthRuleEnabled: Gen[AuthRuleEnabled] = for {
    enabled <- boolean
  } yield AuthRuleEnabled(enabled)(pos)

  def _authRuleCommand: Gen[AdministrationCommand] =
    oneOf(
      _showAuthRules.filterNot(_ => usesCypher5),
      _createAuthRule.filterNot(_ => usesCypher5),
      _alterAuthRule.filterNot(_ => usesCypher5),
      _renameAuthRule.filterNot(_ => usesCypher5),
      _dropAuthRule.filterNot(_ => usesCypher5)
    )

  // Role commands

  def _showRoles: Gen[ShowRoles] = for {
    withUsers <- boolean
    withAuthRules <- boolean
    asCommands <- boolean
    showAll <- boolean
    yields <- _eitherYieldOrWhere
  } yield ShowRoles(
    withUsers,
    if (withUsers || usesCypher5) false else withAuthRules,
    showAll,
    if (usesCypher5) false else asCommands,
    yields
  )(pos)

  def _createRole: Gen[CreateRole] = for {
    roleName <- _stringLiteralOrParameter
    fromRoleName <- option(_stringLiteralOrParameter)
    ifExistsDo <- _ifExistsDo
    immutable <- boolean
  } yield CreateRole(roleName, immutable, fromRoleName, ifExistsDo)(pos)

  def _renameRole: Gen[RenameRole] = for {
    fromRoleName <- _stringLiteralOrParameter
    toRoleName <- _stringLiteralOrParameter
    ifExists <- boolean
  } yield RenameRole(fromRoleName, toRoleName, ifExists)(pos)

  def _dropRole: Gen[DropRole] = for {
    roleName <- _stringLiteralOrParameter
    ifExists <- boolean
  } yield DropRole(roleName, ifExists)(pos)

  def _grantRolesToUsers: Gen[GrantRolesToUsers] = for {
    roleNames <- _listOfStringLiteralOrParam
    userNames <- _listOfStringLiteralOrParam
  } yield GrantRolesToUsers(roleNames, userNames)(pos)

  def _grantRolesToAuthRules: Gen[GrantRolesToAuthRules] = for {
    roleNames <- _listOfStringLiteralOrParam
    ruleNames <- _listOfStringLiteralOrParam
  } yield GrantRolesToAuthRules(roleNames, ruleNames)(pos)

  def _grantRole: Gen[AdministrationCommand] = {
    if (usesCypher5) _grantRolesToUsers
    else oneOf(_grantRolesToUsers, _grantRolesToAuthRules)
  }

  def _revokeRolesFromUsers: Gen[RevokeRolesFromUsers] = for {
    roleNames <- _listOfStringLiteralOrParam
    userNames <- _listOfStringLiteralOrParam
  } yield RevokeRolesFromUsers(roleNames, userNames)(pos)

  def _revokeRolesFromAuthRules: Gen[RevokeRolesFromAuthRules] = for {
    roleNames <- _listOfStringLiteralOrParam
    ruleNames <- _listOfStringLiteralOrParam
  } yield RevokeRolesFromAuthRules(roleNames, ruleNames)(pos)

  def _revokeRole: Gen[AdministrationCommand] = {
    if (usesCypher5) _revokeRolesFromUsers
    else oneOf(_revokeRolesFromUsers, _revokeRolesFromAuthRules)
  }

  def _roleCommand: Gen[AdministrationCommand] = oneOf(
    _showRoles,
    _createRole,
    _renameRole,
    _dropRole,
    _grantRole,
    _revokeRole
  )

  // Privilege commands

  def _revokeType: Gen[RevokeType] = oneOf(RevokeGrantType()(pos), RevokeDenyType()(pos), RevokeBothType()(pos))

  def _dbmsAction: Gen[DbmsAction] = oneOf(Seq(
    AllDbmsAction,
    ExecuteProcedureAction,
    ExecuteBoostedProcedureAction,
    ExecuteAdminProcedureAction,
    ExecuteFunctionAction,
    ExecuteBoostedFunctionAction,
    ImpersonateUserAction,
    AllUserActions,
    ShowUserAction,
    CreateUserAction,
    RenameUserAction,
    SetUserStatusAction,
    SetUserHomeDatabaseAction,
    SetPasswordsAction,
    SetAuthAction,
    AlterUserAction,
    DropUserAction,
    AllRoleActions,
    ShowRoleAction,
    CreateRoleAction,
    RenameRoleAction,
    DropRoleAction,
    AssignRoleAction,
    RemoveRoleAction,
    AllDatabaseManagementActions,
    CreateDatabaseAction,
    CreateCompositeDatabaseAction,
    DropDatabaseAction,
    DropCompositeDatabaseAction,
    CompositeDatabaseManagementActions,
    AllAliasManagementActions,
    CreateAliasAction,
    DropAliasAction,
    AlterAliasAction,
    ShowAliasAction,
    AllPrivilegeActions,
    ShowPrivilegeAction,
    AssignPrivilegeAction,
    RemovePrivilegeAction,
    ServerManagementAction,
    ShowServerAction,
    ShowSettingAction
  ) ++ Seq(
    AllAuthRuleActions,
    CreateAuthRuleAction,
    RenameAuthRuleAction,
    AlterAuthRuleAction,
    DropAuthRuleAction,
    ShowAuthRuleAction,
    AllUserMetadataActions,
    ShowUserMetadataAction,
    SetUserMetadataAction,
    AllSecretManagementActions,
    ReadSecretsAction,
    WriteSecretsAction,
    ShowSecretsAction
  ) // Actions not available in Cypher 5
    .filterNot(_ => usesCypher5))

  def _databaseAction: Gen[DatabaseAction] = oneOf(
    StartDatabaseAction,
    StopDatabaseAction,
    AllDatabaseAction,
    AlterDatabaseAction(usesCypher5),
    SetDatabaseAccessAction(usesCypher5),
    SetDatabaseDefaultLanguageAction(usesCypher5),
    AlterCompositeDatabaseAction(usesCypher5),
    AccessDatabaseAction,
    AllIndexActions,
    CreateIndexAction,
    DropIndexAction,
    ShowIndexAction,
    AllConstraintActions,
    CreateConstraintAction,
    DropConstraintAction,
    ShowConstraintAction,
    AllTokenActions,
    CreateNodeLabelAction,
    CreateRelationshipTypeAction,
    CreatePropertyKeyAction,
    AllTransactionActions,
    ShowTransactionAction,
    TerminateTransactionAction
  )

  def _graphAction: Gen[GraphAction] = oneOf(
    TraverseAction,
    ReadAction,
    MatchAction,
    MergeAdminAction,
    CreateElementAction,
    DeleteElementAction,
    WriteAction,
    RemoveLabelAction,
    SetLabelAction,
    SetPropertyAction,
    AllGraphAction
  )

  def _dbmsQualifier(dbmsAction: DbmsAction): Gen[List[PrivilegeQualifier]] =
    if (dbmsAction == ExecuteProcedureAction || dbmsAction == ExecuteBoostedProcedureAction) {
      // Procedures
      for {
        glob <- _glob
        procedures <- oneOrMore(ProcedureQualifier(glob)(pos))
        qualifier <- frequency(7 -> procedures, 3 -> List(ProcedureQualifier("*")(pos)))
      } yield qualifier
    } else if (dbmsAction == ExecuteFunctionAction || dbmsAction == ExecuteBoostedFunctionAction) {
      // Functions
      for {
        glob <- _glob
        functions <- oneOrMore(FunctionQualifier(glob)(pos))
        qualifier <- frequency(7 -> functions, 3 -> List(FunctionQualifier("*")(pos)))
      } yield qualifier

    } else if (dbmsAction == ReadSecretsAction) {
      // Secrets
      for {
        name <- _nameAsEither
        secrets <- listOfN(1, SecretQualifier(name)(pos))
        qualifier <- frequency(7 -> secrets, 3 -> List(SecretQualifier(Left("*"))(pos)))
      } yield qualifier
    } else if (dbmsAction == ShowSettingAction) {
      // Settings
      for {
        glob <- _glob
        configs <- oneOrMore(SettingQualifier(glob)(pos))
        qualifier <- frequency(7 -> configs, 3 -> List(SettingQualifier("*")(pos)))
      } yield qualifier
    } else if (dbmsAction == ImpersonateUserAction) {
      // impersonation
      for {
        userNames <- _listOfStringLiteralOrParam
        qualifier <- frequency(7 -> userNames.map(UserQualifier(_)(pos)), 3 -> List(UserAllQualifier()(pos)))
      } yield qualifier
    } else {
      // All other dbms privileges have AllQualifier
      List(AllQualifier()(pos))
    }

  def _databaseQualifier(haveUserQualifier: Boolean): Gen[List[DatabasePrivilegeQualifier]] =
    if (haveUserQualifier) {
      for {
        userNames <- _listOfStringLiteralOrParam
        qualifier <- frequency(7 -> userNames.map(UserQualifier(_)(pos)), 3 -> List(UserAllQualifier()(pos)))
      } yield qualifier
    } else {
      List(AllDatabasesQualifier()(pos))
    }

  def _graphQualifier: Gen[List[GraphPrivilegeQualifier]] = for {
    qualifierNames <- oneOrMore(_identifier)
    qualifier <- oneOf(
      qualifierNames.map(RelationshipQualifier(_)(pos)),
      List(RelationshipAllQualifier()(pos)),
      qualifierNames.map(LabelQualifier(_)(pos)),
      List(LabelAllQualifier()(pos)),
      qualifierNames.map(ElementQualifier(_)(pos)),
      List(ElementsAllQualifier()(pos))
    )
  } yield qualifier

  def _propertyRuleGraphQualifier: Gen[List[GraphPrivilegeQualifier]] = for {
    multiQualifiers <- oneOrMore(_identifier)
    variable <- _variable
    expression <- _propertyRuleExpression(variable)
    qualifier <- {
      val nodePatternQualifiers = Seq(
        PatternQualifier(multiQualifiers.map(LabelQualifier(_)(pos)), Some(variable), expression, Node),
        PatternQualifier(List(LabelAllQualifier()(pos)), Some(variable), expression, Node)
      )
      val relPatternQualifiers = Seq(
        PatternQualifier(multiQualifiers.map(RelationshipQualifier(_)(pos)), Some(variable), expression, Relationship),
        PatternQualifier(List(RelationshipAllQualifier()(pos)), Some(variable), expression, Relationship)
      )
      oneOf(
        nodePatternQualifiers ++ relPatternQualifiers
      )
    }
  } yield List(qualifier)

  def _propertyRuleExpression(variable: Variable): Gen[Expression] = for {
    propertyName <- _propertyKeyName
    rhs <- _propertyRuleLiteral
    v <- _variable
    pred <- oneOf(
      _propertyRuleUnaryPredicate(Property(variable, propertyName)(pos)),
      _propertyRuleComparisonPredicate(Property(variable, propertyName)(pos), rhs),
      _propertyRuleUnaryPredicate(Property(v, propertyName)(pos)),
      _propertyRuleComparisonPredicate(Property(v, propertyName)(pos), rhs),
      _propertyRulePropertyInList(Property(v, propertyName)(pos))
    )
  } yield pred

  def _propertyRuleLiteral: Gen[Expression] = {
    oneOf(
      lzy(_stringLit),
      lzy(_booleanLit),
      lzy(_signedDecIntLit),
      lzy(_signedHexIntLit),
      lzy(_signedOctIntLit),
      lzy(_doubleLit),
      lzy(_parameter),
      lzy(_infinityLit),
      lzy(_nanLit)
    )
  }

  def _propertyRuleComparisonPredicate(l: Expression, r: Expression): Gen[Expression] = {
    val predicates = Seq(
      Equals(l, r)(pos),
      NotEquals(l, r)(pos),
      In(l, ListLiteral(Seq(r))(pos))(pos),
      In(r, l)(pos)
    ) ++ _inequalitiesPredicate(l, r)

    oneOf(
      predicates ++ _notExpressions(predicates)
    )
  }

  def _inequalitiesPredicate(l: Expression, r: Expression): Seq[Expression] = Seq(
    GreaterThan(l, r)(pos),
    GreaterThanOrEqual(l, r)(pos),
    LessThan(l, r)(pos),
    LessThanOrEqual(l, r)(pos)
  )

  def _notExpressions(predicates: Seq[Expression]): Seq[Expression] =
    predicates.map(Not(_)(pos))

  def _propertyRuleUnaryPredicate(p: Property): Gen[Expression] = oneOf(
    IsNull(p)(pos),
    Not(IsNull(p)(pos))(pos),
    IsNotNull(p)(pos),
    Not(IsNotNull(p)(pos))(pos)
  )

  def _propertyRulePropertyInList(p: Expression): Gen[Expression] =
    for {
      list <- oneOrMore(_propertyRuleLiteral)
      in = In(p, ListLiteral(list)(pos))(pos)
      expression <- oneOf(in, Not(in)(pos))
    } yield expression

  def _graphQualifierAndResource(graphAction: GraphAction)
    : Gen[(List[GraphPrivilegeQualifier], Option[ActionResourceBase])] =
    if (graphAction == AllGraphAction) {
      // ALL GRAPH PRIVILEGES has AllQualifier and no resource
      (List(AllQualifier()(pos)), None)
    } else if (graphAction == WriteAction) {
      // WRITE has AllElementsQualifier and no resource
      (List(ElementsAllQualifier()(pos)), None)
    } else if (graphAction == SetLabelAction || graphAction == RemoveLabelAction) {
      // SET/REMOVE LABEL have AllLabelQualifier and label resource
      for {
        resourceNames <- oneOrMore(_identifier)
        resource <- oneOf(LabelsResource(resourceNames)(pos), AllLabelResource()(pos))
      } yield (List(LabelAllQualifier()(pos)), Some(resource))
    } else if (graphAction == TraverseAction) {
      // TRAVERSE has either a _graphQualifier or a _propertyRuleGraphQualifier, and no resource
      for {
        qualifier <- oneOf(_graphQualifier, _propertyRuleGraphQualifier)
      } yield (qualifier, None)
    } else if (graphAction == CreateElementAction || graphAction == DeleteElementAction) {
      // CREATE/DELETE ELEMENT have any graph qualifier and no resource
      for {
        qualifier <- _graphQualifier
      } yield (qualifier, None)
    } else if (graphAction == ReadAction || graphAction == MatchAction) {
      // READ, MATCH have either a _graphQualifier or a _propertyRuleGraphQualifier, and property resource
      for {
        qualifier <- oneOf(_graphQualifier, _propertyRuleGraphQualifier)
        resourceNames <- oneOrMore(_identifier)
        resource <- oneOf(PropertiesResource(resourceNames)(pos), AllPropertyResource()(pos))
      } yield (qualifier, Some(resource))
    } else {
      // MERGE, SET PROPERTY have any graph qualifier and property resource
      for {
        qualifier <- _graphQualifier
        resourceNames <- oneOrMore(_identifier)
        resource <- oneOf(PropertiesResource(resourceNames)(pos), AllPropertyResource()(pos))
      } yield (qualifier, Some(resource))
    }

  def _loadQualifierAndAction: Gen[(List[LoadPrivilegeQualifier], LoadActions)] = for {
    // not a name as such but it's a string or parameter so :shrug:
    urlCidr <- _nameAsEither
    loadAll = LoadAllQualifier()(pos)
    loadCidr = LoadCidrQualifier(urlCidr)(pos)
    loadUrl = LoadUrlQualifier(urlCidr)(pos)
    (qualifier, action) <- oneOf((loadAll, LoadAllDataAction), (loadCidr, LoadCidrAction), (loadUrl, LoadUrlAction))
  } yield (List(qualifier), action)

  def _showSupportedPrivileges: Gen[ShowSupportedPrivilegeCommand] = for {
    yields <- _eitherYieldOrWhere
  } yield ShowSupportedPrivilegeCommand(yields)(pos)

  def _showPrivileges: Gen[ShowPrivileges] = for {
    names <- _listOfStringLiteralOrParam
    showRole = ShowRolesPrivileges(names)(pos)
    showUser1 = ShowUsersPrivileges(names)(pos)
    showUser2 = ShowUserPrivileges(None)(pos)
    showAll = ShowAllPrivileges()(pos)
    scope <- oneOf(showRole, showUser1, showUser2, showAll)
    yields <- _eitherYieldOrWhere
  } yield ShowPrivileges(scope, yields)(pos)

  def _showPrivilegeCommands: Gen[ShowPrivilegeCommands] = for {
    names <- _listOfStringLiteralOrParam
    showRole = ShowRolesPrivileges(names)(pos)
    showUser1 = ShowUsersPrivileges(names)(pos)
    showUser2 = ShowUserPrivileges(None)(pos)
    showAll = ShowAllPrivileges()(pos)
    scope <- oneOf(showRole, showUser1, showUser2, showAll)
    asRevoke <- boolean
    yields <- _eitherYieldOrWhere
  } yield ShowPrivilegeCommands(scope, asRevoke, yields)(pos)

  def _dbmsPrivilege: Gen[PrivilegeCommand] = for {
    dbmsAction <- _dbmsAction
    qualifier <- _dbmsQualifier(dbmsAction)
    roleNames <- _listOfStringLiteralOrParam
    revokeType <- _revokeType
    immutable <- boolean
    dbmsGrant = GrantPrivilege(DbmsPrivilege(dbmsAction)(pos), immutable, None, qualifier, roleNames)(pos)
    dbmsDeny = DenyPrivilege(DbmsPrivilege(dbmsAction)(pos), immutable, None, qualifier, roleNames)(pos)
    dbmsRevoke = RevokePrivilege(
      DbmsPrivilege(dbmsAction)(pos),
      immutable,
      None,
      qualifier,
      roleNames,
      revokeType
    )(pos)
    dbms <- oneOf(dbmsGrant, dbmsDeny, dbmsRevoke)
  } yield dbms

  def _databasePrivilege: Gen[PrivilegeCommand] = for {
    databaseAction <- _databaseAction
    dbNames <- oneOrMore(_databaseName)
    databaseScope <-
      if (databaseAction.isInstanceOf[DatabaseAndDbmsAction] && usesCypher5) {
        const(AllDatabasesScope()(pos))
      } else {
        oneOf(
          NamedDatabasesScope(dbNames)(pos),
          AllDatabasesScope()(pos),
          HomeDatabaseScope()(pos)
        )
      }
    databaseQualifier <- _databaseQualifier(databaseAction.isInstanceOf[TransactionManagementAction])
    roleNames <- _listOfStringLiteralOrParam
    revokeType <- _revokeType
    immutable <- boolean
    databaseGrant =
      GrantPrivilege(
        DatabasePrivilege(databaseAction, databaseScope)(pos),
        immutable,
        None,
        databaseQualifier,
        roleNames
      )(pos)
    databaseDeny =
      DenyPrivilege(
        DatabasePrivilege(databaseAction, databaseScope)(pos),
        immutable,
        None,
        databaseQualifier,
        roleNames
      )(pos)
    databaseRevoke =
      RevokePrivilege(
        DatabasePrivilege(databaseAction, databaseScope)(pos),
        immutable,
        None,
        databaseQualifier,
        roleNames,
        revokeType
      )(pos)
    database <- oneOf(databaseGrant, databaseDeny, databaseRevoke)
  } yield database

  def _graphPrivilege: Gen[PrivilegeCommand] = for {
    graphAction <- _graphAction
    graphNames <- oneOrMore(_databaseName)
    graphScope <-
      oneOf(NamedGraphsScope(graphNames)(pos), AllGraphsScope()(pos), HomeGraphScope()(pos))
    (qualifier, maybeResource) <- _graphQualifierAndResource(graphAction)
    roleNames <- _listOfStringLiteralOrParam
    revokeType <- _revokeType
    immutable <- boolean
    graphGrant =
      GrantPrivilege(GraphPrivilege(graphAction, graphScope)(pos), immutable, maybeResource, qualifier, roleNames)(pos)
    graphDeny =
      DenyPrivilege(GraphPrivilege(graphAction, graphScope)(pos), immutable, maybeResource, qualifier, roleNames)(pos)
    graphRevoke =
      RevokePrivilege(
        GraphPrivilege(graphAction, graphScope)(pos),
        immutable,
        maybeResource,
        qualifier,
        roleNames,
        revokeType
      )(pos)
    graph <- oneOf(graphGrant, graphDeny, graphRevoke)
  } yield graph

  def _loadPrivilege: Gen[PrivilegeCommand] = for {
    (qualifier, action) <- _loadQualifierAndAction
    roleNames <- _listOfStringLiteralOrParam
    revokeType <- _revokeType
    immutable <- boolean
    resource <- some(FileResource()(pos))
    loadPriv = LoadPrivilege(action)(pos)
    loadGrant = GrantPrivilege(loadPriv, immutable, resource, qualifier, roleNames)(pos)
    loadDeny = DenyPrivilege(loadPriv, immutable, resource, qualifier, roleNames)(pos)
    loadRevoke = RevokePrivilege(loadPriv, immutable, resource, qualifier, roleNames, revokeType)(pos)
    load <- oneOf(loadGrant, loadDeny, loadRevoke)
  } yield load

  def _privilegeCommand: Gen[AdministrationCommand] = oneOf(
    _showPrivileges,
    _showPrivilegeCommands,
    _dbmsPrivilege,
    _databasePrivilege,
    _graphPrivilege,
    _loadPrivilege,
    _showSupportedPrivileges
  )

  // Database commands

  def _showDatabases: Gen[SingleQuery] = for {
    dbName <- _databaseName
    use <- option(_use)
    scope <- oneOf(
      SingleNamedDatabaseScope(dbName)(pos),
      AllDatabasesScope()(pos),
      DefaultDatabaseScope()(pos),
      HomeDatabaseScope()(pos)
    )
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowDatabasesClause(scope, Some(w), List.empty, yieldAll = false, None, usesCypher5)(pos))
      case Some(Left((y, r))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowDatabasesClause(scope, None, yi, yieldAll = false, Some(w), usesCypher5)(pos)) ++ r
      case _ if yieldAll =>
        Seq(ShowDatabasesClause(
          scope,
          None,
          List.empty,
          yieldAll = true,
          Some(getFullWithStarFromYield),
          usesCypher5
        )(pos))
      case _ =>
        Seq(ShowDatabasesClause(scope, None, List.empty, yieldAll = false, None, usesCypher5)(pos))
    }
    SingleQuery(use.map(u => u +: showClauses).getOrElse(showClauses))(pos)
  }

  def _createDatabase: Gen[CreateDatabase] = for {
    dbName <- _databaseNameNoNamespace
    ifExistsDo <- _ifExistsDo
    wait <- _waitUntilComplete
    options <- _optionsMapAsEitherOrNone
    topologyOption <- option(_topology)
    defaultLanguageVersion <- option(_defaultLanguage)
    shardDefOption <- option(_shardDef(isCreate = true))
    (topology, shard) <- oneOf(const((topologyOption, None)), const((None, shardDefOption)))
  } yield {
    val shardDef = if (usesCypher5) None else shard
    CreateDatabase(dbName, ifExistsDo, options, wait, topology, defaultLanguageVersion, shardDef)(pos)
  }

  def _createCompositeDatabase: Gen[CreateCompositeDatabase] = for {
    dbName <- _databaseNameNoNamespace
    ifExistsDo <- _ifExistsDo
    options <- _optionsMapAsEitherOrNone
    wait <- _waitUntilComplete
    defaultLanguageVersion <- option(_defaultLanguage)
  } yield CreateCompositeDatabase(dbName, ifExistsDo, options, wait, defaultLanguageVersion)(pos)

  def _createReplicaDatabase: Gen[CreateReplicaDatabase] = for {
    dbName <- _databaseNameNoNamespace
    ifExistsDo <- _ifExistsDo
    wait <- _waitUntilComplete
    options <- _optionsMapAsEitherOrNone
    topology <- option(_topology)
    defaultLanguageVersion <- option(_defaultLanguage)
  } yield CreateReplicaDatabase(dbName, ifExistsDo, options, wait, topology, defaultLanguageVersion)(pos)

  def _dropDatabase: Gen[DropDatabase] = for {
    dbName <- _databaseName
    ifExists <- boolean
    composite <- boolean
    additionalAction <- oneOf(DumpData, DestroyData)
    aliasAction <- oneOf(Restrict, CascadeAliases)
    wait <- _waitUntilComplete
  } yield DropDatabase(dbName, ifExists, composite, aliasAction, additionalAction, wait)(pos)

  def _alterDatabase: Gen[AlterDatabase] = for {
    dbName <- _databaseName
    ifExists <- boolean
    options <- _optionsForAlterDatabaseOrNone
    access <- option(_access)
    topology <- option(_topology)
    defaultLanguageVersion <- option(_defaultLanguage)
    shardDefinition <- if (usesCypher5) const(None) else option(_shardDef(isCreate = false))
    replicas <- if (usesCypher5 || topology.nonEmpty) const(None) else option(_replicas)
    optionsToRemove <- _optionsToRemove(hasSetClause =
      access.nonEmpty || topology.nonEmpty || (!options.equals(
        NoOptions
      ) || defaultLanguageVersion.nonEmpty || shardDefinition.nonEmpty || replicas.nonEmpty)
    )
    wait <- _waitUntilComplete
  } yield AlterDatabase(
    dbName,
    ifExists,
    access,
    topology,
    options,
    optionsToRemove,
    wait,
    defaultLanguageVersion,
    shardDefinition,
    replicas
  )(pos)

  def _startDatabase: Gen[StartDatabase] = for {
    dbName <- _databaseName
    wait <- _waitUntilComplete
  } yield StartDatabase(dbName, wait)(pos)

  def _stopDatabase: Gen[StopDatabase] = for {
    dbName <- _databaseName
    wait <- _waitUntilComplete
  } yield StopDatabase(dbName, wait)(pos)

  def _multiDatabaseCommand: Gen[AdministrationCommand] =
    if (usesCypher5) {
      oneOf(
        _createDatabase,
        _createCompositeDatabase,
        _dropDatabase,
        _alterDatabase,
        _startDatabase,
        _stopDatabase
      )
    } else {
      oneOf(
        _createDatabase,
        _createCompositeDatabase,
        _createReplicaDatabase,
        _dropDatabase,
        _alterDatabase,
        _startDatabase,
        _stopDatabase
      )
    }

  def _access: Gen[Access] = for {
    access <- oneOf(ReadOnlyAccess, ReadWriteAccess)
  } yield access

  def _waitUntilComplete: Gen[WaitUntilComplete] = for {
    timeout <- posNum[Long].map(_.toString)
    wait <- oneOf(NoWait()(pos), IndefiniteWait()(pos), TimeoutAfter(timeout)(pos))
  } yield wait

  def _createLocalDatabaseAlias: Gen[CreateLocalDatabaseAlias] = for {
    aliasName <- _databaseName
    targetName <- _databaseName
    ifExistsDo <- _ifExistsDo
    properties <- option(_optionalMapAsEither)
  } yield CreateLocalDatabaseAlias(aliasName, targetName, ifExistsDo, properties)(pos)

  def _createRemoteDatabaseAlias: Gen[CreateRemoteDatabaseAlias] = for {
    aliasName <- _databaseName
    targetName <- _databaseName
    ifExistsDo <- _ifExistsDo
    url <- _nameAsEither
    remoteAliasCredentials <- _remoteAliasCredentials
    driverSettings <- option(_optionalMapAsEither)
    properties <- option(_optionalMapAsEither)
    defaultLanguageVersion <- option(_defaultLanguage)
  } yield CreateRemoteDatabaseAlias(
    aliasName,
    targetName,
    ifExistsDo,
    url,
    remoteAliasCredentials,
    driverSettings,
    properties,
    defaultLanguageVersion
  )(pos)

  def _dropAlias: Gen[DropDatabaseAlias] = for {
    aliasName <- _databaseName
    ifExists <- boolean
  } yield DropDatabaseAlias(aliasName, ifExists)(pos)

  def _alterLocalAlias: Gen[AlterLocalDatabaseAlias] = for {
    aliasName <- _databaseName
    targetName <- option(_databaseName)
    ifExists <- boolean
    properties <-
      if (targetName.isEmpty) some(_optionalMapAsEither)
      else option(_optionalMapAsEither)
  } yield AlterLocalDatabaseAlias(aliasName, targetName, ifExists, properties)(pos)

  def _alterRemoteAlias: Gen[AlterRemoteDatabaseAlias] = for {
    aliasName <- _databaseName
    targetName <- option(_databaseName)
    ifExists <- boolean
    url <- if (targetName.nonEmpty) some(_nameAsEither) else const(None)
    username <- option(_stringLiteralOrParameter)
    password <- option(_password)
    defaultLanguageVersion <- option(_defaultLanguage)
    // All five are not allowed to be None
    driverSettings <-
      if (url.isEmpty && username.isEmpty && password.isEmpty && defaultLanguageVersion.isEmpty)
        some(_optionalMapAsEither)
      else
        option(_optionalMapAsEither)
    properties <- option(_optionalMapAsEither)
  } yield AlterRemoteDatabaseAlias(
    aliasName,
    targetName,
    ifExists,
    url,
    username,
    password,
    driverSettings,
    properties,
    defaultLanguageVersion
  )(pos)

  def _showAliases: Gen[ShowAliases] = for {
    dbName <- option(_databaseName)
    yields <- _eitherYieldOrWhere
    // The test isn't run with the feature flag enabled, so having false here is fine
    oidcCredentialForwardingEnabled <- const(false)
  } yield ShowAliases(dbName, yields, usesCypher5, oidcCredentialForwardingEnabled)(pos)

  def _aliasCommands: Gen[AdministrationCommand] = oneOf(
    _createLocalDatabaseAlias,
    _createRemoteDatabaseAlias,
    _dropAlias,
    _alterLocalAlias,
    _alterRemoteAlias,
    _showAliases
  )

  // Server commands

  def _serverCommand: Gen[AdministrationCommand] = oneOf(
    _enableServer,
    _alterServer,
    _renameServer,
    _dropServer,
    _deallocateServer,
    _reallocateDatabases,
    _showServers
  )

  def _enableServer: Gen[EnableServer] = for {
    serverName <- _nameAsEither
    options <- _optionsMapAsEitherOrNone
  } yield EnableServer(serverName, options)(pos)

  def _alterServer: Gen[AlterServer] = for {
    serverName <- _nameAsEither
    options <- _optionsMapAsEither
  } yield AlterServer(serverName, options)(pos)

  def _renameServer: Gen[RenameServer] = for {
    serverName <- _nameAsEither
    newName <- _nameAsEither
  } yield RenameServer(serverName, newName)(pos)

  def _showServers: Gen[ShowServers] = for {
    yields <- _eitherYieldOrWhere
  } yield ShowServers(yields)(pos)

  def _dropServer: Gen[DropServer] = for {
    serverName <- _nameAsEither
  } yield DropServer(serverName)(pos)

  def _deallocateServer: Gen[DeallocateServers] = for {
    dryRun <- boolean
    servers <- _listOfNameOfEither
  } yield DeallocateServers(dryRun, servers)(pos)

  def _reallocateDatabases: Gen[ReallocateDatabases] = for {
    dryRun <- boolean
  } yield ReallocateDatabases(dryRun)(pos)

  // Top level administration command

  def _adminCommand: Gen[AdministrationCommand] = for {
    command <-
      oneOf(
        _userCommand,
        _authRuleCommand,
        _roleCommand,
        _privilegeCommand,
        _multiDatabaseCommand,
        _aliasCommands,
        _serverCommand
      )
    use <- frequency(1 -> some(_use), 9 -> const(None))
  } yield command.withGraph(use)

  // Top level statement
  // ----------------------------------

  def _statement: Gen[Statement] = oneOf(
    _query,
    _schemaCommand,
    _showCommands,
    _adminCommand
  )
}
