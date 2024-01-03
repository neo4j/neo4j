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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdministrationAction
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AllAliasManagementActions
import org.neo4j.cypher.internal.ast.AllConstraintActions
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllDatabaseAction
import org.neo4j.cypher.internal.ast.AllDatabaseManagementActions
import org.neo4j.cypher.internal.ast.AllDatabasesQualifier
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllDbmsAction
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
import org.neo4j.cypher.internal.ast.AllTokenActions
import org.neo4j.cypher.internal.ast.AllTransactionActions
import org.neo4j.cypher.internal.ast.AllUserActions
import org.neo4j.cypher.internal.ast.AlterAliasAction
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterServer
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AlterUserAction
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.CompositeDatabaseManagementActions
import org.neo4j.cypher.internal.ast.ConstraintVersion0
import org.neo4j.cypher.internal.ast.ConstraintVersion1
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateAliasAction
import org.neo4j.cypher.internal.ast.CreateBtreeNodeIndex
import org.neo4j.cypher.internal.ast.CreateBtreeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateCompositeDatabase
import org.neo4j.cypher.internal.ast.CreateCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateIndexOldSyntax
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreatePointNodeIndex
import org.neo4j.cypher.internal.ast.CreatePointRelationshipIndex
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRangeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.CreateVectorNodeIndex
import org.neo4j.cypher.internal.ast.CreateVectorRelationshipIndex
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseResource
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DeallocateServers
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropAliasAction
import org.neo4j.cypher.internal.ast.DropCompositeDatabaseAction
import org.neo4j.cypher.internal.ast.DropConstraintAction
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropDatabaseAdditionalAction
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropIndex
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropPropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropServer
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.EnableServer
import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.FileResource
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.ImpersonateUserAction
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadAllDataAction
import org.neo4j.cypher.internal.ast.LoadAllQualifier
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.LoadCidrAction
import org.neo4j.cypher.internal.ast.LoadCidrQualifier
import org.neo4j.cypher.internal.ast.LoadPrivilege
import org.neo4j.cypher.internal.ast.LoadUrlAction
import org.neo4j.cypher.internal.ast.LoadUrlQualifier
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.MatchAction
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.ReadAdministrationCommand
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.ReallocateDatabases
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.RelationshipAllQualifier
import org.neo4j.cypher.internal.ast.RelationshipQualifier
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveHomeDatabaseAction
import org.neo4j.cypher.internal.ast.RemoveItem
import org.neo4j.cypher.internal.ast.RemoveLabelAction
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePrivilegeAction
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.RemoveRoleAction
import org.neo4j.cypher.internal.ast.RemovedSyntax
import org.neo4j.cypher.internal.ast.RenameRole
import org.neo4j.cypher.internal.ast.RenameRoleAction
import org.neo4j.cypher.internal.ast.RenameServer
import org.neo4j.cypher.internal.ast.RenameUser
import org.neo4j.cypher.internal.ast.RenameUserAction
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.RevokeBothType
import org.neo4j.cypher.internal.ast.RevokeDenyType
import org.neo4j.cypher.internal.ast.RevokeGrantType
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.RevokeRolesFromUsers
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.SeekOrScan
import org.neo4j.cypher.internal.ast.ServerManagementAction
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
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
import org.neo4j.cypher.internal.ast.SetUserHomeDatabaseAction
import org.neo4j.cypher.internal.ast.SetUserStatusAction
import org.neo4j.cypher.internal.ast.SettingQualifier
import org.neo4j.cypher.internal.ast.ShowAliasAction
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAllPrivileges
import org.neo4j.cypher.internal.ast.ShowConstraintAction
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexAction
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivilegeScope
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowServerAction
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSettingAction
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.ShowUserAction
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
import org.neo4j.cypher.internal.ast.StatementWithGraph
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.TimeoutAfter
import org.neo4j.cypher.internal.ast.Topology
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
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
import org.neo4j.cypher.internal.ast.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingHint
import org.neo4j.cypher.internal.ast.UsingIndexHintType
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.UsingTextIndexType
import org.neo4j.cypher.internal.ast.ValidSyntax
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.ASTFactory
import org.neo4j.cypher.internal.ast.factory.ASTFactory.MergeActionType
import org.neo4j.cypher.internal.ast.factory.ASTFactory.StringPos
import org.neo4j.cypher.internal.ast.factory.AccessType
import org.neo4j.cypher.internal.ast.factory.AccessType.READ_ONLY
import org.neo4j.cypher.internal.ast.factory.AccessType.READ_WRITE
import org.neo4j.cypher.internal.ast.factory.ActionType
import org.neo4j.cypher.internal.ast.factory.CallInTxsOnErrorBehaviourType
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.ast.factory.ConstraintVersion
import org.neo4j.cypher.internal.ast.factory.CreateIndexTypes
import org.neo4j.cypher.internal.ast.factory.HintIndexType
import org.neo4j.cypher.internal.ast.factory.ParameterType
import org.neo4j.cypher.internal.ast.factory.ParserCypherTypeName
import org.neo4j.cypher.internal.ast.factory.ScopeType
import org.neo4j.cypher.internal.ast.factory.ShowCommandFilterTypes
import org.neo4j.cypher.internal.ast.factory.SimpleEither
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.MapProjectionElement
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships
import org.neo4j.cypher.internal.expressions.MatchMode.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.RepeatableElements
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternAtom
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.ProcedureOutput
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
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
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.parser.javacc.EntityType
import org.neo4j.cypher.internal.util.DeprecatedIdentifierUnicode
import org.neo4j.cypher.internal.util.DeprecatedIdentifierWhitespaceUnicode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.NullType
import org.neo4j.cypher.internal.util.symbols.PathType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType

import java.lang
import java.nio.charset.StandardCharsets
import java.util
import java.util.stream.Collectors

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.language.implicitConversions

final case class Privilege(
  privilegeType: PrivilegeType,
  resource: ActionResource,
  qualifier: util.List[PrivilegeQualifier],
  immutable: Boolean
)

trait DecorateTuple {

  class AsScala[A](op: => A) {
    def asScala: A = op
  }

  implicit def asScalaEither[L, R](i: SimpleEither[L, R]): AsScala[Either[L, R]] = {
    new AsScala(if (i.getRight == null) Left[L, R](i.getLeft) else Right[L, R](i.getRight))
  }
}

object TupleConverter extends DecorateTuple

import org.neo4j.cypher.internal.ast.factory.neo4j.TupleConverter.asScalaEither

class Neo4jASTFactory(query: String, astExceptionFactory: ASTExceptionFactory, logger: InternalNotificationLogger)
    extends ASTFactory[
      Statement,
      Query,
      Clause,
      Return,
      ReturnItem,
      ReturnItems,
      SortItem,
      PatternPart,
      NodePattern,
      RelationshipPattern,
      Option[Range],
      SetClause,
      SetItem,
      RemoveItem,
      ProcedureResultItem,
      UsingHint,
      Expression,
      LabelExpression,
      FunctionInvocation,
      Parameter,
      Variable,
      Property,
      MapProjectionElement,
      UseGraph,
      StatementWithGraph,
      AdministrationCommand,
      SchemaCommand,
      Yield,
      Where,
      DatabaseScope,
      WaitUntilComplete,
      AdministrationAction,
      GraphScope,
      Privilege,
      ActionResource,
      PrivilegeQualifier,
      SubqueryCall.InTransactionsParameters,
      SubqueryCall.InTransactionsBatchParameters,
      SubqueryCall.InTransactionsErrorParameters,
      SubqueryCall.InTransactionsReportParameters,
      InputPosition,
      EntityType,
      GraphPatternQuantifier,
      PatternAtom,
      DatabaseName,
      PatternPart.Selector,
      MatchMode,
      PatternElement
    ] {

  override def newSingleQuery(p: InputPosition, clauses: util.List[Clause]): Query = {
    if (clauses.isEmpty) {
      throw new Neo4jASTConstructionException("A valid Cypher query has to contain at least 1 clause")
    }
    SingleQuery(clauses.asScala.toList)(p)
  }

  override def newSingleQuery(clauses: util.List[Clause]): Query = {
    if (clauses.isEmpty) {
      throw new Neo4jASTConstructionException("A valid Cypher query has to contain at least 1 clause")
    }
    val pos = clauses.get(0).position
    SingleQuery(clauses.asScala.toList)(pos)
  }

  override def newUnion(p: InputPosition, lhs: Query, rhs: Query, all: Boolean): Query = {
    val rhsQuery =
      rhs match {
        case x: SingleQuery => x
        case other =>
          throw new Neo4jASTConstructionException(
            s"The Neo4j AST encodes Unions as a left-deep tree, so the rhs query must always be a SingleQuery. Got `$other`"
          )
      }

    if (all) UnionAll(lhs, rhsQuery)(p)
    else UnionDistinct(lhs, rhsQuery)(p)
  }

  override def directUseClause(p: InputPosition, name: DatabaseName): UseGraph = {
    name match {
      case NamespacedName(nameComponents, namespace) =>
        namespace match {
          case Some(pattern) => UseGraph(GraphDirectReference(CatalogName(pattern +: nameComponents))(name.position))(p)
          case None          => UseGraph(GraphDirectReference(CatalogName(nameComponents))(name.position))(p)
        }
      case ParameterName(_) => throw new Neo4jASTConstructionException("invalid graph reference")
    }
  }

  override def functionUseClause(p: InputPosition, function: FunctionInvocation): UseGraph = {
    UseGraph(GraphFunctionReference(function)(function.position))(p)
  }

  override def newReturnClause(
    p: InputPosition,
    distinct: Boolean,
    returnItems: ReturnItems,
    order: util.List[SortItem],
    orderPosition: InputPosition,
    skip: Expression,
    skipPosition: InputPosition,
    limit: Expression,
    limitPosition: InputPosition
  ): Return = {
    val orderList = order.asScala.toList
    Return(
      distinct,
      returnItems,
      if (order.isEmpty) None else Some(OrderBy(orderList)(orderPosition)),
      Option(skip).map(e => Skip(e)(skipPosition)),
      Option(limit).map(e => Limit(e)(limitPosition))
    )(p)
  }

  override def newReturnItems(p: InputPosition, returnAll: Boolean, returnItems: util.List[ReturnItem]): ReturnItems = {
    ReturnItems(returnAll, returnItems.asScala.toList)(p)
  }

  override def newReturnItem(p: InputPosition, e: Expression, v: Variable): ReturnItem = {
    AliasedReturnItem(e, v)(p)
  }

  override def newReturnItem(p: InputPosition, e: Expression, eStartOffset: Int, eEndOffset: Int): ReturnItem = {

    val name = query.substring(eStartOffset, eEndOffset + 1)
    UnaliasedReturnItem(e, name)(p)
  }

  override def orderDesc(p: InputPosition, e: Expression): SortItem = DescSortItem(e)(p)

  override def orderAsc(p: InputPosition, e: Expression): SortItem = AscSortItem(e)(p)

  override def createClause(p: InputPosition, patterns: util.List[PatternPart]): Clause = {
    val patternList: Seq[NonPrefixedPatternPart] = patterns.asScala.toList.map {
      case p: NonPrefixedPatternPart  => p
      case p: PatternPartWithSelector => throw pathSelectorCannotBeUsedInClauseException("CREATE", p.selector)
    }

    Create(Pattern.ForUpdate(patternList)(patterns.asScala.map(_.position).minBy(_.offset)))(p)
  }

  override def matchClause(
    p: InputPosition,
    optional: Boolean,
    matchMode: MatchMode,
    patterns: util.List[PatternPart],
    patternPos: InputPosition,
    hints: util.List[UsingHint],
    where: Where
  ): Clause = {
    val patternList: Seq[PatternPartWithSelector] = patterns.asScala.toList.map {
      case part: PatternPartWithSelector => part
      case part: NonPrefixedPatternPart  => PatternPartWithSelector(allPathSelector(part.position), part)
    }
    val finalMatchMode = if (matchMode == null) MatchMode.default(p) else matchMode
    Match(
      optional,
      finalMatchMode,
      Pattern.ForMatch(patternList)(patternPos),
      if (hints == null) Nil else hints.asScala.toList,
      Option(where)
    )(
      p
    )
  }

  override def usingIndexHint(
    p: InputPosition,
    v: Variable,
    labelOrRelType: String,
    properties: util.List[String],
    seekOnly: Boolean,
    indexType: HintIndexType
  ): UsingHint =
    ast.UsingIndexHint(
      v,
      LabelOrRelTypeName(labelOrRelType)(p),
      properties.asScala.toList.map(PropertyKeyName(_)(p)),
      if (seekOnly) SeekOnly else SeekOrScan,
      usingIndexType(indexType)
    )(p)

  private def usingIndexType(indexType: HintIndexType): UsingIndexHintType = indexType match {
    case HintIndexType.ANY => UsingAnyIndexType
    case HintIndexType.BTREE =>
      throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidHintIndexType(indexType))
    case HintIndexType.TEXT  => UsingTextIndexType
    case HintIndexType.RANGE => UsingRangeIndexType
    case HintIndexType.POINT => UsingPointIndexType
  }

  override def usingJoin(p: InputPosition, joinVariables: util.List[Variable]): UsingHint =
    UsingJoinHint(joinVariables.asScala.toList)(p)

  override def usingScan(p: InputPosition, v: Variable, labelOrRelType: String): UsingHint =
    UsingScanHint(v, LabelOrRelTypeName(labelOrRelType)(p))(p)

  override def withClause(p: InputPosition, r: Return, where: Where): Clause =
    With(r.distinct, r.returnItems, r.orderBy, r.skip, r.limit, Option(where))(p)

  override def whereClause(p: InputPosition, where: Expression): Where =
    Where(where)(p)

  override def setClause(p: InputPosition, setItems: util.List[SetItem]): SetClause =
    SetClause(setItems.asScala.toList)(p)

  override def setProperty(property: Property, value: Expression): SetItem =
    SetPropertyItem(property, value)(property.position)

  override def setVariable(variable: Variable, value: Expression): SetItem =
    SetExactPropertiesFromMapItem(variable, value)(variable.position)

  override def addAndSetVariable(variable: Variable, value: Expression): SetItem =
    SetIncludingPropertiesFromMapItem(variable, value)(variable.position)

  override def setLabels(
    variable: Variable,
    labels: util.List[StringPos[InputPosition]],
    containsIs: Boolean
  ): SetItem =
    SetLabelItem(variable, labels.asScala.toList.map(sp => LabelName(sp.string)(sp.pos)), containsIs)(variable.position)

  override def removeClause(p: InputPosition, removeItems: util.List[RemoveItem]): Clause =
    Remove(removeItems.asScala.toList)(p)

  override def removeProperty(property: Property): RemoveItem = RemovePropertyItem(property)

  override def removeLabels(
    variable: Variable,
    labels: util.List[StringPos[InputPosition]],
    containsIs: Boolean
  ): RemoveItem =
    RemoveLabelItem(variable, labels.asScala.toList.map(sp => LabelName(sp.string)(sp.pos)), containsIs)(
      variable.position
    )

  override def deleteClause(p: InputPosition, detach: Boolean, expressions: util.List[Expression]): Clause =
    Delete(expressions.asScala.toList, detach)(p)

  override def unwindClause(p: InputPosition, e: Expression, v: Variable): Clause = Unwind(e, v)(p)

  override def mergeClause(
    p: InputPosition,
    pattern: PatternPart,
    setClauses: util.List[SetClause],
    actionTypes: util.List[MergeActionType],
    positions: util.List[InputPosition]
  ): Clause = {
    val patternForMerge: NonPrefixedPatternPart = pattern match {
      case p: NonPrefixedPatternPart   => p
      case pp: PatternPartWithSelector => throw pathSelectorCannotBeUsedInClauseException("MERGE", pp.selector)
    }

    val clausesIter = setClauses.iterator()
    val positionItr = positions.iterator()
    val actions = actionTypes.asScala.toList.map {
      case MergeActionType.OnMatch =>
        OnMatch(clausesIter.next())(positionItr.next)
      case MergeActionType.OnCreate =>
        OnCreate(clausesIter.next())(positionItr.next)
    }

    Merge(patternForMerge, actions)(p)
  }

  override def callClause(
    p: InputPosition,
    namespacePosition: InputPosition,
    procedureNamePosition: InputPosition,
    procedureResultPosition: InputPosition,
    namespace: util.List[String],
    name: String,
    arguments: util.List[Expression],
    yieldAll: Boolean,
    resultItems: util.List[ProcedureResultItem],
    where: Where
  ): Clause =
    UnresolvedCall(
      Namespace(namespace.asScala.toList)(namespacePosition),
      ProcedureName(name)(procedureNamePosition),
      if (arguments == null) None else Some(arguments.asScala.toList),
      Option(resultItems).map(items =>
        ProcedureResult(items.asScala.toList.toIndexedSeq, Option(where))(procedureResultPosition)
      ),
      yieldAll
    )(p)

  override def callResultItem(p: InputPosition, name: String, v: Variable): ProcedureResultItem =
    if (v == null) ProcedureResultItem(Variable(name)(p))(p)
    else ProcedureResultItem(ProcedureOutput(name)(v.position), v)(p)

  override def loadCsvClause(
    p: InputPosition,
    headers: Boolean,
    source: Expression,
    v: Variable,
    fieldTerminator: String
  ): Clause = {
    LoadCSV.fromUrl(headers, source, v, Option(fieldTerminator).map(StringLiteral(_)(p)))(p)
  }

  override def foreachClause(p: InputPosition, v: Variable, list: Expression, clauses: util.List[Clause]): Clause =
    Foreach(v, list, clauses.asScala.toList)(p)

  override def subqueryInTransactionsParams(
    p: InputPosition,
    batchParams: SubqueryCall.InTransactionsBatchParameters,
    errorParams: SubqueryCall.InTransactionsErrorParameters,
    reportParams: SubqueryCall.InTransactionsReportParameters
  ): SubqueryCall.InTransactionsParameters =
    SubqueryCall.InTransactionsParameters(
      Option(batchParams),
      Option(errorParams),
      Option(reportParams)
    )(p)

  override def subqueryInTransactionsBatchParameters(
    p: InputPosition,
    batchSize: Expression
  ): SubqueryCall.InTransactionsBatchParameters =
    SubqueryCall.InTransactionsBatchParameters(batchSize)(p)

  override def subqueryInTransactionsErrorParameters(
    p: InputPosition,
    onErrorBehaviour: CallInTxsOnErrorBehaviourType
  ): SubqueryCall.InTransactionsErrorParameters = {
    onErrorBehaviour match {
      case CallInTxsOnErrorBehaviourType.ON_ERROR_CONTINUE =>
        SubqueryCall.InTransactionsErrorParameters(OnErrorContinue)(p)
      case CallInTxsOnErrorBehaviourType.ON_ERROR_BREAK =>
        SubqueryCall.InTransactionsErrorParameters(OnErrorBreak)(p)
      case CallInTxsOnErrorBehaviourType.ON_ERROR_FAIL =>
        SubqueryCall.InTransactionsErrorParameters(OnErrorFail)(p)
    }
  }

  override def subqueryInTransactionsReportParameters(
    p: InputPosition,
    v: Variable
  ): SubqueryCall.InTransactionsReportParameters =
    SubqueryCall.InTransactionsReportParameters(v)(p)

  override def subqueryClause(
    p: InputPosition,
    subquery: Query,
    inTransactions: SubqueryCall.InTransactionsParameters
  ): Clause =
    SubqueryCall(subquery, Option(inTransactions))(p)

  // PATTERNS

  override def namedPattern(v: Variable, pattern: PatternPart): PatternPart =
    NamedPatternPart(v, pattern.asInstanceOf[AnonymousPatternPart])(v.position)

  override def shortestPathPattern(p: InputPosition, patternElement: PatternElement): PatternPart =
    ShortestPathsPatternPart(patternElement, single = true)(p)

  override def allShortestPathsPattern(p: InputPosition, patternElement: PatternElement): PatternPart =
    ShortestPathsPatternPart(patternElement, single = false)(p)

  override def pathPattern(patternElement: PatternElement): PatternPart =
    PathPatternPart(patternElement)

  override def patternWithSelector(
    selector: PatternPart.Selector,
    patternPart: PatternPart
  ): PatternPartWithSelector = {
    val nonPrefixedPatternPart = patternPart match {
      case npp: NonPrefixedPatternPart => npp
      case pp: PatternPartWithSelector =>
        throw new IllegalArgumentException(
          s"Expected a pattern without a selector, got: [${pp.getClass.getSimpleName}]: $pp"
        )
    }

    PatternPartWithSelector(selector, nonPrefixedPatternPart)
  }

  override def patternElement(atoms: util.List[PatternAtom]): PatternElement = {

    val iterator = atoms.iterator().asScala.buffered

    var factors = Seq.empty[PathFactor]
    while (iterator.hasNext) {
      iterator.next() match {
        case n: NodePattern =>
          var patternElement: SimplePattern = n
          while (iterator.hasNext && iterator.head.isInstanceOf[RelationshipPattern]) {
            val relPattern = iterator.next()
            // we trust in the parser to alternate nodes and relationships
            val rightNodePattern = iterator.next()
            patternElement = RelationshipChain(
              patternElement,
              relPattern.asInstanceOf[RelationshipPattern],
              rightNodePattern.asInstanceOf[NodePattern]
            )(patternElement.position)
          }
          factors = factors :+ patternElement
        case element: QuantifiedPath    => factors = factors :+ element
        case element: ParenthesizedPath => factors = factors :+ element
        case _: RelationshipPattern     => throw new IllegalStateException("Abbreviated patterns are not supported yet")
      }
    }

    val pathElement: PatternElement = factors match {
      case Seq(element) => element
      case factors =>
        val position = factors.head.position
        PathConcatenation(factors)(position)
    }
    pathElement
  }

  override def anyPathSelector(
    count: String,
    countPosition: InputPosition,
    position: InputPosition
  ): PatternPart.Selector = {
    PatternPart.AnyPath(defaultCountValue(count, countPosition, position))(position)
  }

  override def allPathSelector(position: InputPosition): PatternPart.Selector =
    PatternPart.AllPaths()(position)

  override def anyShortestPathSelector(
    count: String,
    countPosition: InputPosition,
    position: InputPosition
  ): PatternPart.Selector =
    PatternPart.AnyShortestPath(defaultCountValue(count, countPosition, position))(position)

  override def allShortestPathSelector(position: InputPosition): PatternPart.Selector =
    PatternPart.AllShortestPaths()(position)

  override def shortestGroupsSelector(
    count: String,
    countPosition: InputPosition,
    position: InputPosition
  ): PatternPart.Selector =
    PatternPart.ShortestGroups(defaultCountValue(count, countPosition, position))(position)

  private def defaultCountValue(count: String, countPosition: InputPosition, position: InputPosition) =
    if (count != null) {
      UnsignedDecimalIntegerLiteral(count)(countPosition)
    } else {
      UnsignedDecimalIntegerLiteral("1")(position)
    }

  override def nodePattern(
    p: InputPosition,
    v: Variable,
    labelExpression: LabelExpression,
    properties: Expression,
    predicate: Expression
  ): NodePattern = {
    NodePattern(Option(v), Option(labelExpression), Option(properties), Option(predicate))(p)
  }

  override def relationshipPattern(
    p: InputPosition,
    left: Boolean,
    right: Boolean,
    v: Variable,
    labelExpression: LabelExpression,
    pathLength: Option[Range],
    properties: Expression,
    predicate: Expression
  ): RelationshipPattern = {
    val direction =
      if (left && !right) SemanticDirection.INCOMING
      else if (!left && right) SemanticDirection.OUTGOING
      else SemanticDirection.BOTH

    val range =
      pathLength match {
        case null    => None
        case None    => Some(None)
        case Some(r) => Some(Some(r))
      }

    RelationshipPattern(
      Option(v),
      Option(labelExpression),
      range,
      Option(properties),
      Option(predicate),
      direction
    )(p)
  }

  override def pathLength(
    p: InputPosition,
    pMin: InputPosition,
    pMax: InputPosition,
    minLength: String,
    maxLength: String
  ): Option[Range] = {
    if (minLength == null && maxLength == null) {
      None
    } else {
      val min = if (minLength == "") None else Some(UnsignedDecimalIntegerLiteral(minLength)(pMin))
      val max = if (maxLength == "") None else Some(UnsignedDecimalIntegerLiteral(maxLength)(pMax))
      Some(Range(min, max)(if (pMin != null) pMin else p))
    }
  }

  override def intervalPathQuantifier(
    position: InputPosition,
    positionLowerBound: InputPosition,
    positionUpperBound: InputPosition,
    lowerBoundText: String,
    upperBoundText: String
  ): GraphPatternQuantifier = {
    val lowerBound =
      if (lowerBoundText == null) None else Some(UnsignedDecimalIntegerLiteral(lowerBoundText)(positionLowerBound))
    val upperBound =
      if (upperBoundText == null) None else Some(UnsignedDecimalIntegerLiteral(upperBoundText)(positionUpperBound))
    IntervalQuantifier(lowerBound, upperBound)(position)
  }

  override def fixedPathQuantifier(
    p: InputPosition,
    valuePos: InputPosition,
    value: String
  ): GraphPatternQuantifier = {
    FixedQuantifier(UnsignedDecimalIntegerLiteral(value)(valuePos))(p)
  }

  override def plusPathQuantifier(
    p: InputPosition
  ): GraphPatternQuantifier = {
    PlusQuantifier()(p)
  }

  override def starPathQuantifier(
    p: InputPosition
  ): GraphPatternQuantifier = {
    StarQuantifier()(p)
  }

  override def repeatableElements(p: InputPosition): MatchMode = {
    RepeatableElements()(p)
  }

  override def differentRelationships(p: InputPosition): MatchMode = {
    DifferentRelationships()(p)
  }

  override def parenthesizedPathPattern(
    p: InputPosition,
    internalPattern: PatternPart,
    where: Expression,
    length: GraphPatternQuantifier
  ): PatternAtom = {
    val nonPrefixedPatternPart: NonPrefixedPatternPart = internalPattern match {
      case p: NonPrefixedPatternPart => p
      case pp: PatternPartWithSelector =>
        val pathPatternKind = if (length == null) "parenthesized" else "quantified"
        throw pathSelectorNotAllowedWithinPathPatternKindException(pathPatternKind, pp.selector)
    }

    if (length != null)
      QuantifiedPath(nonPrefixedPatternPart, length, Option(where))(p)
    else {
      ParenthesizedPath(nonPrefixedPatternPart, Option(where))(p)
    }
  }

  override def quantifiedRelationship(
    rel: RelationshipPattern,
    quantifier: GraphPatternQuantifier
  ): PatternAtom = {
    // represent -[rel]->+ as (()-[rel]->())+
    val pos = rel.position
    val pattern = PathPatternPart(
      RelationshipChain(
        NodePattern(None, None, None, None)(pos),
        rel,
        NodePattern(None, None, None, None)(pos)
      )(pos)
    )
    parenthesizedPathPattern(pos, pattern, where = null, quantifier)
  }

  private def pathSelectorCannotBeUsedInClauseException(
    clauseName: String,
    selector: PatternPart.Selector
  ): Exception = {
    val p = selector.position
    astExceptionFactory.syntaxException(
      new Neo4jASTConstructionException(
        s"Path selectors such as `${selector.prettified}` cannot be used in a $clauseName clause, but only in a MATCH clause."
      ),
      p.offset,
      p.line,
      p.column
    )
  }

  private def pathSelectorNotAllowedWithinPathPatternKindException(
    pathPatternKind: String,
    selector: PatternPart.Selector
  ): Exception = {
    val p = selector.position
    astExceptionFactory.syntaxException(
      new Neo4jASTConstructionException(
        s"Path selectors such as `${selector.prettified}` are not supported within $pathPatternKind path patterns."
      ),
      p.offset,
      p.line,
      p.column
    )
  }

  // EXPRESSIONS

  override def newVariable(p: InputPosition, name: String): Variable = Variable(name)(p)

  override def newParameter(p: InputPosition, v: Variable, t: ParameterType): Parameter = {
    ExplicitParameter(v.name, transformParameterType(t))(p)
  }

  override def newParameter(p: InputPosition, offset: String, t: ParameterType): Parameter = {
    ExplicitParameter(offset, transformParameterType(t))(p)
  }

  private def transformParameterType(t: ParameterType) = {
    t match {
      case ParameterType.ANY    => CTAny
      case ParameterType.STRING => CTString
      case ParameterType.MAP    => CTMap
      case _                    => throw new IllegalArgumentException("unknown parameter type: " + t.toString)
    }
  }

  override def newSensitiveStringParameter(p: InputPosition, v: Variable): Parameter =
    new ExplicitParameter(v.name, CTString)(p) with SensitiveParameter

  override def newSensitiveStringParameter(p: InputPosition, offset: String): Parameter =
    new ExplicitParameter(offset, CTString)(p) with SensitiveParameter

  override def newDouble(p: InputPosition, image: String): Expression = DecimalDoubleLiteral(image)(p)

  override def newDecimalInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedDecimalIntegerLiteral("-" + image)(p)
    else SignedDecimalIntegerLiteral(image)(p)

  override def newHexInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedHexIntegerLiteral("-" + image)(p)
    else SignedHexIntegerLiteral(image)(p)

  override def newOctalInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedOctalIntegerLiteral("-" + image)(p)
    else SignedOctalIntegerLiteral(image)(p)

  override def newString(p: InputPosition, image: String): Expression = StringLiteral(image)(p)

  override def newTrueLiteral(p: InputPosition): Expression = True()(p)

  override def newFalseLiteral(p: InputPosition): Expression = False()(p)

  override def newInfinityLiteral(p: InputPosition): Expression = Infinity()(p)

  override def newNaNLiteral(p: InputPosition): Expression = NaN()(p)

  override def newNullLiteral(p: InputPosition): Expression = Null()(p)

  override def listLiteral(p: InputPosition, values: util.List[Expression]): Expression = {
    ListLiteral(values.asScala.toList)(p)
  }

  override def mapLiteral(
    p: InputPosition,
    keys: util.List[StringPos[InputPosition]],
    values: util.List[Expression]
  ): Expression = {

    if (keys.size() != values.size()) {
      throw new Neo4jASTConstructionException(
        s"Map have the same number of keys and values, but got keys `${pretty(keys)}` and values `${pretty(values)}`"
      )
    }

    var i = 0
    val pairs = new Array[(PropertyKeyName, Expression)](keys.size())

    while (i < keys.size()) {
      val key = keys.get(i)
      pairs(i) = PropertyKeyName(key.string)(key.pos) -> values.get(i)
      i += 1
    }

    MapExpression(pairs.toIndexedSeq)(p)
  }

  override def property(subject: Expression, propertyKeyName: StringPos[InputPosition]): Property =
    Property(subject, PropertyKeyName(propertyKeyName.string)(propertyKeyName.pos))(subject.position)

  override def or(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Or(lhs, rhs)(p)

  override def xor(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Xor(lhs, rhs)(p)

  override def and(p: InputPosition, lhs: Expression, rhs: Expression): Expression = And(lhs, rhs)(p)

  override def ands(exprs: util.List[Expression]): Expression = Ands(exprs.asScala)(exprs.get(0).position)

  override def not(p: InputPosition, e: Expression): Expression = Not(e)(p)

  override def plus(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Add(lhs, rhs)(p)

  override def minus(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Subtract(lhs, rhs)(p)

  override def multiply(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Multiply(lhs, rhs)(p)

  override def divide(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Divide(lhs, rhs)(p)

  override def modulo(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Modulo(lhs, rhs)(p)

  override def pow(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Pow(lhs, rhs)(p)

  override def unaryPlus(e: Expression): Expression = unaryPlus(e.position, e)

  override def unaryPlus(p: InputPosition, e: Expression): Expression = UnaryAdd(e)(p)

  override def unaryMinus(p: InputPosition, e: Expression): Expression = UnarySubtract(e)(p)

  override def eq(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Equals(lhs, rhs)(p)

  override def neq(p: InputPosition, lhs: Expression, rhs: Expression): Expression = InvalidNotEquals(lhs, rhs)(p)

  override def neq2(p: InputPosition, lhs: Expression, rhs: Expression): Expression = NotEquals(lhs, rhs)(p)

  override def lte(p: InputPosition, lhs: Expression, rhs: Expression): Expression = LessThanOrEqual(lhs, rhs)(p)

  override def gte(p: InputPosition, lhs: Expression, rhs: Expression): Expression = GreaterThanOrEqual(lhs, rhs)(p)

  override def lt(p: InputPosition, lhs: Expression, rhs: Expression): Expression = LessThan(lhs, rhs)(p)

  override def gt(p: InputPosition, lhs: Expression, rhs: Expression): Expression = GreaterThan(lhs, rhs)(p)

  override def regeq(p: InputPosition, lhs: Expression, rhs: Expression): Expression = RegexMatch(lhs, rhs)(p)

  override def startsWith(p: InputPosition, lhs: Expression, rhs: Expression): Expression = StartsWith(lhs, rhs)(p)

  override def endsWith(p: InputPosition, lhs: Expression, rhs: Expression): Expression = EndsWith(lhs, rhs)(p)

  override def contains(p: InputPosition, lhs: Expression, rhs: Expression): Expression = Contains(lhs, rhs)(p)

  override def in(p: InputPosition, lhs: Expression, rhs: Expression): Expression = In(lhs, rhs)(p)

  override def isNull(p: InputPosition, e: Expression): Expression = IsNull(e)(p)

  override def isNotNull(p: InputPosition, e: Expression): Expression = IsNotNull(e)(p)

  override def isTyped(p: InputPosition, e: Expression, javaType: ParserCypherTypeName): Expression = {
    val scalaType = convertCypherType(javaType)
    IsTyped(e, scalaType)(p)
  }

  override def isNotTyped(p: InputPosition, e: Expression, javaType: ParserCypherTypeName): Expression = {
    val scalaType = convertCypherType(javaType)
    IsNotTyped(e, scalaType)(p)
  }

  override def listLookup(list: Expression, index: Expression): Expression = ContainerIndex(list, index)(index.position)

  override def listSlice(p: InputPosition, list: Expression, start: Expression, end: Expression): Expression = {
    ListSlice(list, Option(start), Option(end))(p)
  }

  override def newCountStar(p: InputPosition): Expression = CountStar()(p)

  override def functionInvocation(
    p: InputPosition,
    functionNamePosition: InputPosition,
    namespace: util.List[String],
    name: String,
    distinct: Boolean,
    arguments: util.List[Expression]
  ): FunctionInvocation = {
    FunctionInvocation(
      Namespace(namespace.asScala.toList)(p),
      FunctionName(name)(functionNamePosition),
      distinct,
      arguments.asScala.toIndexedSeq
    )(p)
  }

  override def listComprehension(
    p: InputPosition,
    v: Variable,
    list: Expression,
    where: Expression,
    projection: Expression
  ): Expression =
    ListComprehension(v, list, Option(where), Option(projection))(p)

  override def patternComprehension(
    p: InputPosition,
    relationshipPatternPosition: InputPosition,
    v: Variable,
    pattern: PatternPart,
    where: Expression,
    projection: Expression
  ): Expression =
    PatternComprehension(
      Option(v),
      RelationshipsPattern(pattern.element.asInstanceOf[RelationshipChain])(relationshipPatternPosition),
      Option(where),
      projection
    )(p, None, None)

  override def reduceExpression(
    p: InputPosition,
    acc: Variable,
    accExpr: Expression,
    v: Variable,
    list: Expression,
    innerExpr: Expression
  ): Expression =
    ReduceExpression(acc, accExpr, v, list, innerExpr)(p)

  override def allExpression(p: InputPosition, v: Variable, list: Expression, where: Expression): Expression =
    AllIterablePredicate(v, list, Option(where))(p)

  override def anyExpression(p: InputPosition, v: Variable, list: Expression, where: Expression): Expression =
    AnyIterablePredicate(v, list, Option(where))(p)

  override def noneExpression(p: InputPosition, v: Variable, list: Expression, where: Expression): Expression =
    NoneIterablePredicate(v, list, Option(where))(p)

  override def singleExpression(p: InputPosition, v: Variable, list: Expression, where: Expression): Expression =
    SingleIterablePredicate(v, list, Option(where))(p)

  override def patternExpression(p: InputPosition, pattern: PatternPart): Expression =
    pattern match {
      case paths: ShortestPathsPatternPart =>
        ShortestPathExpression(paths)
      case _ =>
        PatternExpression(RelationshipsPattern(pattern.element.asInstanceOf[RelationshipChain])(p))(
          None,
          None
        )
    }

  /** Exists and Count allow for PatternList and Optional Where, convert here to give a unified Exists / Count
   * containing a semantically valid Query. */
  private def convertSubqueryExpressionToUnifiedExpression(
    matchMode: MatchMode,
    patterns: util.List[PatternPart],
    query: Query,
    where: Where
  ): Query = {
    if (query != null) {
      query
    } else {
      val patternParts = patterns.asScala.toList.map {
        case p: PatternPartWithSelector => p
        case p: NonPrefixedPatternPart  => PatternPartWithSelector(allPathSelector(p.position), p)
      }
      val patternPos = patternParts.head.position
      val finalMatchMode = if (matchMode == null) MatchMode.default(patternPos) else matchMode
      SingleQuery(
        Seq(
          Match(optional = false, finalMatchMode, Pattern.ForMatch(patternParts)(patternPos), Seq.empty, Option(where))(
            patternPos
          )
        )
      )(patternPos)
    }
  }

  override def existsExpression(
    p: InputPosition,
    matchMode: MatchMode,
    patterns: util.List[PatternPart],
    query: Query,
    where: Where
  ): Expression = {
    ExistsExpression(convertSubqueryExpressionToUnifiedExpression(matchMode, patterns, query, where))(
      p,
      None,
      None
    )
  }

  override def countExpression(
    p: InputPosition,
    matchMode: MatchMode,
    patterns: util.List[PatternPart],
    query: Query,
    where: Where
  ): Expression = {
    CountExpression(convertSubqueryExpressionToUnifiedExpression(matchMode, patterns, query, where))(p, None, None)
  }

  override def collectExpression(
    p: InputPosition,
    query: Query
  ): Expression = {
    CollectExpression(query)(
      p,
      None,
      None
    )
  }

  override def mapProjection(p: InputPosition, v: Variable, items: util.List[MapProjectionElement]): Expression =
    MapProjection(v, items.asScala.toList)(p)

  override def mapProjectionLiteralEntry(property: StringPos[InputPosition], value: Expression): MapProjectionElement =
    LiteralEntry(PropertyKeyName(property.string)(property.pos), value)(value.position)

  override def mapProjectionProperty(property: StringPos[InputPosition]): MapProjectionElement =
    PropertySelector(PropertyKeyName(property.string)(property.pos))(property.pos)

  override def mapProjectionVariable(v: Variable): MapProjectionElement =
    VariableSelector(v)(v.position)

  override def mapProjectionAll(p: InputPosition): MapProjectionElement =
    AllPropertiesSelector()(p)

  override def caseExpression(
    p: InputPosition,
    e: Expression,
    whens: util.List[Expression],
    thens: util.List[Expression],
    elze: Expression
  ): Expression = {

    if (whens.size() != thens.size()) {
      throw new Neo4jASTConstructionException(
        s"Case expressions have the same number of whens and thens, but got whens `${pretty(whens)}` and thens `${pretty(thens)}`"
      )
    }

    val alternatives = new Array[(Expression, Expression)](whens.size())
    var i = 0
    while (i < whens.size()) {
      alternatives(i) = whens.get(i) -> thens.get(i)
      i += 1
    }
    CaseExpression(Option(e), alternatives.toIndexedSeq, Option(elze))(p)
  }

  override def inputPosition(offset: Int, line: Int, column: Int): InputPosition = InputPosition(offset, line, column)

  // Commands

  override def useGraph(command: StatementWithGraph, graph: UseGraph): StatementWithGraph = {
    command.withGraph(Option(graph))
  }

  // Show Commands

  override def yieldClause(
    p: InputPosition,
    returnAll: Boolean,
    returnItemList: util.List[ReturnItem],
    returnItemsP: InputPosition,
    order: util.List[SortItem],
    orderPos: InputPosition,
    skip: Expression,
    skipPosition: InputPosition,
    limit: Expression,
    limitPosition: InputPosition,
    where: Where
  ): Yield = {

    val returnItems = ReturnItems(returnAll, returnItemList.asScala.toList)(returnItemsP)

    Yield(
      returnItems,
      Option(order.asScala.toList).filter(_.nonEmpty).map(o => OrderBy(o)(orderPos)),
      Option(skip).map(s => Skip(s)(skipPosition)),
      Option(limit).map(l => Limit(l)(limitPosition)),
      Option(where)
    )(p)
  }

  override def showIndexClause(
    p: InputPosition,
    initialIndexType: ShowCommandFilterTypes,
    brief: Boolean,
    verbose: Boolean,
    where: Where,
    yieldClause: Yield
  ): Clause = {
    val indexType = initialIndexType match {
      case ShowCommandFilterTypes.ALL      => AllIndexes
      case ShowCommandFilterTypes.BTREE    => BtreeIndexes
      case ShowCommandFilterTypes.RANGE    => RangeIndexes
      case ShowCommandFilterTypes.FULLTEXT => FulltextIndexes
      case ShowCommandFilterTypes.TEXT     => TextIndexes
      case ShowCommandFilterTypes.POINT    => PointIndexes
      case ShowCommandFilterTypes.VECTOR   => VectorIndexes
      case ShowCommandFilterTypes.LOOKUP   => LookupIndexes
      case t => throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidShowFilterType("indexes", t))
    }
    val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldClause)
    ShowIndexesClause(indexType, brief, verbose, Option(where), yieldedItems, yieldAll)(p)
  }

  override def showConstraintClause(
    p: InputPosition,
    initialConstraintType: ShowCommandFilterTypes,
    brief: Boolean,
    verbose: Boolean,
    where: Where,
    yieldClause: Yield
  ): Clause = {
    val constraintType: ShowConstraintType = initialConstraintType match {
      case ShowCommandFilterTypes.ALL                     => AllConstraints
      case ShowCommandFilterTypes.UNIQUE                  => UniqueConstraints
      case ShowCommandFilterTypes.NODE_UNIQUE             => NodeUniqueConstraints
      case ShowCommandFilterTypes.RELATIONSHIP_UNIQUE     => RelUniqueConstraints
      case ShowCommandFilterTypes.KEY                     => KeyConstraints
      case ShowCommandFilterTypes.NODE_KEY                => NodeKeyConstraints
      case ShowCommandFilterTypes.RELATIONSHIP_KEY        => RelKeyConstraints
      case ShowCommandFilterTypes.EXIST                   => ExistsConstraints(ValidSyntax)
      case ShowCommandFilterTypes.OLD_EXISTS              => ExistsConstraints(RemovedSyntax)
      case ShowCommandFilterTypes.OLD_EXIST               => ExistsConstraints(ValidSyntax)
      case ShowCommandFilterTypes.NODE_EXIST              => NodeExistsConstraints(ValidSyntax)
      case ShowCommandFilterTypes.NODE_OLD_EXISTS         => NodeExistsConstraints(RemovedSyntax)
      case ShowCommandFilterTypes.NODE_OLD_EXIST          => NodeExistsConstraints(ValidSyntax)
      case ShowCommandFilterTypes.RELATIONSHIP_EXIST      => RelExistsConstraints(ValidSyntax)
      case ShowCommandFilterTypes.RELATIONSHIP_OLD_EXISTS => RelExistsConstraints(RemovedSyntax)
      case ShowCommandFilterTypes.RELATIONSHIP_OLD_EXIST  => RelExistsConstraints(ValidSyntax)
      case ShowCommandFilterTypes.PROP_TYPE               => PropTypeConstraints
      case ShowCommandFilterTypes.NODE_PROP_TYPE          => NodePropTypeConstraints
      case ShowCommandFilterTypes.RELATIONSHIP_PROP_TYPE  => RelPropTypeConstraints
      case t => throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidShowFilterType("constraints", t))
    }
    val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldClause)
    ShowConstraintsClause(constraintType, brief, verbose, Option(where), yieldedItems, yieldAll)(p)
  }

  override def showProcedureClause(
    p: InputPosition,
    currentUser: Boolean,
    user: String,
    where: Where,
    yieldClause: Yield
  ): Clause = {
    // either we have 'EXECUTABLE BY user', 'EXECUTABLE [BY CURRENT USER]' or nothing
    val executableBy = if (user != null) Some(User(user)) else if (currentUser) Some(CurrentUser) else None
    val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldClause)
    ShowProceduresClause(executableBy, Option(where), yieldedItems, yieldAll)(p)
  }

  override def showFunctionClause(
    p: InputPosition,
    initialFunctionType: ShowCommandFilterTypes,
    currentUser: Boolean,
    user: String,
    where: Where,
    yieldClause: Yield
  ): Clause = {
    val functionType = initialFunctionType match {
      case ShowCommandFilterTypes.ALL          => AllFunctions
      case ShowCommandFilterTypes.BUILT_IN     => BuiltInFunctions
      case ShowCommandFilterTypes.USER_DEFINED => UserDefinedFunctions
      case t => throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidShowFilterType("functions", t))
    }

    // either we have 'EXECUTABLE BY user', 'EXECUTABLE [BY CURRENT USER]' or nothing
    val executableBy = if (user != null) Some(User(user)) else if (currentUser) Some(CurrentUser) else None

    val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldClause)
    ShowFunctionsClause(functionType, executableBy, Option(where), yieldedItems, yieldAll)(p)
  }

  override def showTransactionsClause(
    p: InputPosition,
    ids: SimpleEither[util.List[String], Expression],
    where: Where,
    yieldClause: Yield
  ): Clause = {
    val scalaIds =
      ids.asScala.left.map(_.asScala.toList) // if left: map the string list to scala, if right: changes nothing
    val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldClause)
    ShowTransactionsClause(scalaIds, Option(where), yieldedItems, yieldAll)(p)
  }

  override def terminateTransactionsClause(
    p: InputPosition,
    ids: SimpleEither[util.List[String], Expression],
    where: Where,
    yieldClause: Yield
  ): Clause = {
    val scalaIds =
      ids.asScala.left.map(_.asScala.toList) // if left: map the string list to scala, if right: changes nothing
    val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldClause)
    TerminateTransactionsClause(scalaIds, yieldedItems, yieldAll, Option(where).map(_.position))(p)
  }

  override def showSettingsClause(
    p: InputPosition,
    names: SimpleEither[util.List[String], Expression],
    where: Where,
    yieldClause: Yield
  ): Clause = {
    val namesAsScala = names.asScala.left.map(_.asScala.toList)
    val (yieldAll, yieldedItems) = getYieldAllAndYieldItems(yieldClause)
    ShowSettingsClause(namesAsScala, Option(where), yieldedItems, yieldAll)(p)
  }

  private def getYieldAllAndYieldItems(yieldClause: Yield): (Boolean, List[CommandResultItem]) = {
    val yieldAll = Option(yieldClause).exists(_.returnItems.includeExisting)
    val yieldedItems = Option(yieldClause)
      .map(_.returnItems.items.map(item => {
        // yield is always parsed as `variable` with potentially `AS variable` after
        val variable = item.expression.asInstanceOf[LogicalVariable]
        val aliasedVariable: LogicalVariable = item.alias.getOrElse(variable)
        CommandResultItem(variable.name, aliasedVariable)(item.position)
      }).toList)
      .getOrElse(List.empty)
    (yieldAll, yieldedItems)
  }

  override def turnYieldToWith(yieldClause: Yield): Clause = {
    val returnItems = yieldClause.returnItems
    val itemOrder = if (returnItems.items.nonEmpty) Some(returnItems.items.map(_.name).toList) else None
    val (orderBy, where) = CommandClause.updateAliasedVariablesFromYieldInOrderByAndWhere(yieldClause)
    With(
      distinct = false,
      ReturnItems(includeExisting = true, Seq(), itemOrder)(returnItems.position),
      orderBy,
      yieldClause.skip,
      yieldClause.limit,
      where,
      withType = ParsedAsYield
    )(yieldClause.position)
  }

  // Schema Commands
  // Constraint Commands

  override def createConstraint(
    p: InputPosition,
    constraintType: ConstraintType,
    replace: Boolean,
    ifNotExists: Boolean,
    constraintName: SimpleEither[String, Parameter],
    variable: Variable,
    label: StringPos[InputPosition],
    javaProperties: util.List[Property],
    javaPropertyType: ParserCypherTypeName,
    options: SimpleEither[util.Map[String, Expression], Parameter],
    containsOn: Boolean,
    constraintVersion: ConstraintVersion
  ): SchemaCommand = {
    // Convert ConstraintVersion from Java to Scala
    val constraintVersionScala = constraintVersion match {
      case ConstraintVersion.CONSTRAINT_VERSION_0 => ConstraintVersion0
      case ConstraintVersion.CONSTRAINT_VERSION_1 => ConstraintVersion1
      case ConstraintVersion.CONSTRAINT_VERSION_2 => ConstraintVersion2
    }

    val name = Option(constraintName).map(_.asScala)
    val properties = javaProperties.asScala.toSeq
    constraintType match {
      case ConstraintType.NODE_UNIQUE => ast.CreateNodePropertyUniquenessConstraint(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
      case ConstraintType.REL_UNIQUE => ast.CreateRelationshipPropertyUniquenessConstraint(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
      case ConstraintType.NODE_KEY => ast.CreateNodeKeyConstraint(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
      case ConstraintType.REL_KEY => ast.CreateRelationshipKeyConstraint(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
      case ConstraintType.NODE_EXISTS | ConstraintType.NODE_IS_NOT_NULL =>
        validateSingleProperty(properties, constraintType)
        ast.CreateNodePropertyExistenceConstraint(
          variable,
          LabelName(label.string)(label.pos),
          properties.head,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
      case ConstraintType.REL_EXISTS | ConstraintType.REL_IS_NOT_NULL =>
        validateSingleProperty(properties, constraintType)
        ast.CreateRelationshipPropertyExistenceConstraint(
          variable,
          RelTypeName(label.string)(label.pos),
          properties.head,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
      case ConstraintType.NODE_IS_TYPED =>
        validateSingleProperty(properties, constraintType)
        val scalaPropertyType = convertCypherType(javaPropertyType)
        ast.CreateNodePropertyTypeConstraint(
          variable,
          LabelName(label.string)(label.pos),
          properties.head,
          scalaPropertyType,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
      case ConstraintType.REL_IS_TYPED =>
        validateSingleProperty(properties, constraintType)
        val scalaPropertyTypes = convertCypherType(javaPropertyType)
        ast.CreateRelationshipPropertyTypeConstraint(
          variable,
          RelTypeName(label.string)(label.pos),
          properties.head,
          scalaPropertyTypes,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          containsOn,
          constraintVersionScala
        )(p)
    }
  }

  override def dropConstraint(
    p: InputPosition,
    name: SimpleEither[String, Parameter],
    ifExists: Boolean
  ): DropConstraintOnName =
    DropConstraintOnName(name.asScala, ifExists)(p)

  override def dropConstraint(
    p: InputPosition,
    constraintType: ConstraintType,
    variable: Variable,
    label: StringPos[InputPosition],
    javaProperties: util.List[Property]
  ): SchemaCommand = {
    val properties = javaProperties.asScala.toSeq
    constraintType match {
      case ConstraintType.NODE_UNIQUE =>
        DropPropertyUniquenessConstraint(variable, LabelName(label.string)(label.pos), properties)(p)
      case ConstraintType.NODE_KEY => DropNodeKeyConstraint(variable, LabelName(label.string)(label.pos), properties)(p)
      case ConstraintType.NODE_EXISTS =>
        validateSingleProperty(properties, constraintType)
        DropNodePropertyExistenceConstraint(variable, LabelName(label.string)(label.pos), properties.head)(p)
      case ConstraintType.REL_EXISTS =>
        validateSingleProperty(properties, constraintType)
        DropRelationshipPropertyExistenceConstraint(variable, RelTypeName(label.string)(label.pos), properties.head)(p)
      case _ =>
        // ConstraintType.NODE_IS_NOT_NULL, ConstraintType.REL_IS_NOT_NULL, ConstraintType.REL_UNIQUE, ConstraintType.REL_KEY
        throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand)
    }
  }

  private def validateSingleProperty(seq: Seq[_], constraintType: ConstraintType): Unit = {
    if (seq.size != 1)
      throw new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(constraintType))
  }

  private def convertCypherType(javaType: ParserCypherTypeName): CypherType = {
    val pos = inputPosition(javaType.getOffset, javaType.getLine, javaType.getColumn)
    val cypherTypeName = javaType match {
      case ParserCypherTypeName.NOTHING =>
        NothingType()(pos)
      case ParserCypherTypeName.NULL =>
        NullType()(pos)
      case ParserCypherTypeName.BOOLEAN =>
        BooleanType(isNullable = true)(pos)
      case ParserCypherTypeName.BOOLEAN_NOT_NULL =>
        BooleanType(isNullable = false)(pos)
      case ParserCypherTypeName.STRING =>
        StringType(isNullable = true)(pos)
      case ParserCypherTypeName.STRING_NOT_NULL =>
        StringType(isNullable = false)(pos)
      case ParserCypherTypeName.INTEGER =>
        IntegerType(isNullable = true)(pos)
      case ParserCypherTypeName.INTEGER_NOT_NULL =>
        IntegerType(isNullable = false)(pos)
      case ParserCypherTypeName.FLOAT =>
        FloatType(isNullable = true)(pos)
      case ParserCypherTypeName.FLOAT_NOT_NULL =>
        FloatType(isNullable = false)(pos)
      case ParserCypherTypeName.DATE =>
        DateType(isNullable = true)(pos)
      case ParserCypherTypeName.DATE_NOT_NULL =>
        DateType(isNullable = false)(pos)
      case ParserCypherTypeName.LOCAL_TIME =>
        LocalTimeType(isNullable = true)(pos)
      case ParserCypherTypeName.LOCAL_TIME_NOT_NULL =>
        LocalTimeType(isNullable = false)(pos)
      case ParserCypherTypeName.ZONED_TIME =>
        ZonedTimeType(isNullable = true)(pos)
      case ParserCypherTypeName.ZONED_TIME_NOT_NULL =>
        ZonedTimeType(isNullable = false)(pos)
      case ParserCypherTypeName.LOCAL_DATETIME =>
        LocalDateTimeType(isNullable = true)(pos)
      case ParserCypherTypeName.LOCAL_DATETIME_NOT_NULL =>
        LocalDateTimeType(isNullable = false)(pos)
      case ParserCypherTypeName.ZONED_DATETIME =>
        ZonedDateTimeType(isNullable = true)(pos)
      case ParserCypherTypeName.ZONED_DATETIME_NOT_NULL =>
        ZonedDateTimeType(isNullable = false)(pos)
      case ParserCypherTypeName.DURATION =>
        DurationType(isNullable = true)(pos)
      case ParserCypherTypeName.DURATION_NOT_NULL =>
        DurationType(isNullable = false)(pos)
      case ParserCypherTypeName.POINT =>
        PointType(isNullable = true)(pos)
      case ParserCypherTypeName.POINT_NOT_NULL =>
        PointType(isNullable = false)(pos)
      case ParserCypherTypeName.NODE =>
        NodeType(isNullable = true)(pos)
      case ParserCypherTypeName.NODE_NOT_NULL =>
        NodeType(isNullable = false)(pos)
      case ParserCypherTypeName.RELATIONSHIP =>
        RelationshipType(isNullable = true)(pos)
      case ParserCypherTypeName.RELATIONSHIP_NOT_NULL =>
        RelationshipType(isNullable = false)(pos)
      case ParserCypherTypeName.MAP =>
        MapType(isNullable = true)(pos)
      case ParserCypherTypeName.MAP_NOT_NULL =>
        MapType(isNullable = false)(pos)
      case l: ParserCypherTypeName.ListParserCypherTypeName =>
        val inner = convertCypherType(l.getInnerType)
        ListType(inner, l.isNullable)(pos)
      case ParserCypherTypeName.PATH =>
        PathType(isNullable = true)(pos)
      case ParserCypherTypeName.PATH_NOT_NULL =>
        PathType(isNullable = false)(pos)
      case ParserCypherTypeName.PROPERTY_VALUE =>
        PropertyValueType(isNullable = true)(pos)
      case ParserCypherTypeName.PROPERTY_VALUE_NOT_NULL =>
        PropertyValueType(isNullable = false)(pos)
      case ParserCypherTypeName.ANY =>
        AnyType(isNullable = true)(pos)
      case ParserCypherTypeName.ANY_NOT_NULL =>
        AnyType(isNullable = false)(pos)
      case dynamicUnion: ParserCypherTypeName.ClosedDynamicUnionParserCypherTypeName =>
        val unionOfTypes: Set[CypherType] = dynamicUnion.getUnionTypes.stream().map[CypherType](unionType =>
          convertCypherType(unionType)
        ).toList.asScala.toSet
        ClosedDynamicUnionType(unionOfTypes)(pos)
      case ct =>
        throw new Neo4jASTConstructionException(s"Unknown Cypher type: $ct")
    }

    cypherTypeName.simplify
  }

  // Index Commands

  override def createLookupIndex(
    p: InputPosition,
    replace: Boolean,
    ifNotExists: Boolean,
    isNode: Boolean,
    indexName: SimpleEither[String, Parameter],
    variable: Variable,
    functionName: StringPos[InputPosition],
    functionParameter: Variable,
    options: SimpleEither[util.Map[String, Expression], Parameter]
  ): CreateLookupIndex = {
    val function = FunctionInvocation(
      FunctionName(functionName.string)(functionName.pos),
      distinct = false,
      IndexedSeq(functionParameter)
    )(functionName.pos)
    val name = Option(indexName).map(_.asScala)
    CreateLookupIndex(
      variable,
      isNode,
      function,
      name,
      ifExistsDo(replace, ifNotExists),
      asOptionsAst(options)
    )(p)
  }

  override def createIndexWithOldSyntax(
    p: InputPosition,
    label: StringPos[InputPosition],
    properties: util.List[StringPos[InputPosition]]
  ): CreateIndexOldSyntax = {
    CreateIndexOldSyntax(
      LabelName(label.string)(label.pos),
      properties.asScala.toList.map(prop => PropertyKeyName(prop.string)(prop.pos))
    )(p)
  }

  override def createIndex(
    p: InputPosition,
    replace: Boolean,
    ifNotExists: Boolean,
    isNode: Boolean,
    indexName: SimpleEither[String, Parameter],
    variable: Variable,
    label: StringPos[InputPosition],
    javaProperties: util.List[Property],
    options: SimpleEither[util.Map[String, Expression], Parameter],
    indexType: CreateIndexTypes
  ): CreateIndex = {
    val properties = javaProperties.asScala.toList
    val name = Option(indexName).map(_.asScala)
    (indexType, isNode) match {
      case (CreateIndexTypes.DEFAULT, true) =>
        CreateRangeNodeIndex(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          fromDefault = true
        )(p)
      case (CreateIndexTypes.DEFAULT, false) =>
        CreateRangeRelationshipIndex(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          fromDefault = true
        )(p)
      case (CreateIndexTypes.RANGE, true) =>
        CreateRangeNodeIndex(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          fromDefault = false
        )(p)
      case (CreateIndexTypes.RANGE, false) =>
        CreateRangeRelationshipIndex(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options),
          fromDefault = false
        )(p)
      case (CreateIndexTypes.BTREE, true) =>
        CreateBtreeNodeIndex(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (CreateIndexTypes.BTREE, false) =>
        CreateBtreeRelationshipIndex(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (CreateIndexTypes.TEXT, true) =>
        CreateTextNodeIndex(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (CreateIndexTypes.TEXT, false) =>
        CreateTextRelationshipIndex(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (CreateIndexTypes.POINT, true) =>
        CreatePointNodeIndex(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (CreateIndexTypes.POINT, false) =>
        CreatePointRelationshipIndex(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (CreateIndexTypes.VECTOR, true) =>
        CreateVectorNodeIndex(
          variable,
          LabelName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (CreateIndexTypes.VECTOR, false) =>
        CreateVectorRelationshipIndex(
          variable,
          RelTypeName(label.string)(label.pos),
          properties,
          name,
          ifExistsDo(replace, ifNotExists),
          asOptionsAst(options)
        )(p)
      case (t, _) =>
        throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidCreateIndexType(t))
    }
  }

  override def createFulltextIndex(
    p: InputPosition,
    replace: Boolean,
    ifNotExists: Boolean,
    isNode: Boolean,
    indexName: SimpleEither[String, Parameter],
    variable: Variable,
    labels: util.List[StringPos[InputPosition]],
    javaProperties: util.List[Property],
    options: SimpleEither[util.Map[String, Expression], Parameter]
  ): CreateIndex = {
    val properties = javaProperties.asScala.toList
    val name = Option(indexName).map(_.asScala)
    if (isNode) {
      val labelNames = labels.asScala.toList.map(stringPos => LabelName(stringPos.string)(stringPos.pos))
      CreateFulltextNodeIndex(
        variable,
        labelNames,
        properties,
        name,
        ifExistsDo(replace, ifNotExists),
        asOptionsAst(options)
      )(p)
    } else {
      val relTypeNames = labels.asScala.toList.map(stringPos => RelTypeName(stringPos.string)(stringPos.pos))
      CreateFulltextRelationshipIndex(
        variable,
        relTypeNames,
        properties,
        name,
        ifExistsDo(replace, ifNotExists),
        asOptionsAst(options)
      )(p)
    }
  }

  override def dropIndex(p: InputPosition, name: SimpleEither[String, Parameter], ifExists: Boolean): DropIndexOnName =
    DropIndexOnName(name.asScala, ifExists)(p)

  override def dropIndex(
    p: InputPosition,
    label: StringPos[InputPosition],
    javaProperties: util.List[StringPos[InputPosition]]
  ): DropIndex = {
    val properties = javaProperties.asScala.map(property => PropertyKeyName(property.string)(property.pos)).toList
    DropIndex(LabelName(label.string)(label.pos), properties)(p)
  }

  // Administration Commands
  // Role commands

  override def createRole(
    p: InputPosition,
    replace: Boolean,
    roleName: SimpleEither[String, Parameter],
    from: SimpleEither[String, Parameter],
    ifNotExists: Boolean
  ): CreateRole = {
    CreateRole(roleName.asScala, Option(from).map(_.asScala), ifExistsDo(replace, ifNotExists))(p)
  }

  override def dropRole(p: InputPosition, roleName: SimpleEither[String, Parameter], ifExists: Boolean): DropRole = {
    DropRole(roleName.asScala, ifExists)(p)
  }

  override def renameRole(
    p: InputPosition,
    fromRoleName: SimpleEither[String, Parameter],
    toRoleName: SimpleEither[String, Parameter],
    ifExists: Boolean
  ): RenameRole = {
    RenameRole(fromRoleName.asScala, toRoleName.asScala, ifExists)(p)
  }

  override def showRoles(
    p: InputPosition,
    WithUsers: Boolean,
    showAll: Boolean,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ShowRoles = {
    ShowRoles(WithUsers, showAll, yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
  }

  override def grantRoles(
    p: InputPosition,
    roles: util.List[SimpleEither[String, Parameter]],
    users: util.List[SimpleEither[String, Parameter]]
  ): GrantRolesToUsers = {
    GrantRolesToUsers(roles.asScala.map(_.asScala).toSeq, users.asScala.map(_.asScala).toSeq)(p)
  }

  override def revokeRoles(
    p: InputPosition,
    roles: util.List[SimpleEither[String, Parameter]],
    users: util.List[SimpleEither[String, Parameter]]
  ): RevokeRolesFromUsers = {
    RevokeRolesFromUsers(roles.asScala.map(_.asScala).toSeq, users.asScala.map(_.asScala).toSeq)(p)
  }

  // User commands

  override def createUser(
    p: InputPosition,
    replace: Boolean,
    ifNotExists: Boolean,
    username: SimpleEither[String, Parameter],
    password: Expression,
    encrypted: Boolean,
    changeRequired: Boolean,
    suspended: lang.Boolean,
    homeDatabase: DatabaseName
  ): AdministrationCommand = {
    val homeAction = if (homeDatabase == null) None else Some(SetHomeDatabaseAction(homeDatabase))
    val userOptions = UserOptions(Some(changeRequired), asBooleanOption(suspended), homeAction)
    CreateUser(username.asScala, encrypted, password, userOptions, ifExistsDo(replace, ifNotExists))(p)
  }

  override def dropUser(p: InputPosition, ifExists: Boolean, username: SimpleEither[String, Parameter]): DropUser = {
    DropUser(username.asScala, ifExists)(p)
  }

  override def renameUser(
    p: InputPosition,
    fromUserName: SimpleEither[String, Parameter],
    toUserName: SimpleEither[String, Parameter],
    ifExists: Boolean
  ): RenameUser = {
    RenameUser(fromUserName.asScala, toUserName.asScala, ifExists)(p)
  }

  override def setOwnPassword(
    p: InputPosition,
    currentPassword: Expression,
    newPassword: Expression
  ): SetOwnPassword = {
    SetOwnPassword(newPassword, currentPassword)(p)
  }

  override def alterUser(
    p: InputPosition,
    ifExists: Boolean,
    username: SimpleEither[String, Parameter],
    password: Expression,
    encrypted: Boolean,
    changeRequired: lang.Boolean,
    suspended: lang.Boolean,
    homeDatabase: DatabaseName,
    removeHome: Boolean
  ): AlterUser = {
    val maybePassword = Option(password)
    val isEncrypted = if (maybePassword.isDefined) Some(encrypted) else None
    val homeAction =
      if (removeHome) Some(RemoveHomeDatabaseAction)
      else if (homeDatabase == null) None
      else Some(SetHomeDatabaseAction(homeDatabase))
    val userOptions = UserOptions(asBooleanOption(changeRequired), asBooleanOption(suspended), homeAction)
    AlterUser(username.asScala, isEncrypted, maybePassword, userOptions, ifExists)(p)
  }

  override def passwordExpression(password: Parameter): Expression =
    new ExplicitParameter(password.name, CTString)(password.position) with SensitiveParameter

  override def passwordExpression(p: InputPosition, password: String): Expression =
    SensitiveStringLiteral(password.getBytes(StandardCharsets.UTF_8))(p)

  override def showUsers(p: InputPosition, yieldExpr: Yield, returnWithoutGraph: Return, where: Where): ShowUsers = {
    ShowUsers(yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
  }

  override def showCurrentUser(
    p: InputPosition,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ShowCurrentUser = {
    ShowCurrentUser(yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
  }

  // Privilege commands

  override def showSupportedPrivileges(
    p: InputPosition,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ReadAdministrationCommand = ShowSupportedPrivilegeCommand(yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)

  override def showAllPrivileges(
    p: InputPosition,
    asCommand: Boolean,
    asRevoke: Boolean,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ReadAdministrationCommand = {
    if (asCommand) {
      ShowPrivilegeCommands(ShowAllPrivileges()(p), asRevoke, yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
    } else {
      ShowPrivileges(ShowAllPrivileges()(p), yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
    }
  }

  override def showRolePrivileges(
    p: InputPosition,
    roles: util.List[SimpleEither[String, Parameter]],
    asCommand: Boolean,
    asRevoke: Boolean,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ReadAdministrationCommand = {
    if (asCommand) {
      ShowPrivilegeCommands(
        ShowRolesPrivileges(roles.asScala.map(_.asScala).toList)(p),
        asRevoke,
        yieldOrWhere(yieldExpr, returnWithoutGraph, where)
      )(p)
    } else {
      ShowPrivileges(
        ShowRolesPrivileges(roles.asScala.map(_.asScala).toList)(p),
        yieldOrWhere(yieldExpr, returnWithoutGraph, where)
      )(p)
    }
  }

  override def showUserPrivileges(
    p: InputPosition,
    users: util.List[SimpleEither[String, Parameter]],
    asCommand: Boolean,
    asRevoke: Boolean,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ReadAdministrationCommand = {
    if (asCommand) {
      ShowPrivilegeCommands(userPrivilegeScope(p, users), asRevoke, yieldOrWhere(yieldExpr, returnWithoutGraph, where))(
        p
      )
    } else {
      ShowPrivileges(userPrivilegeScope(p, users), yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
    }
  }

  private def userPrivilegeScope(
    p: InputPosition,
    users: util.List[SimpleEither[String, Parameter]]
  ): ShowPrivilegeScope = {
    if (Option(users).isDefined) {
      ShowUsersPrivileges(users.asScala.map(_.asScala).toList)(p)
    } else {
      ShowUserPrivileges(None)(p)
    }
  }

  override def grantPrivilege(
    p: InputPosition,
    roles: util.List[SimpleEither[String, Parameter]],
    privilege: Privilege
  ): AdministrationCommand =
    GrantPrivilege(
      privilege.privilegeType,
      privilege.immutable,
      Option(privilege.resource),
      privilege.qualifier.asScala.toList,
      roles.asScala.map(_.asScala).toSeq
    )(p)

  override def denyPrivilege(
    p: InputPosition,
    roles: util.List[SimpleEither[String, Parameter]],
    privilege: Privilege
  ): AdministrationCommand =
    DenyPrivilege(
      privilege.privilegeType,
      privilege.immutable,
      Option(privilege.resource),
      privilege.qualifier.asScala.toList,
      roles.asScala.map(_.asScala).toSeq
    )(p)

  override def revokePrivilege(
    p: InputPosition,
    roles: util.List[SimpleEither[String, Parameter]],
    privilege: Privilege,
    revokeGrant: Boolean,
    revokeDeny: Boolean
  ): AdministrationCommand = (revokeGrant, revokeDeny) match {
    case (true, false) => RevokePrivilege(
        privilege.privilegeType,
        privilege.immutable,
        Option(privilege.resource),
        privilege.qualifier.asScala.toList,
        roles.asScala.map(_.asScala).toSeq,
        RevokeGrantType()(p)
      )(p)
    case (false, true) => RevokePrivilege(
        privilege.privilegeType,
        privilege.immutable,
        Option(privilege.resource),
        privilege.qualifier.asScala.toList,
        roles.asScala.map(_.asScala).toSeq,
        RevokeDenyType()(p)
      )(p)
    case _ => RevokePrivilege(
        privilege.privilegeType,
        privilege.immutable,
        Option(privilege.resource),
        privilege.qualifier.asScala.toList,
        roles.asScala.map(_.asScala).toSeq,
        RevokeBothType()(p)
      )(p)
  }

  override def databasePrivilege(
    p: InputPosition,
    action: AdministrationAction,
    scope: DatabaseScope,
    qualifier: util.List[PrivilegeQualifier],
    immutable: Boolean
  ): Privilege =
    Privilege(
      DatabasePrivilege(action.asInstanceOf[DatabaseAction], scope)(p),
      null,
      qualifier,
      immutable
    )

  override def dbmsPrivilege(
    p: InputPosition,
    action: AdministrationAction,
    qualifier: util.List[PrivilegeQualifier],
    immutable: Boolean
  ): Privilege =
    Privilege(DbmsPrivilege(action.asInstanceOf[DbmsAction])(p), null, qualifier, immutable)

  override def graphPrivilege(
    p: InputPosition,
    action: AdministrationAction,
    scope: GraphScope,
    resource: ActionResource,
    qualifier: util.List[PrivilegeQualifier],
    immutable: Boolean
  ): Privilege =
    Privilege(GraphPrivilege(action.asInstanceOf[GraphAction], scope)(p), resource, qualifier, immutable)

  override def loadPrivilege(
    p: InputPosition,
    url: SimpleEither[String, Parameter],
    cidr: SimpleEither[String, Parameter],
    immutable: Boolean
  ): Privilege = {
    if (url != null) {
      Privilege(
        LoadPrivilege(LoadUrlAction)(p),
        FileResource()(p),
        util.List.of(LoadUrlQualifier(url.asScala)(p)),
        immutable
      )
    } else if (cidr != null) {
      Privilege(
        LoadPrivilege(LoadCidrAction)(p),
        FileResource()(p),
        util.List.of(LoadCidrQualifier(cidr.asScala)(p)),
        immutable
      )
    } else {
      Privilege(LoadPrivilege(LoadAllDataAction)(p), FileResource()(p), util.List.of(LoadAllQualifier()(p)), immutable)
    }
  }

  override def privilegeAction(action: ActionType): AdministrationAction = action match {
    case ActionType.DATABASE_ALL          => AllDatabaseAction
    case ActionType.ACCESS                => AccessDatabaseAction
    case ActionType.DATABASE_START        => StartDatabaseAction
    case ActionType.DATABASE_STOP         => StopDatabaseAction
    case ActionType.INDEX_ALL             => AllIndexActions
    case ActionType.INDEX_CREATE          => CreateIndexAction
    case ActionType.INDEX_DROP            => DropIndexAction
    case ActionType.INDEX_SHOW            => ShowIndexAction
    case ActionType.CONSTRAINT_ALL        => AllConstraintActions
    case ActionType.CONSTRAINT_CREATE     => CreateConstraintAction
    case ActionType.CONSTRAINT_DROP       => DropConstraintAction
    case ActionType.CONSTRAINT_SHOW       => ShowConstraintAction
    case ActionType.CREATE_TOKEN          => AllTokenActions
    case ActionType.CREATE_PROPERTYKEY    => CreatePropertyKeyAction
    case ActionType.CREATE_LABEL          => CreateNodeLabelAction
    case ActionType.CREATE_RELTYPE        => CreateRelationshipTypeAction
    case ActionType.TRANSACTION_ALL       => AllTransactionActions
    case ActionType.TRANSACTION_SHOW      => ShowTransactionAction
    case ActionType.TRANSACTION_TERMINATE => TerminateTransactionAction

    case ActionType.DBMS_ALL                      => AllDbmsAction
    case ActionType.USER_ALL                      => AllUserActions
    case ActionType.USER_SHOW                     => ShowUserAction
    case ActionType.USER_ALTER                    => AlterUserAction
    case ActionType.USER_CREATE                   => CreateUserAction
    case ActionType.USER_DROP                     => DropUserAction
    case ActionType.USER_RENAME                   => RenameUserAction
    case ActionType.USER_PASSWORD                 => SetPasswordsAction
    case ActionType.USER_STATUS                   => SetUserStatusAction
    case ActionType.USER_HOME                     => SetUserHomeDatabaseAction
    case ActionType.USER_IMPERSONATE              => ImpersonateUserAction
    case ActionType.ROLE_ALL                      => AllRoleActions
    case ActionType.ROLE_SHOW                     => ShowRoleAction
    case ActionType.ROLE_CREATE                   => CreateRoleAction
    case ActionType.ROLE_DROP                     => DropRoleAction
    case ActionType.ROLE_RENAME                   => RenameRoleAction
    case ActionType.ROLE_ASSIGN                   => AssignRoleAction
    case ActionType.ROLE_REMOVE                   => RemoveRoleAction
    case ActionType.DATABASE_MANAGEMENT           => AllDatabaseManagementActions
    case ActionType.DATABASE_CREATE               => CreateDatabaseAction
    case ActionType.DATABASE_DROP                 => DropDatabaseAction
    case ActionType.DATABASE_COMPOSITE_MANAGEMENT => CompositeDatabaseManagementActions
    case ActionType.DATABASE_COMPOSITE_CREATE     => CreateCompositeDatabaseAction
    case ActionType.DATABASE_COMPOSITE_DROP       => DropCompositeDatabaseAction
    case ActionType.DATABASE_ALTER                => AlterDatabaseAction
    case ActionType.SET_DATABASE_ACCESS           => SetDatabaseAccessAction
    case ActionType.ALIAS_MANAGEMENT              => AllAliasManagementActions
    case ActionType.ALIAS_CREATE                  => CreateAliasAction
    case ActionType.ALIAS_DROP                    => DropAliasAction
    case ActionType.ALIAS_ALTER                   => AlterAliasAction
    case ActionType.ALIAS_SHOW                    => ShowAliasAction
    case ActionType.PRIVILEGE_ALL                 => AllPrivilegeActions
    case ActionType.PRIVILEGE_ASSIGN              => AssignPrivilegeAction
    case ActionType.PRIVILEGE_REMOVE              => RemovePrivilegeAction
    case ActionType.PRIVILEGE_SHOW                => ShowPrivilegeAction
    case ActionType.EXECUTE_FUNCTION              => ExecuteFunctionAction
    case ActionType.EXECUTE_BOOSTED_FUNCTION      => ExecuteBoostedFunctionAction
    case ActionType.EXECUTE_PROCEDURE             => ExecuteProcedureAction
    case ActionType.EXECUTE_BOOSTED_PROCEDURE     => ExecuteBoostedProcedureAction
    case ActionType.EXECUTE_ADMIN_PROCEDURE       => ExecuteAdminProcedureAction
    case ActionType.SERVER_SHOW                   => ShowServerAction
    case ActionType.SERVER_MANAGEMENT             => ServerManagementAction
    case ActionType.SETTING_SHOW                  => ShowSettingAction

    case ActionType.GRAPH_ALL          => AllGraphAction
    case ActionType.GRAPH_WRITE        => WriteAction
    case ActionType.GRAPH_CREATE       => CreateElementAction
    case ActionType.GRAPH_MERGE        => MergeAdminAction
    case ActionType.GRAPH_DELETE       => DeleteElementAction
    case ActionType.GRAPH_LABEL_SET    => SetLabelAction
    case ActionType.GRAPH_LABEL_REMOVE => RemoveLabelAction
    case ActionType.GRAPH_PROPERTY_SET => SetPropertyAction
    case ActionType.GRAPH_MATCH        => MatchAction
    case ActionType.GRAPH_READ         => ReadAction
    case ActionType.GRAPH_TRAVERSE     => TraverseAction
  }

  // Resources

  override def propertiesResource(p: InputPosition, properties: util.List[String]): ActionResource =
    PropertiesResource(properties.asScala.toSeq)(p)

  override def allPropertiesResource(p: InputPosition): ActionResource = AllPropertyResource()(p)

  override def labelsResource(p: InputPosition, labels: util.List[String]): ActionResource =
    LabelsResource(labels.asScala.toSeq)(p)

  override def allLabelsResource(p: InputPosition): ActionResource = AllLabelResource()(p)

  override def databaseResource(p: InputPosition): ActionResource = DatabaseResource()(p)

  override def noResource(p: InputPosition): ActionResource = NoResource()(p)

  override def labelQualifier(p: InputPosition, label: String): PrivilegeQualifier = LabelQualifier(label)(p)

  override def allLabelsQualifier(p: InputPosition): PrivilegeQualifier = LabelAllQualifier()(p)

  override def relationshipQualifier(p: InputPosition, relationshipType: String): PrivilegeQualifier =
    RelationshipQualifier(relationshipType)(p)

  override def allRelationshipsQualifier(p: InputPosition): PrivilegeQualifier = RelationshipAllQualifier()(p)

  override def elementQualifier(p: InputPosition, name: String): PrivilegeQualifier = ElementQualifier(name)(p)

  override def allElementsQualifier(p: InputPosition): PrivilegeQualifier = ElementsAllQualifier()(p)

  override def patternQualifier(
    qualifiers: util.List[PrivilegeQualifier],
    variable: Variable,
    expression: Expression
  ): PrivilegeQualifier =
    PatternQualifier(qualifiers.asScala.toList, Option(variable), expression)

  override def allQualifier(): util.List[PrivilegeQualifier] = {
    val list = new util.ArrayList[PrivilegeQualifier]()
    list.add(AllQualifier()(InputPosition.NONE))
    list
  }

  override def allDatabasesQualifier(): util.List[PrivilegeQualifier] = {
    val list = new util.ArrayList[PrivilegeQualifier]()
    list.add(AllDatabasesQualifier()(InputPosition.NONE))
    list
  }

  override def userQualifier(users: util.List[SimpleEither[String, Parameter]]): util.List[PrivilegeQualifier] = {
    val list = new util.ArrayList[PrivilegeQualifier]()
    users.forEach(u => list.add(UserQualifier(u.asScala)(InputPosition.NONE)))
    list
  }

  override def allUsersQualifier(): util.List[PrivilegeQualifier] = {
    val list = new util.ArrayList[PrivilegeQualifier]()
    list.add(UserAllQualifier()(InputPosition.NONE))
    list
  }

  override def functionQualifier(p: InputPosition, functions: util.List[String]): util.List[PrivilegeQualifier] = {
    val list = new util.ArrayList[PrivilegeQualifier]()
    functions.forEach(f => list.add(FunctionQualifier(f)(p)))
    list
  }

  override def procedureQualifier(p: InputPosition, procedures: util.List[String]): util.List[PrivilegeQualifier] = {
    val list = new util.ArrayList[PrivilegeQualifier]()
    procedures.forEach(proc => list.add(ProcedureQualifier(proc)(p)))
    list
  }

  override def settingQualifier(p: InputPosition, names: util.List[String]): util.List[PrivilegeQualifier] = {
    val list = new util.ArrayList[PrivilegeQualifier]()
    names.forEach(proc => list.add(SettingQualifier(proc)(p)))
    list
  }

  override def graphScope(
    p: InputPosition,
    graphNames: util.List[DatabaseName],
    scopeType: ScopeType
  ): GraphScope = {
    scopeType match {
      case ScopeType.ALL     => AllGraphsScope()(p)
      case ScopeType.HOME    => HomeGraphScope()(p)
      case ScopeType.DEFAULT => DefaultGraphScope()(p)
      case ScopeType.NAMED   => NamedGraphsScope(graphNames.asScala.toSeq)(p)
    }
  }

  override def databaseScope(
    p: InputPosition,
    databaseNames: util.List[DatabaseName],
    scopeType: ScopeType
  ): DatabaseScope = {
    scopeType match {
      case ScopeType.ALL     => AllDatabasesScope()(p)
      case ScopeType.HOME    => HomeDatabaseScope()(p)
      case ScopeType.DEFAULT => DefaultDatabaseScope()(p)
      case ScopeType.NAMED   => NamedDatabasesScope(databaseNames.asScala.toSeq)(p)
    }
  }

  // Server commands

  override def enableServer(
    p: InputPosition,
    serverName: SimpleEither[String, Parameter],
    options: SimpleEither[util.Map[String, Expression], Parameter]
  ): EnableServer =
    EnableServer(serverName.asScala, asOptionsAst(options))(p)

  override def alterServer(
    p: InputPosition,
    serverName: SimpleEither[String, Parameter],
    options: SimpleEither[util.Map[String, Expression], Parameter]
  ): AlterServer =
    AlterServer(serverName.asScala, asOptionsAst(options))(p)

  override def renameServer(
    p: InputPosition,
    serverName: SimpleEither[String, Parameter],
    newName: SimpleEither[String, Parameter]
  ): RenameServer =
    RenameServer(serverName.asScala, newName.asScala)(p)

  override def dropServer(p: InputPosition, serverName: SimpleEither[String, Parameter]): DropServer =
    DropServer(serverName.asScala)(p)

  override def showServers(
    p: InputPosition,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ShowServers =
    ShowServers(yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)

  override def deallocateServers(
    p: InputPosition,
    dryRun: Boolean,
    serverNames: util.List[SimpleEither[String, Parameter]]
  ): DeallocateServers =
    DeallocateServers(dryRun, serverNames.asScala.map(_.asScala).toList)(p)

  override def reallocateDatabases(p: InputPosition, dryRun: Boolean): ReallocateDatabases =
    ReallocateDatabases(dryRun)(p)

  // Database commands

  override def createDatabase(
    p: InputPosition,
    replace: Boolean,
    databaseName: DatabaseName,
    ifNotExists: Boolean,
    wait: WaitUntilComplete,
    options: SimpleEither[util.Map[String, Expression], Parameter],
    topologyPrimaries: Integer,
    topologySecondaries: Integer
  ): CreateDatabase = {
    val primaryOpt = Option(topologyPrimaries).map(_.intValue())
    val secondaryOpt = Option(topologySecondaries).map(_.intValue())
    CreateDatabase(
      databaseName,
      ifExistsDo(replace, ifNotExists),
      asOptionsAst(options),
      wait,
      if (primaryOpt.nonEmpty || secondaryOpt.nonEmpty) Some(Topology(primaryOpt, secondaryOpt)) else None
    )(p)
  }

  override def createCompositeDatabase(
    p: InputPosition,
    replace: Boolean,
    compositeDatabaseName: DatabaseName,
    ifNotExists: Boolean,
    options: SimpleEither[util.Map[String, Expression], Parameter],
    wait: WaitUntilComplete
  ): AdministrationCommand = {
    CreateCompositeDatabase(compositeDatabaseName, ifExistsDo(replace, ifNotExists), asOptionsAst(options), wait)(p)
  }

  override def dropDatabase(
    p: InputPosition,
    databaseName: DatabaseName,
    ifExists: Boolean,
    composite: Boolean,
    dumpData: Boolean,
    wait: WaitUntilComplete
  ): DropDatabase = {
    val action: DropDatabaseAdditionalAction =
      if (dumpData) {
        DumpData
      } else {
        DestroyData
      }

    DropDatabase(databaseName, ifExists, composite, action, wait)(p)
  }

  override def alterDatabase(
    p: InputPosition,
    databaseName: DatabaseName,
    ifExists: Boolean,
    accessType: AccessType,
    topologyPrimaries: Integer,
    topologySecondaries: Integer,
    options: util.Map[String, Expression],
    optionsToRemove: util.Set[String],
    waitClause: WaitUntilComplete
  ): AlterDatabase = {
    val access = Option(accessType) map {
      case READ_ONLY  => ReadOnlyAccess
      case READ_WRITE => ReadWriteAccess
    }
    val primaryOpt = Option(topologyPrimaries).map(_.intValue())
    val secondaryOpt = Option(topologySecondaries).map(_.intValue())
    val opts = if (options != null) OptionsMap(options.asScala.toMap) else NoOptions
    val optsToRemove: Set[String] = optionsToRemove.asScala.toSet

    AlterDatabase(
      databaseName,
      ifExists,
      access,
      if (primaryOpt.nonEmpty || secondaryOpt.nonEmpty) Some(Topology(primaryOpt, secondaryOpt)) else None,
      opts,
      optsToRemove,
      waitClause
    )(p)
  }

  override def showDatabase(
    p: InputPosition,
    scope: DatabaseScope,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ShowDatabase = {
    if (yieldExpr != null) {
      ShowDatabase(scope, Some(Left((yieldExpr, Option(returnWithoutGraph)))))(p)
    } else {
      ShowDatabase(scope, Option(where).map(e => Right(e)))(p)
    }
  }

  override def databaseScope(
    p: InputPosition,
    databaseName: DatabaseName,
    isDefault: Boolean,
    isHome: Boolean
  ): DatabaseScope = {
    if (databaseName != null) {
      SingleNamedDatabaseScope(databaseName)(p)
    } else if (isDefault) {
      DefaultDatabaseScope()(p)
    } else if (isHome) {
      HomeDatabaseScope()(p)
    } else {
      AllDatabasesScope()(p)
    }
  }

  override def startDatabase(
    p: InputPosition,
    databaseName: DatabaseName,
    wait: WaitUntilComplete
  ): StartDatabase = {
    StartDatabase(databaseName, wait)(p)
  }

  override def stopDatabase(
    p: InputPosition,
    databaseName: DatabaseName,
    wait: WaitUntilComplete
  ): StopDatabase = {
    StopDatabase(databaseName, wait)(p)
  }

  override def wait(wait: Boolean, seconds: Long): WaitUntilComplete = {
    if (!wait) {
      NoWait
    } else if (seconds > 0) {
      TimeoutAfter(seconds)
    } else {
      IndefiniteWait
    }
  }

  override def databaseName(p: InputPosition, names: util.List[String]): DatabaseName = NamespacedName(names)(p)

  override def databaseName(param: Parameter): DatabaseName = ParameterName(param)(param.position)

  // Alias commands

  override def createLocalDatabaseAlias(
    p: InputPosition,
    replace: Boolean,
    aliasName: DatabaseName,
    targetName: DatabaseName,
    ifNotExists: Boolean,
    properties: SimpleEither[util.Map[String, Expression], Parameter]
  ): CreateLocalDatabaseAlias = {
    CreateLocalDatabaseAlias(
      aliasName,
      targetName,
      ifExistsDo(replace, ifNotExists),
      Option(properties).map(asExpressionMapAst)
    )(p)
  }

  override def createRemoteDatabaseAlias(
    p: InputPosition,
    replace: Boolean,
    aliasName: DatabaseName,
    targetName: DatabaseName,
    ifNotExists: Boolean,
    url: SimpleEither[String, Parameter],
    username: SimpleEither[String, Parameter],
    password: Expression,
    driverSettings: SimpleEither[util.Map[String, Expression], Parameter],
    properties: SimpleEither[util.Map[String, Expression], Parameter]
  ): CreateRemoteDatabaseAlias = {
    CreateRemoteDatabaseAlias(
      aliasName,
      targetName,
      ifExistsDo(replace, ifNotExists),
      url.asScala,
      username.asScala,
      password,
      Option(driverSettings).map(asExpressionMapAst),
      Option(properties).map(asExpressionMapAst)
    )(p)
  }

  override def alterLocalDatabaseAlias(
    p: InputPosition,
    aliasName: DatabaseName,
    targetName: DatabaseName,
    ifExists: Boolean,
    properties: SimpleEither[util.Map[String, Expression], Parameter]
  ): AlterLocalDatabaseAlias = {
    AlterLocalDatabaseAlias(
      aliasName,
      Option(targetName),
      ifExists,
      Option(properties).map(asExpressionMapAst)
    )(p)
  }

  override def alterRemoteDatabaseAlias(
    p: InputPosition,
    aliasName: DatabaseName,
    targetName: DatabaseName,
    ifExists: Boolean,
    url: SimpleEither[String, Parameter],
    username: SimpleEither[String, Parameter],
    password: Expression,
    driverSettings: SimpleEither[util.Map[String, Expression], Parameter],
    properties: SimpleEither[util.Map[String, Expression], Parameter]
  ): AlterRemoteDatabaseAlias = {
    AlterRemoteDatabaseAlias(
      aliasName,
      Option(targetName),
      ifExists,
      Option(url).map(_.asScala),
      Option(username).map(_.asScala),
      Option(password),
      Option(driverSettings).map(asExpressionMapAst),
      Option(properties).map(asExpressionMapAst)
    )(p)
  }

  override def dropAlias(
    p: InputPosition,
    aliasName: DatabaseName,
    ifExists: Boolean
  ): DropDatabaseAlias = {
    DropDatabaseAlias(aliasName, ifExists)(p)
  }

  override def showAliases(
    p: InputPosition,
    aliasName: DatabaseName,
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): ShowAliases =
    ShowAliases(
      Option(aliasName),
      yieldOrWhere(yieldExpr, returnWithoutGraph, where)
    )(p)

  private def ifExistsDo(replace: Boolean, ifNotExists: Boolean): IfExistsDo = {
    (replace, ifNotExists) match {
      case (true, true)   => IfExistsInvalidSyntax
      case (true, false)  => IfExistsReplace
      case (false, true)  => IfExistsDoNothing
      case (false, false) => IfExistsThrowError
    }
  }

  private def yieldOrWhere(
    yieldExpr: Yield,
    returnWithoutGraph: Return,
    where: Where
  ): Option[Either[(Yield, Option[Return]), Where]] = {
    if (yieldExpr != null) {
      Some(Left(yieldExpr -> Option(returnWithoutGraph)))
    } else if (where != null) {
      Some(Right(where))
    } else {
      None
    }
  }

  private def asBooleanOption(bool: lang.Boolean): Option[Boolean] =
    if (bool == null) None else Some(bool.booleanValue())

  private def asOptionsAst(options: SimpleEither[util.Map[String, Expression], Parameter]) =
    Option(options).map(_.asScala) match {
      case Some(Left(map))    => OptionsMap(map.asScala.toMap)
      case Some(Right(param)) => OptionsParam(param)
      case None               => NoOptions
    }

  private def asExpressionMapAst(driverSettings: SimpleEither[util.Map[String, Expression], Parameter])
    : Either[Map[String, Expression], Parameter] =
    driverSettings.asScala match {
      case Left(map)    => Left(map.asScala.toMap)
      case Right(param) => Right(param)
    }

  private def pretty[T <: AnyRef](ts: util.List[T]): String = {
    ts.stream().map[String](t => t.toString).collect(Collectors.joining(","))
  }

  override def labelConjunction(
    p: InputPosition,
    lhs: LabelExpression,
    rhs: LabelExpression,
    containsIs: Boolean
  ): LabelExpression =
    LabelExpression.Conjunctions.flat(lhs, rhs, p, containsIs)

  override def labelDisjunction(
    p: InputPosition,
    lhs: LabelExpression,
    rhs: LabelExpression,
    containsIs: Boolean
  ): LabelExpression = {
    LabelExpression.Disjunctions.flat(lhs, rhs, p, containsIs)
  }

  override def labelNegation(p: InputPosition, e: LabelExpression, containsIs: Boolean): LabelExpression =
    LabelExpression.Negation(e, containsIs)(p)

  override def labelWildcard(p: InputPosition, containsIs: Boolean): LabelExpression =
    LabelExpression.Wildcard(containsIs)(p)

  override def labelLeaf(p: InputPosition, n: String, entityType: EntityType, containsIs: Boolean): LabelExpression =
    entityType match {
      case EntityType.NODE                 => Leaf(LabelName(n)(p), containsIs)
      case EntityType.NODE_OR_RELATIONSHIP => Leaf(LabelOrRelTypeName(n)(p), containsIs)
      case EntityType.RELATIONSHIP         => Leaf(RelTypeName(n)(p), containsIs)
    }

  override def labelColonConjunction(
    p: InputPosition,
    lhs: LabelExpression,
    rhs: LabelExpression,
    containsIs: Boolean
  ): LabelExpression =
    LabelExpression.ColonConjunction(lhs, rhs, containsIs)(p)

  override def labelColonDisjunction(
    p: InputPosition,
    lhs: LabelExpression,
    rhs: LabelExpression,
    containsIs: Boolean
  ): LabelExpression =
    LabelExpression.ColonDisjunction(lhs, rhs, containsIs)(p)

  override def labelExpressionPredicate(subject: Expression, exp: LabelExpression): Expression =
    LabelExpressionPredicate(subject, exp)(subject.position)

  override def nodeType(): EntityType = EntityType.NODE

  override def relationshipType(): EntityType = EntityType.RELATIONSHIP

  override def nodeOrRelationshipType(): EntityType = EntityType.NODE_OR_RELATIONSHIP

  override def addDeprecatedIdentifierUnicodeNotification(
    p: InputPosition,
    char: Character,
    identifier: String
  ): Unit = {
    if (logger != null) {
      if (char == '\u0085') {
        logger.log(DeprecatedIdentifierWhitespaceUnicode(p, char, identifier))
      } else {
        logger.log(DeprecatedIdentifierUnicode(p, char, identifier))
      }
    }
  }
}
