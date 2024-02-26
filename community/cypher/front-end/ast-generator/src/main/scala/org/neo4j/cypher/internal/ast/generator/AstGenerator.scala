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

import org.neo4j.cypher.internal.ast.Access
import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.ActionResource
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
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.CompositeDatabaseManagementActions
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateAliasAction
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
import org.neo4j.cypher.internal.ast.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyTypeConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.CreatePointNodeIndex
import org.neo4j.cypher.internal.ast.CreatePointRelationshipIndex
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRangeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRelationshipKeyConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyTypeConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyUniquenessConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.CreateVectorNodeIndex
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.DatabasePrivilegeQualifier
import org.neo4j.cypher.internal.ast.DbmsAction
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
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.DropIndexOnName
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
import org.neo4j.cypher.internal.ast.GraphPrivilegeQualifier
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.ImpersonateUserAction
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
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.MatchAction
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.NamedDatabasesScope
import org.neo4j.cypher.internal.ast.NamedGraphsScope
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ParameterName
import org.neo4j.cypher.internal.ast.ParsedAsYield
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PrivilegeCommand
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.ReadAction
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
import org.neo4j.cypher.internal.ast.RevokeType
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
import org.neo4j.cypher.internal.ast.ShowIndexType
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
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
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsBatchParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsConcurrencyParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.TimeoutAfter
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
import org.neo4j.cypher.internal.ast.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
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
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasALabelOrType
import org.neo4j.cypher.internal.expressions.HasAnyLabel
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
import org.neo4j.cypher.internal.expressions.Namespace
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
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.ProcedureOutput
import org.neo4j.cypher.internal.expressions.Property
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
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.ListType
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
  def validString: Gen[String] =
    nonEmptyListOf(char).map(_.mkString).suchThat(!_.matches("^.*\\\\[u,U].*$"))

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
 */
//noinspection ScalaWeakerAccess
class AstGenerator(simpleStrings: Boolean = true, allowedVarNames: Option[Seq[String]] = None) {
  // HELPERS
  // ==========================================================================

  protected val pos: InputPosition = InputPosition.NONE

  def string: Gen[String] =
    if (simpleStrings) alphaLowerChar.map(_.toString)
    else validString

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
    string.flatMap(StringLiteral(_)(pos, pos))

  def _sensitiveStringLiteral: Gen[SensitiveStringLiteral] =
    // Needs to be '******' since all sensitive strings get rendered as such
    // Would normally get rewritten as SensitiveAutoParameter which can be generated as parameter when needed
    const(SensitiveStringLiteral("******".getBytes(StandardCharsets.UTF_8))(pos, pos))

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

  def _unsignedDecIntLit: Gen[UnsignedDecimalIntegerLiteral] =
    _unsignedIntString("", 10).map(UnsignedDecimalIntegerLiteral(_)(pos))

  def _signedDecIntLit: Gen[SignedDecimalIntegerLiteral] =
    _signedIntString("", 10).map(SignedDecimalIntegerLiteral(_)(pos))

  def _signedHexIntLit: Gen[SignedHexIntegerLiteral] =
    _signedIntString("0x", 16).map(SignedHexIntegerLiteral(_)(pos))

  def _signedOctIntLitOldSyntax: Gen[SignedOctalIntegerLiteral] =
    _signedIntString("0", 8).map(SignedOctalIntegerLiteral(_)(pos))

  def _signedOctIntLit: Gen[SignedOctalIntegerLiteral] =
    _signedIntString("0o", 8).map(SignedOctalIntegerLiteral(_)(pos))

  def _doubleLit: Gen[DecimalDoubleLiteral] =
    Arbitrary.arbDouble.arbitrary.map(_.toString).map(DecimalDoubleLiteral(_)(pos))

  def _parameter: Gen[Parameter] =
    _identifier.map(ExplicitParameter(_, AnyType(isNullable = true)(pos))(pos))

  def _stringParameter: Gen[Parameter] = _identifier.map(ExplicitParameter(_, CTString)(pos))

  def _mapParameter: Gen[Parameter] = _identifier.map(ExplicitParameter(_, CTMap)(pos))

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
    } yield Variable(name)(pos)
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
    res <- oneOf(
      Not(r)(pos),
      IsNull(r)(pos),
      IsNotNull(r)(pos),
      IsTyped(r, typeName)(pos),
      IsNotTyped(r, typeName)(pos),
      IsNormalized(r, normalForm)(pos),
      IsNotNormalized(r, normalForm)(pos)
    )
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
    innerPredicate <- option(_expression)
  } yield FilterScope(variable, innerPredicate)(pos)

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

  def _functionName: Gen[FunctionName] = for {
    name <- _identifier
  } yield FunctionName(name)(pos)

  def _functionInvocation: Gen[FunctionInvocation] = for {
    namespace <- _namespace
    functionName <- _functionName
    distinct <- boolean
    args <- zeroOrMore(_expression)
  } yield FunctionInvocation(namespace, functionName, distinct, args.toIndexedSeq)(pos)

  def _glob: Gen[String] = for {
    parts <- zeroOrMore(_identifier)
    name <- _identifier
  } yield parts.mkString(".") ++ name

  def _countStar: Gen[CountStar] =
    const(CountStar()(pos))

  // Patterns
  // ----------------------------------

  def _relationshipsPattern: Gen[RelationshipsPattern] = for {
    chain <- _relationshipChain
  } yield RelationshipsPattern(chain)(pos)

  def _patternExpr: Gen[PatternExpression] = for {
    pattern <- _relationshipsPattern
  } yield PatternExpression(pattern)(None, None)

  def _shortestPaths: Gen[ShortestPathsPatternPart] = for {
    element <- _patternElement
    single <- boolean
  } yield ShortestPathsPatternPart(element, single)(pos)

  def _shortestPathExpr: Gen[ShortestPathExpression] = for {
    pattern <- _shortestPaths
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
  } yield PatternComprehension(namedPath, pattern, predicate, projection)(pos, Some(introducedVariables.toSet), Some(scopeDependencies.toSet))

  // Expression
  // ----------------------------------

  def _expression: Gen[Expression] =
    frequency(
      5 -> oneOf(
        lzy(_nullLit),
        lzy(_stringLit),
        lzy(_booleanLit),
        lzy(_signedDecIntLit),
        lzy(_signedHexIntLit),
        lzy(_signedOctIntLitOldSyntax),
        lzy(_signedOctIntLit),
        lzy(_doubleLit),
        lzy(_variable),
        lzy(_parameter),
        lzy(_infinityLit),
        lzy(_nanLit)
      ),
      1 -> oneOf(
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
    )

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

    oneOf(
      _hasLabels(),
      _hasAnyLabel(),
      _hasLabelsOrTypes(),
      _hasTypes,
      lzy(HasALabelOrType(variable)(pos)),
      lzy(HasALabel(variable)(pos))
    )
  }

  def _labelExpression(entityType: Option[EntityType], containsIs: Boolean): Gen[LabelExpression] = {

    def _labelExpressionConjunction(): Gen[LabelExpression.Conjunctions] = for {
      lhs <- _labelExpression(entityType, containsIs)
      rhs <- _labelExpression(entityType, containsIs)
    } yield LabelExpression.Conjunctions.flat(lhs, rhs, pos, containsIs)

    def _labelExpressionDisjunction(): Gen[LabelExpression.Disjunctions] = for {
      lhs <- _labelExpression(entityType, containsIs)
      rhs <- _labelExpression(entityType, containsIs)
    } yield LabelExpression.Disjunctions.flat(lhs, rhs, pos, containsIs)

    for {
      labelExpr <- frequency(
        5 -> oneOf[LabelExpression](
          lzy(LabelExpression.Wildcard(containsIs)(pos)),
          lzy(entityType match {
            case Some(NODE_TYPE)         => _labelName.map(Leaf(_, containsIs))
            case Some(RELATIONSHIP_TYPE) => _relTypeName.map(Leaf(_, containsIs))
            case None                    => _labelOrTypeName.map(Leaf(_, containsIs))
          })
        ),
        1 -> oneOf[LabelExpression](
          lzy(_labelExpressionConjunction()),
          lzy(_labelExpressionDisjunction()),
          lzy(_labelExpression(entityType, containsIs).map(LabelExpression.Negation(_, containsIs)(pos)))
        )
      )
    } yield labelExpr
  }

  // PATTERNS
  // ==========================================================================

  def _nodePattern: Gen[NodePattern] = for {
    variable <- option(_variable)
    containsIs <- boolean
    labelExpression <- option(_labelExpression(Some(NODE_TYPE), containsIs))
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
    lower <- option(_unsignedDecIntLit)
    upper <- option(_unsignedDecIntLit)
  } yield Range(lower, upper)(pos)

  def _semanticDirection: Gen[SemanticDirection] =
    oneOf(
      SemanticDirection.OUTGOING,
      SemanticDirection.INCOMING,
      SemanticDirection.BOTH
    )

  def _relationshipPattern: Gen[RelationshipPattern] = for {
    variable <- option(_variable)
    containsIs <- boolean
    labelExpression <- option(_labelExpression(Some(RELATIONSHIP_TYPE), containsIs))
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

  def _relationshipChain: Gen[RelationshipChain] = for {
    element <- _pathPrimary
    relationship <- _relationshipPattern
    rightNode <- _nodePattern
  } yield RelationshipChain(element, relationship, rightNode)(pos)

  def _pathPrimary: Gen[SimplePattern] = oneOf(
    _nodePattern,
    lzy(_relationshipChain)
  )

  def _generalQuantifier: Gen[IntervalQuantifier] = for {
    lower <- option(_unsignedDecIntLit)
    upper <- option(_unsignedDecIntLit)
  } yield IntervalQuantifier(lower, upper)(pos)

  def _fixedQuantifier: Gen[FixedQuantifier] = for {
    value <- _unsignedDecIntLit
  } yield FixedQuantifier(value)(pos)

  def _quantifier: Gen[GraphPatternQuantifier] = oneOf(
    const(StarQuantifier()(pos)),
    const(PlusQuantifier()(pos)),
    _generalQuantifier,
    _fixedQuantifier
  )

  def _quantifiedPath: Gen[QuantifiedPath] = for {
    primary <- _pathFactor
    quantifier <- _quantifier
    where <- option(_expression)
  } yield QuantifiedPath(PatternPart(primary), quantifier, where)(pos)

  def _pathFactor: Gen[PathFactor] = oneOf(
    lzy(_quantifiedPath),
    lzy(_pathPrimary)
  )

  def _pathConcatenation: Gen[PathConcatenation] = for {
    elements <- twoOrMore(_pathFactor)
  } yield PathConcatenation(elements)(pos)

  def _patternElement: Gen[PatternElement] = oneOf(
    lzy(_pathFactor),
    lzy(_pathConcatenation)
  )

  def _selector: Gen[Selector] = for {
    count <- _unsignedDecIntLit
    selector <- oneOf(
      lzy(AnyPath(count)(pos)),
      lzy(AllPaths()(pos)),
      lzy(AnyShortestPath(count)(pos)),
      lzy(AllShortestPaths()(pos)),
      lzy(ShortestGroups(count)(pos))
    )
  } yield selector

  def _anonPatternPart: Gen[AnonymousPatternPart] = for {
    element <- _patternElement
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

  def _patternPartWithSelector: Gen[PatternPartWithSelector] =
    for {
      part <- _nonPrefixedPatternPart
      selector <- _selector
    } yield PatternPartWithSelector(selector, part)

  def _patternForMatch: Gen[Pattern.ForMatch] = for {
    parts <- oneOrMore(_patternPartWithSelector)
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
    properties <- option(oneOf(_map, _parameter))
  } yield NodePattern(variable, labelExpression, properties, None)(pos)

  def _insertNodeLabelExpression(containsIs: Boolean): Gen[LabelExpression] = {

    def _insertLabelExpressionConjunction(): Gen[LabelExpression.Conjunctions] = for {
      lhs <- _insertNodeLabelExpression(containsIs)
      rhs <- _insertNodeLabelExpression(containsIs)
    } yield LabelExpression.Conjunctions.flat(lhs, rhs, pos, containsIs)

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
    properties <- option(oneOf(_map, _parameter))
    direction <- _semanticDirection
  } yield RelationshipPattern(variable, Some(labelExpression), None, properties, None, direction)(pos)

  // CLAUSES
  // ==========================================================================

  def _returnItem: Gen[ReturnItem] = for {
    expr <- _expression
    variable <- _variable
    item <- oneOf(
      UnaliasedReturnItem(expr, "")(pos),
      AliasedReturnItem(expr, variable)(pos)
    )
  } yield item

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

  def _with: Gen[With] = for {
    distinct <- boolean
    inclExisting <- boolean
    retItems <- oneOrMore(_returnItem)
    orderBy <- option(_orderBy)
    skip <- option(_skip)
    limit <- option(_limit)
    where <- option(_where)
  } yield With(distinct, ReturnItems(inclExisting, retItems)(pos), orderBy, skip, limit, where)(pos)

  def _return: Gen[Return] = for {
    distinct <- boolean
    inclExisting <- boolean
    retItems <- oneOrMore(_returnItem)
    orderBy <- option(_orderBy)
    skip <- option(_skip)
    limit <- option(_limit)
  } yield Return(distinct, ReturnItems(inclExisting, retItems)(pos), orderBy, skip, limit)(pos)

  def _yield: Gen[Yield] = for {
    retItems <- oneOrMore(_yieldItem)
    orderBy <- option(_orderBy)
    skip <- option(_signedDecIntLit.map(Skip(_)(pos)))
    limit <- option(_signedDecIntLit.map(Limit(_)(pos)))
    where <- option(_where)
  } yield Yield(ReturnItems(includeExisting = false, retItems)(pos), orderBy, skip, limit, where)(pos)

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
  } yield Match(optional, matchMode, pattern, hints, where)(pos)

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
  } yield Unwind(expression, variable)(pos)

  def _setItem: Gen[SetItem] = for {
    variable <- _variable
    labels <- oneOrMore(_labelName)
    property <- _property
    expression <- _expression
    containsIs <- boolean
    item <- oneOf(
      SetLabelItem(variable, labels, containsIs)(pos),
      SetPropertyItem(property, expression)(pos),
      SetExactPropertiesFromMapItem(variable, expression)(pos),
      SetIncludingPropertiesFromMapItem(variable, expression)(pos)
    )
  } yield item

  def _removeItem: Gen[RemoveItem] = for {
    variable <- _variable
    labels <- oneOrMore(_labelName)
    containsIs <- boolean
    property <- _property
    item <- oneOf(
      RemoveLabelItem(variable, labels, containsIs)(pos),
      RemovePropertyItem(property)
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

  def _procedureName: Gen[ProcedureName] = for {
    name <- _identifier
  } yield ProcedureName(name)(pos)

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
    procedureNamespace <- _namespace
    procedureName <- _procedureName
    declaredArguments <- option(zeroOrMore(_expression))
    declaredResult <- option(_procedureResult)
    yieldAll <- if (declaredResult.isDefined) const(false) else boolean // can't have both YIELD * and declare results
  } yield UnresolvedCall(procedureNamespace, procedureName, declaredArguments, declaredResult, yieldAll)(pos)

  def _foreach: Gen[Foreach] = for {
    variable <- _variable
    expression <- _expression
    updates <- oneOrMore(_clause)
  } yield Foreach(variable, expression, updates)(pos)

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
  } yield UsingJoinHint(variables)(pos)

  def _usingScanHint: Gen[UsingScanHint] = for {
    variable <- _variable
    labelOrRelType <- _labelOrTypeName
  } yield UsingScanHint(variable, labelOrRelType)(pos)

  def _hint: Gen[UsingHint] = oneOf(
    _usingIndexHint,
    _usingJoinHint,
    _usingScanHint
  )

  // Queries
  // ----------------------------------

  def _use: Gen[UseGraph] = for {
    names <- listOfN(1, _identifier)
    function <- _functionInvocation
    graphRef <- oneOf(
      GraphDirectReference(CatalogName(names))(pos),
      GraphFunctionReference(function)(pos)
    )
  } yield UseGraph(graphRef)(pos)

  def _subqueryCall: Gen[SubqueryCall] = for {
    innerQuery <- _query
    params <- option(_inTransactionsParameters)
  } yield SubqueryCall(innerQuery, params)(pos)

  def _inTransactionsParameters: Gen[InTransactionsParameters] =
    for {
      batchSize <- option(_expression)
      concurrency <- option(option(_expression))
      onErrorBehaviour <- option(oneOf[InTransactionsOnErrorBehaviour](OnErrorContinue, OnErrorBreak, OnErrorFail))
      reportAs <- option(string)
    } yield InTransactionsParameters(
      batchSize.map(InTransactionsBatchParameters(_)(pos)),
      concurrency.map(InTransactionsConcurrencyParameters(_)(pos)),
      onErrorBehaviour.map(InTransactionsErrorParameters(_)(pos)),
      reportAs.map(v => InTransactionsReportParameters(Variable(s"`$v`")(pos))(pos))
    )(pos)

  def _clause: Gen[Clause] = oneOf(
    lzy(_use),
    lzy(_with),
    lzy(_return),
    lzy(_match),
    lzy(_create),
    lzy(_insert),
    lzy(_unwind),
    lzy(_set),
    lzy(_remove),
    lzy(_delete),
    lzy(_merge),
    lzy(_call),
    lzy(_foreach),
    lzy(_loadCsv),
    lzy(_subqueryCall)
  )

  def _singleQuery: Gen[SingleQuery] = for {
    s <- choose(1, 1)
    clauses <- listOfN(s, _clause)
  } yield SingleQuery(clauses)(pos)

  def _union: Gen[Union] = for {
    lhs <- _query
    rhs <- _singleQuery
    union <- oneOf(
      UnionDistinct(lhs, rhs)(pos),
      UnionAll(lhs, rhs)(pos)
    )
  } yield union

  def _query: Gen[Query] = frequency(
    5 -> lzy(_singleQuery),
    1 -> lzy(_union)
  )

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
    constraintType <- oneOf(
      AllConstraints,
      UniqueConstraints,
      NodeUniqueConstraints,
      RelUniqueConstraints,
      ExistsConstraints(ValidSyntax),
      NodeExistsConstraints(ValidSyntax),
      RelExistsConstraints(ValidSyntax),
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
            brief = false,
            verbose = false,
            Some(w),
            List.empty,
            yieldAll = false
          )(pos)
        )
      case Some(Left((y, Some(r)))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(
          ShowIndexesClause(
            indexType,
            brief = false,
            verbose = false,
            None,
            yi,
            yieldAll = false
          )(pos),
          w,
          r
        )
      case Some(Left((y, None))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(
          ShowIndexesClause(
            indexType,
            brief = false,
            verbose = false,
            None,
            yi,
            yieldAll = false
          )(pos),
          w
        )
      case _ if yieldAll =>
        Seq(
          ShowIndexesClause(
            indexType,
            brief = false,
            verbose = false,
            None,
            List.empty,
            yieldAll = true
          )(pos),
          getFullWithStarFromYield
        )
      case _ =>
        Seq(
          ShowIndexesClause(
            indexType,
            brief = false,
            verbose = false,
            None,
            List.empty,
            yieldAll = false
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
        Seq(
          ShowConstraintsClause(
            constraintType,
            brief = false,
            verbose = false,
            Some(w),
            List.empty,
            yieldAll = false
          )(pos)
        )
      case Some(Left((y, Some(r)))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(
          ShowConstraintsClause(constraintType, brief = false, verbose = false, None, yi, yieldAll = false)(pos),
          w,
          r
        )
      case Some(Left((y, None))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(
          ShowConstraintsClause(constraintType, brief = false, verbose = false, None, yi, yieldAll = false)(pos),
          w
        )
      case _ if yieldAll =>
        Seq(
          ShowConstraintsClause(
            constraintType,
            brief = false,
            verbose = false,
            None,
            List.empty,
            yieldAll = true
          )(pos),
          getFullWithStarFromYield
        )
      case _ =>
        Seq(
          ShowConstraintsClause(
            constraintType,
            brief = false,
            verbose = false,
            None,
            List.empty,
            yieldAll = false
          )(pos)
        )
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showProcedures: Gen[Query] = for {
    name <- _identifier
    exec <- option(oneOf(CurrentUser, User(name)))
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
    use <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowProceduresClause(exec, Some(w), List.empty, yieldAll = false)(pos))
      case Some(Left((y, Some(r)))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowProceduresClause(exec, None, yi, yieldAll = false)(pos), w, r)
      case Some(Left((y, None))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowProceduresClause(exec, None, yi, yieldAll = false)(pos), w)
      case _ if yieldAll =>
        Seq(ShowProceduresClause(exec, None, List.empty, yieldAll = true)(pos), getFullWithStarFromYield)
      case _ =>
        Seq(ShowProceduresClause(exec, None, List.empty, yieldAll = false)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _showFunctions: Gen[Query] = for {
    name <- _identifier
    funcType <- oneOf(AllFunctions, BuiltInFunctions, UserDefinedFunctions)
    exec <- option(oneOf(CurrentUser, User(name)))
    yields <- _eitherYieldOrWhere
    yieldAll <- boolean
    use <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w)) =>
        Seq(ShowFunctionsClause(funcType, exec, Some(w), List.empty, yieldAll = false)(pos))
      case Some(Left((y, Some(r)))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowFunctionsClause(funcType, exec, None, yi, yieldAll = false)(pos), w, r)
      case Some(Left((y, None))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowFunctionsClause(funcType, exec, None, yi, yieldAll = false)(pos), w)
      case _ if yieldAll =>
        Seq(ShowFunctionsClause(funcType, exec, None, List.empty, yieldAll = true)(pos), getFullWithStarFromYield)
      case _ =>
        Seq(ShowFunctionsClause(funcType, exec, None, List.empty, yieldAll = false)(pos))
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
    val showClauses = yields match {
      case Some(Right(w)) => Seq(ShowTransactionsClause(ids, Some(w), List.empty, yieldAll = false)(pos))
      case Some(Left((y, Some(r)))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowTransactionsClause(ids, None, yi, yieldAll = false)(pos), w, r)
      case Some(Left((y, None))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowTransactionsClause(ids, None, yi, yieldAll = false)(pos), w)
      case _ if yieldAll =>
        Seq(ShowTransactionsClause(ids, None, List.empty, yieldAll = true)(pos), getFullWithStarFromYield)
      case _ => Seq(ShowTransactionsClause(ids, None, List.empty, yieldAll = false)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    SingleQuery(fullClauses)(pos)
  }

  def _terminateTransactions: Gen[Query] = for {
    ids <- namesOrNameExpression
    yields <- option(_yield)
    yieldAll <- boolean
    returns <- option(_return)
    use <- option(_use)
  } yield {
    val terminateClauses = (yields, returns) match {
      case (Some(y), Some(r)) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(TerminateTransactionsClause(ids, yi, yieldAll = false, None)(pos), w, r)
      case (Some(y), None) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(TerminateTransactionsClause(ids, yi, yieldAll = false, None)(pos), w)
      case _ if yieldAll =>
        Seq(TerminateTransactionsClause(ids, List.empty, yieldAll = true, None)(pos), getFullWithStarFromYield)
      case _ => Seq(TerminateTransactionsClause(ids, List.empty, yieldAll = false, None)(pos))
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
      case Some(Right(w)) => Seq(ShowSettingsClause(names, Some(w), List.empty, yieldAll = false)(pos))
      case Some(Left((y, Some(r)))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowSettingsClause(names, None, yi, yieldAll = false)(pos), w, r)
      case Some(Left((y, None))) =>
        val (w, yi) = turnYieldToWith(y)
        Seq(ShowSettingsClause(names, None, yi, yieldAll = false)(pos), w)
      case _ if yieldAll =>
        Seq(ShowSettingsClause(names, None, List.empty, yieldAll = true)(pos), getFullWithStarFromYield)
      case _ => Seq(ShowSettingsClause(names, None, List.empty, yieldAll = false)(pos))
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
        if (showFirst) show ++ terminate else terminate ++ show
      } else if (additionalTerminate.isEmpty) {
        // Only additional show, make show only command
        // add base show to ensure at least 2 clauses
        // (can be a mix of different show commands)
        show ++ additionalShow.flatten
      } else if (additionalShow.isEmpty) {
        // Only additional terminate, make terminate only command
        // add base terminate to ensure at least 2 clauses
        terminate ++ additionalTerminate.flatten
      } else {
        // multiple additional clauses, add all together and mix the order they appear in
        // (keeping the yield/with together with its respective clause)
        val allPairs = Seq(show, terminate) ++ additionalShow ++ additionalTerminate
        val scrambled = Random.shuffle(allPairs)
        scrambled.flatten
      }

    val clausesWithReturn = clauses :+ returns
    val fullClauses = use.map(u => u +: clausesWithReturn).getOrElse(clausesWithReturn)
    SingleQuery(fullClauses)(pos)
  }

  private def showAsPartOfCombined: Gen[Seq[Clause]] = for {
    ids <- namesOrNameExpression
    constraintType <- _constraintType
    indexType <- _indexType
    funcType <- oneOf(AllFunctions, BuiltInFunctions, UserDefinedFunctions)
    name <- _identifier
    exec <- option(oneOf(CurrentUser, User(name)))
    yields <- _yield
    yieldAll <- boolean
    clause <- oneOf(
      (item: List[CommandResultItem], all: Boolean) => ShowTransactionsClause(ids, None, item, all)(pos),
      (item: List[CommandResultItem], all: Boolean) => ShowFunctionsClause(funcType, exec, None, item, all)(pos),
      (item: List[CommandResultItem], all: Boolean) => ShowProceduresClause(exec, None, item, all)(pos),
      (item: List[CommandResultItem], all: Boolean) => ShowSettingsClause(ids, None, item, all)(pos),
      (item: List[CommandResultItem], all: Boolean) =>
        ShowConstraintsClause(constraintType, brief = false, verbose = false, None, item, all)(pos),
      (item: List[CommandResultItem], all: Boolean) =>
        ShowIndexesClause(indexType, brief = false, verbose = false, None, item, all)(pos)
    )
  } yield {
    val (withClause, items) = turnYieldToWith(yields)
    if (yieldAll) Seq(clause(List.empty, true), getFullWithStarFromYield)
    else Seq(clause(items, false), withClause)
  }

  private def terminateAsPartOfCombined: Gen[Seq[Clause]] = for {
    ids <- namesOrNameExpression
    yields <- _yield
    yieldAll <- boolean
  } yield {
    val (withClause, items) = turnYieldToWith(yields)
    if (yieldAll)
      Seq(TerminateTransactionsClause(ids, List.empty, yieldAll = true, None)(pos), getFullWithStarFromYield)
    else Seq(TerminateTransactionsClause(ids, items, yieldAll = false, None)(pos), withClause)
  }

  /* names for show commands:
   * - can be an expression or a list of strings
   * - a singular string is parsed as string expression
   * - no names gives an empty list
   * - two or more names give an name list
   */
  private def namesOrNameExpression: Gen[Either[List[String], Expression]] = for {
    multiIdList <- twoOrMore(string)
    idList <- oneOf(List.empty, multiIdList)
    expr <- _expression
    ids <- oneOf(Left(idList), Right(expr))
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
      ReturnItems(includeExisting = true, Seq(), itemOrder)(returnItems.position),
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
      ReturnItems(includeExisting = true, Seq())(pos),
      None,
      None,
      None,
      None,
      withType = ParsedAsYield
    )(pos)

  def _showCommands: Gen[Query] = oneOf(
    _showIndexes,
    _showConstraints,
    _showProcedures,
    _showFunctions,
    _showTransactions,
    _terminateTransactions,
    _showSettings,
    _combinedCommands
  )

  // Schema commands
  // ----------------------------------

  def _variableProperty: Gen[Property] = for {
    map <- _variable
    key <- _propertyKeyName
  } yield Property(map, key)(pos)

  def _listOfProperties: Gen[List[Property]] = for {
    props <- oneOrMore(_variableProperty)
  } yield props

  def _cypherTypeName: Gen[CypherType] = for {
    _type <- oneOf(allCypherTypeNamesFromReflection)
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
    val innerTypes = reflections.getSubTypesOf[CypherType](classOf[CypherType]).asScala.toSet
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

    val supportedInnerTypes = innerTypes.filter(_.hasCypherParserSupport)

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
    name <- option(_nameAsEither)
    ifExistsDo <- _ifExistsDo
    options <- _optionsMapAsEitherOrNone
    fromDefault <- boolean
    use <- option(_use)
    rangeNodeIndex = CreateRangeNodeIndex(variable, labelName, props, name, ifExistsDo, options, fromDefault, use)(pos)
    rangeRelIndex =
      CreateRangeRelationshipIndex(variable, relType, props, name, ifExistsDo, options, fromDefault, use)(pos)
    lookupNodeIndex = CreateLookupIndex(
      variable,
      isNodeIndex = true,
      FunctionInvocation(FunctionName(Labels.name)(pos), distinct = false, IndexedSeq(variable))(pos),
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    lookupRelIndex = CreateLookupIndex(
      variable,
      isNodeIndex = false,
      FunctionInvocation(FunctionName(Type.name)(pos), distinct = false, IndexedSeq(variable))(pos),
      name,
      ifExistsDo,
      options,
      use
    )(pos)
    fulltextNodeIndex = CreateFulltextNodeIndex(variable, labels, props, name, ifExistsDo, options, use)(pos)
    fulltextRelIndex = CreateFulltextRelationshipIndex(variable, types, props, name, ifExistsDo, options, use)(pos)
    textNodeIndex = CreateTextNodeIndex(variable, labelName, props, name, ifExistsDo, options, use)(pos)
    textRelIndex = CreateTextRelationshipIndex(variable, relType, props, name, ifExistsDo, options, use)(pos)
    pointNodeIndex = CreatePointNodeIndex(variable, labelName, props, name, ifExistsDo, options, use)(pos)
    pointRelIndex = CreatePointRelationshipIndex(variable, relType, props, name, ifExistsDo, options, use)(pos)
    vectorNodeIndex = CreateVectorNodeIndex(variable, labelName, props, name, ifExistsDo, options, use)(pos)
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
      vectorNodeIndex
    )
  } yield command

  def _dropIndex: Gen[DropIndexOnName] = for {
    name <- _nameAsEither
    ifExists <- boolean
    use <- option(_use)
  } yield DropIndexOnName(name, ifExists, use)(pos)

  def _createConstraint: Gen[SchemaCommand] = for {
    variable <- _variable
    labelName <- _labelName
    relTypeName <- _relTypeName
    props <- _listOfProperties
    prop <- _variableProperty
    propType <- _cypherTypeName
    name <- option(_nameAsEither)
    ifExistsDo <- _ifExistsDo
    containsOn <- boolean
    options <- _optionsMapAsEitherOrNone
    use <- option(_use)
    nodeKey = CreateNodeKeyConstraint(
      variable,
      labelName,
      props,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    relKey = CreateRelationshipKeyConstraint(
      variable,
      relTypeName,
      props,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    nodeUniqueness = CreateNodePropertyUniquenessConstraint(
      variable,
      labelName,
      Seq(prop),
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    compositeUniqueness = CreateNodePropertyUniquenessConstraint(
      variable,
      labelName,
      props,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    relUniqueness = CreateRelationshipPropertyUniquenessConstraint(
      variable,
      relTypeName,
      props,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    nodeExistence = CreateNodePropertyExistenceConstraint(
      variable,
      labelName,
      prop,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    relExistence = CreateRelationshipPropertyExistenceConstraint(
      variable,
      relTypeName,
      prop,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    nodePropType = CreateNodePropertyTypeConstraint(
      variable,
      labelName,
      prop,
      propType,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
      use
    )(pos)
    relPropType = CreateRelationshipPropertyTypeConstraint(
      variable,
      relTypeName,
      prop,
      propType,
      name,
      ifExistsDo,
      options,
      containsOn,
      ConstraintVersion2,
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
    name <- _nameAsEither
    ifExists <- boolean
    use <- option(_use)
  } yield DropConstraintOnName(name, ifExists, use)(pos)

  def _indexCommand: Gen[SchemaCommand] = oneOf(_createIndex, _dropIndex)

  def _constraintCommand: Gen[SchemaCommand] = oneOf(_createConstraint, _dropConstraint)

  def _schemaCommand: Gen[SchemaCommand] = oneOf(_indexCommand, _constraintCommand)

  // Administration commands
  // ----------------------------------

  def _nameAsEither: Gen[Either[String, Parameter]] = for {
    name <- _identifier
    param <- _stringParameter
    finalName <- oneOf(Left(name), Right(param))
  } yield finalName

  def _databaseName: Gen[DatabaseName] = for {
    namespacedName <- _namespacedName
    param <- _stringParameter
    finalName <- oneOf(namespacedName, ParameterName(param)(pos))
  } yield finalName

  def _databaseNameNoNamespace: Gen[DatabaseName] = for {
    name <- listOfN(1, _identifier)
    param <- _stringParameter
    finalName <- oneOf(NamespacedName(name)(pos), ParameterName(param)(pos))
  } yield finalName

  def _optionsMapAsEitherOrNone: Gen[Options] = for {
    map <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    param <- _mapParameter
    finalMap <- oneOf(OptionsMap(map), OptionsParam(param), NoOptions)
  } yield finalMap

  def _optionsForAlterDatabaseOrNone: Gen[Options] = for {
    map <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    finalMap <- oneOf(OptionsMap(map), NoOptions)
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
    finalMap <- oneOf(OptionsMap(map), OptionsParam(param))
  } yield finalMap

  def _optionalMapAsEither: Gen[Either[Map[String, Expression], Parameter]] = for {
    map <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    param <- _mapParameter
    finalMap <- oneOf(Left(map), Right(param))
  } yield finalMap

  def _listOfNameOfEither: Gen[List[Either[String, Parameter]]] = for {
    names <- oneOrMore(_nameAsEither)
  } yield names

  def _password: Gen[Expression] =
    oneOf(_sensitiveStringParameter, _sensitiveAutoStringParameter, _sensitiveStringLiteral)

  def _ifExistsDo: Gen[IfExistsDo] =
    oneOf(IfExistsReplace, IfExistsDoNothing, IfExistsThrowError, IfExistsInvalidSyntax)

  def _namespacedName: Gen[NamespacedName] = for {
    name <- listOfN(1, _identifier)
    namespace <- _identifier
    maybeNamespace <- option(namespace)
  } yield NamespacedName(name, maybeNamespace)(pos)

  def _topology: Gen[Topology] = for {
    primaries <- option(chooseNum[Int](1, Integer.MAX_VALUE))
    secondaries <-
      if (primaries.nonEmpty)
        option[Int](chooseNum[Int](0, Integer.MAX_VALUE))
      else
        some[Int](chooseNum[Int](0, Integer.MAX_VALUE))
  } yield Topology(primaries, secondaries)

  // User commands

  def _showUsers: Gen[ShowUsers] = for {
    yields <- _eitherYieldOrWhere
  } yield ShowUsers(yields)(pos)

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
    userName <- _nameAsEither
    isEncryptedPassword <- boolean
    password <- _password
    requirePasswordChange <- boolean
    suspended <- option(boolean)
    homeDatabase <- option(_setHomeDatabaseAction)
    ifExistsDo <- _ifExistsDo
    // requirePasswordChange is parsed as 'Some(true)' if omitted in query,
    // prettifier explicitly adds it so 'None' would be prettified and re-parsed to 'Some(true)'
    // hence the explicit 'Some(requirePasswordChange)'
  } yield CreateUser(userName, isEncryptedPassword, password, UserOptions(Some(requirePasswordChange), suspended, homeDatabase), ifExistsDo)(pos)

  def _renameUser: Gen[RenameUser] = for {
    fromUserName <- _nameAsEither
    toUserName <- _nameAsEither
    ifExists <- boolean
  } yield RenameUser(fromUserName, toUserName, ifExists)(pos)

  def _dropUser: Gen[DropUser] = for {
    userName <- _nameAsEither
    ifExists <- boolean
  } yield DropUser(userName, ifExists)(pos)

  def _alterUser: Gen[AlterUser] = for {
    userName <- _nameAsEither
    ifExists <- boolean
    password <- option(_password)
    requirePasswordChange <- option(boolean)
    isEncryptedPassword <- if (password.isEmpty) const(None) else some(boolean)
    suspended <- option(boolean)
    // All four are not allowed to be None and REMOVE HOME DATABASE is only valid by itself
    homeDatabase <-
      if (password.isEmpty && requirePasswordChange.isEmpty && suspended.isEmpty)
        oneOf(some(_setHomeDatabaseAction), some(RemoveHomeDatabaseAction))
      else option(_setHomeDatabaseAction)
  } yield AlterUser(userName, isEncryptedPassword, password, UserOptions(requirePasswordChange, suspended, homeDatabase), ifExists)(pos)

  def _setHomeDatabaseAction: Gen[SetHomeDatabaseAction] = _databaseName.map(db => SetHomeDatabaseAction(db))

  def _setOwnPassword: Gen[SetOwnPassword] = for {
    newPassword <- _password
    oldPassword <- _password
  } yield SetOwnPassword(newPassword, oldPassword)(pos)

  def _userCommand: Gen[AdministrationCommand] = oneOf(
    _showUsers,
    _showCurrentUser,
    _createUser,
    _renameUser,
    _dropUser,
    _alterUser,
    _setOwnPassword
  )

  // Role commands

  def _showRoles: Gen[ShowRoles] = for {
    withUsers <- boolean
    showAll <- boolean
    yields <- _eitherYieldOrWhere
  } yield ShowRoles(withUsers, showAll, yields)(pos)

  def _createRole: Gen[CreateRole] = for {
    roleName <- _nameAsEither
    fromRoleName <- option(_nameAsEither)
    ifExistsDo <- _ifExistsDo
  } yield CreateRole(roleName, fromRoleName, ifExistsDo)(pos)

  def _renameRole: Gen[RenameRole] = for {
    fromRoleName <- _nameAsEither
    toRoleName <- _nameAsEither
    ifExists <- boolean
  } yield RenameRole(fromRoleName, toRoleName, ifExists)(pos)

  def _dropRole: Gen[DropRole] = for {
    roleName <- _nameAsEither
    ifExists <- boolean
  } yield DropRole(roleName, ifExists)(pos)

  def _grantRole: Gen[GrantRolesToUsers] = for {
    roleNames <- _listOfNameOfEither
    userNames <- _listOfNameOfEither
  } yield GrantRolesToUsers(roleNames, userNames)(pos)

  def _revokeRole: Gen[RevokeRolesFromUsers] = for {
    roleNames <- _listOfNameOfEither
    userNames <- _listOfNameOfEither
  } yield RevokeRolesFromUsers(roleNames, userNames)(pos)

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

  def _dbmsAction: Gen[DbmsAction] = oneOf(
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
    AlterDatabaseAction,
    SetDatabaseAccessAction,
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
  )

  def _databaseAction: Gen[DatabaseAction] = oneOf(
    StartDatabaseAction,
    StopDatabaseAction,
    AllDatabaseAction,
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
        userNames <- _listOfNameOfEither
        qualifier <- frequency(7 -> userNames.map(UserQualifier(_)(pos)), 3 -> List(UserAllQualifier()(pos)))
      } yield qualifier
    } else {
      // All other dbms privileges have AllQualifier
      List(AllQualifier()(pos))
    }

  def _databaseQualifier(haveUserQualifier: Boolean): Gen[List[DatabasePrivilegeQualifier]] =
    if (haveUserQualifier) {
      for {
        userNames <- _listOfNameOfEither
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
    qualifier <- oneOf(
      PatternQualifier(multiQualifiers.map(LabelQualifier(_)(pos)), Some(variable), expression),
      PatternQualifier(List(LabelAllQualifier()(pos)), Some(variable), expression)
    )
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
      lzy(_signedOctIntLitOldSyntax),
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
      In(l, ListLiteral(Seq(r))(pos))(pos)
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
    : Gen[(List[GraphPrivilegeQualifier], Option[ActionResource])] =
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
    names <- _listOfNameOfEither
    showRole = ShowRolesPrivileges(names)(pos)
    showUser1 = ShowUsersPrivileges(names)(pos)
    showUser2 = ShowUserPrivileges(None)(pos)
    showAll = ShowAllPrivileges()(pos)
    scope <- oneOf(showRole, showUser1, showUser2, showAll)
    yields <- _eitherYieldOrWhere
  } yield ShowPrivileges(scope, yields)(pos)

  def _showPrivilegeCommands: Gen[ShowPrivilegeCommands] = for {
    names <- _listOfNameOfEither
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
    roleNames <- _listOfNameOfEither
    revokeType <- _revokeType
    immutable <- boolean
    dbmsGrant = GrantPrivilege.dbmsAction(dbmsAction, immutable, roleNames, qualifier)(pos)
    dbmsDeny = DenyPrivilege.dbmsAction(dbmsAction, immutable, roleNames, qualifier)(pos)
    dbmsRevoke = RevokePrivilege.dbmsAction(dbmsAction, immutable, roleNames, revokeType, qualifier)(pos)
    dbms <- oneOf(dbmsGrant, dbmsDeny, dbmsRevoke)
  } yield dbms

  def _databasePrivilege: Gen[PrivilegeCommand] = for {
    databaseAction <- _databaseAction
    dbNames <- oneOrMore(_databaseName)
    databaseScope <- oneOf(
      NamedDatabasesScope(dbNames)(pos),
      AllDatabasesScope()(pos),
      DefaultDatabaseScope()(pos),
      HomeDatabaseScope()(pos)
    )
    databaseQualifier <- _databaseQualifier(databaseAction.isInstanceOf[TransactionManagementAction])
    roleNames <- _listOfNameOfEither
    revokeType <- _revokeType
    immutable <- boolean
    databaseGrant =
      GrantPrivilege.databaseAction(databaseAction, immutable, databaseScope, roleNames, databaseQualifier)(pos)
    databaseDeny =
      DenyPrivilege.databaseAction(databaseAction, immutable, databaseScope, roleNames, databaseQualifier)(pos)
    databaseRevoke =
      RevokePrivilege.databaseAction(
        databaseAction,
        immutable,
        databaseScope,
        roleNames,
        revokeType,
        databaseQualifier
      )(pos)
    database <- oneOf(databaseGrant, databaseDeny, databaseRevoke)
  } yield database

  def _graphPrivilege: Gen[PrivilegeCommand] = for {
    graphAction <- _graphAction
    graphNames <- oneOrMore(_databaseName)
    graphScope <-
      oneOf(NamedGraphsScope(graphNames)(pos), AllGraphsScope()(pos), DefaultGraphScope()(pos), HomeGraphScope()(pos))
    (qualifier, maybeResource) <- _graphQualifierAndResource(graphAction)
    roleNames <- _listOfNameOfEither
    revokeType <- _revokeType
    immutable <- boolean
    graphGrant =
      GrantPrivilege.graphAction(graphAction, immutable, maybeResource, graphScope, qualifier, roleNames)(pos)
    graphDeny = DenyPrivilege.graphAction(graphAction, immutable, maybeResource, graphScope, qualifier, roleNames)(pos)
    graphRevoke =
      RevokePrivilege.graphAction(graphAction, immutable, maybeResource, graphScope, qualifier, roleNames, revokeType)(
        pos
      )
    graph <- oneOf(graphGrant, graphDeny, graphRevoke)
  } yield graph

  def _loadPrivilege: Gen[PrivilegeCommand] = for {
    (qualifier, action) <- _loadQualifierAndAction
    roleNames <- _listOfNameOfEither
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

  def _showDatabase: Gen[ShowDatabase] = for {
    dbName <- _databaseName
    scope <- oneOf(
      SingleNamedDatabaseScope(dbName)(pos),
      AllDatabasesScope()(pos),
      DefaultDatabaseScope()(pos),
      HomeDatabaseScope()(pos)
    )
    yields <- _eitherYieldOrWhere
  } yield ShowDatabase(scope, yields)(pos)

  def _createDatabase: Gen[CreateDatabase] = for {
    dbName <- _databaseNameNoNamespace
    ifExistsDo <- _ifExistsDo
    wait <- _waitUntilComplete
    options <- _optionsMapAsEitherOrNone
    topology <- option(_topology)
  } yield CreateDatabase(dbName, ifExistsDo, options, wait, topology)(pos)

  def _createCompositeDatabase: Gen[CreateCompositeDatabase] = for {
    dbName <- _databaseName
    ifExistsDo <- _ifExistsDo
    options <- _optionsMapAsEitherOrNone
    wait <- _waitUntilComplete
  } yield CreateCompositeDatabase(dbName, ifExistsDo, options, wait)(pos)

  def _dropDatabase: Gen[DropDatabase] = for {
    dbName <- _databaseName
    ifExists <- boolean
    composite <- boolean
    additionalAction <- Gen.oneOf(DumpData, DestroyData)
    wait <- _waitUntilComplete
  } yield DropDatabase(dbName, ifExists, composite, additionalAction, wait)(pos)

  def _alterDatabase: Gen[AlterDatabase] = for {
    dbName <- _databaseName
    ifExists <- boolean
    options <- _optionsForAlterDatabaseOrNone
    access <- option(_access)
    topology <- option(_topology)
    optionsToRemove <- _optionsToRemove(hasSetClause =
      access.nonEmpty || topology.nonEmpty || (!options.equals(NoOptions))
    )
    wait <- _waitUntilComplete
  } yield AlterDatabase(dbName, ifExists, access, topology, options, optionsToRemove, wait)(pos)

  def _startDatabase: Gen[StartDatabase] = for {
    dbName <- _databaseName
    wait <- _waitUntilComplete
  } yield StartDatabase(dbName, wait)(pos)

  def _stopDatabase: Gen[StopDatabase] = for {
    dbName <- _databaseName
    wait <- _waitUntilComplete
  } yield StopDatabase(dbName, wait)(pos)

  def _multiDatabaseCommand: Gen[AdministrationCommand] = oneOf(
    _showDatabase,
    _createDatabase,
    _createCompositeDatabase,
    _dropDatabase,
    _alterDatabase,
    _startDatabase,
    _stopDatabase
  )

  def _access: Gen[Access] = for {
    access <- oneOf(ReadOnlyAccess, ReadWriteAccess)
  } yield access

  def _waitUntilComplete: Gen[WaitUntilComplete] = for {
    timeout <- posNum[Long]
    wait <- oneOf(NoWait, IndefiniteWait, TimeoutAfter(timeout))
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
    username <- _nameAsEither
    password <- _password
    driverSettings <- option(_optionalMapAsEither)
    properties <- option(_optionalMapAsEither)
  } yield CreateRemoteDatabaseAlias(aliasName, targetName, ifExistsDo, url, username, password, driverSettings, properties)(pos)

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
    username <- option(_nameAsEither)
    password <- option(_password)
    // All four are not allowed to be None
    driverSettings <-
      if (url.isEmpty && username.isEmpty && password.isEmpty)
        some(_optionalMapAsEither)
      else
        option(_optionalMapAsEither)
    properties <- option(_optionalMapAsEither)
  } yield AlterRemoteDatabaseAlias(aliasName, targetName, ifExists, url, username, password, driverSettings, properties)(pos)

  def _showAliases: Gen[ShowAliases] = for {
    dbName <- option(_databaseName)
    yields <- _eitherYieldOrWhere
  } yield ShowAliases(dbName, yields)(pos)

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
      oneOf(_userCommand, _roleCommand, _privilegeCommand, _multiDatabaseCommand, _aliasCommands, _serverCommand)
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
