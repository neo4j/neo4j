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

import org.neo4j.common.EntityType
import org.neo4j.cypher.QueryPlanTestSupport.StubExecutionPlan
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AllDatabasesScope
import org.neo4j.cypher.internal.ast.AllFunctions
import org.neo4j.cypher.internal.ast.AllIndexes
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.FileResource
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IndefiniteWait
import org.neo4j.cypher.internal.ast.KeyConstraints
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LoadAllDataAction
import org.neo4j.cypher.internal.ast.LoadAllQualifier
import org.neo4j.cypher.internal.ast.LoadCidrAction
import org.neo4j.cypher.internal.ast.LoadCidrQualifier
import org.neo4j.cypher.internal.ast.LoadUrlAction
import org.neo4j.cypher.internal.ast.LoadUrlQualifier
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.NamespacedName
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.NodePropTypeConstraints
import org.neo4j.cypher.internal.ast.NodeUniqueConstraints
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.ProcedureAllQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.PropTypeConstraints
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.RelKeyConstraints
import org.neo4j.cypher.internal.ast.RelPropTypeConstraints
import org.neo4j.cypher.internal.ast.RelUniqueConstraints
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.TraverseAction
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.ast.User
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserDefinedFunctions
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.ValidSyntax
import org.neo4j.cypher.internal.ast.VectorIndexes
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.ArgumentAsc
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AllScope
import org.neo4j.cypher.internal.logical.plans.AllowedNonAdministrationCommands
import org.neo4j.cypher.internal.logical.plans.AlterDatabase
import org.neo4j.cypher.internal.logical.plans.AlterLocalDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.AlterServer
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ArgumentTracker
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDatabaseAction
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActions
import org.neo4j.cypher.internal.logical.plans.AssertAllowedDbmsActionsOrSelf
import org.neo4j.cypher.internal.logical.plans.AssertManagementActionNotBlocked
import org.neo4j.cypher.internal.logical.plans.AssertNotBlockedDropAlias
import org.neo4j.cypher.internal.logical.plans.AssertNotBlockedRemoteAliasManagement
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.CopyRolePrivileges
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.CreateConstraint
import org.neo4j.cypher.internal.logical.plans.CreateDatabase
import org.neo4j.cypher.internal.logical.plans.CreateFulltextIndex
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateLocalDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.CreateLookupIndex
import org.neo4j.cypher.internal.logical.plans.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.CreateRole
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.DeallocateServer
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DenyDatabaseAction
import org.neo4j.cypher.internal.logical.plans.DenyDbmsAction
import org.neo4j.cypher.internal.logical.plans.DenyGraphAction
import org.neo4j.cypher.internal.logical.plans.DenyLoadAction
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForConstraint
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForFulltextIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExistsForLookupIndex
import org.neo4j.cypher.internal.logical.plans.DoNothingIfNotExists
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropDatabase
import org.neo4j.cypher.internal.logical.plans.DropDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.DropRole
import org.neo4j.cypher.internal.logical.plans.DropServer
import org.neo4j.cypher.internal.logical.plans.DropUser
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.EnableServer
import org.neo4j.cypher.internal.logical.plans.EnsureNodeExists
import org.neo4j.cypher.internal.logical.plans.EnsureValidNonSystemDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureValidNumberOfDatabases
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.GrantDatabaseAction
import org.neo4j.cypher.internal.logical.plans.GrantDbmsAction
import org.neo4j.cypher.internal.logical.plans.GrantGraphAction
import org.neo4j.cypher.internal.logical.plans.GrantLoadAction
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.HomeScope
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeek.partitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeek.partitionedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeek.relationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NFABuilder
import org.neo4j.cypher.internal.logical.plans.NamedScope
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeKey
import org.neo4j.cypher.internal.logical.plans.NodePropertyExistence
import org.neo4j.cypher.internal.logical.plans.NodePropertyType
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueness
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedIntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PreserveOrder
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipKey
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyExistence
import org.neo4j.cypher.internal.logical.plans.RelationshipPropertyType
import org.neo4j.cypher.internal.logical.plans.RelationshipUniqueness
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RenameRole
import org.neo4j.cypher.internal.logical.plans.RenameServer
import org.neo4j.cypher.internal.logical.plans.RenameUser
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RequireRole
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeGraphAction
import org.neo4j.cypher.internal.logical.plans.RevokeLoadAction
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetOwnPassword
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.ShowAliases
import org.neo4j.cypher.internal.logical.plans.ShowConstraints
import org.neo4j.cypher.internal.logical.plans.ShowCurrentUser
import org.neo4j.cypher.internal.logical.plans.ShowDatabase
import org.neo4j.cypher.internal.logical.plans.ShowFunctions
import org.neo4j.cypher.internal.logical.plans.ShowIndexes
import org.neo4j.cypher.internal.logical.plans.ShowPrivilegeCommands
import org.neo4j.cypher.internal.logical.plans.ShowPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowProcedures
import org.neo4j.cypher.internal.logical.plans.ShowRoles
import org.neo4j.cypher.internal.logical.plans.ShowServers
import org.neo4j.cypher.internal.logical.plans.ShowSettings
import org.neo4j.cypher.internal.logical.plans.ShowSupportedPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowTransactions
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedNodeScan
import org.neo4j.cypher.internal.logical.plans.SimulatedSelection
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StartDatabase
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Selector
import org.neo4j.cypher.internal.logical.plans.StopDatabase
import org.neo4j.cypher.internal.logical.plans.TerminateTransactions
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.WaitForCompletion
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.anonVar
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.details
import org.neo4j.cypher.internal.plandescription.LogicalPlan2PlanDescriptionTest.planDescription
import org.neo4j.cypher.internal.plandescription.asPrettyString.PrettyStringInterpolator
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.ExactSize
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound.Unlimited
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.values.storable.Values.stringValue
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable
import scala.collection.immutable.ListSet
import scala.language.implicitConversions

object LogicalPlan2PlanDescriptionTest {

  def anonVar(number: String) = s"anon_$number"

  def details(infos: Seq[String]): Details = Details(infos.map(asPrettyString.raw))

  def details(info: String): Details = Details(asPrettyString.raw(info))

  def planDescription(
    id: Id,
    name: String,
    children: Children,
    arguments: Seq[Argument] = Seq.empty,
    variables: Set[String] = Set.empty
  ): PlanDescriptionImpl = PlanDescriptionImpl(id, name, children, arguments, variables.map(asPrettyString.raw))
}

class LogicalPlan2PlanDescriptionTest extends CypherFunSuite with TableDrivenPropertyChecks
    with AstConstructionTestSupport {

  private val RUNTIME_VERSION = RuntimeVersion.currentVersion
  private val PLANNER_VERSION = PlannerVersion.currentVersion

  implicit val idGen: IdGen = new SequentialIdGen()
  private val effectiveCardinalities = new EffectiveCardinalities
  private val providedOrders = new ProvidedOrders
  private val id = Id.INVALID_ID

  implicit def lift(amount: Double): EffectiveCardinality = EffectiveCardinality(amount)

  private def attach[P <: LogicalPlan](
    plan: P,
    effectiveCardinality: EffectiveCardinality,
    providedOrder: ProvidedOrder = ProvidedOrder.empty
  ): P = {
    effectiveCardinalities.set(plan.id, effectiveCardinality)
    providedOrders.set(plan.id, providedOrder)
    plan
  }

  private val privLhsLP = attach(AssertAllowedDbmsActions(ShowUserAction), 2.0, providedOrder = ProvidedOrder.empty)

  private val lhsLP =
    attach(AllNodesScan(varFor("a"), Set.empty), EffectiveCardinality(2.0, Some(10.0)), ProvidedOrder.empty)

  private val lhsRelLP = attach(
    UndirectedAllRelationshipsScan(varFor("r"), varFor("a"), varFor("b"), Set.empty),
    EffectiveCardinality(2.0, Some(10.0)),
    ProvidedOrder.empty
  )

  private val lhsPD =
    PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(details("a"), EstimatedRows(2, Some(10))), Set(pretty"a"))

  private val lhsRelPD = PlanDescriptionImpl(
    id,
    "UndirectedAllRelationshipsScan",
    NoChildren,
    Seq(details("(a)-[r]-(b)"), EstimatedRows(2, Some(10))),
    Set(pretty"r", pretty"a", pretty"b")
  )

  private val rhsLP = attach(AllNodesScan(varFor("b"), Set.empty), 2.0, providedOrder = ProvidedOrder.empty)

  private val rhsPD =
    PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(details("b"), EstimatedRows(2, Some(10))), Set(pretty"b"))

  test("Validate all arguments") {
    assertGood(
      attach(
        AllNodesScan(varFor("a"), Set.empty),
        EffectiveCardinality(1.0, Some(15.0)),
        ProvidedOrder.asc(varFor("a"))
      ),
      planDescription(
        id,
        "AllNodesScan",
        NoChildren,
        Seq(
          details("a"),
          EstimatedRows(1, Some(15)),
          Order(asPrettyString.raw("a ASC")),
          RUNTIME_VERSION,
          Planner("COST"),
          PlannerImpl("IDP"),
          PLANNER_VERSION
        ),
        Set("a")
      ),
      validateAllArgs = true
    )

    assertGood(
      attach(
        AllNodesScan(varFor("  UNNAMED111"), Set.empty),
        EffectiveCardinality(1.0, Some(10.0)),
        ProvidedOrder.asc(varFor("  UNNAMED111"))
      ),
      planDescription(
        id,
        "AllNodesScan",
        NoChildren,
        Seq(
          details(anonVar("111")),
          EstimatedRows(1, Some(10)),
          Order(asPrettyString.raw(s"${anonVar("111")} ASC")),
          RUNTIME_VERSION,
          Planner("COST"),
          PlannerImpl("IDP"),
          PLANNER_VERSION
        ),
        Set(anonVar("111"))
      ),
      validateAllArgs = true
    )

    assertGood(
      attach(
        Input(Seq(varFor("n1"), varFor("n2")), Seq(varFor("r")), Seq(varFor("v1"), varFor("v2")), nullable = false),
        EffectiveCardinality(42.3, Some(132))
      ),
      planDescription(
        id,
        "Input",
        NoChildren,
        Seq(
          details(Seq("n1", "n2", "r", "v1", "v2")),
          EstimatedRows(42.3, Some(132)),
          RUNTIME_VERSION,
          Planner("COST"),
          PlannerImpl("IDP"),
          PLANNER_VERSION
        ),
        Set("n1", "n2", "r", "v1", "v2")
      ),
      validateAllArgs = true
    )
  }

  // Leaf Plans
  test("AllNodesScan") {
    assertGood(
      attach(AllNodesScan(varFor("a"), Set.empty), 1.0, providedOrder = ProvidedOrder.asc(varFor("a"))),
      planDescription(id, "AllNodesScan", NoChildren, Seq(details("a"), Order(asPrettyString.raw("a ASC"))), Set("a"))
    )

    assertGood(
      attach(
        AllNodesScan(varFor("  UNNAMED111"), Set.empty),
        1.0,
        providedOrder = ProvidedOrder.asc(varFor("  UNNAMED111"))
      ),
      planDescription(
        id,
        "AllNodesScan",
        NoChildren,
        Seq(details(anonVar("111")), Order(asPrettyString.raw(s"${anonVar("111")} ASC"))),
        Set(anonVar("111"))
      )
    )

    assertGood(
      attach(
        AllNodesScan(varFor("b"), Set.empty),
        42.0,
        providedOrder = ProvidedOrder.asc(varFor("b")).desc(prop("b", "foo"))
      ),
      planDescription(
        id,
        "AllNodesScan",
        NoChildren,
        Seq(details("b"), Order(asPrettyString.raw("b ASC, b.foo DESC"))),
        Set("b")
      )
    )
  }

  test("PartitionedAllNodesScan") {
    assertGood(
      attach(PartitionedAllNodesScan(varFor("a"), Set.empty), 1.0, providedOrder = ProvidedOrder.asc(varFor("a"))),
      planDescription(
        id,
        "PartitionedAllNodesScan",
        NoChildren,
        Seq(details("a"), Order(asPrettyString.raw("a ASC"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        PartitionedAllNodesScan(varFor("  UNNAMED111"), Set.empty),
        1.0,
        providedOrder = ProvidedOrder.asc(varFor("  UNNAMED111"))
      ),
      planDescription(
        id,
        "PartitionedAllNodesScan",
        NoChildren,
        Seq(details(anonVar("111")), Order(asPrettyString.raw(s"${anonVar("111")} ASC"))),
        Set(anonVar("111"))
      )
    )

    assertGood(
      attach(
        PartitionedAllNodesScan(varFor("b"), Set.empty),
        42.0,
        providedOrder = ProvidedOrder.asc(varFor("b")).desc(prop("b", "foo"))
      ),
      planDescription(
        id,
        "PartitionedAllNodesScan",
        NoChildren,
        Seq(details("b"), Order(asPrettyString.raw("b ASC, b.foo DESC"))),
        Set("b")
      )
    )
  }

  test("NodeByLabelScan") {
    assertGood(
      attach(NodeByLabelScan(varFor("node"), label("X"), Set.empty, IndexOrderNone), 33.0),
      planDescription(id, "NodeByLabelScan", NoChildren, Seq(details("node:X")), Set("node"))
    )

    assertGood(
      attach(NodeByLabelScan(varFor("  UNNAMED123"), label("X"), Set.empty, IndexOrderNone), 33.0),
      planDescription(id, "NodeByLabelScan", NoChildren, Seq(details(s"${anonVar("123")}:X")), Set(anonVar("123")))
    )
  }

  test("PartitionedNodeByLabelScan") {
    assertGood(
      attach(PartitionedNodeByLabelScan(varFor("node"), label("X"), Set.empty), 33.0),
      planDescription(id, "PartitionedNodeByLabelScan", NoChildren, Seq(details("node:X")), Set("node"))
    )

    assertGood(
      attach(PartitionedNodeByLabelScan(varFor("  UNNAMED123"), label("X"), Set.empty), 33.0),
      planDescription(
        id,
        "PartitionedNodeByLabelScan",
        NoChildren,
        Seq(details(s"${anonVar("123")}:X")),
        Set(anonVar("123"))
      )
    )
  }

  test("UnionNodeByLabelScan") {
    assertGood(
      attach(
        UnionNodeByLabelsScan(varFor("node"), Seq(label("X"), label("Y"), label("Z")), Set.empty, IndexOrderNone),
        33.0
      ),
      planDescription(id, "UnionNodeByLabelsScan", NoChildren, Seq(details("node:X|Y|Z")), Set("node"))
    )

    assertGood(
      attach(
        UnionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(
        id,
        "UnionNodeByLabelsScan",
        NoChildren,
        Seq(details(s"${anonVar("123")}:X|Y|Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("PartitionedUnionNodeByLabelScan") {
    assertGood(
      attach(
        PartitionedUnionNodeByLabelsScan(varFor("node"), Seq(label("X"), label("Y"), label("Z")), Set.empty),
        33.0
      ),
      planDescription(id, "PartitionedUnionNodeByLabelsScan", NoChildren, Seq(details("node:X|Y|Z")), Set("node"))
    )

    assertGood(
      attach(
        PartitionedUnionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "PartitionedUnionNodeByLabelsScan",
        NoChildren,
        Seq(details(s"${anonVar("123")}:X|Y|Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("UnionRelationshipTypesScan") {
    assertGood(
      attach(
        DirectedUnionRelationshipTypesScan(
          varFor("r"),
          varFor("x"),
          Seq(relType("A"), relType("B"), relType("C")),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedUnionRelationshipTypesScan",
        NoChildren,
        Seq(details("(x)-[r:A|B|C]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        UndirectedUnionRelationshipTypesScan(
          varFor("r"),
          varFor("x"),
          Seq(relType("A"), relType("B"), relType("C")),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedUnionRelationshipTypesScan",
        NoChildren,
        Seq(details("(x)-[r:A|B|C]-(y)")),
        Set("r", "x", "y")
      )
    )
  }

  test("PartitionedUnionRelationshipTypesScan") {
    assertGood(
      attach(
        PartitionedDirectedUnionRelationshipTypesScan(
          varFor("r"),
          varFor("x"),
          Seq(relType("A"), relType("B"), relType("C")),
          varFor("y"),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedDirectedUnionRelationshipTypesScan",
        NoChildren,
        Seq(details("(x)-[r:A|B|C]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        PartitionedUndirectedUnionRelationshipTypesScan(
          varFor("r"),
          varFor("x"),
          Seq(relType("A"), relType("B"), relType("C")),
          varFor("y"),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedUndirectedUnionRelationshipTypesScan",
        NoChildren,
        Seq(details("(x)-[r:A|B|C]-(y)")),
        Set("r", "x", "y")
      )
    )
  }

  test("IntersectionNodeByLabelScan") {
    assertGood(
      attach(
        IntersectionNodeByLabelsScan(
          varFor("node"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(id, "IntersectionNodeByLabelsScan", NoChildren, Seq(details("node:X&Y&Z")), Set("node"))
    )

    assertGood(
      attach(
        IntersectionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty,
          IndexOrderNone
        ),
        33.0
      ),
      planDescription(
        id,
        "IntersectionNodeByLabelsScan",
        NoChildren,
        Seq(details(s"${anonVar("123")}:X&Y&Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("PartitionedIntersectionNodeByLabelScan") {
    assertGood(
      attach(
        PartitionedIntersectionNodeByLabelsScan(
          varFor("node"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "PartitionedIntersectionNodeByLabelsScan",
        NoChildren,
        Seq(details("node:X&Y&Z")),
        Set("node")
      )
    )

    assertGood(
      attach(
        PartitionedIntersectionNodeByLabelsScan(
          varFor("  UNNAMED123"),
          Seq(label("X"), label("Y"), label("Z")),
          Set.empty
        ),
        33.0
      ),
      planDescription(
        id,
        "PartitionedIntersectionNodeByLabelsScan",
        NoChildren,
        Seq(details(s"${anonVar("123")}:X&Y&Z")),
        Set(anonVar("123"))
      )
    )
  }

  test("NodeByIdSeek") {
    assertGood(
      attach(
        NodeByIdSeek(varFor("node"), ManySeekableArgs(ListLiteral(Seq(number("1"), number("32")))(pos)), Set.empty),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", NoChildren, Seq(details("node WHERE id(node) IN [1, 32]")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("node"),
          ManySeekableArgs(AutoExtractedParameter("autolist_0", CTList(CTAny))(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", NoChildren, Seq(details("node WHERE id(node) IN $autolist_0")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("node"),
          ManySeekableArgs(ExplicitParameter("listParam", CTList(CTAny), ExactSize(5))(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", NoChildren, Seq(details("node WHERE id(node) IN $listParam")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("node"),
          SingleSeekableArg(AutoExtractedParameter("autoint_0", CTInteger)(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(id, "NodeByIdSeek", NoChildren, Seq(details("node WHERE id(node) = $autoint_0")), Set("node"))
    )

    assertGood(
      attach(
        NodeByIdSeek(
          varFor("  UNNAMED11"),
          ManySeekableArgs(ListLiteral(Seq(number("1"), number("32")))(pos)),
          Set.empty
        ),
        333.0
      ),
      planDescription(
        id,
        "NodeByIdSeek",
        NoChildren,
        Seq(details(s"${anonVar("11")} WHERE id(${anonVar("11")}) IN [1, 32]")),
        Set(anonVar("11"))
      )
    )
  }

  test("NodeByElementIdSeek") {
    assertGood(
      attach(
        NodeByElementIdSeek(varFor("node"), ManySeekableArgs(listOfString("some-id", "other-id")), Set.empty),
        333.0
      ),
      planDescription(
        id,
        "NodeByElementIdSeek",
        NoChildren,
        Seq(details("node WHERE elementId(node) IN [\"some-id\", \"other-id\"]")),
        Set("node")
      )
    )

    assertGood(
      attach(
        NodeByElementIdSeek(
          varFor("node"),
          ManySeekableArgs(autoParameter("autolist_0", CTList(CTAny))),
          Set.empty
        ),
        333.0
      ),
      planDescription(
        id,
        "NodeByElementIdSeek",
        NoChildren,
        Seq(details("node WHERE elementId(node) IN $autolist_0")),
        Set("node")
      )
    )

    assertGood(
      attach(
        NodeByElementIdSeek(varFor("node"), SingleSeekableArg(stringLiteral("some-id")), Set.empty),
        333.0
      ),
      planDescription(
        id,
        "NodeByElementIdSeek",
        NoChildren,
        Seq(details("node WHERE elementId(node) = \"some-id\"")),
        Set("node")
      )
    )
  }

  test("NodeIndexSeek") {
    assertGood(
      attach(nodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL, cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop,Foo)"), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop,Foo)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "NodeIndexScan",
        NoChildren,
        Seq(details(
          "RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL, cache[x.Prop], cache[x.Foo]"
        )),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres')"), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres')", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\", cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres')", unique = true), 23.0),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        NoChildren,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')"), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')", unique = true), 23.0),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        NoChildren,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop > 9)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop > 9")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop < 9)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop < 9")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(9 <= Prop <= 11)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop >= 9 AND Prop <= 11")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop STARTS WITH 'Foo')"), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop STARTS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop STARTS WITH 'Foo')", indexType = IndexType.TEXT), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details("TEXT INDEX x:Label(Prop) WHERE Prop STARTS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop STARTS WITH 'Foo')", indexType = IndexType.FULLTEXT), 23.0),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details("FULLTEXT INDEX x:Label(Prop) WHERE Prop STARTS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop ENDS WITH 'Foo')"), 23.0),
      planDescription(
        id,
        "NodeIndexEndsWithScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop ENDS WITH \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop CONTAINS 'Foo')"), 23.0),
      planDescription(
        id,
        "NodeIndexContainsScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop CONTAINS \"Foo\"")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 10,Foo = 12)"), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop = 10 AND Foo = 12")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop = 10,Foo = 12)", unique = true), 23.0),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        NoChildren,
        Seq(details("UNIQUE x:Label(Prop, Foo) WHERE Prop = 10 AND Foo = 12")),
        Set("x")
      )
    )

    assertGood(
      attach(nodeIndexSeek("x:Label(Prop > 10,Foo)", indexType = IndexType.RANGE), 23.0),
      planDescription(
        id,
        "NodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop > 10 AND Foo IS NOT NULL")),
        Set("x")
      )
    )

    // This is ManyQueryExpression with only a single expression. That is possible to get, but the test utility IndexSeek cannot create those.
    assertGood(
      attach(
        NodeUniqueIndexSeek(
          varFor("x"),
          LabelToken("Label", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          ManyQueryExpression(ListLiteral(Seq(stringLiteral("Andres")))(pos)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE
        ),
        95.0
      ),
      planDescription(
        id,
        "NodeUniqueIndexSeek",
        NoChildren,
        Seq(details("UNIQUE x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(
        NodeIndexSeek(
          varFor("x"),
          LabelToken("Label", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(
            FunctionInvocation(
              MapExpression(Seq(
                (key("x"), number("1")),
                (key("y"), number("2")),
                (key("crs"), stringLiteral("cartesian"))
              ))(pos),
              FunctionName(Point.name)(pos)
            ),
            number("10"),
            inclusive = true
          ))(pos)),
          Set.empty,
          IndexOrderNone,
          IndexType.POINT,
          supportPartitionedScan = false
        ),
        95.0
      ),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details("POINT INDEX x:Label(Prop) WHERE point.distance(Prop, point(1, 2, \"cartesian\")) <= 10")),
        Set("x")
      )
    )

    assertGood(
      attach(
        NodeIndexSeek(
          varFor("x"),
          LabelToken("Label", LabelId(0)),
          Seq(IndexedProperty(PropertyKeyToken("Prop", PropertyKeyId(0)), DoNotGetValue, NODE_TYPE)),
          RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(PointBoundingBoxRange(
            FunctionInvocation(
              MapExpression(Seq(
                (key("x"), number("0")),
                (key("y"), number("0")),
                (key("crs"), stringLiteral("cartesian"))
              ))(pos),
              FunctionName(Point.name)(pos)
            ),
            FunctionInvocation(
              MapExpression(Seq(
                (key("x"), number("10")),
                (key("y"), number("10")),
                (key("crs"), stringLiteral("cartesian"))
              ))(pos),
              FunctionName(Point.name)(pos)
            )
          ))(pos)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        95.0
      ),
      planDescription(
        id,
        "NodeIndexSeekByRange",
        NoChildren,
        Seq(details(
          "RANGE INDEX x:Label(Prop) WHERE point.withinBBox(Prop, point(0, 0, \"cartesian\"), point(10, 10, \"cartesian\"))"
        )),
        Set("x")
      )
    )
  }

  test("PartitionedNodeIndexSeek") {
    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IS NOT NULL, cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop,Foo)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop,Foo)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexScan",
        NoChildren,
        Seq(details(
          "RANGE INDEX x:Label(Prop, Foo) WHERE Prop IS NOT NULL AND Foo IS NOT NULL, cache[x.Prop], cache[x.Foo]"
        )),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop = 'Andres')"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\"")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop = 'Andres')", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop = \"Andres\", cache[x.Prop]")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop = 'Andres' OR 'Pontus')"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop IN [\"Andres\", \"Pontus\"]")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop > 9)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop > 9")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(Prop < 9)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop < 9")),
        Set("x")
      )
    )

    assertGood(
      attach(partitionedNodeIndexSeek("x:Label(9 <= Prop <= 11)"), 23.0),
      planDescription(
        id,
        "PartitionedNodeIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX x:Label(Prop) WHERE Prop >= 9 AND Prop <= 11")),
        Set("x")
      )
    )
  }

  test("RelationshipIndexSeek") {
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop)]->(y)"), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop)]-(y)"), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop CONTAINS 'Foo')]->(y)"), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexContainsScan",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop CONTAINS \"Foo\"")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop CONTAINS 'Foo')]-(y)"), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexContainsScan",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop CONTAINS \"Foo\"")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop ENDS WITH 'Foo')]->(y)", indexType = IndexType.TEXT), 23.0),
      planDescription(
        id,
        "DirectedRelationshipIndexEndsWithScan",
        NoChildren,
        Seq(details("TEXT INDEX (x)-[r:R(Prop)]->(y) WHERE Prop ENDS WITH \"Foo\"")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop ENDS WITH 'Foo')]-(y)"), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipIndexEndsWithScan",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop ENDS WITH \"Foo\"")),
        Set("r", "x", "y")
      )
    )
  }

  test("PartitionedRelationshipIndexSeek") {
    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop)]->(y)"), 23.0),
      planDescription(
        id,
        "PartitionedDirectedRelationshipIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop)]-(y)"), 23.0),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipIndexScan",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop IS NOT NULL")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedDirectedRelationshipIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )
    assertGood(
      attach(partitionedRelationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue), 23.0),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )
  }

  test("RelationshipUniqueIndexSeek") {
    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "DirectedRelationshipUniqueIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(1 < Prop < 123)]->(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "DirectedRelationshipUniqueIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop > 1 AND Prop < 123, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]->(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "DirectedRelationshipUniqueIndexSeek(Locking)",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]->(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      ),
      readOnly = false
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipUniqueIndexSeek",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(1 < Prop < 123)]-(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipUniqueIndexSeekByRange",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop > 1 AND Prop < 123, cache[r.Prop]")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(relationshipIndexSeek("(x)-[r:R(Prop = 42)]-(y)", getValue = _ => GetValue, unique = true), 23.0),
      planDescription(
        id,
        "UndirectedRelationshipUniqueIndexSeek(Locking)",
        NoChildren,
        Seq(details("RANGE INDEX (x)-[r:R(Prop)]-(y) WHERE Prop = 42, cache[r.Prop]")),
        Set("r", "x", "y")
      ),
      readOnly = false
    )
  }

  test("AllRelationshipsScan") {
    assertGood(
      attach(
        DirectedAllRelationshipsScan(varFor("r"), varFor("x"), varFor("y"), Set.empty),
        23.0
      ),
      planDescription(
        id,
        "DirectedAllRelationshipsScan",
        NoChildren,
        Seq(details("(x)-[r]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        UndirectedAllRelationshipsScan(varFor("r"), varFor("x"), varFor("y"), Set.empty),
        23.0
      ),
      planDescription(
        id,
        "UndirectedAllRelationshipsScan",
        NoChildren,
        Seq(details("(x)-[r]-(y)")),
        Set("r", "x", "y")
      )
    )
  }

  test("RelationshipTypeScan") {
    assertGood(
      attach(
        DirectedRelationshipTypeScan(
          varFor("r"),
          varFor("x"),
          RelTypeName("R")(InputPosition.NONE),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "DirectedRelationshipTypeScan",
        NoChildren,
        Seq(details("(x)-[r:R]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipTypeScan(
          varFor("r"),
          varFor("x"),
          RelTypeName("R")(InputPosition.NONE),
          varFor("y"),
          Set.empty,
          IndexOrderNone
        ),
        23.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipTypeScan",
        NoChildren,
        Seq(details("(x)-[r:R]-(y)")),
        Set("r", "x", "y")
      )
    )
  }

  test("PartitionedRelationshipTypeScan") {
    assertGood(
      attach(
        PartitionedDirectedRelationshipTypeScan(
          varFor("r"),
          varFor("x"),
          RelTypeName("R")(InputPosition.NONE),
          varFor("y"),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedDirectedRelationshipTypeScan",
        NoChildren,
        Seq(details("(x)-[r:R]->(y)")),
        Set("r", "x", "y")
      )
    )

    assertGood(
      attach(
        PartitionedUndirectedRelationshipTypeScan(
          varFor("r"),
          varFor("x"),
          RelTypeName("R")(InputPosition.NONE),
          varFor("y"),
          Set.empty
        ),
        23.0
      ),
      planDescription(
        id,
        "PartitionedUndirectedRelationshipTypeScan",
        NoChildren,
        Seq(details("(x)-[r:R]-(y)")),
        Set("r", "x", "y")
      )
    )
  }

  test("MultiNodeIndexSeek") {
    assertGood(
      attach(
        MultiNodeIndexSeek(Seq(
          nodeIndexSeek("x:Label(Prop = 10,Foo = 12)", unique = true).asInstanceOf[NodeIndexSeekLeafPlan],
          nodeIndexSeek("y:Label(Prop = 12)", unique = false).asInstanceOf[NodeIndexSeekLeafPlan]
        )),
        230.0
      ),
      planDescription(
        id,
        "MultiNodeIndexSeek",
        NoChildren,
        Seq(details(Seq(
          "UNIQUE x:Label(Prop, Foo) WHERE Prop = 10 AND Foo = 12",
          "RANGE INDEX y:Label(Prop) WHERE Prop = 12"
        ))),
        Set("x", "y")
      )
    )
  }

  test("ProduceResult") {
    assertGood(
      attach(ProduceResult(lhsLP, Seq(varFor("a"), varFor("b"), varFor("c\nd"))), 12.0),
      planDescription(id, "ProduceResults", SingleChild(lhsPD), Seq(details(Seq("a", "b", "`c d`"))), Set("a"))
    )
  }

  test("Argument") {
    assertGood(
      attach(plans.Argument(Set.empty), 95.0),
      planDescription(id, "EmptyRow", NoChildren, Seq.empty, Set.empty)
    )

    assertGood(
      attach(plans.Argument(Set(varFor("a"), varFor("b"))), 95.0),
      planDescription(id, "Argument", NoChildren, Seq(details("a, b")), Set("a", "b"))
    )
  }

  test("RelationshipByIdSeek") {
    assertGood(
      attach(
        DirectedRelationshipByIdSeek(varFor("r"), SingleSeekableArg(number("1")), varFor("a"), varFor("b"), Set.empty),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE id(r) = 1")),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByIdSeek(
          varFor("r"),
          SingleSeekableArg(number("1")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE id(r) = 1")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByIdSeek(
          varFor("r"),
          ManySeekableArgs(ListLiteral(Seq(number("1")))(pos)),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE id(r) = 1")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByIdSeek(
          varFor("r"),
          ManySeekableArgs(ListLiteral(Seq(number("1"), number("2")))(pos)),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE id(r) IN [1, 2]")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByIdSeek(
          varFor("r"),
          SingleSeekableArg(number("1")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByIdSeek",
        NoChildren,
        Seq(details("(a)-[r]-(b) WHERE id(r) = 1")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByIdSeek(
          varFor("  UNNAMED2"),
          SingleSeekableArg(number("1")),
          varFor("a"),
          varFor("  UNNAMED32"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByIdSeek",
        NoChildren,
        Seq(details(s"(a)-[${anonVar("2")}]-(${anonVar("32")}) WHERE id(${anonVar("2")}) = 1")),
        Set(anonVar("2"), "a", anonVar("32"), "x")
      )
    )
  }

  test("RelationshipByElementIdSeek") {
    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("b"),
          Set.empty
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          ManySeekableArgs(listOfString("some-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        DirectedRelationshipByElementIdSeek(
          varFor("r"),
          ManySeekableArgs(listOfString("some-id", "other-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "DirectedRelationshipByElementIdSeek",
        NoChildren,
        Seq(details("(a)-[r]->(b) WHERE elementId(r) IN [\"some-id\", \"other-id\"]")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByElementIdSeek(
          varFor("r"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("b"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByElementIdSeek",
        NoChildren,
        Seq(details("(a)-[r]-(b) WHERE elementId(r) = \"some-id\"")),
        Set("r", "a", "b", "x")
      )
    )

    assertGood(
      attach(
        UndirectedRelationshipByElementIdSeek(
          varFor("  UNNAMED2"),
          SingleSeekableArg(stringLiteral("some-id")),
          varFor("a"),
          varFor("  UNNAMED32"),
          Set(varFor("x"))
        ),
        70.0
      ),
      planDescription(
        id,
        "UndirectedRelationshipByElementIdSeek",
        NoChildren,
        Seq(details(s"(a)-[${anonVar("2")}]-(${anonVar("32")}) WHERE elementId(${anonVar("2")}) = \"some-id\"")),
        Set(anonVar("2"), "a", anonVar("32"), "x")
      )
    )
  }

  test("LoadCSV") {
    assertGood(
      attach(
        LoadCSV(
          lhsLP,
          stringLiteral("file:///tmp/foo.csv"),
          varFor("u"),
          NoHeaders,
          None,
          legacyCsvQuoteEscaping = false,
          csvBufferSize = 2
        ),
        27.6
      ),
      planDescription(id, "LoadCSV", SingleChild(lhsPD), Seq(details("u")), Set("u", "a"))
    )

    assertGood(
      attach(
        LoadCSV(
          lhsLP,
          stringLiteral("file:///tmp/foo.csv"),
          varFor("  UNNAMED2"),
          NoHeaders,
          None,
          legacyCsvQuoteEscaping = false,
          csvBufferSize = 2
        ),
        27.6
      ),
      planDescription(id, "LoadCSV", SingleChild(lhsPD), Seq(details(anonVar("2"))), Set(anonVar("2"), "a"))
    )
  }

  test("Input") {
    assertGood(
      attach(
        Input(Seq(varFor("n1"), varFor("n2")), Seq(varFor("r")), Seq(varFor("v1"), varFor("v2")), nullable = false),
        4.0
      ),
      planDescription(
        id,
        "Input",
        NoChildren,
        Seq(details(Seq("n1", "n2", "r", "v1", "v2"))),
        Set("n1", "n2", "r", "v1", "v2")
      )
    )
  }

  test("NodeCountFromCountStore") {
    assertGood(
      attach(NodeCountFromCountStore(varFor("x"), List(Some(label("LabelName"))), Set.empty), 54.2),
      planDescription(id, "NodeCountFromCountStore", NoChildren, Seq(details("count( (:LabelName) ) AS x")), Set("x"))
    )

    assertGood(
      attach(
        NodeCountFromCountStore(varFor("x"), List(Some(label("LabelName")), Some(label("LabelName2"))), Set.empty),
        54.2
      ),
      planDescription(
        id,
        "NodeCountFromCountStore",
        NoChildren,
        Seq(details("count( (:LabelName), (:LabelName2) ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(NodeCountFromCountStore(varFor("  UNNAMED123"), List(Some(label("LabelName"))), Set.empty), 54.2),
      planDescription(
        id,
        "NodeCountFromCountStore",
        NoChildren,
        Seq(details(s"count( (:LabelName) ) AS ${anonVar("123")}")),
        Set(anonVar("123"))
      )
    )

    assertGood(
      attach(NodeCountFromCountStore(varFor("x"), List(None, None), Set.empty), 54.2),
      planDescription(id, "NodeCountFromCountStore", NoChildren, Seq(details("count( (), () ) AS x")), Set("x"))
    )
  }

  test("ProcedureCall") {
    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName("foo")(pos)
    val qualifiedName = QualifiedName(ns.parts, name.name)
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature =
      ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)
    val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))
    val call = ResolvedCall(signature, Seq(varFor("a1")), callResults)(pos)

    assertGood(
      attach(ProcedureCall(lhsLP, call), 33.2),
      planDescription(
        id,
        "ProcedureCall",
        SingleChild(lhsPD),
        Seq(details("my.proc.foo(a1) :: (x :: INTEGER, y :: LIST<NODE>)")),
        Set("a", "x", "y")
      )
    )
  }

  test("RelationshipCountFromCountStore") {
    assertGood(
      attach(
        RelationshipCountFromCountStore(varFor("x"), None, Seq(relType("LIKES"), relType("LOVES")), None, Set.empty),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        NoChildren,
        Seq(details("count( ()-[:LIKES|LOVES]->() ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(varFor("  UNNAMED122"), None, Seq(relType("LIKES")), None, Set.empty),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        NoChildren,
        Seq(details(s"count( ()-[:LIKES]->() ) AS ${anonVar("122")}")),
        Set(anonVar("122"))
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(
          varFor("x"),
          Some(label("StartLabel")),
          Seq(relType("LIKES"), relType("LOVES")),
          None,
          Set.empty
        ),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        NoChildren,
        Seq(details("count( (:StartLabel)-[:LIKES|LOVES]->() ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(
          varFor("x"),
          Some(label("StartLabel")),
          Seq(relType("LIKES"), relType("LOVES")),
          Some(label("EndLabel")),
          Set.empty
        ),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        NoChildren,
        Seq(details("count( (:StartLabel)-[:LIKES|LOVES]->(:EndLabel) ) AS x")),
        Set("x")
      )
    )

    assertGood(
      attach(
        RelationshipCountFromCountStore(
          varFor("x"),
          Some(label("StartLabel")),
          Seq.empty,
          Some(label("EndLabel")),
          Set.empty
        ),
        54.2
      ),
      planDescription(
        id,
        "RelationshipCountFromCountStore",
        NoChildren,
        Seq(details("count( (:StartLabel)-[]->(:EndLabel) ) AS x")),
        Set("x")
      )
    )
  }

  test("CreateRangeIndex") {
    assertGood(
      attach(
        CreateIndex(None, IndexType.RANGE, label("Label"), List(key("prop")), Some(Left("$indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("RANGE INDEX `$indexName` FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.RANGE, label("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(id, "CreateIndex", NoChildren, Seq(details("RANGE INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          label("Label"),
          List(key("prop")),
          Some(Right(parameter("indexName", CTString))),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("""RANGE INDEX $indexName FOR (:Label) ON (prop) OPTIONS {indexProvider: "range-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(label("Label"), List(key("prop")), IndexType.RANGE, None, NoOptions)),
          IndexType.RANGE,
          label("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("RANGE INDEX FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("RANGE INDEX FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(None, IndexType.RANGE, relType("Label"), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("RANGE INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.RANGE, relType("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("RANGE INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          relType("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("""RANGE INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "range-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(relType("Label"), List(key("prop")), IndexType.RANGE, None, NoOptions)),
          IndexType.RANGE,
          relType("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("RANGE INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("RANGE INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.RANGE,
          label("Label"),
          List(key("prop")),
          Some(Left("$indexName")),
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("RANGE INDEX `$indexName` FOR (:Label) ON (prop) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateLookupIndex") {
    assertGood(
      attach(CreateLookupIndex(None, EntityType.NODE, Some(Left("indexName")), NoOptions), 63.2),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("LOOKUP INDEX indexName FOR (n) ON EACH labels(n)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          Some(DoNothingIfExistsForLookupIndex(EntityType.NODE, None, NoOptions)),
          EntityType.NODE,
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("LOOKUP INDEX FOR (n) ON EACH labels(n)")),
            Set.empty
          )
        ),
        Seq(details("LOOKUP INDEX FOR (n) ON EACH labels(n)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          None,
          EntityType.NODE,
          Some(Left("$indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("token-lookup-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(
          details("""LOOKUP INDEX `$indexName` FOR (n) ON EACH labels(n) OPTIONS {indexProvider: "token-lookup-1.0"}""")
        ),
        Set.empty
      )
    )

    assertGood(
      attach(CreateLookupIndex(None, EntityType.RELATIONSHIP, Some(Left("indexName")), NoOptions), 63.2),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("LOOKUP INDEX indexName FOR ()-[r]-() ON EACH type(r)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          Some(DoNothingIfExistsForLookupIndex(
            EntityType.RELATIONSHIP,
            Some(Right(parameter("indexName", CTString))),
            NoOptions
          )),
          EntityType.RELATIONSHIP,
          Some(Right(parameter("indexName", CTString))),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)")),
            Set.empty
          )
        ),
        Seq(details("LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateLookupIndex(
          None,
          EntityType.RELATIONSHIP,
          Some(Left("indexName")),
          OptionsMap(Map("indexConfig" -> MapExpression(Seq.empty)(pos)))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("LOOKUP INDEX indexName FOR ()-[r]-() ON EACH type(r) OPTIONS {indexConfig: {}}")),
        Set.empty
      )
    )
  }

  test("CreateFulltextIndex") {
    assertGood(
      attach(
        CreateFulltextIndex(None, Left(List(label("Label"))), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("FULLTEXT INDEX indexName FOR (:Label) ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(None, Left(List(label("Label"))), List(key("prop1"), key("prop2")), None, NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("FULLTEXT INDEX FOR (:Label) ON EACH [prop1, prop2]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Left(List(label("Label1"), label("Label2"))),
          List(key("prop")),
          Some(Right(parameter("$indexName", CTString))),
          OptionsMap(Map("indexProvider" -> stringLiteral("fulltext-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details(
          """FULLTEXT INDEX $`$indexName` FOR (:Label1|Label2) ON EACH [prop] OPTIONS {indexProvider: "fulltext-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          Some(DoNothingIfExistsForFulltextIndex(Left(List(label("Label"))), List(key("prop")), None, NoOptions)),
          Left(List(label("Label"))),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("FULLTEXT INDEX FOR (:Label) ON EACH [prop]")),
            Set.empty
          )
        ),
        Seq(details("FULLTEXT INDEX FOR (:Label) ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(None, Right(List(relType("Label"))), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("FULLTEXT INDEX indexName FOR ()-[:Label]-() ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Right(List(relType("Label"), relType("Type"))),
          List(key("prop1"), key("prop2")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("FULLTEXT INDEX FOR ()-[:Label|Type]-() ON EACH [prop1, prop2]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Right(List(relType("Label"))),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("fulltext-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details(
          """FULLTEXT INDEX indexName FOR ()-[:Label]-() ON EACH [prop] OPTIONS {indexProvider: "fulltext-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          Some(DoNothingIfExistsForFulltextIndex(Right(List(relType("Label"))), List(key("prop")), None, NoOptions)),
          Right(List(relType("Label"))),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("FULLTEXT INDEX FOR ()-[:Label]-() ON EACH [prop]")),
            Set.empty
          )
        ),
        Seq(details("FULLTEXT INDEX FOR ()-[:Label]-() ON EACH [prop]")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateFulltextIndex(
          None,
          Left(List(label("Label"))),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsParam(parameter("ops", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("FULLTEXT INDEX indexName FOR (:Label) ON EACH [prop] OPTIONS $ops")),
        Set.empty
      )
    )
  }

  test("CreateTextIndex") {
    assertGood(
      attach(
        CreateIndex(None, IndexType.TEXT, label("Label"), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("TEXT INDEX indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.TEXT, label("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(id, "CreateIndex", NoChildren, Seq(details("TEXT INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          label("Label"),
          List(key("prop")),
          Some(Right(parameter("indexName", CTString))),
          OptionsMap(Map("indexProvider" -> stringLiteral("text-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("""TEXT INDEX $indexName FOR (:Label) ON (prop) OPTIONS {indexProvider: "text-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(label("Label"), List(key("prop")), IndexType.TEXT, None, NoOptions)),
          IndexType.TEXT,
          label("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("TEXT INDEX FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("TEXT INDEX FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(None, IndexType.TEXT, relType("Label"), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("TEXT INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.TEXT, relType("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(id, "CreateIndex", NoChildren, Seq(details("TEXT INDEX FOR ()-[:Label]-() ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          relType("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("text-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("""TEXT INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "text-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(relType("Label"), List(key("prop")), IndexType.TEXT, None, NoOptions)),
          IndexType.TEXT,
          relType("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("TEXT INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("TEXT INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.TEXT,
          label("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("TEXT INDEX indexName FOR (:Label) ON (prop) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreatPointIndex") {
    assertGood(
      attach(
        CreateIndex(None, IndexType.POINT, label("Label"), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("POINT INDEX indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.POINT, label("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(id, "CreateIndex", NoChildren, Seq(details("POINT INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          label("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("point-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("""POINT INDEX indexName FOR (:Label) ON (prop) OPTIONS {indexProvider: "point-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(
            label("Label"),
            List(key("prop")),
            IndexType.POINT,
            Some(Right(parameter("indexName", CTString))),
            NoOptions
          )),
          IndexType.POINT,
          label("Label"),
          List(key("prop")),
          Some(Right(parameter("indexName", CTString))),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("POINT INDEX $indexName FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("POINT INDEX $indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(None, IndexType.POINT, relType("Label"), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("POINT INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.POINT, relType("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("POINT INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          relType("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("point-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("""POINT INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "point-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(relType("Label"), List(key("prop")), IndexType.POINT, None, NoOptions)),
          IndexType.POINT,
          relType("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("POINT INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("POINT INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.POINT,
          label("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("POINT INDEX indexName FOR (:Label) ON (prop) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateVectorIndex") {
    assertGood(
      attach(
        CreateIndex(None, IndexType.VECTOR, label("Label"), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("VECTOR INDEX indexName FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.VECTOR, label("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(id, "CreateIndex", NoChildren, Seq(details("VECTOR INDEX FOR (:Label) ON (prop)")), Set.empty)
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.VECTOR,
          label("Label"),
          List(key("prop")),
          Some(Right(parameter("indexName", CTString))),
          OptionsMap(Map("indexProvider" -> stringLiteral("vector-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("""VECTOR INDEX $indexName FOR (:Label) ON (prop) OPTIONS {indexProvider: "vector-1.0"}""")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(label("Label"), List(key("prop")), IndexType.VECTOR, None, NoOptions)),
          IndexType.VECTOR,
          label("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("VECTOR INDEX FOR (:Label) ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("VECTOR INDEX FOR (:Label) ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(None, IndexType.VECTOR, relType("Label"), List(key("prop")), Some(Left("indexName")), NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("VECTOR INDEX indexName FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(CreateIndex(None, IndexType.VECTOR, relType("Label"), List(key("prop")), None, NoOptions), 63.2),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("VECTOR INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.VECTOR,
          relType("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("vector-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(
          details("""VECTOR INDEX indexName FOR ()-[:Label]-() ON (prop) OPTIONS {indexProvider: "vector-1.0"}""")
        ),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          Some(DoNothingIfExistsForIndex(relType("Label"), List(key("prop")), IndexType.VECTOR, None, NoOptions)),
          IndexType.VECTOR,
          relType("Label"),
          List(key("prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(INDEX)",
            NoChildren,
            Seq(details("VECTOR INDEX FOR ()-[:Label]-() ON (prop)")),
            Set.empty
          )
        ),
        Seq(details("VECTOR INDEX FOR ()-[:Label]-() ON (prop)")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateIndex(
          None,
          IndexType.VECTOR,
          label("Label"),
          List(key("prop")),
          Some(Left("indexName")),
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateIndex",
        NoChildren,
        Seq(details("VECTOR INDEX indexName FOR (:Label) ON (prop) OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("DropIndexOnName") {
    assertGood(
      attach(DropIndexOnName(Left("indexName"), ifExists = false), 63.2),
      planDescription(id, "DropIndex", NoChildren, Seq(details("INDEX indexName")), Set.empty)
    )

    assertGood(
      attach(DropIndexOnName(Left("indexName"), ifExists = true), 63.2),
      planDescription(id, "DropIndex", NoChildren, Seq(details("INDEX indexName IF EXISTS")), Set.empty)
    )

    assertGood(
      attach(DropIndexOnName(Right(parameter("indexName", CTString)), ifExists = false), 63.2),
      planDescription(id, "DropIndex", NoChildren, Seq(details("INDEX $indexName")), Set.empty)
    )
  }

  test("ShowIndexes") {
    assertGood(
      attach(ShowIndexes(AllIndexes, List.empty, List.empty, yieldAll = true), 1.0),
      planDescription(id, "ShowIndexes", NoChildren, Seq(details("allIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(RangeIndexes, List.empty, List.empty, yieldAll = false), 1.0),
      planDescription(id, "ShowIndexes", NoChildren, Seq(details("rangeIndexes, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(FulltextIndexes, List.empty, List.empty, yieldAll = true), 1.0),
      planDescription(id, "ShowIndexes", NoChildren, Seq(details("fulltextIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(TextIndexes, List.empty, List.empty, yieldAll = true), 1.0),
      planDescription(id, "ShowIndexes", NoChildren, Seq(details("textIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(PointIndexes, List.empty, List.empty, yieldAll = true), 1.0),
      planDescription(id, "ShowIndexes", NoChildren, Seq(details("pointIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(ShowIndexes(VectorIndexes, List.empty, List.empty, yieldAll = true), 1.0),
      planDescription(id, "ShowIndexes", NoChildren, Seq(details("vectorIndexes, allColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowIndexes(
          LookupIndexes,
          List("xxx", "yyy", "vvv").map(s => ShowColumn(s)(pos)),
          List(
            CommandResultItem("xxx", varFor("xxx"))(pos),
            CommandResultItem("yyy", varFor("zzz"))(pos),
            CommandResultItem("vvv", varFor("vvv"))(pos)
          ),
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowIndexes",
        NoChildren,
        Seq(details("lookupIndexes, columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("CreateNodeUniquePropertyConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, NodeUniqueness, label("Label"), Seq(prop(" x", "prop")), None, NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeUniqueness,
          label("Label"),
          Seq(prop("x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeUniqueness,
          label("Label"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop1, x.prop2) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeUniqueness,
          label("Label"),
          List(prop("x", "prop")),
          Some(Left("$constraintName")),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details(
          """CONSTRAINT `$constraintName` FOR (x:Label) REQUIRE (x.prop) IS UNIQUE OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodeUniqueness,
            None,
            NoOptions
          )),
          NodeUniqueness,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeUniqueness,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS UNIQUE OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateRelationshipUniquePropertyConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, RelationshipUniqueness, relType("REL_TYPE"), Seq(prop(" x", "prop")), None, NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipUniqueness,
          relType("REL_TYPE"),
          Seq(prop("x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipUniqueness,
          relType("REL_TYPE"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop1, x.prop2) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipUniqueness,
          relType("REL_TYPE"),
          List(prop("x", "prop")),
          Some(Right(parameter("constraintName", CTString))),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details(
          """CONSTRAINT $constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop) IS UNIQUE OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("REL_TYPE"),
            Seq(prop(" x", "prop")),
            RelationshipUniqueness,
            None,
            NoOptions
          )),
          RelationshipUniqueness,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipUniqueness,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS UNIQUE OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateNodeKeyConstraint") {
    assertGood(
      attach(CreateConstraint(None, NodeKey, label("Label"), Seq(prop(" x", "prop")), None, NoOptions), 63.2),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeKey,
          label("Label"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop1, x.prop2) IS NODE KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeKey,
          label("Label"),
          List(prop("x", "prop")),
          Some(Right(parameter("constraintName", CTString))),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details(
          """CONSTRAINT $constraintName FOR (x:Label) REQUIRE (x.prop) IS NODE KEY OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodeKey,
            Some(Left("constraintName")),
            NoOptions
          )),
          NodeKey,
          label("Label"),
          Seq(prop(" x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT constraintName FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT constraintName FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodeKey,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NODE KEY OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateRelationshipKeyConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, RelationshipKey, relType("REL_TYPE"), Seq(prop(" x", "prop")), None, NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipKey,
          relType("REL_TYPE"),
          Seq(prop("x", "prop1"), prop("x", "prop2")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop1, x.prop2) IS RELATIONSHIP KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipKey,
          relType("REL_TYPE"),
          List(prop("x", "prop")),
          Some(Right(parameter("constraintName", CTString))),
          OptionsMap(Map("indexProvider" -> stringLiteral("range-1.0")))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details(
          """CONSTRAINT $constraintName FOR ()-[x:REL_TYPE]-() REQUIRE (x.prop) IS RELATIONSHIP KEY OPTIONS {indexProvider: "range-1.0"}"""
        )),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("REL_TYPE"),
            Seq(prop(" x", "prop")),
            RelationshipKey,
            Some(Left("constraintName")),
            NoOptions
          )),
          RelationshipKey,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT constraintName FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT constraintName FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipKey,
          relType("REL_TYPE"),
          Seq(prop(" x", "prop")),
          None,
          OptionsParam(parameter("options", CTMap))
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR ()-[` x`:REL_TYPE]-() REQUIRE (` x`.prop) IS RELATIONSHIP KEY OPTIONS $options")),
        Set.empty
      )
    )
  }

  test("CreateNodePropertyExistenceConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, NodePropertyExistence, label("Label"), Seq(prop(" x", "prop")), None, NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyExistence,
          label("Label"),
          Seq(prop("x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodePropertyExistence,
            None,
            NoOptions
          )),
          NodePropertyExistence,
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NOT NULL")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )
  }

  test("CreateRelationshipPropertyExistenceConstraint") {
    assertGood(
      attach(
        CreateConstraint(None, RelationshipPropertyExistence, relType("R"), Seq(prop(" x", "prop")), None, NoOptions),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyExistence,
          relType("R"),
          Seq(prop(" x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("R"),
            Seq(prop(" x", "prop")),
            RelationshipPropertyExistence,
            None,
            NoOptions
          )),
          RelationshipPropertyExistence,
          relType("R"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS NOT NULL")),
        Set.empty
      )
    )
  }

  test("CreateNodePropertyTypeConstraint") {
    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyType(IntegerType(isNullable = true)(pos)),
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS :: INTEGER")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          NodePropertyType(
            ClosedDynamicUnionType(Set(
              ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
              StringType(isNullable = true)(pos)
            ))(pos)
          ),
          label("Label"),
          Seq(prop("x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR (x:Label) REQUIRE (x.prop) IS :: STRING | LIST<BOOLEAN>")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            label("Label"),
            Seq(prop(" x", "prop")),
            NodePropertyType(ZonedDateTimeType(isNullable = true)(pos)),
            None,
            NoOptions
          )),
          NodePropertyType(ZonedDateTimeType(isNullable = true)(pos)),
          label("Label"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS :: ZONED DATETIME")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR (` x`:Label) REQUIRE (` x`.prop) IS :: ZONED DATETIME")),
        Set.empty
      )
    )
  }

  test("CreateRelationshipPropertyTypeConstraint") {
    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyType(FloatType(isNullable = true)(pos)),
          relType("R"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: FLOAT")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          None,
          RelationshipPropertyType(LocalTimeType(isNullable = true)(pos)),
          relType("R"),
          Seq(prop(" x", "prop")),
          Some(Left("constraintName")),
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        NoChildren,
        Seq(details("CONSTRAINT constraintName FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: LOCAL TIME")),
        Set.empty
      )
    )

    assertGood(
      attach(
        CreateConstraint(
          Some(DoNothingIfExistsForConstraint(
            relType("R"),
            Seq(prop(" x", "prop")),
            RelationshipPropertyType(
              ClosedDynamicUnionType(Set(
                DurationType(isNullable = true)(pos),
                DateType(isNullable = true)(pos)
              ))(pos)
            ),
            None,
            NoOptions
          )),
          RelationshipPropertyType(
            ClosedDynamicUnionType(Set(
              DurationType(isNullable = true)(pos),
              DateType(isNullable = true)(pos)
            ))(pos)
          ),
          relType("R"),
          Seq(prop(" x", "prop")),
          None,
          NoOptions
        ),
        63.2
      ),
      planDescription(
        id,
        "CreateConstraint",
        SingleChild(
          planDescription(
            id,
            "DoNothingIfExists(CONSTRAINT)",
            NoChildren,
            Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: DATE | DURATION")),
            Set.empty
          )
        ),
        Seq(details("CONSTRAINT FOR ()-[` x`:R]-() REQUIRE (` x`.prop) IS :: DATE | DURATION")),
        Set.empty
      )
    )
  }

  test("DropConstraintOnName") {
    assertGood(
      attach(DropConstraintOnName(Left("name"), ifExists = false), 63.2),
      planDescription(id, "DropConstraint", NoChildren, Seq(details("CONSTRAINT name")), Set.empty)
    )

    assertGood(
      attach(DropConstraintOnName(Left("name"), ifExists = true), 63.2),
      planDescription(id, "DropConstraint", NoChildren, Seq(details("CONSTRAINT name IF EXISTS")), Set.empty)
    )

    assertGood(
      attach(DropConstraintOnName(Right(parameter("name", CTString)), ifExists = false), 63.2),
      planDescription(id, "DropConstraint", NoChildren, Seq(details("CONSTRAINT $name")), Set.empty)
    )
  }

  test("ShowConstraints") {
    assertGood(
      attach(
        ShowConstraints(constraintType = AllConstraints, List.empty, List.empty, yieldAll = false),
        1.0
      ),
      planDescription(id, "ShowConstraints", NoChildren, Seq(details("allConstraints, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(constraintType = UniqueConstraints, List.empty, List.empty, yieldAll = true),
        1.0
      ),
      planDescription(id, "ShowConstraints", NoChildren, Seq(details("uniquenessConstraints, allColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodeUniqueConstraints,
          List.empty,
          List.empty,
          yieldAll = true
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("nodeUniquenessConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelUniqueConstraints,
          List.empty,
          List.empty,
          yieldAll = true
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("relationshipUniquenessConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(constraintType = KeyConstraints, List.empty, List.empty, yieldAll = false),
        1.0
      ),
      planDescription(id, "ShowConstraints", NoChildren, Seq(details("keyConstraints, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodeKeyConstraints,
          List.empty,
          List.empty,
          yieldAll = false
        ),
        1.0
      ),
      planDescription(id, "ShowConstraints", NoChildren, Seq(details("nodeKeyConstraints, defaultColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(constraintType = RelKeyConstraints, List.empty, List.empty, yieldAll = false),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("relationshipKeyConstraints, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = ExistsConstraints(ValidSyntax),
          List.empty,
          List.empty,
          yieldAll = true
        ),
        1.0
      ),
      planDescription(id, "ShowConstraints", NoChildren, Seq(details("existenceConstraints, allColumns")), Set.empty)
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodeExistsConstraints(),
          List.empty,
          List.empty,
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("nodeExistenceConstraints, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelExistsConstraints(),
          List.empty,
          List.empty,
          yieldAll = true
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("relationshipExistenceConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = PropTypeConstraints,
          List.empty,
          List.empty,
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("propertyTypeConstraints, defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = NodePropTypeConstraints,
          List.empty,
          List.empty,
          yieldAll = true
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("nodePropertyTypeConstraints, allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowConstraints(
          constraintType = RelPropTypeConstraints,
          List("xxx", "yyy", "vvv").map(s => ShowColumn(s)(pos)),
          List(
            CommandResultItem("xxx", varFor("xxx"))(pos),
            CommandResultItem("yyy", varFor("zzz"))(pos),
            CommandResultItem("vvv", varFor("vvv"))(pos)
          ),
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowConstraints",
        NoChildren,
        Seq(details("relationshipPropertyTypeConstraints, columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("ShowProcedures") {
    assertGood(
      attach(ShowProcedures(None, List.empty, List.empty, yieldAll = false), 1.0),
      planDescription(
        id,
        "ShowProcedures",
        NoChildren,
        Seq(details("proceduresForUser(all), defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(ShowProcedures(Some(CurrentUser), List.empty, List.empty, yieldAll = true), 1.0),
      planDescription(
        id,
        "ShowProcedures",
        NoChildren,
        Seq(details("proceduresForUser(current), allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowProcedures(
          Some(User("foo")),
          List("xxx", "yyy", "vvv").map(s => ShowColumn(s)(pos)),
          List(
            CommandResultItem("xxx", varFor("xxx"))(pos),
            CommandResultItem("yyy", varFor("zzz"))(pos),
            CommandResultItem("vvv", varFor("vvv"))(pos)
          ),
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowProcedures",
        NoChildren,
        Seq(details("proceduresForUser(foo), columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("ShowFunctions") {
    assertGood(
      attach(ShowFunctions(AllFunctions, None, List.empty, List.empty, yieldAll = false), 1.0),
      planDescription(
        id,
        "ShowFunctions",
        NoChildren,
        Seq(details("allFunctions, functionsForUser(all), defaultColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowFunctions(BuiltInFunctions, Some(CurrentUser), List.empty, List.empty, yieldAll = true),
        1.0
      ),
      planDescription(
        id,
        "ShowFunctions",
        NoChildren,
        Seq(details("builtInFunctions, functionsForUser(current), allColumns")),
        Set.empty
      )
    )

    assertGood(
      attach(
        ShowFunctions(
          UserDefinedFunctions,
          Some(User("foo")),
          List("xxx", "yyy", "vvv").map(s => ShowColumn(s)(pos)),
          List(
            CommandResultItem("xxx", varFor("xxx"))(pos),
            CommandResultItem("yyy", varFor("zzz"))(pos),
            CommandResultItem("vvv", varFor("vvv"))(pos)
          ),
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowFunctions",
        NoChildren,
        Seq(details("userDefinedFunctions, functionsForUser(foo), columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("ShowSettings") {
    val defaultColumns = List("xxx", "yyy").map(s => ShowColumn(s)(pos))
    assertGood(
      attach(ShowSettings(Left(List.empty[String]), defaultColumns, List.empty, yieldAll = true), 1.0),
      planDescription(
        id,
        "ShowSettings",
        NoChildren,
        Seq(details("allSettings, allColumns")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowSettings(Left(List("Foo", "Bar")), defaultColumns, List.empty, yieldAll = false),
        1.0
      ),
      planDescription(
        id,
        "ShowSettings",
        NoChildren,
        Seq(details("settings(Foo, Bar), defaultColumns")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowSettings(
          Right(stringLiteral("foo.*")),
          List("xxx", "yyy", "vvv").map(s => ShowColumn(s)(pos)),
          List(
            CommandResultItem("xxx", varFor("xxx"))(pos),
            CommandResultItem("yyy", varFor("zzz"))(pos),
            CommandResultItem("vvv", varFor("vvv"))(pos)
          ),
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowSettings",
        NoChildren,
        Seq(details("settings(foo.*), columns(xxx, yyy AS zzz, vvv)")),
        Set("xxx", "zzz", "vvv")
      )
    )
  }

  test("ShowTransactions") {
    val defaultColumns = List("xxx", "yyy").map(s => ShowColumn(s)(pos))

    assertGood(
      attach(ShowTransactions(Left(List.empty), defaultColumns, List.empty, yieldAll = false), 1.0),
      planDescription(
        id,
        "ShowTransactions",
        NoChildren,
        Seq(details("defaultColumns, allTransactions")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          Left(List("db1-transaction-123")),
          defaultColumns,
          List.empty,
          yieldAll = true
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        NoChildren,
        Seq(details("allColumns, transactions(db1-transaction-123)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          Left(List("db1-transaction-123", "db2-transaction-456")),
          List("xxx", "yyy", "vvv").map(s => ShowColumn(s)(pos)),
          List(
            CommandResultItem("xxx", varFor("xxx"))(pos),
            CommandResultItem("yyy", varFor("zzz"))(pos),
            CommandResultItem("vvv", varFor("vvv"))(pos)
          ),
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        NoChildren,
        Seq(details("columns(xxx, yyy AS zzz, vvv), transactions(db1-transaction-123, db2-transaction-456)")),
        Set("xxx", "zzz", "vvv")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          Right(parameter("foo", CTAny)),
          defaultColumns,
          List.empty,
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        NoChildren,
        Seq(details("defaultColumns, transactions($foo)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          Right(varFor("foo")),
          defaultColumns,
          List.empty,
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        NoChildren,
        Seq(details("defaultColumns, transactions(foo)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        ShowTransactions(
          Right(Add(varFor("foo"), stringLiteral("123"))(pos)),
          defaultColumns,
          List.empty,
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "ShowTransactions",
        NoChildren,
        Seq(details("defaultColumns, transactions(foo + 123)")),
        Set("xxx", "yyy")
      )
    )
  }

  test("TerminateTransactions") {
    val defaultColumns = List("xxx", "yyy").map(s => ShowColumn(s)(pos))

    assertGood(
      attach(
        TerminateTransactions(Left(List("db1-transaction-123")), defaultColumns, List.empty, yieldAll = false),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        NoChildren,
        Seq(details("defaultColumns, transactions(db1-transaction-123)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        TerminateTransactions(
          Left(List("db1-transaction-123", "db2-transaction-456")),
          defaultColumns,
          List(CommandResultItem("xxx", varFor("xxx"))(pos), CommandResultItem("yyy", varFor("zzz"))(pos)),
          yieldAll = false
        ),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        NoChildren,
        Seq(details("columns(xxx, yyy AS zzz), transactions(db1-transaction-123, db2-transaction-456)")),
        Set("xxx", "zzz")
      )
    )

    assertGood(
      attach(
        TerminateTransactions(Right(parameter("foo", CTAny)), defaultColumns, List.empty, yieldAll = true),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        NoChildren,
        Seq(details("allColumns, transactions($foo)")),
        Set("xxx", "yyy")
      )
    )

    assertGood(
      attach(
        TerminateTransactions(Right(number("123")), defaultColumns, List.empty, yieldAll = true),
        1.0
      ),
      planDescription(
        id,
        "TerminateTransactions",
        NoChildren,
        Seq(details("allColumns, transactions(123)")),
        Set("xxx", "yyy")
      )
    )
  }

  test("Aggregation") {
    // Aggregation 1 grouping, 0 aggregating
    assertGood(
      attach(Aggregation(lhsLP, Map(varFor("a") -> varFor("a")), Map.empty), 17.5),
      planDescription(id, "EagerAggregation", SingleChild(lhsPD), Seq(details("a")), Set("a"))
    )

    // Aggregation 2 grouping, 0 aggregating
    assertGood(
      attach(
        Aggregation(lhsLP, Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c")), Map.empty),
        17.5
      ),
      planDescription(id, "EagerAggregation", SingleChild(lhsPD), Seq(details("a, c AS b")), Set("a", "b"))
    )

    val countFunction =
      FunctionInvocation(FunctionName(Count.name)(pos), distinct = false, IndexedSeq(varFor("c")))(pos)
    val collectDistinctFunction =
      FunctionInvocation(FunctionName(Collect.name)(pos), distinct = true, IndexedSeq(varFor("c")))(pos)
    val orderedCollectDistinctFunction =
      FunctionInvocation(FunctionName(Collect.name)(pos), distinct = true, IndexedSeq(varFor("c")), ArgumentAsc)(pos)

    // Aggregation 1 grouping, 1 aggregating
    assertGood(
      attach(
        Aggregation(lhsLP, Map(varFor("a") -> varFor("a")), Map(varFor("count") -> countFunction)),
        1.3
      ),
      planDescription(
        id,
        "EagerAggregation",
        SingleChild(lhsPD),
        Seq(details("a, count(c) AS count")),
        Set("a", "count")
      )
    )

    // Aggregation 2 grouping, 2 aggregating
    assertGood(
      attach(
        Aggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c")),
          Map(varFor("count(c)") -> countFunction, varFor("collect") -> collectDistinctFunction)
        ),
        1.3
      ),
      planDescription(
        id,
        "EagerAggregation",
        SingleChild(lhsPD),
        Seq(details("a, c AS b, count(c) AS `count(c)`, collect(DISTINCT c) AS collect")),
        Set("a", "b", "`count(c)`", "collect")
      )
    )

    assertGood(
      attach(
        Aggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c")),
          Map(varFor("count(c)") -> countFunction, varFor("collect") -> orderedCollectDistinctFunction)
        ),
        1.3
      ),
      planDescription(
        id,
        "EagerAggregation",
        SingleChild(lhsPD),
        Seq(details("a, c AS b, count(c) AS `count(c)`, collect(ASC DISTINCT c) AS collect")),
        Set("a", "b", "`count(c)`", "collect")
      )
    )

    // Distinct 1 grouping
    assertGood(
      attach(Distinct(lhsLP, Map(varFor("  a@23") -> varFor("  a@23"))), 45.9),
      planDescription(id, "Distinct", SingleChild(lhsPD), Seq(details("a")), Set("a"))
    )

    // Distinct 2 grouping
    assertGood(
      attach(Distinct(lhsLP, Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"))), 45.9),
      planDescription(id, "Distinct", SingleChild(lhsPD), Seq(details("a, c AS b")), Set("a", "b"))
    )

    // OrderedDistinct 1 column, 1 sorted
    assertGood(
      attach(OrderedDistinct(lhsLP, Map(varFor("a") -> varFor("a")), Seq(varFor("a"))), 45.9),
      planDescription(id, "OrderedDistinct", SingleChild(lhsPD), Seq(details("a")), Set("a"))
    )

    // OrderedDistinct 3 column, 2 sorted
    assertGood(
      attach(
        OrderedDistinct(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Seq(varFor("d"), varFor("a"))
        ),
        45.9
      ),
      planDescription(id, "OrderedDistinct", SingleChild(lhsPD), Seq(details("d, a, c AS b")), Set("a", "b", "d"))
    )

    // OrderedAggregation 1 grouping, 0 aggregating, 1 sorted
    assertGood(
      attach(
        OrderedAggregation(lhsLP, Map(varFor("a") -> varFor("a")), Map.empty, Seq(varFor("a"))),
        17.5
      ),
      planDescription(id, "OrderedAggregation", SingleChild(lhsPD), Seq(details("a")), Set("a"))
    )

    // OrderedAggregation 3 grouping, 0 aggregating, 2 sorted
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map.empty,
          Seq(varFor("d"), varFor("a"))
        ),
        17.5
      ),
      planDescription(id, "OrderedAggregation", SingleChild(lhsPD), Seq(details("d, a, c AS b")), Set("a", "b", "d"))
    )

    // OrderedAggregation 3 grouping, 1 aggregating with alias, 2 sorted
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("count") -> countFunction),
          Seq(varFor("d"), varFor("a"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        SingleChild(lhsPD),
        Seq(details("d, a, c AS b, count(c) AS count")),
        Set("a", "b", "d", "count")
      )
    )

    // OrderedAggregation 3 grouping, 1 aggregating without alias, 2 sorted
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("collect(DISTINCT c)") -> collectDistinctFunction),
          Seq(varFor("d"), varFor("a"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        SingleChild(lhsPD),
        Seq(details("d, a, c AS b, collect(DISTINCT c) AS `collect(DISTINCT c)`")),
        Set("a", "b", "d", "`collect(DISTINCT c)`")
      )
    )

    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("collect(DISTINCT c)") -> orderedCollectDistinctFunction),
          Seq(varFor("d"), varFor("a"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        SingleChild(lhsPD),
        Seq(details("d, a, c AS b, collect(ASC DISTINCT c) AS `collect(DISTINCT c)`")),
        Set("a", "b", "d", "`collect(DISTINCT c)`")
      )
    )

    // OrderedAggregation 3 grouping, 1 aggregating with alias, sorted on aliased column
    assertGood(
      attach(
        OrderedAggregation(
          lhsLP,
          Map(varFor("a") -> varFor("a"), varFor("b") -> varFor("c"), varFor("d") -> varFor("d")),
          Map(varFor("collect(DISTINCT c)") -> collectDistinctFunction),
          Seq(varFor("d"), varFor("c"))
        ),
        1.3
      ),
      planDescription(
        id,
        "OrderedAggregation",
        SingleChild(lhsPD),
        Seq(details("d, c AS b, a, collect(DISTINCT c) AS `collect(DISTINCT c)`")),
        Set("a", "b", "d", "`collect(DISTINCT c)`")
      )
    )
  }

  test("FunctionInvocation") {
    val namespace = List("ns")
    val name = "datetime"
    val args = IndexedSeq(number("23391882379"))
    val functionSignature = UserFunctionSignature(
      QualifiedName(Seq.empty, "datetime"),
      IndexedSeq(FieldSignature("Input", CTAny, Some(stringValue("DEFAULT_TEMPORAL_ARGUMENT")))),
      BooleanType(isNullable = true)(InputPosition.NONE),
      None,
      None,
      isAggregate = false,
      1,
      builtIn = true
    )

    val functionInvocation = FunctionInvocation(
      namespace = Namespace(namespace)(pos),
      functionName = FunctionName(name)(pos),
      distinct = false,
      args = args
    )(pos)
    assertGood(
      attach(SetRelationshipProperty(lhsLP, varFor("x"), key("prop"), functionInvocation), 1.0),
      planDescription(
        id,
        "SetProperty",
        SingleChild(lhsPD),
        Seq(details("x.prop = ns.datetime(23391882379)")),
        Set("a", "x")
      )
    )

    val resolvedFunctionInvocation = ResolvedFunctionInvocation(
      qualifiedName = QualifiedName(namespace, name),
      fcnSignature = Some(functionSignature),
      callArguments = args
    )(pos)
    assertGood(
      attach(SetRelationshipProperty(lhsLP, varFor("x"), key("prop"), resolvedFunctionInvocation), 1.0),
      planDescription(
        id,
        "SetProperty",
        SingleChild(lhsPD),
        Seq(details("x.prop = ns.datetime(23391882379)")),
        Set("a", "x")
      )
    )
  }

  test("Create") {
    val properties = MapExpression(Seq(
      (key("y"), number("1")),
      (key("crs"), stringLiteral("cartesian"))
    ))(pos)

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set.empty, None),
            CreateRelationship(varFor("r"), varFor("x"), relType("R"), varFor("y"), SemanticDirection.INCOMING, None)
          )
        ),
        32.2
      ),
      planDescription(id, "Create", SingleChild(lhsPD), Seq(details(Seq("(x)", "(x)<-[r:R]-(y)"))), Set("a", "x", "r"))
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set(label("Label")), None),
            CreateRelationship(
              varFor("  UNNAMED67"),
              varFor("x"),
              relType("R"),
              varFor("y"),
              SemanticDirection.INCOMING,
              None
            )
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        SingleChild(lhsPD),
        Seq(details(Seq("(x:Label)", s"(x)<-[${anonVar("67")}:R]-(y)"))),
        Set("a", "x", anonVar("67"))
      )
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set(label("Label1"), label("Label2")), None),
            CreateRelationship(varFor("r"), varFor("x"), relType("R"), varFor("y"), SemanticDirection.INCOMING, None)
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        SingleChild(lhsPD),
        Seq(details(Seq("(x:Label1:Label2)", "(x)<-[r:R]-(y)"))),
        Set("a", "x", "r")
      )
    )

    assertGood(
      attach(
        Create(
          lhsLP,
          Seq(
            CreateNode(varFor("x"), Set(label("Label")), Some(properties)),
            CreateRelationship(
              varFor("r"),
              varFor("x"),
              relType("R"),
              varFor("y"),
              SemanticDirection.INCOMING,
              Some(properties)
            )
          )
        ),
        32.2
      ),
      planDescription(
        id,
        "Create",
        SingleChild(lhsPD),
        Seq(details(Seq("(x:Label {y: 1, crs: \"cartesian\"})", "(x)<-[r:R {y: 1, crs: \"cartesian\"}]-(y)"))),
        Set("a", "x", "r")
      )
    )
  }

  test("Merge") {
    val properties = MapExpression(Seq(
      (key("x"), number("1")),
      (key("y"), stringLiteral("two"))
    ))(pos)

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set.empty, None)),
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            None
          )),
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(id, "Merge", SingleChild(lhsPD), Seq(details(Seq("CREATE (x), (x)<-[r:R]-(y)"))), Set("a"))
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), None)),
          Seq.empty,
          Seq(SetLabelPattern(varFor("x"), Seq(label("NEW")))),
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        SingleChild(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON MATCH SET x:NEW"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), None)),
          Seq.empty,
          Seq.empty,
          Seq(SetLabelPattern(varFor("x"), Seq(label("NEW")))),
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        SingleChild(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON CREATE SET x:NEW"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), None)),
          Seq.empty,
          Seq(SetLabelPattern(varFor("x"), Seq(label("ON_MATCH")))),
          Seq(SetLabelPattern(varFor("x"), Seq(label("ON_CREATE")))),
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        SingleChild(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON MATCH SET x:ON_MATCH", "ON CREATE SET x:ON_CREATE"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set.empty, Some(properties))),
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            Some(properties)
          )),
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        SingleChild(lhsPD),
        Seq(details(Seq("CREATE (x: {x: 1, y: \"two\"}), (x)<-[r:R {x: 1, y: \"two\"}]-(y)"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), Some(properties))),
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            Some(properties)
          )),
          Seq.empty,
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        SingleChild(lhsPD),
        Seq(details(Seq("CREATE (x:L {x: 1, y: \"two\"}), (x)<-[r:R {x: 1, y: \"two\"}]-(y)"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq.empty,
          Seq(CreateRelationship(
            varFor("r"),
            varFor("x"),
            relType("R"),
            varFor("y"),
            SemanticDirection.INCOMING,
            None
          )),
          Seq.empty,
          Seq.empty,
          Set(varFor("x"), varFor("y"))
        ),
        32.2
      ),
      planDescription(
        id,
        "LockingMerge",
        SingleChild(lhsPD),
        Seq(details(Seq("CREATE (x)<-[r:R]-(y)", "LOCK(x, y)"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Merge(
          lhsLP,
          Seq(CreateNode(varFor("x"), Set(label("L")), None)),
          Seq.empty,
          Seq(SetNodePropertiesPattern(varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2"))))),
          Seq.empty,
          Set.empty
        ),
        32.2
      ),
      planDescription(
        id,
        "Merge",
        SingleChild(lhsPD),
        Seq(details(Seq("CREATE (x:L)", "ON MATCH SET x.p1 = 1, x.p2 = 2"))),
        Set("a")
      )
    )

  }

  test("foreach") {
    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(SetNodePropertyPattern(varFor("x"), PropertyKeyName("prop")(InputPosition.NONE), stringLiteral("foo")))
        ),
        32.2
      ),
      planDescription(id, "Foreach", SingleChild(lhsPD), Seq(details(Seq("i IN $p", "SET x.prop = \"foo\""))), Set("a"))
    )

    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(RemoveLabelPattern(varFor("x"), Seq(label("L"), label("M"))))
        ),
        32.2
      ),
      planDescription(id, "Foreach", SingleChild(lhsPD), Seq(details(Seq("i IN $p", "REMOVE x:L:M"))), Set("a"))
    )

    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(org.neo4j.cypher.internal.ir.DeleteExpression(varFor("x"), detachDelete = true))
        ),
        32.2
      ),
      planDescription(id, "Foreach", SingleChild(lhsPD), Seq(details(Seq("i IN $p", "DETACH DELETE x"))), Set("a"))
    )
    assertGood(
      attach(
        Foreach(
          lhsLP,
          varFor("i"),
          parameter("p", CTList(CTInteger)),
          Seq(org.neo4j.cypher.internal.ir.DeleteExpression(varFor("x"), detachDelete = false))
        ),
        32.2
      ),
      planDescription(id, "Foreach", SingleChild(lhsPD), Seq(details(Seq("i IN $p", "DELETE x"))), Set("a"))
    )
  }

  test("Delete") {
    assertGood(
      attach(DeleteExpression(lhsLP, prop("x", "prop")), 34.5),
      planDescription(id, "Delete", SingleChild(lhsPD), Seq(details("x.prop")), Set("a"))
    )

    assertGood(
      attach(DeleteNode(lhsLP, varFor("x")), 34.5),
      planDescription(id, "Delete", SingleChild(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DeletePath(lhsLP, varFor("x")), 34.5),
      planDescription(id, "Delete", SingleChild(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DeleteRelationship(lhsLP, varFor("x")), 34.5),
      planDescription(id, "Delete", SingleChild(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DetachDeleteExpression(lhsLP, varFor("x")), 34.5),
      planDescription(id, "DetachDelete", SingleChild(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DetachDeleteNode(lhsLP, varFor("x")), 34.5),
      planDescription(id, "DetachDelete", SingleChild(lhsPD), Seq(details("x")), Set("a"))
    )

    assertGood(
      attach(DetachDeletePath(lhsLP, varFor("x")), 34.5),
      planDescription(id, "DetachDelete", SingleChild(lhsPD), Seq(details("x")), Set("a"))
    )
  }

  test("Eager") {
    assertGood(
      attach(Eager(lhsLP, ListSet.empty), 34.5),
      planDescription(id, "Eager", SingleChild(lhsPD), Seq.empty, Set("a"))
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.Unknown)), 34.5),
      planDescription(id, "Eager", SingleChild(lhsPD), Seq.empty, Set("a"))
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.UpdateStrategyEager)), 34.5),
      planDescription(id, "Eager", SingleChild(lhsPD), Seq(details(Seq("updateStrategy=eager"))), Set("a"))
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.LabelReadRemoveConflict(label("Foo")))), 34.5),
      planDescription(
        id,
        "Eager",
        SingleChild(lhsPD),
        Seq(details(Seq("read/remove conflict for label: Foo"))),
        Set("a")
      )
    )

    {
      val reason = EagernessReason.LabelReadRemoveConflict(label("Foo")).withConflict(Conflict(Id(1), Id(2)))
      assertGood(
        attach(Eager(lhsLP, ListSet(reason)), 34.5),
        planDescription(
          id,
          "Eager",
          SingleChild(lhsPD),
          Seq(details(Seq("read/remove conflict for label: Foo (Operator: 1 vs 2)"))),
          Set("a")
        )
      )
    }

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.LabelReadSetConflict(label("Foo")))), 34.5),
      planDescription(id, "Eager", SingleChild(lhsPD), Seq(details(Seq("read/set conflict for label: Foo"))), Set("a"))
    )

    assertGood(
      attach(Eager(lhsRelLP, ListSet(EagernessReason.TypeReadSetConflict(relType("Foo")))), 34.5),
      planDescription(
        id,
        "Eager",
        SingleChild(lhsRelPD),
        Seq(details(Seq("read/set conflict for relationship type: Foo"))),
        Set("r", "a", "b")
      )
    )

    assertGood(
      attach(Eager(lhsLP, ListSet(EagernessReason.ReadDeleteConflict("b"))), 34.5),
      planDescription(
        id,
        "Eager",
        SingleChild(lhsPD),
        Seq(details(Seq("read/delete conflict for variable: b"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Eager(
          lhsLP,
          ListSet(EagernessReason.ReadDeleteConflict("b"), EagernessReason.LabelReadSetConflict(label("Foo")))
        ),
        34.5
      ),
      planDescription(
        id,
        "Eager",
        SingleChild(lhsPD),
        Seq(details(Seq("read/delete conflict for variable: b", "read/set conflict for label: Foo"))),
        Set("a")
      )
    )

    assertGood(
      attach(
        Eager(
          lhsRelLP,
          ListSet(EagernessReason.ReadDeleteConflict("r"), EagernessReason.TypeReadSetConflict(relType("Foo")))
        ),
        34.5
      ),
      planDescription(
        id,
        "Eager",
        SingleChild(lhsRelPD),
        Seq(details(Seq("read/delete conflict for variable: r", "read/set conflict for relationship type: Foo"))),
        Set("r", "a", "b")
      )
    )

    {
      val reason1 = EagernessReason.ReadDeleteConflict("b").withConflict(Conflict(Id(1), Id(2)))
      val reason2 = EagernessReason.LabelReadSetConflict(label("Foo")).withConflict(Conflict(Id(3), Id(4)))
      assertGood(
        attach(
          Eager(
            lhsLP,
            ListSet(reason1, reason2)
          ),
          34.5
        ),
        planDescription(
          id,
          "Eager",
          SingleChild(lhsPD),
          Seq(details(Seq(
            "read/delete conflict for variable: b (Operator: 1 vs 2)",
            "read/set conflict for label: Foo (Operator: 3 vs 4)"
          ))),
          Set("a")
        )
      )
    }

    {
      val reason1 = EagernessReason.ReadDeleteConflict("r").withConflict(Conflict(Id(1), Id(2)))
      val reason2 = EagernessReason.TypeReadSetConflict(relType("Foo")).withConflict(Conflict(Id(3), Id(4)))
      assertGood(
        attach(
          Eager(
            lhsRelLP,
            ListSet(reason1, reason2)
          ),
          34.5
        ),
        planDescription(
          id,
          "Eager",
          SingleChild(lhsRelPD),
          Seq(details(Seq(
            "read/delete conflict for variable: r (Operator: 1 vs 2)",
            "read/set conflict for relationship type: Foo (Operator: 3 vs 4)"
          ))),
          Set("r", "a", "b")
        )
      )
    }
    {
      val reason = {
        EagernessReason.Summarized(Map(
          EagernessReason.ReadDeleteConflict("r") -> EagernessReason.SummaryEntry(Conflict(Id(1), Id(2)), 123),
          EagernessReason.TypeReadSetConflict(relType("REL")) -> EagernessReason.SummaryEntry(
            Conflict(Id(3), Id(4)),
            321
          )
        ))
      }
      assertGood(
        attach(
          Eager(
            lhsRelLP,
            ListSet(reason)
          ),
          34.5
        ),
        planDescription(
          id,
          "Eager",
          SingleChild(lhsRelPD),
          Seq(details(Seq(
            "read/delete conflict for variable: r (Operator: 1 vs 2, and 122 more conflicting operators)",
            "read/set conflict for relationship type: REL (Operator: 3 vs 4, and 320 more conflicting operators)"
          ))),
          Set("r", "a", "b")
        )
      )
    }
  }

  test("EmptyResult") {
    assertGood(
      attach(EmptyResult(lhsLP), 34.5),
      planDescription(id, "EmptyResult", SingleChild(lhsPD), Seq.empty, Set("a"))
    )
  }

  test("ErrorPlan") {
    assertGood(
      attach(ErrorPlan(lhsLP, new Exception("Exception")), 12.5),
      planDescription(id, "Error", SingleChild(lhsPD), Seq.empty, Set("a"))
    )
  }

  test("Expand") {
    assertGood(
      attach(Expand(lhsLP, varFor("a"), OUTGOING, Seq.empty, varFor("  UNNAMED4"), varFor("r1"), ExpandAll), 95.0),
      planDescription(
        id,
        "Expand(All)",
        SingleChild(lhsPD),
        Seq(details(s"(a)-[r1]->(${anonVar("4")})")),
        Set("a", anonVar("4"), "r1")
      )
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), INCOMING, Seq(relType("R")), varFor("y"), varFor("r1"), ExpandAll), 95.0),
      planDescription(id, "Expand(All)", SingleChild(lhsPD), Seq(details("(a)<-[r1:R]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(
        Expand(lhsLP, varFor("a"), BOTH, Seq(relType("R1"), relType("R2")), varFor("y"), varFor("r1"), ExpandAll),
        95.0
      ),
      planDescription(id, "Expand(All)", SingleChild(lhsPD), Seq(details("(a)-[r1:R1|R2]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), OUTGOING, Seq.empty, varFor("y"), varFor("r1"), ExpandInto), 113.0),
      planDescription(id, "Expand(Into)", SingleChild(lhsPD), Seq(details("(a)-[r1]->(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(Expand(lhsLP, varFor("a"), INCOMING, Seq(relType("R")), varFor("y"), varFor("r1"), ExpandInto), 113.0),
      planDescription(id, "Expand(Into)", SingleChild(lhsPD), Seq(details("(a)<-[r1:R]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(
        Expand(lhsLP, varFor("a"), BOTH, Seq(relType("R1"), relType("R2")), varFor("y"), varFor("r1"), ExpandInto),
        113.0
      ),
      planDescription(id, "Expand(Into)", SingleChild(lhsPD), Seq(details("(a)-[r1:R1|R2]-(y)")), Set("a", "y", "r1"))
    )

    assertGood(
      attach(
        Expand(
          lhsLP,
          varFor("a"),
          BOTH,
          Seq(relType("R1"), relType("R2")),
          varFor("y"),
          varFor("  UNNAMED1"),
          ExpandInto
        ),
        113.0
      ),
      planDescription(
        id,
        "Expand(Into)",
        SingleChild(lhsPD),
        Seq(details(s"(a)-[${anonVar("1")}:R1|R2]-(y)")),
        Set("a", "y", anonVar("1"))
      )
    )
  }

  test("Limit") {
    assertGood(
      attach(Limit(lhsLP, number("1")), 113.0),
      planDescription(id, "Limit", SingleChild(lhsPD), Seq(details("1")), Set("a"))
    )
  }

  test("CachedProperties") {
    assertGood(
      attach(CacheProperties(lhsLP, Set(cachedProp("n", "prop"))), 113.0),
      planDescription(id, "CacheProperties", SingleChild(lhsPD), Seq(details("cache[n.prop]")), Set("a"))
    )

    assertGood(
      attach(CacheProperties(lhsLP, Set(prop("n", "prop1"), prop("n", "prop2"))), 113.0),
      planDescription(id, "CacheProperties", SingleChild(lhsPD), Seq(details(Seq("n.prop1", "n.prop2"))), Set("a"))
    )
  }

  test("OptionalExpand") {
    val predicate1 = Equals(varFor("to"), parameter("  AUTOINT2", CTInteger))(pos)
    val predicate2 = LessThan(varFor("a"), number("2002"))(pos)

    // Without predicate
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          Seq(relType("R")),
          varFor("  UNNAMED5"),
          varFor("r"),
          ExpandAll,
          None
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        SingleChild(lhsPD),
        Seq(details(s"(a)<-[r:R]-(${anonVar("5")})")),
        Set("a", anonVar("5"), "r")
      )
    )

    // With predicate and no relationship types
    assertGood(
      attach(
        OptionalExpand(lhsLP, varFor("a"), OUTGOING, Seq(), varFor("to"), varFor("r"), ExpandAll, Some(predicate1)),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        SingleChild(lhsPD),
        Seq(details("(a)-[r]->(to) WHERE to = $autoint_2")),
        Set("a", "to", "r")
      )
    )

    // With predicate and relationship types
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          BOTH,
          Seq(relType("R")),
          varFor("to"),
          varFor("r"),
          ExpandAll,
          Some(And(predicate1, predicate2)(pos))
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        SingleChild(lhsPD),
        Seq(details("(a)-[r:R]-(to) WHERE to = $autoint_2 AND a < 2002")),
        Set("a", "to", "r")
      )
    )

    // With multiple relationship types
    assertGood(
      attach(
        OptionalExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          Seq(relType("R1"), relType("R2")),
          varFor("to"),
          varFor("r"),
          ExpandAll,
          None
        ),
        12.0
      ),
      planDescription(
        id,
        "OptionalExpand(All)",
        SingleChild(lhsPD),
        Seq(details("(a)<-[r:R1|R2]-(to)")),
        Set("a", "to", "r")
      )
    )
  }

  test("Projection") {
    val pathExpression = PathExpression(NodePathStep(
      varFor("c"),
      SingleRelationshipPathStep(varFor("  UNNAMED42"), OUTGOING, Some(varFor("  UNNAMED32")), NilPathStep()(pos))(pos)
    )(pos))(pos)

    assertGood(
      attach(Projection(lhsLP, Map(varFor("x") -> varFor("y"))), 2345.0),
      planDescription(id, "Projection", SingleChild(lhsPD), Seq(details("y AS x")), Set("a", "x"))
    )

    assertGood(
      attach(Projection(lhsLP, Map(varFor("x") -> pathExpression)), 2345.0),
      planDescription(
        id,
        "Projection",
        SingleChild(lhsPD),
        Seq(details(s"(c)-[${anonVar("42")}]->(${anonVar("32")}) AS x")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(
        Projection(lhsLP, Map(varFor("x") -> varFor("  UNNAMED42"), varFor("n.prop") -> prop("n", "prop"))),
        2345.0
      ),
      planDescription(
        id,
        "Projection",
        SingleChild(lhsPD),
        Seq(details(Seq(s"${anonVar("42")} AS x", "n.prop AS `n.prop`"))),
        Set("a", "x", "`n.prop`")
      )
    )

    // Projection should show up in the order they were specified in
    assertGood(
      attach(
        Projection(lhsLP, Map(varFor("n.prop") -> prop("n", "prop"), varFor("x") -> varFor("y"))),
        2345.0
      ),
      planDescription(
        id,
        "Projection",
        SingleChild(lhsPD),
        Seq(details(Seq("n.prop AS `n.prop`", "y AS x"))),
        Set("a", "x", "`n.prop`")
      )
    )
  }

  test("Selection") {
    val predicate1 = In(varFor("x"), parameter("  AUTOLIST1", CTList(CTInteger)))(pos)
    val predicate2 = LessThan(prop("a", "prop1"), number("2002"))(pos)
    val predicate3 = GreaterThanOrEqual(cachedProp("a", "prop1"), number("1001"))(pos)
    val predicate4 = AndedPropertyInequalities(varFor("a"), prop("a", "prop"), NonEmptyList(predicate2, predicate3))

    assertGood(
      attach(Selection(Seq(predicate1, predicate4), lhsLP), 2345.0),
      planDescription(
        id,
        "Filter",
        SingleChild(lhsPD),
        Seq(details("x IN $autolist_1 AND a.prop1 < 2002 AND cache[a.prop1] >= 1001")),
        Set("a")
      )
    )
  }

  test("Skip") {
    assertGood(
      attach(Skip(lhsLP, number("78")), 2345.0),
      planDescription(id, "Skip", SingleChild(lhsPD), Seq(details("78")), Set("a"))
    )
  }

  test("FindShortestPaths") {
    val relPredicate =
      VariablePredicate(varFor("r"), Equals(prop("r", "prop"), parameter("  AUTOSTRING1", CTString))(pos))

    // with(out) relationship types
    // length variations

    // simple length
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            None,
            PatternRelationship(
              varFor("r"),
              (varFor("  UNNAMED23"), varFor("y")),
              SemanticDirection.BOTH,
              Seq.empty,
              SimplePatternLength
            ),
            single = true
          )(null),
          Seq.empty,
          Seq.empty,
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        SingleChild(lhsPD),
        Seq(details(s"(${anonVar("23")})-[r]-(y)")),
        Set("r", "a", anonVar("23"), "y")
      )
    )

    // fixed length of 1
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            None,
            PatternRelationship(
              varFor("r"),
              (varFor("  UNNAMED23"), varFor("y")),
              SemanticDirection.BOTH,
              Seq.empty,
              VarPatternLength(1, Some(1))
            ),
            single = true
          )(null),
          Seq.empty,
          Seq.empty,
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        SingleChild(lhsPD),
        Seq(details(s"(${anonVar("23")})-[r]-(y)")),
        Set("r", "a", anonVar("23"), "y")
      )
    )

    // without: predicates, path name, relationship type
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            None,
            PatternRelationship(
              varFor("r"),
              (varFor("  UNNAMED23"), varFor("y")),
              SemanticDirection.BOTH,
              Seq.empty,
              VarPatternLength(2, Some(4))
            ),
            single = true
          )(null),
          Seq.empty,
          Seq.empty,
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        SingleChild(lhsPD),
        Seq(details(s"(${anonVar("23")})-[r*2..4]-(y)")),
        Set("r", "a", anonVar("23"), "y")
      )
    )

    // with: predicates, path name, relationship type
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            Some(varFor("  UNNAMED12")),
            PatternRelationship(
              varFor("r"),
              (varFor("a"), varFor("y")),
              SemanticDirection.BOTH,
              Seq(relType("R")),
              VarPatternLength(2, Some(4))
            ),
            single = true
          )(null),
          Seq.empty,
          Seq(relPredicate),
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        SingleChild(lhsPD),
        Seq(details(
          s"${anonVar("12")} = (a)-[r:R*2..4]-(y) WHERE all(r IN relationships(${anonVar("12")}) WHERE r.prop = $$autostring_1)"
        )),
        Set("r", "a", "y", anonVar("12"))
      )
    )

    // with: predicates, UNNAMED variables, relationship type, unbounded max length
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            Some(varFor("  UNNAMED12")),
            PatternRelationship(
              varFor("r"),
              (varFor("a"), varFor("  UNNAMED2")),
              SemanticDirection.BOTH,
              Seq(relType("R")),
              VarPatternLength(2, None)
            ),
            single = true
          )(null),
          Seq.empty,
          Seq(relPredicate),
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        SingleChild(lhsPD),
        Seq(details(
          s"${anonVar("12")} = (a)-[r:R*2..]-(${anonVar("2")}) WHERE all(r IN relationships(${anonVar("12")}) WHERE r.prop = $$autostring_1)"
        )),
        Set("r", "a", anonVar("2"), anonVar("12"))
      )
    )

    // with: predicates, UNNAMED variables, relationship type, unbounded max length
    assertGood(
      attach(
        FindShortestPaths(
          lhsLP,
          ShortestRelationshipPattern(
            Some(varFor("  UNNAMED12")),
            PatternRelationship(
              varFor("r"),
              (varFor("a"), varFor("  UNNAMED2")),
              SemanticDirection.BOTH,
              Seq(relType("R")),
              VarPatternLength(1, None)
            ),
            single = true
          )(null),
          Seq.empty,
          Seq(relPredicate),
          Seq.empty
        ),
        2345.0
      ),
      planDescription(
        id,
        "ShortestPath",
        SingleChild(lhsPD),
        Seq(details(
          s"${anonVar("12")} = (a)-[r:R*]-(${anonVar("2")}) WHERE all(r IN relationships(${anonVar("12")}) WHERE r.prop = $$autostring_1)"
        )),
        Set("r", "a", anonVar("2"), anonVar("12"))
      )
    )
  }

  test("StatefulShortestPath") {
    val solvedExpressionStr = "SHORTEST 5 PATHS (a)-[`  UNNAMED0`]->*(`  b@45`)"
    val nfa = {
      val builder = new NFABuilder(varFor("a"))
      builder
        .setFinalState(builder.getLastState)
        .build()
    }
    assertGood(
      attach(
        StatefulShortestPath(
          lhsLP,
          varFor("a"),
          varFor("b"),
          nfa,
          ExpandAll,
          None,
          Set.empty,
          Set.empty,
          Set.empty,
          Set.empty,
          Selector.Shortest(5),
          solvedExpressionStr,
          false
        ),
        2345.0
      ),
      planDescription(
        id,
        "StatefulShortestPath(All)",
        SingleChild(lhsPD),
        Seq(details(
          "SHORTEST 5 PATHS (a)-[`anon_0`]->*(`b`)"
        )),
        Set("a")
      )
    )
    assertGood(
      attach(
        StatefulShortestPath(
          lhsLP,
          varFor("a"),
          varFor("b"),
          nfa,
          ExpandInto,
          None,
          Set.empty,
          Set.empty,
          Set.empty,
          Set.empty,
          Selector.Shortest(5),
          solvedExpressionStr,
          false
        ),
        2345.0
      ),
      planDescription(
        id,
        "StatefulShortestPath(Into)",
        SingleChild(lhsPD),
        Seq(details(
          "SHORTEST 5 PATHS (a)-[`anon_0`]->*(`b`)"
        )),
        Set("a")
      )
    )
  }

  test("Optional") {
    assertGood(
      attach(Optional(lhsLP, Set(varFor("a"))), 113.0),
      planDescription(id, "Optional", SingleChild(lhsPD), Seq(details("a")), Set("a"))
    )
  }

  test("Anti") {
    assertGood(attach(Anti(lhsLP), 113.0), planDescription(id, "Anti", SingleChild(lhsPD), Seq.empty, Set("a")))
  }

  test("ProjectEndpoints") {
    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq.empty,
          direction = SemanticDirection.OUTGOING,
          VarPatternLength(1, Some(1))
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        SingleChild(lhsPD),
        Seq(details("(start)-[r]->(end)")),
        Set("a", "start", "r", "end")
      )
    )

    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq.empty,
          direction = SemanticDirection.OUTGOING,
          SimplePatternLength
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        SingleChild(lhsPD),
        Seq(details("(start)-[r]->(end)")),
        Set("a", "start", "r", "end")
      )
    )

    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq.empty,
          direction = SemanticDirection.INCOMING,
          VarPatternLength(1, None)
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        SingleChild(lhsPD),
        Seq(details("(start)<-[r*]-(end)")),
        Set("a", "start", "r", "end")
      )
    )

    assertGood(
      attach(
        ProjectEndpoints(
          lhsLP,
          varFor("r"),
          varFor("start"),
          startInScope = true,
          varFor("end"),
          endInScope = true,
          Seq(relType("R")),
          direction = SemanticDirection.BOTH,
          VarPatternLength(1, Some(3))
        ),
        234.2
      ),
      planDescription(
        id,
        "ProjectEndpoints",
        SingleChild(lhsPD),
        Seq(details("(start)-[r:R*..3]-(end)")),
        Set("a", "start", "r", "end")
      )
    )
  }

  test("VarExpand") {
    val predicate = (varName: String) => Equals(prop(varName, "prop"), parameter("  AUTODOUBLE1", CTFloat))(pos)
    val nodePredicate = VariablePredicate(varFor("x"), predicate("x"))
    val nodePredicate2 = VariablePredicate(varFor("x2"), predicate("x2"))
    val relationshipPredicate = VariablePredicate(varFor("r"), predicate("r"))

    // -- PruningVarExpand --

    // With nodePredicate and relationshipPredicate
    assertGood(
      attach(
        PruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          varFor("y"),
          1,
          4,
          Seq(nodePredicate),
          Seq(relationshipPredicate)
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        SingleChild(lhsPD),
        Seq(details(
          "p = (a)-[:R*..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(r IN relationships(p) WHERE r.prop = $autodouble_1)"
        )),
        Set("a", "y")
      )
    )

    // With nodePredicate, without relationshipPredicate
    assertGood(
      attach(
        PruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          varFor("y"),
          2,
          4,
          Seq(nodePredicate),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        SingleChild(lhsPD),
        Seq(details("p = (a)-[:R*2..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1)")),
        Set("a", "y")
      )
    )

    // With 2 nodePredicates, without relationshipPredicate
    assertGood(
      attach(
        PruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          varFor("y"),
          2,
          4,
          Seq(nodePredicate, nodePredicate2),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        SingleChild(lhsPD),
        Seq(details(
          "p = (a)-[:R*2..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(x2 IN nodes(p) WHERE x2.prop = $autodouble_1)"
        )),
        Set("a", "y")
      )
    )

    // Without predicates, without relationship type
    assertGood(
      attach(
        PruningVarExpand(lhsLP, varFor("a"), SemanticDirection.OUTGOING, Seq(), varFor("y"), 2, 4, Seq(), Seq()),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning)",
        SingleChild(lhsPD),
        Seq(details("(a)-[*2..4]->(y)")),
        Set("a", "y")
      )
    )

    // -- BFSPruningVarExpand --

    // With nodePredicate and relationshipPredicate
    assertGood(
      attach(
        BFSPruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          varFor("y"),
          includeStartNode = false,
          maxLength = 4,
          depthName = Some(varFor("depth")),
          mode = ExpandAll,
          nodePredicates = Seq(nodePredicate),
          relationshipPredicates = Seq(relationshipPredicate)
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning,BFS,All)",
        SingleChild(lhsPD),
        Seq(details(
          "p = (a)-[:R*..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(r IN relationships(p) WHERE r.prop = $autodouble_1) depth"
        )),
        Set("a", "y", "depth")
      )
    )

    // With nodePredicate, without relationshipPredicate
    assertGood(
      attach(
        BFSPruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(relType("R")),
          varFor("y"),
          includeStartNode = true,
          4,
          depthName = Some(varFor("depth")),
          mode = ExpandAll,
          Seq(nodePredicate),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning,BFS,All)",
        SingleChild(lhsPD),
        Seq(details("p = (a)-[:R*0..4]->(y) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) depth")),
        Set("a", "y", "depth")
      )
    )

    // Without predicates, without relationship type
    assertGood(
      attach(
        BFSPruningVarExpand(
          lhsLP,
          varFor("a"),
          SemanticDirection.OUTGOING,
          Seq(),
          varFor("y"),
          includeStartNode = false,
          4,
          depthName = Some(varFor("depth")),
          mode = ExpandInto,
          Seq(),
          Seq()
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(Pruning,BFS,Into)",
        SingleChild(lhsPD),
        Seq(details("(a)-[*..4]->(y) depth")),
        Set("a", "y", "depth")
      )
    )

    // -- VarExpand --

    // With unnamed variables, without predicates
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          INCOMING,
          Seq(relType("LIKES"), relType("LOVES")),
          varFor("  UNNAMED123"),
          varFor("  UNNAMED99"),
          VarPatternLength(1, Some(1)),
          ExpandAll
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        SingleChild(lhsPD),
        Seq(details(s"(a)<-[${anonVar("99")}:LIKES|LOVES]-(${anonVar("123")})")),
        Set("a", anonVar("99"), anonVar("123"))
      )
    )

    // With nodePredicate and relationshipPredicate
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          INCOMING,
          Seq(relType("LIKES"), relType("LOVES")),
          varFor("to"),
          varFor("rel"),
          VarPatternLength(1, Some(1)),
          ExpandAll,
          Seq(nodePredicate),
          Seq(relationshipPredicate)
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        SingleChild(lhsPD),
        Seq(details(
          "p = (a)<-[rel:LIKES|LOVES]-(to) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1) AND all(r IN relationships(p) WHERE r.prop = $autodouble_1)"
        )),
        Set("a", "to", "rel")
      )
    )

    // With nodePredicate, without relationshipPredicate, with length
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          INCOMING,
          INCOMING,
          Seq(relType("LIKES"), relType("LOVES")),
          varFor("to"),
          varFor("rel"),
          VarPatternLength(2, Some(3)),
          ExpandAll,
          Seq(nodePredicate)
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        SingleChild(lhsPD),
        Seq(details("p = (a)<-[rel:LIKES|LOVES*2..3]-(to) WHERE all(x IN nodes(p) WHERE x.prop = $autodouble_1)")),
        Set("a", "to", "rel")
      )
    )

    // With unbounded length
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          OUTGOING,
          OUTGOING,
          Seq(relType("LIKES"), relType("LOVES")),
          varFor("to"),
          varFor("rel"),
          VarPatternLength(2, None),
          ExpandAll
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        SingleChild(lhsPD),
        Seq(details("(a)-[rel:LIKES|LOVES*2..]->(to)")),
        Set("a", "to", "rel")
      )
    )

    // With fixed length
    assertGood(
      attach(
        VarExpand(
          lhsLP,
          varFor("a"),
          OUTGOING,
          OUTGOING,
          Seq(relType("LIKES"), relType("LOVES")),
          varFor("to"),
          varFor("rel"),
          VarPatternLength(2, Some(2)),
          ExpandAll
        ),
        1.0
      ),
      planDescription(
        id,
        "VarLengthExpand(All)",
        SingleChild(lhsPD),
        Seq(details("(a)-[rel:LIKES|LOVES*2]->(to)")),
        Set("a", "to", "rel")
      )
    )
  }

  test("Updates") {
    // RemoveLabels
    assertGood(
      attach(RemoveLabels(lhsLP, varFor("x"), Set(label("L1"))), 1.0),
      planDescription(id, "RemoveLabels", SingleChild(lhsPD), Seq(details("x:L1")), Set("a", "x"))
    )

    assertGood(
      attach(RemoveLabels(lhsLP, varFor("x"), Set(label("L1"), label("L2"))), 1.0),
      planDescription(id, "RemoveLabels", SingleChild(lhsPD), Seq(details("x:L1:L2")), Set("a", "x"))
    )

    // SetLabels
    assertGood(
      attach(SetLabels(lhsLP, varFor("x"), Set(label("L1"))), 1.0),
      planDescription(id, "SetLabels", SingleChild(lhsPD), Seq(details("x:L1")), Set("a", "x"))
    )

    assertGood(
      attach(SetLabels(lhsLP, varFor("x"), Set(label("L1"), label("L2"))), 1.0),
      planDescription(id, "SetLabels", SingleChild(lhsPD), Seq(details("x:L1:L2")), Set("a", "x"))
    )

    val map = MapExpression(Seq(
      (key("foo"), number("1")),
      (key("bar"), number("2"))
    ))(pos)
    val prettifiedMapExpr = "{foo: 1, bar: 2}"

    // Set From Map
    assertGood(
      attach(SetNodePropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = true), 1.0),
      planDescription(
        id,
        "SetNodePropertiesFromMap",
        SingleChild(lhsPD),
        Seq(details(s"x = $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetNodePropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = false), 1.0),
      planDescription(
        id,
        "SetNodePropertiesFromMap",
        SingleChild(lhsPD),
        Seq(details(s"x += $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetRelationshipPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = true), 1.0),
      planDescription(
        id,
        "SetRelationshipPropertiesFromMap",
        SingleChild(lhsPD),
        Seq(details(s"x = $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetRelationshipPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = false), 1.0),
      planDescription(
        id,
        "SetRelationshipPropertiesFromMap",
        SingleChild(lhsPD),
        Seq(details(s"x += $prettifiedMapExpr")),
        Set("a", "x")
      )
    )

    assertGood(
      attach(SetPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = true), 1.0),
      planDescription(id, "SetPropertiesFromMap", SingleChild(lhsPD), Seq(details(s"x = $prettifiedMapExpr")), Set("a"))
    )

    assertGood(
      attach(SetPropertiesFromMap(lhsLP, varFor("x"), map, removeOtherProps = false), 1.0),
      planDescription(
        id,
        "SetPropertiesFromMap",
        SingleChild(lhsPD),
        Seq(details(s"x += $prettifiedMapExpr")),
        Set("a")
      )
    )

    // Set
    assertGood(
      attach(SetProperty(lhsLP, varFor("x"), key("prop"), number("1")), 1.0),
      planDescription(id, "SetProperty", SingleChild(lhsPD), Seq(details("x.prop = 1")), Set("a"))
    )

    assertGood(
      attach(SetNodeProperty(lhsLP, varFor("x"), key("prop"), number("1")), 1.0),
      planDescription(id, "SetProperty", SingleChild(lhsPD), Seq(details("x.prop = 1")), Set("a", "x"))
    )

    assertGood(
      attach(SetRelationshipProperty(lhsLP, varFor("x"), key("prop"), number("1")), 1.0),
      planDescription(id, "SetProperty", SingleChild(lhsPD), Seq(details("x.prop = 1")), Set("a", "x"))
    )
    // Set multiple properties
    assertGood(
      attach(SetProperties(lhsLP, varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2")))), 1.0),
      planDescription(id, "SetProperties", SingleChild(lhsPD), Seq(details("x.p1 = 1, x.p2 = 2")), Set("a"))
    )

    assertGood(
      attach(SetNodeProperties(lhsLP, varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2")))), 1.0),
      planDescription(id, "SetProperties", SingleChild(lhsPD), Seq(details("x.p1 = 1, x.p2 = 2")), Set("a", "x"))
    )

    assertGood(
      attach(SetNodeProperties(lhsLP, varFor("x"), Seq((key("p1"), number("1")), (key("p2"), number("2")))), 1.0),
      planDescription(id, "SetProperties", SingleChild(lhsPD), Seq(details("x.p1 = 1, x.p2 = 2")), Set("a", "x"))
    )

  }

  test("Sort") {
    // Sort
    assertGood(
      attach(Sort(lhsLP, Seq(Ascending(varFor("a")))), 1.0),
      planDescription(id, "Sort", SingleChild(lhsPD), Seq(details("a ASC")), Set("a"))
    )

    assertGood(
      attach(Sort(lhsLP, Seq(Descending(varFor("a")), Ascending(varFor("y")))), 1.0),
      planDescription(id, "Sort", SingleChild(lhsPD), Seq(details("a DESC, y ASC")), Set("a"))
    )

    // Top
    assertGood(
      attach(Top(lhsLP, Seq(Ascending(varFor("a"))), number("3")), 1.0),
      planDescription(id, "Top", SingleChild(lhsPD), Seq(details("a ASC LIMIT 3")), Set("a"))
    )

    assertGood(
      attach(Top(lhsLP, Seq(Descending(varFor("a")), Ascending(varFor("y"))), number("3")), 1.0),
      planDescription(id, "Top", SingleChild(lhsPD), Seq(details("a DESC, y ASC LIMIT 3")), Set("a"))
    )

    // Partial Sort
    assertGood(
      attach(PartialSort(lhsLP, Seq(Ascending(varFor("a"))), Seq(Descending(varFor("y")))), 1.0),
      planDescription(id, "PartialSort", SingleChild(lhsPD), Seq(details("a ASC, y DESC")), Set("a"))
    )

    // Partial Top
    assertGood(
      attach(PartialTop(lhsLP, Seq(Ascending(varFor("a"))), Seq(Descending(varFor("y"))), number("3")), 1.0),
      planDescription(id, "PartialTop", SingleChild(lhsPD), Seq(details("a ASC, y DESC LIMIT 3")), Set("a"))
    )
  }

  test("Unwind") {
    assertGood(
      attach(UnwindCollection(lhsLP, varFor("x"), varFor("list")), 1.0),
      planDescription(id, "Unwind", SingleChild(lhsPD), Seq(details("list AS x")), Set("a", "x"))
    )
  }

  test("PartitionedUnwind") {
    assertGood(
      attach(PartitionedUnwindCollection(lhsLP, varFor("x"), varFor("list")), 1.0),
      planDescription(id, "PartitionedUnwind", SingleChild(lhsPD), Seq(details("list AS x")), Set("a", "x"))
    )
  }

  test("PreserveOrder") {
    assertGood(
      attach(PreserveOrder(lhsLP), 1.0),
      planDescription(id, "PreserveOrder", SingleChild(lhsPD), Seq.empty, Set("a"))
    )
  }

  test("Admin") {
    val adminPlanDescription: PlanDescriptionImpl =
      planDescription(id, "AdministrationCommand", NoChildren, Seq.empty, Set.empty)

    assertGood(
      attach(
        AllowedNonAdministrationCommands(
          SingleQuery(Seq(ShowProceduresClause(None, None, List.empty, yieldAll = false)(pos)))(pos)
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(ShowUsers(privLhsLP, List(), None, None), 1.0), adminPlanDescription)

    assertGood(attach(ShowCurrentUser(List(), None, None), 1.0), adminPlanDescription)

    assertGood(
      attach(
        CreateUser(
          privLhsLP,
          util.Left("name"),
          isEncryptedPassword = false,
          varFor("password"),
          requirePasswordChange = false,
          suspended = None,
          defaultDatabase = None
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
          isEncryptedPassword = Some(true),
          None,
          requirePasswordChange = Some(true),
          suspended = Some(false),
          defaultDatabase = None
        ),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(SetOwnPassword(stringLiteral("oldPassword"), stringLiteral("newPassword")), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(ShowRoles(privLhsLP, withUsers = false, showAll = true, List(), None, None), 1.0),
      adminPlanDescription
    )

    assertGood(attach(DropRole(privLhsLP, util.Left("role")), 1.0), adminPlanDescription)

    assertGood(attach(CreateRole(privLhsLP, util.Left("role")), 1.0), adminPlanDescription)

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
          UserQualifier(util.Left("user1"))(pos),
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
          UserQualifier(util.Left("user1"))(pos),
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
            Some(Variable("n")(pos)),
            Equals(Property(Variable("n")(pos), PropertyKeyName("prop1")(pos))(pos), Null.NULL)(pos)
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
          ShowUsersPrivileges(List(util.Left("user1"), util.Right(parameter("user2", CTString))))(pos),
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
          ShowUsersPrivileges(List(util.Left("user1"), util.Right(parameter("user2", CTString))))(pos),
          asRevoke = false,
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
        ShowDatabase(AllDatabasesScope()(pos), verbose = false, List(varFor("foo"), varFor("bar")), None, None),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(CreateDatabase(privLhsLP, util.Left("db1"), NoOptions, IfExistsDoNothing, isComposite = false, None), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(DropDatabase(privLhsLP, NamespacedName("db1")(pos), DumpData, forceComposite = false), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(
        AlterDatabase(privLhsLP, NamespacedName("db1")(pos), Some(ReadOnlyAccess), None, NoOptions, Set.empty),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(
        AlterDatabase(privLhsLP, NamespacedName("db1")(pos), Some(ReadWriteAccess), None, NoOptions, Set.empty),
        1.0
      ),
      adminPlanDescription
    )

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

    assertGood(attach(StartDatabase(privLhsLP, NamespacedName("db1")(pos)), 1.0), adminPlanDescription)

    assertGood(attach(StopDatabase(privLhsLP, NamespacedName("db1")(pos)), 1.0), adminPlanDescription)

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
          util.Left("user"),
          varFor("password"),
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
          Some(privLhsLP),
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
          Some(Left(Map("some" -> StringLiteral("prop")(pos))))
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

    assertGood(
      attach(EnsureValidNonSystemDatabase(privLhsLP, NamespacedName("db1")(pos), "action1"), 1.0),
      adminPlanDescription
    )

    assertGood(
      attach(
        EnsureValidNumberOfDatabases(CreateDatabase(
          privLhsLP,
          util.Left("db1"),
          NoOptions,
          IfExistsDoNothing,
          isComposite = false,
          None
        )),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(attach(LogSystemCommand(privLhsLP, "command1"), 1.0), adminPlanDescription)

    assertGood(attach(DoNothingIfNotExists(privLhsLP, "User", util.Left("user1"), "delete"), 1.0), adminPlanDescription)

    assertGood(attach(DoNothingIfExists(privLhsLP, "User", util.Left("user1")), 1.0), adminPlanDescription)

    assertGood(
      attach(
        EnsureNodeExists(privLhsLP, "User", util.Left("user1"), labelDescription = "User", action = "delete"),
        1.0
      ),
      adminPlanDescription
    )

    assertGood(
      attach(AssertNotCurrentUser(privLhsLP, util.Left("user1"), "verb1", "validation message"), 1.0),
      adminPlanDescription
    )

    assertGood(attach(AssertAllowedDbmsActionsOrSelf(util.Left("user1"), DropRoleAction), 1.0), adminPlanDescription)

    assertGood(
      attach(AssertAllowedDatabaseAction(StopDatabaseAction, NamespacedName("db1")(pos), None), 1.0),
      adminPlanDescription
    )

    assertGood(attach(AssertManagementActionNotBlocked(CreateDatabaseAction), 1.0), adminPlanDescription)

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
          IndefiniteWait
        ),
        1.0
      ),
      adminPlanDescription
    )
  }

  test("AntiConditionalApply") {
    assertGood(
      attach(AntiConditionalApply(lhsLP, rhsLP, Seq(varFor("c"))), 2345.0),
      planDescription(id, "AntiConditionalApply", TwoChildren(lhsPD, rhsPD), Seq.empty, Set("a", "b", "c"))
    )
  }

  test("AntiSemiApply") {
    assertGood(
      attach(AntiSemiApply(lhsLP, rhsLP), 2345.0),
      planDescription(id, "AntiSemiApply", TwoChildren(lhsPD, rhsPD), Seq.empty, Set("a"))
    )
  }

  test("ConditionalApply") {
    assertGood(
      attach(ConditionalApply(lhsLP, rhsLP, Seq(varFor("c"))), 2345.0),
      planDescription(id, "ConditionalApply", TwoChildren(lhsPD, rhsPD), Seq.empty, Set("a", "b", "c"))
    )
  }

  test("Apply") {
    assertGood(
      attach(Apply(lhsLP, rhsLP), 2345.0),
      planDescription(id, "Apply", TwoChildren(lhsPD, rhsPD), Seq.empty, Set("a", "b"))
    )
  }

  test("AssertSameNode") {
    assertGood(
      attach(AssertSameNode(varFor("n"), lhsLP, rhsLP), 2345.0),
      planDescription(id, "AssertSameNode", TwoChildren(lhsPD, rhsPD), Seq(details(Seq("n"))), Set("a", "b", "n"))
    )
  }

  test("AssertSameRelationship") {
    assertGood(
      attach(AssertSameRelationship(varFor("r"), lhsLP, rhsLP), 2345.0),
      planDescription(
        id,
        "AssertSameRelationship",
        TwoChildren(lhsPD, rhsPD),
        Seq(details(Seq("r"))),
        Set("a", "b", "r")
      )
    )
  }

  test("CartesianProduct") {
    assertGood(
      attach(CartesianProduct(lhsLP, rhsLP), 2345.0),
      planDescription(id, "CartesianProduct", TwoChildren(lhsPD, rhsPD), Seq.empty, Set("a", "b"))
    )
  }

  test("NodeHashJoin") {
    assertGood(
      attach(NodeHashJoin(Set(varFor("a")), lhsLP, rhsLP), 2345.0),
      planDescription(id, "NodeHashJoin", TwoChildren(lhsPD, rhsPD), Seq(details("a")), Set("a", "b"))
    )
  }

  test("ForeachApply") {
    val testCases = Seq(
      ("a", ListLiteral(Seq(number("1"), number("2")))(pos)) ->
        "a IN [1, 2]",
      ("b", parameter("param", CTList(CTInteger))) ->
        "b IN $param",
      ("c", ListLiteral(Seq.empty)(pos)) ->
        "c IN []",
      ("d", FunctionInvocation(number("1"), FunctionName("range")(pos), number("100"))) ->
        "d IN range(1, 100)"
    )

    for (((variable, expr), expectedDetails) <- testCases) {
      assertGood(
        attach(ForeachApply(lhsLP, rhsLP, varFor(variable), expr), 2345.0),
        planDescription(id, "Foreach", TwoChildren(lhsPD, rhsPD), Seq(details(expectedDetails)), Set("a"))
      )
    }
  }

  test("LetSelectOrSemiApply") {
    assertGood(
      attach(LetSelectOrSemiApply(lhsLP, rhsLP, varFor("x"), Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(id, "LetSelectOrSemiApply", TwoChildren(lhsPD, rhsPD), Seq(details("a.foo = 42")), Set("a", "x"))
    )
  }

  test("LetSelectOrAntiSemiApply") {
    assertGood(
      attach(LetSelectOrAntiSemiApply(lhsLP, rhsLP, varFor("x"), Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(
        id,
        "LetSelectOrAntiSemiApply",
        TwoChildren(lhsPD, rhsPD),
        Seq(details("a.foo = 42")),
        Set("a", "x")
      )
    )
  }

  test("LetSemiApply") {
    assertGood(
      attach(LetSemiApply(lhsLP, rhsLP, varFor("x")), 2345.0),
      planDescription(id, "LetSemiApply", TwoChildren(lhsPD, rhsPD), Seq(details("x")), Set("a", "x"))
    )
  }

  test("LetAntiSemiApply") {
    assertGood(
      attach(LetAntiSemiApply(lhsLP, rhsLP, varFor("x")), 2345.0),
      planDescription(id, "LetAntiSemiApply", TwoChildren(lhsPD, rhsPD), Seq.empty, Set("a", "x"))
    )
  }

  test("LeftOuterHashJoin") {
    assertGood(
      attach(LeftOuterHashJoin(Set(varFor("a")), lhsLP, rhsLP), 2345.0),
      planDescription(id, "NodeLeftOuterHashJoin", TwoChildren(lhsPD, rhsPD), Seq(details("a")), Set("a", "b"))
    )
  }

  test("RightOuterHashJoin") {
    assertGood(
      attach(RightOuterHashJoin(Set(varFor("a")), lhsLP, rhsLP), 2345.0),
      planDescription(id, "NodeRightOuterHashJoin", TwoChildren(lhsPD, rhsPD), Seq(details("a")), Set("a", "b"))
    )
  }

  test("RollUpApply") {
    assertGood(
      attach(RollUpApply(lhsLP, rhsLP, varFor("collection"), varFor("x")), 2345.0),
      planDescription(
        id,
        "RollUpApply",
        TwoChildren(lhsPD, rhsPD),
        Seq(details(Seq("collection", "x"))),
        Set("a", "collection")
      )
    )
  }

  test("SelectOrAntiSemiApply") {
    assertGood(
      attach(SelectOrAntiSemiApply(lhsLP, rhsLP, Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(id, "SelectOrAntiSemiApply", TwoChildren(lhsPD, rhsPD), Seq(details("a.foo = 42")), Set("a"))
    )
  }

  test("SelectOrSemiApply") {
    assertGood(
      attach(SelectOrSemiApply(lhsLP, rhsLP, Equals(prop("a", "foo"), number("42"))(pos)), 2345.0),
      planDescription(id, "SelectOrSemiApply", TwoChildren(lhsPD, rhsPD), Seq(details("a.foo = 42")), Set("a"))
    )
  }

  test("SemiApply") {
    assertGood(
      attach(SemiApply(lhsLP, rhsLP), 2345.0),
      planDescription(id, "SemiApply", TwoChildren(lhsPD, rhsPD), Seq.empty, Set("a"))
    )
  }

  test("TransactionForeach") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          onErrorBehaviour = OnErrorContinue,
          maybeReportAs = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        TwoChildren(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR CONTINUE")),
        Set("a")
      )
    )
  }

  test("TransactionForeach with status") {
    assertGood(
      attach(
        TransactionForeach(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          onErrorBehaviour = OnErrorBreak,
          maybeReportAs = Some(varFor("status"))
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionForeach",
        TwoChildren(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR BREAK REPORT STATUS AS status")),
        Set("a", "status")
      )
    )
  }

  test("TransactionApply") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = None
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        TwoChildren(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR FAIL")),
        Set("a", "b")
      )
    )
  }

  test("TransactionApply with status") {
    assertGood(
      attach(
        TransactionApply(
          lhsLP,
          rhsLP,
          batchSize = number("100"),
          onErrorBehaviour = OnErrorFail,
          maybeReportAs = Some(varFor("status"))
        ),
        2345.0
      ),
      planDescription(
        id,
        "TransactionApply",
        TwoChildren(lhsPD, rhsPD),
        Seq(details("IN TRANSACTIONS OF 100 ROWS ON ERROR FAIL REPORT STATUS AS status")),
        Set("a", "b", "status")
      )
    )
  }

  test("TriadicBuild") {
    assertGood(
      attach(TriadicBuild(lhsLP, varFor("a"), varFor("b"), Some(Id(1))), 113.0),
      planDescription(id, "TriadicBuild", SingleChild(lhsPD), Seq(details("(a)--(b)")), Set("a"))
    )
  }

  test("TriadicFilter") {
    assertGood(
      attach(TriadicFilter(lhsLP, positivePredicate = true, varFor("a"), varFor("b"), Some(Id(1))), 113.0),
      planDescription(id, "TriadicFilter", SingleChild(lhsPD), Seq(details("WHERE (a)--(b)")), Set("a"))
    )

    assertGood(
      attach(TriadicFilter(lhsLP, positivePredicate = false, varFor("a"), varFor("b"), Some(Id(1))), 113.0),
      planDescription(id, "TriadicFilter", SingleChild(lhsPD), Seq(details("WHERE NOT (a)--(b)")), Set("a"))
    )
  }

  test("TriadicSelection") {
    assertGood(
      attach(TriadicSelection(lhsLP, rhsLP, positivePredicate = true, varFor("a"), varFor("b"), varFor("c")), 2345.0),
      planDescription(id, "TriadicSelection", TwoChildren(lhsPD, rhsPD), Seq(details("WHERE (a)--(c)")), Set("a", "b"))
    )

    assertGood(
      attach(TriadicSelection(lhsLP, rhsLP, positivePredicate = false, varFor("a"), varFor("b"), varFor("c")), 2345.0),
      planDescription(
        id,
        "TriadicSelection",
        TwoChildren(lhsPD, rhsPD),
        Seq(details("WHERE NOT (a)--(c)")),
        Set("a", "b")
      )
    )
  }

  test("Union") {
    // leafs no overlapping variables
    assertGood(
      attach(Union(lhsLP, rhsLP), 2345.0),
      planDescription(id, "Union", TwoChildren(lhsPD, rhsPD), Seq.empty, Set.empty)
    )

    // leafs with overlapping variables
    val lp = attach(AllNodesScan(varFor("a"), Set.empty), 2.0, providedOrder = ProvidedOrder.empty)
    val pd = planDescription(id, "AllNodesScan", NoChildren, Seq(details("a")), Set("a"))
    assertGood(
      attach(Union(lhsLP, lp), 2345.0),
      planDescription(id, "Union", TwoChildren(lhsPD, pd), Seq.empty, Set("a"))
    )
  }

  test("ValueHashJoin") {
    assertGood(
      attach(ValueHashJoin(lhsLP, rhsLP, Equals(prop("a", "foo"), prop("b", "foo"))(pos)), 2345.0),
      planDescription(id, "ValueHashJoin", TwoChildren(lhsPD, rhsPD), Seq(details("a.foo = b.foo")), Set("a", "b"))
    )
  }

  test("ArgumentTracker") {
    assertGood(
      attach(ArgumentTracker(lhsLP), 113.0),
      planDescription(id, "ArgumentTracker", SingleChild(lhsPD), Seq(), Set("a"))
    )
  }

  test("Repeat(Trail)") {
    assertGood(
      attach(
        Trail(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("start"),
          varFor("end"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          Set(varFor("  r@1")),
          Set.empty,
          Set.empty,
          reverseGroupVariableProjections = false
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(Trail)",
        TwoChildren(lhsPD, rhsPD),
        List(details("(start) (...){0, *} (end)")),
        Set("r", "a", "anon_2", "start", "end")
      )
    )

    assertGood(
      attach(
        Trail(
          lhsLP,
          rhsLP,
          Repetition(0, Unlimited),
          varFor("  UNNAMED0"),
          varFor("  end@1"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          Set(varFor("  r@1")),
          Set.empty,
          Set.empty,
          reverseGroupVariableProjections = false
        ),
        2345.0
      ),
      planDescription(
        id,
        "Repeat(Trail)",
        TwoChildren(lhsPD, rhsPD),
        List(details("(anon_0) (...){0, *} (end)")),
        Set("r", "a", "anon_2", "anon_0", "end")
      )
    )
  }

  test("BidirectionalRepeat && RepeatOptions") {
    assertGood(
      attach(
        BidirectionalRepeatTrail(
          lhsLP,
          RepeatOptions(lhsLP, rhsLP),
          Repetition(0, Unlimited),
          varFor("  UNNAMED0"),
          varFor("  end@1"),
          varFor("  a@1"),
          varFor("  UNNAMED1"),
          Set(
            variableGrouping(varFor("  a@1"), varFor("  a@2")),
            variableGrouping(varFor("  UNNAMED1"), varFor("  UNNAMED2"))
          ),
          Set(variableGrouping(varFor("  r@1"), varFor("  r@2"))),
          Set(varFor("  r@1")),
          Set.empty,
          Set.empty,
          reverseGroupVariableProjections = false
        ),
        2345.0
      ),
      planDescription(
        id,
        "BidirectionalRepeat(Trail)",
        TwoChildren(
          lhsPD,
          planDescription(
            id,
            "RepeatOptions",
            TwoChildren(lhsPD, rhsPD),
            List(),
            Set("a")
          )
        ),
        List(details("(anon_0) (...){0, *} (end)")),
        Set("r", "a", "anon_2", "anon_0", "end")
      )
    )
  }

  test("SimulatedNodeScan") {
    assertGood(
      attach(SimulatedNodeScan(varFor("a"), 1000), 1.0, providedOrder = ProvidedOrder.asc(varFor("a"))),
      planDescription(
        id,
        "SimulatedNodeScan",
        NoChildren,
        Seq(details(Seq("a", "1000")), Order(asPrettyString.raw("a ASC"))),
        Set("a")
      )
    )
  }

  test("SimulatedExpand") {
    assertGood(
      attach(SimulatedExpand(lhsLP, varFor("a"), varFor("r1"), varFor("b"), 1.0), 95.0),
      planDescription(
        id,
        "SimulatedExpand",
        SingleChild(lhsPD),
        Seq(details(Seq("(a)-[r1]->(b)", "1.0"))),
        Set("a", "r1", "b")
      )
    )
  }

  test("SimulatedSelection") {
    assertGood(
      attach(SimulatedSelection(lhsLP, 1.0), 2345.0),
      planDescription(
        id,
        "SimulatedFilter",
        SingleChild(lhsPD),
        Seq(details("1.0")),
        Set("a")
      )
    )
  }

  test("RuntimeConstant") {
    val namespace = List("ns")
    val name = "datetime"
    val args = IndexedSeq(number("23391882379"))

    val functionInvocation = FunctionInvocation(
      namespace = Namespace(namespace)(pos),
      functionName = FunctionName(name)(pos),
      distinct = false,
      args = args
    )(pos)

    assertGood(
      attach(
        Projection(
          lhsLP,
          immutable.Map(varFor("function") -> RuntimeConstant(varFor("x"), functionInvocation))
        ),
        12345.0
      ),
      planDescription(
        id,
        "Projection",
        SingleChild(lhsPD),
        Seq(details("ns.datetime(23391882379) AS function")),
        Set("a", "function")
      )
    )

    assertGood(
      attach(
        Projection(
          lhsLP,
          immutable.Map(varFor("function") -> RuntimeConstant(
            varFor("y"),
            RuntimeConstant(varFor("x"), functionInvocation)
          ))
        ),
        12345.0
      ),
      planDescription(
        id,
        "Projection",
        SingleChild(lhsPD),
        Seq(details("ns.datetime(23391882379) AS function")),
        Set("a", "function")
      )
    )
  }

  private def assertGood(
    logicalPlan: LogicalPlan,
    expectedPlanDescription: InternalPlanDescription,
    validateAllArgs: Boolean = false,
    readOnly: Boolean = true
  ): Unit = {
    val producedPlanDescription = LogicalPlan2PlanDescription.create(
      logicalPlan,
      IDPPlannerName,
      readOnly,
      effectiveCardinalities,
      withRawCardinalities = false,
      withDistinctness = false,
      providedOrders = providedOrders,
      StubExecutionPlan().operatorMetadata
    )

    def shouldValidateArg(arg: Argument) =
      validateAllArgs ||
        !(arg.isInstanceOf[PlannerImpl] ||
          arg.isInstanceOf[Planner] ||
          arg.isInstanceOf[EstimatedRows] ||
          arg.isInstanceOf[Version] ||
          arg.isInstanceOf[RuntimeVersion] ||
          arg.isInstanceOf[PlannerVersion])

    def shouldBeEqual(a: InternalPlanDescription, b: InternalPlanDescription): Unit = {
      withClue("name")(a.name should equal(b.name))
      if (validateAllArgs) {
        withClue("arguments(all)")(a.arguments should equal(b.arguments))
      } else {
        val aArgsToValidate = a.arguments.filter(shouldValidateArg)
        val bArgsToValidate = b.arguments.filter(shouldValidateArg)

        withClue("arguments")(aArgsToValidate should equal(bArgsToValidate))
      }
      withClue("variables")(a.variables should equal(b.variables))
    }

    shouldBeEqual(producedPlanDescription, expectedPlanDescription)

    withClue("children") {
      (expectedPlanDescription.children, producedPlanDescription.children) match {
        case (NoChildren, NoChildren) =>
        case (SingleChild(expectedChild), SingleChild(producedChild)) =>
          shouldBeEqual(expectedChild, producedChild)
        case (TwoChildren(expectedLhs, expectedRhs), TwoChildren(producedLhs, producedRhs)) =>
          shouldBeEqual(expectedLhs, producedLhs)
          shouldBeEqual(expectedRhs, producedRhs)
        case (expected, produced) =>
          fail(s"${expected.getClass} does not equal ${produced.getClass}")
      }
    }
  }

  private def cachedProp(varName: String, propName: String): CachedProperty =
    CachedProperty(varFor(varName), varFor(varName), PropertyKeyName(propName)(pos), NODE_TYPE)(pos)

  private def label(name: String): LabelName = LabelName(name)(pos)

  private def relType(name: String): RelTypeName = RelTypeName(name)(pos)

  private def key(name: String): PropertyKeyName = PropertyKeyName(name)(pos)

  private def number(i: String): SignedDecimalIntegerLiteral = SignedDecimalIntegerLiteral(i)(pos)

  private def stringLiteral(s: String): StringLiteral = StringLiteral(s)(pos)
}
