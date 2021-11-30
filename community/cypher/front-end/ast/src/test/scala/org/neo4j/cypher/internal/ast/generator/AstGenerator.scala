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
package org.neo4j.cypher.internal.ast.generator

import org.neo4j.cypher.internal.ast.Access
import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.AliasedReturnItem
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
import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.AlterDatabaseAlias
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AlterUserAction
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConstraintVersion
import org.neo4j.cypher.internal.ast.ConstraintVersion0
import org.neo4j.cypher.internal.ast.ConstraintVersion1
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateBtreeNodeIndex
import org.neo4j.cypher.internal.ast.CreateBtreeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateDatabaseAlias
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateIndexOldSyntax
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreatePointNodeIndex
import org.neo4j.cypher.internal.ast.CreatePointRelationshipIndex
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRangeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabasePrivilegeQualifier
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DeleteElementAction
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DeprecatedSyntax
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.DestroyData
import org.neo4j.cypher.internal.ast.DropConstraintAction
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropDatabase
import org.neo4j.cypher.internal.ast.DropDatabaseAction
import org.neo4j.cypher.internal.ast.DropDatabaseAlias
import org.neo4j.cypher.internal.ast.DropIndex
import org.neo4j.cypher.internal.ast.DropIndexAction
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRole
import org.neo4j.cypher.internal.ast.DropRoleAction
import org.neo4j.cypher.internal.ast.DropUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DropUser
import org.neo4j.cypher.internal.ast.DropUserAction
import org.neo4j.cypher.internal.ast.DumpData
import org.neo4j.cypher.internal.ast.ElementQualifier
import org.neo4j.cypher.internal.ast.ElementsAllQualifier
import org.neo4j.cypher.internal.ast.ExecuteAdminProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteBoostedProcedureAction
import org.neo4j.cypher.internal.ast.ExecuteFunctionAction
import org.neo4j.cypher.internal.ast.ExecuteProcedureAction
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.FunctionQualifier
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
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
import org.neo4j.cypher.internal.ast.LabelAllQualifier
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.LabelsResource
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.LookupIndexes
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.MatchAction
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.MergeAction
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.NewSyntax
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.OldValidSyntax
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.PeriodicCommitHint
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PrivilegeCommand
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.ProcedureQualifier
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryPart
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.ReadAction
import org.neo4j.cypher.internal.ast.ReadOnlyAccess
import org.neo4j.cypher.internal.ast.ReadWriteAccess
import org.neo4j.cypher.internal.ast.RelExistsConstraints
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
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUserPrivileges
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.StartDatabase
import org.neo4j.cypher.internal.ast.StartDatabaseAction
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StopDatabase
import org.neo4j.cypher.internal.ast.StopDatabaseAction
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsParameters
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.TimeoutAfter
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
import org.neo4j.cypher.internal.ast.UsingBtreeIndexType
import org.neo4j.cypher.internal.ast.UsingHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.UsingTextIndexType
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.YieldOrWhere
import org.neo4j.cypher.internal.ast.generator.AstGenerator.boolean
import org.neo4j.cypher.internal.ast.generator.AstGenerator.char
import org.neo4j.cypher.internal.ast.generator.AstGenerator.oneOrMore
import org.neo4j.cypher.internal.ast.generator.AstGenerator.tuple
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
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.In
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
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.MapProjectionElement
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.ProcedureOutput
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertySelector
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
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
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
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Gen.alphaLowerChar
import org.scalacheck.Gen.choose
import org.scalacheck.Gen.const
import org.scalacheck.Gen.frequency
import org.scalacheck.Gen.listOf
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

object AstGenerator {
  val OR_MORE_UPPER_BOUND = 3

  def zeroOrMore[T](gen: Gen[T]): Gen[List[T]] =
    choose(0, OR_MORE_UPPER_BOUND).flatMap(listOfN(_, gen))

  def zeroOrMore[T](seq: Seq[T]): Gen[Seq[T]] =
    choose(0, Math.min(OR_MORE_UPPER_BOUND, seq.size)).flatMap(pick(_, seq))

  def oneOrMore[T](gen: Gen[T]): Gen[List[T]] =
    choose(1, OR_MORE_UPPER_BOUND).flatMap(listOfN(_, gen))

  def oneOrMore[T](seq: Seq[T]): Gen[Seq[T]] =
    choose(1, Math.min(OR_MORE_UPPER_BOUND, seq.size)).flatMap(pick(_, seq))

  def tuple[A, B](ga: Gen[A], gb: Gen[B]): Gen[(A, B)] = for {
    a <- ga
    b <- gb
  } yield (a, b)

  def boolean: Gen[Boolean] =
    Arbitrary.arbBool.arbitrary

  def char: Gen[Char] =
    Arbitrary.arbChar.arbitrary.suchThat(acceptedByParboiled)

  def acceptedByParboiled(c: Char): Boolean = {
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

}

/**
 * Random query generation
 * Implements instances of Gen[T] for all query ast nodes
 * Generated queries are syntactically (but not semantically) valid
 */
class AstGenerator(simpleStrings: Boolean = true, allowedVarNames: Option[Seq[String]] = None) {

  // HELPERS
  // ==========================================================================

  protected var paramCount = 0
  protected val pos : InputPosition = InputPosition.NONE

  def string: Gen[String] =
    if (simpleStrings) alphaLowerChar.map(_.toString)
    else listOf(char).map(_.mkString)


  // IDENTIFIERS
  // ==========================================================================

  def _identifier: Gen[String] =
    if (simpleStrings) alphaLowerChar.map(_.toString)
    else nonEmptyListOf(char).map(_.mkString)

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
    string.flatMap(StringLiteral(_)(pos))

  def _sensitiveStringLiteral: Gen[SensitiveStringLiteral] =
    // Needs to be '******' since all sensitive strings get rendered as such
    // Would normally get rewritten as SensitiveAutoParameter which can be generated as parameter when needed
    const(SensitiveStringLiteral("******".getBytes(StandardCharsets.UTF_8))(pos))

  def _booleanLit: Gen[BooleanLiteral] =
    oneOf(True()(pos), False()(pos))

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

  def _signedIntLit: Gen[SignedIntegerLiteral] = oneOf(
    _signedDecIntLit,
    _signedHexIntLit,
    _signedOctIntLitOldSyntax,
    _signedOctIntLit
  )

  def _doubleLit: Gen[DecimalDoubleLiteral] =
    Arbitrary.arbDouble.arbitrary.map(_.toString).map(DecimalDoubleLiteral(_)(pos))

  def _parameter: Gen[Parameter] =
    _identifier.map(Parameter(_, AnyType.instance)(pos))

  def _stringParameter: Gen[Parameter] = _identifier.map(Parameter(_, CTString)(pos))

  def _mapParameter: Gen[Parameter] = _identifier.map(Parameter(_, CTMap)(pos))

  def _sensitiveStringParameter: Gen[Parameter with SensitiveParameter] =
    _identifier.map(new ExplicitParameter(_, CTString)(pos) with SensitiveParameter)

  def _sensitiveAutoStringParameter: Gen[Parameter with SensitiveAutoParameter] =
    _identifier.map(new ExplicitParameter(_, CTString)(pos) with SensitiveAutoParameter)

  def _variable: Gen[Variable] = {
    val nameGen = allowedVarNames match {
      case None => _identifier
      case Some(Seq()) => const("").suchThat(_ => false)
      case Some(names) =>  oneOf(names)
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

  def _predicateComparisonChain: Gen[Expression] = for {
    exprs <- listOfN(4, _expression)
    pairs = exprs.sliding(2)
    gens = pairs.map(p => _predicateComparisonPar(p.head, p.last)).toList
    chain <- sequence(gens)(Buildable.buildableCanBuildFrom)
  } yield Ands(chain)(pos)

  def _predicateUnary: Gen[Expression] = for {
    r <- _expression
    res <- oneOf(
      Not(r)(pos),
      IsNull(r)(pos),
      IsNotNull(r)(pos)
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

  def _hasLabelsOrTypes: Gen[HasLabelsOrTypes] = for {
    expression <- _expression
    labels <- oneOrMore(_labelOrTypeName)
  } yield HasLabelsOrTypes(expression, labels)(pos)

  // Collections
  // ----------------------------------

  def _map: Gen[MapExpression] = for {
    items <- zeroOrMore(tuple(_propertyKeyName, _expression))
  } yield MapExpression(items)(pos)

  def _mapStringKeys: Gen[Map[String, Expression]] = for {
    items <- zeroOrMore(tuple(_identifier, _expression))
  } yield items.toMap

  def _property: Gen[Property] = for {
    map <- _expression
    key <- _propertyKeyName
  } yield Property(map, key)(pos)

  def _mapProjectionElement: Gen[MapProjectionElement] =
    oneOf(
      for {key <- _propertyKeyName; exp <- _expression} yield LiteralEntry(key, exp)(pos),
      for {id <- _variable} yield VariableSelector(id)(pos),
      for {id <- _variable} yield PropertySelector(id)(pos),
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

  def _case: Gen[CaseExpression] = for {
    expression <- option(_expression)
    alternatives <- oneOrMore(tuple(_expression, _expression))
    default <- option(_expression)
  } yield CaseExpression(expression, alternatives, default)(pos)

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

  def _countStar: Gen[CountStar] =
    const(CountStar()(pos))

  // Patterns
  // ----------------------------------

  def _relationshipsPattern: Gen[RelationshipsPattern] = for {
    chain <- _relationshipChain
  } yield RelationshipsPattern(chain)(pos)

  def _patternExpr: Gen[PatternExpression] = for {
    pattern <- _relationshipsPattern
  } yield PatternExpression(pattern)(Set.empty, "", "")

  def _shortestPaths: Gen[ShortestPaths] = for {
    element <- _patternElement
    single <- boolean
  } yield ShortestPaths(element, single)(pos)

  def _shortestPathExpr: Gen[ShortestPathExpression] = for {
    pattern <- _shortestPaths
  } yield ShortestPathExpression(pattern)

  def _existsSubClause: Gen[ExistsSubClause] = for {
    pattern <- _pattern
    where <- option(_expression)
    outerScope <- zeroOrMore(_variable)
  } yield ExistsSubClause(pattern, where)(pos, outerScope.toSet)

  def _patternComprehension: Gen[PatternComprehension] = for {
    namedPath <- option(_variable)
    pattern <- _relationshipsPattern
    predicate <- option(_expression)
    projection <- _expression
    outerScope <- zeroOrMore(_variable)
  } yield PatternComprehension(namedPath, pattern, predicate, projection)(pos, outerScope.toSet, "", "")

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
        lzy(_parameter)
      ),
      1 -> oneOf(
        lzy(_predicateComparison),
        lzy(_predicateUnary),
        lzy(_predicateBinary),
        lzy(_predicateComparisonChain),
        lzy(_iterablePredicate),
        lzy(_hasLabelsOrTypes),
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
        lzy(_existsSubClause),
        lzy(_patternComprehension)
      )
    )

  // PATTERNS
  // ==========================================================================

  def _nodePattern: Gen[NodePattern] = for {
    variable <- option(_variable)
    labels <- zeroOrMore(_labelName)
    properties <- option(oneOf(_map, _parameter))
    predicate <- variable match {
      case Some(_) => option(_expression) // Only generate WHERE if we have a variable name.
      case None => const(None)
    }
  } yield NodePattern(variable, labels, properties, predicate)(pos)

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
    types <- zeroOrMore(_relTypeName)
    length <- option(option(_range))
    properties <- option(oneOf(_map, _parameter))
    direction <- _semanticDirection
  } yield RelationshipPattern(variable, types, length, properties, direction, legacyTypeSeparator = false)(pos)

  def _relationshipChain: Gen[RelationshipChain] = for {
    element <- _patternElement
    relationship <- _relationshipPattern
    rightNode <- _nodePattern
  } yield RelationshipChain(element, relationship, rightNode)(pos)

  def _patternElement: Gen[PatternElement] = oneOf(
    _nodePattern,
    lzy(_relationshipChain)
  )

  def _anonPatternPart: Gen[AnonymousPatternPart] = for {
    element <- _patternElement
    single <- boolean
    part <- oneOf(
      EveryPath(element),
      ShortestPaths(element, single)(pos)
    )
  } yield part

  def _namedPatternPart: Gen[NamedPatternPart] = for {
    variable <- _variable
    part <- _anonPatternPart
  } yield NamedPatternPart(variable, part)(pos)

  def _patternPart: Gen[PatternPart] =
    oneOf(
      _anonPatternPart,
      _namedPatternPart
    )

  def _pattern: Gen[Pattern] = for {
    parts <- oneOrMore(_patternPart)
  } yield Pattern(parts)(pos)

  def _patternSingle: Gen[Pattern] = for {
    part <- _patternPart
  } yield Pattern(Seq(part))(pos)

  // CLAUSES
  // ==========================================================================

  def _returnItem: Gen[ReturnItem] = for {
    expr <- _expression
    variable <- _variable
    item <- oneOf(
      UnaliasedReturnItem(expr, "")(pos),
      AliasedReturnItem(expr, variable)(pos, isAutoAliased = false)
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

  def _returnItems1: Gen[ReturnItems] = for {
    retItems <- oneOrMore(_returnItem)
  } yield ReturnItems(includeExisting = false, retItems)(pos)

  def _returnItems2: Gen[ReturnItems] = for {
    retItems <- zeroOrMore(_returnItem)
  } yield ReturnItems(includeExisting = true, retItems)(pos)

  def _returnItems: Gen[ReturnItems] =
    oneOf(_returnItems1, _returnItems2)

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
  }  yield item

  def _match: Gen[Match] = for {
    optional <- boolean
    pattern <- _pattern
    hints <- zeroOrMore(_hint)
    where <- option(_where)
  } yield Match(optional, pattern, hints, where)(pos)

  def _create: Gen[Create] = for {
    pattern <- _pattern
  } yield Create(pattern)(pos)

  def _unwind: Gen[Unwind] = for {
    expression <- _expression
    variable <- _variable
  } yield Unwind(expression, variable)(pos)

  def _setItem: Gen[SetItem] = for {
    variable <- _variable
    labels <- oneOrMore(_labelName)
    property <- _property
    expression <- _expression
    item <- oneOf(
      SetLabelItem(variable, labels)(pos),
      SetPropertyItem(property, expression)(pos),
      SetExactPropertiesFromMapItem(variable, expression)(pos),
      SetIncludingPropertiesFromMapItem(variable, expression)(pos)
    )
  } yield item

  def _removeItem: Gen[RemoveItem] = for {
    variable <- _variable
    labels <- oneOrMore(_labelName)
    property <- _property
    item <- oneOf(
      RemoveLabelItem(variable, labels)(pos),
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
    pattern <- _patternSingle
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
    indexType <- oneOf(UsingAnyIndexType, UsingBtreeIndexType, UsingTextIndexType)
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
    expression <- _expression
  } yield UseGraph(expression)(pos)

  def _subqueryCall: Gen[SubqueryCall] = for {
    part <- _queryPart
    params <- option(_inTransactionsParameters)
  } yield SubqueryCall(part, params)(pos)

  def _inTransactionsParameters: Gen[InTransactionsParameters] = for {
    batchSize <- option(_expression)
  } yield InTransactionsParameters(batchSize)(pos)

  def _clause: Gen[Clause] = oneOf(
    lzy(_use),
    lzy(_with),
    lzy(_return),
    lzy(_match),
    lzy(_create),
    lzy(_unwind),
    lzy(_set),
    lzy(_remove),
    lzy(_delete),
    lzy(_merge),
    lzy(_call),
    lzy(_foreach),
    lzy(_loadCsv),
    lzy(_subqueryCall),
  )

  def _singleQuery: Gen[SingleQuery] = for {
    s <- choose(1, 1)
    clauses <- listOfN(s, _clause)
  } yield SingleQuery(clauses)(pos)

  def _union: Gen[Union] = for {
    part <- _queryPart
    single <- _singleQuery
    union <- oneOf(
      UnionDistinct(part, single)(pos),
      UnionAll(part, single)(pos)
    )
  } yield union

  def _queryPart: Gen[QueryPart] = frequency(
    5 -> lzy(_singleQuery),
    1 -> lzy(_union)
  )

  def _regularQuery: Gen[Query] = for {
    part <- _queryPart
  } yield Query(None, part)(pos)

  def _periodicCommitHint: Gen[PeriodicCommitHint] = for {
    size <- option(_unsignedDecIntLit)
  } yield PeriodicCommitHint(size)(pos)

  def _bulkImportQuery: Gen[Query] = for {
    periodicCommitHint <- option(_periodicCommitHint)
    load <- _loadCsv
  } yield Query(periodicCommitHint, SingleQuery(Seq(load))(pos))(pos)

  def _query: Gen[Query] = frequency(
    10 -> _regularQuery,
    1 -> _bulkImportQuery
  )

  // Show commands
  // ----------------------------------

  def _indexType: Gen[(ShowIndexType, Option[Boolean])] = for {
    verbose   <- frequency(8 -> const(None), 2 -> some(boolean)) // option(boolean) but None more often than Some
    // BRIEF/VERBOSE is only allowed with ALL and BTREE
    indexType <- oneOf((AllIndexes, verbose), (BtreeIndexes, verbose), (RangeIndexes, None), (FulltextIndexes, None),
                       (TextIndexes, None), (PointIndexes, None), (LookupIndexes, None))
  } yield indexType

  def _listOfLabels: Gen[List[LabelName]] = for {
    labels <- oneOrMore(_labelName)
  } yield labels

  def _listOfRelTypes: Gen[List[RelTypeName]] = for {
    types <- oneOrMore(_relTypeName)
  } yield types

  def _constraintInfo: Gen[(ShowConstraintType, Option[Boolean], YieldOrWhere)] = for {
    unfilteredVerbose         <- frequency(8 -> const(None), 2 -> some(boolean)) // option(boolean) but None more often than Some
    unfilteredYields          <- _eitherYieldOrWhere
    // For existence constraint: new syntax don't allow BRIEF/VERBOSE, deprecated syntax don't allow YIELD/WHERE
    (exists, verbose, yields) <- oneOf((NewSyntax, None, unfilteredYields), (DeprecatedSyntax, unfilteredVerbose, None), (OldValidSyntax, unfilteredVerbose, unfilteredYields))
    types                     <- oneOf(AllConstraints, UniqueConstraints, ExistsConstraints(exists), NodeExistsConstraints(exists), RelExistsConstraints(exists), NodeKeyConstraints)
  } yield (types, verbose, yields)

  def _showIndexes: Gen[Query] = for {
    (indexType, verbose) <- _indexType
    use                  <- option(_use)
    yields               <- _eitherYieldOrWhere
  } yield {
    val showClauses = (yields, verbose) match {
      case (Some(Right(w)), _)           => Seq(ShowIndexesClause(indexType, brief = false, verbose = false, Some(w), hasYield = false)(pos))
      case (Some(Left((y, Some(r)))), _) => Seq(ShowIndexesClause(indexType, brief = false, verbose = false, None, hasYield = true)(pos), y, r)
      case (Some(Left((y, None))), _)    => Seq(ShowIndexesClause(indexType, brief = false, verbose = false, None, hasYield = true)(pos), y)
      case (_, Some(v))                  => Seq(ShowIndexesClause(indexType, !v, v, None, hasYield = false)(pos))
      case _                             => Seq(ShowIndexesClause(indexType, brief = false, verbose = false, None, hasYield = false)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    Query(None, SingleQuery(fullClauses)(pos))(pos)
  }

  def _showConstraints: Gen[Query] = for {
    (constraintType, verbose, yields) <- _constraintInfo
    use                               <- option(_use)
  } yield {
    val showClauses = (yields, verbose) match {
      case (Some(Right(w)), _)           => Seq(ShowConstraintsClause(constraintType, brief = false, verbose = false, Some(w), hasYield = false)(pos))
      case (Some(Left((y, Some(r)))), _) => Seq(ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = true)(pos), y, r)
      case (Some(Left((y, None))), _)    => Seq(ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = true)(pos), y)
      case (_, Some(v))                  => Seq(ShowConstraintsClause(constraintType, !v, v, None, hasYield = false)(pos))
      case _                             => Seq(ShowConstraintsClause(constraintType, brief = false, verbose = false, None, hasYield = false)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    Query(None, SingleQuery(fullClauses)(pos))(pos)
  }

  def _showProcedures: Gen[Query] = for {
    name    <- _identifier
    exec    <- option(oneOf(CurrentUser, User(name)))
    yields  <- _eitherYieldOrWhere
    use     <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w))           => Seq(ShowProceduresClause(exec, Some(w), hasYield = false)(pos))
      case Some(Left((y, Some(r)))) => Seq(ShowProceduresClause(exec, None, hasYield = true)(pos), y, r)
      case Some(Left((y, None)))    => Seq(ShowProceduresClause(exec, None, hasYield = true)(pos), y)
      case _                        => Seq(ShowProceduresClause(exec, None, hasYield = false)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    Query(None, SingleQuery(fullClauses)(pos))(pos)
  }

  def _showFunctions: Gen[Query] = for {
    name     <- _identifier
    funcType <- oneOf(AllFunctions, BuiltInFunctions, UserDefinedFunctions)
    exec     <- option(oneOf(CurrentUser, User(name)))
    yields   <- _eitherYieldOrWhere
    use      <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w))           => Seq(ShowFunctionsClause(funcType, exec, Some(w), hasYield = false)(pos))
      case Some(Left((y, Some(r)))) => Seq(ShowFunctionsClause(funcType, exec, None, hasYield = true)(pos), y, r)
      case Some(Left((y, None)))    => Seq(ShowFunctionsClause(funcType, exec, None, hasYield = true)(pos), y)
      case _                        => Seq(ShowFunctionsClause(funcType, exec, None, hasYield = false)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    Query(None, SingleQuery(fullClauses)(pos))(pos)
  }

  def _showTransactions: Gen[Query] = for {
    idList <- zeroOrMore(string)
    param  <- _parameter
    ids    <- oneOf(Left(idList), Right(param))
    yields <- _eitherYieldOrWhere
    use    <- option(_use)
  } yield {
    val showClauses = yields match {
      case Some(Right(w))           => Seq(ShowTransactionsClause(ids, Some(w), hasYield = false)(pos))
      case Some(Left((y, Some(r)))) => Seq(ShowTransactionsClause(ids, None, hasYield = true)(pos), y, r)
      case Some(Left((y, None)))    => Seq(ShowTransactionsClause(ids, None, hasYield = true)(pos), y)
      case _                        => Seq(ShowTransactionsClause(ids, None, hasYield = false)(pos))
    }
    val fullClauses = use.map(u => u +: showClauses).getOrElse(showClauses)
    Query(None, SingleQuery(fullClauses)(pos))(pos)
  }

  def _terminateTransactions: Gen[Query] = for {
    idList <- zeroOrMore(string)
    param  <- _parameter
    ids    <- oneOf(Left(idList), Right(param))
    use    <- option(_use)
  } yield {
    val terminateClause = Seq(TerminateTransactionsClause(ids)(pos))
    val fullClauses = use.map(u => u +: terminateClause).getOrElse(terminateClause)
    Query(None, SingleQuery(fullClauses)(pos))(pos)
  }

  def _showCommands: Gen[Query] = oneOf(_showIndexes, _showConstraints, _showProcedures, _showFunctions, _showTransactions, _terminateTransactions)

  // Schema commands
  // ----------------------------------

  def _variableProperty: Gen[Property] = for {
    map <- _variable
    key <- _propertyKeyName
  } yield Property(map, key)(pos)

  def _listOfProperties: Gen[List[Property]] = for {
    props <- oneOrMore(_variableProperty)
  } yield props

  def _constraintVersion: Gen[ConstraintVersion] = oneOf(ConstraintVersion0, ConstraintVersion1, ConstraintVersion2)

  def _constraintVersionZeroOrTwo: Gen[ConstraintVersion] = oneOf(ConstraintVersion0, ConstraintVersion2)

  def _createIndex: Gen[CreateIndex] = for {
    variable          <- _variable
    labelName         <- _labelName
    labels            <- _listOfLabels
    relType           <- _relTypeName
    types             <- _listOfRelTypes
    props             <- _listOfProperties
    name              <- option(_identifier)
    ifExistsDo        <- _ifExistsDo
    options           <- _optionsMapAsEither
    fromDefault       <- boolean
    use               <- option(_use)
    btreeNodeIndex    = CreateBtreeNodeIndex(variable, labelName, props, name, ifExistsDo, options, use)(pos)
    btreeRelIndex     = CreateBtreeRelationshipIndex(variable, relType, props, name, ifExistsDo, options, use)(pos)
    rangeNodeIndex    = CreateRangeNodeIndex(variable, labelName, props, name, ifExistsDo, options, fromDefault, use)(pos)
    rangeRelIndex     = CreateRangeRelationshipIndex(variable, relType, props, name, ifExistsDo, options, fromDefault, use)(pos)
    lookupNodeIndex   = CreateLookupIndex(variable, isNodeIndex = true, FunctionInvocation(FunctionName(Labels.name)(pos), distinct = false, IndexedSeq(variable))(pos), name, ifExistsDo, options, use)(pos)
    lookupRelIndex    = CreateLookupIndex(variable, isNodeIndex = false, FunctionInvocation(FunctionName(Type.name)(pos), distinct = false, IndexedSeq(variable))(pos), name, ifExistsDo, options, use)(pos)
    fulltextNodeIndex = CreateFulltextNodeIndex(variable, labels, props, name, ifExistsDo, options, use)(pos)
    fulltextRelIndex  = CreateFulltextRelationshipIndex(variable, types, props, name, ifExistsDo, options, use)(pos)
    textNodeIndex     = CreateTextNodeIndex(variable, labelName, props, name, ifExistsDo, options, use)(pos)
    textRelIndex      = CreateTextRelationshipIndex(variable, relType, props, name, ifExistsDo, options, use)(pos)
    pointNodeIndex    = CreatePointNodeIndex(variable, labelName, props, name, ifExistsDo, options, use)(pos)
    pointRelIndex     = CreatePointRelationshipIndex(variable, relType, props, name, ifExistsDo, options, use)(pos)
    command           <- oneOf(btreeNodeIndex, btreeRelIndex, rangeNodeIndex, rangeRelIndex, lookupNodeIndex, lookupRelIndex,
                               fulltextNodeIndex, fulltextRelIndex, textNodeIndex, textRelIndex, pointNodeIndex, pointRelIndex)
  } yield command

  def _dropIndex: Gen[DropIndexOnName] = for {
    name     <- _identifier
    ifExists <- boolean
    use      <- option(_use)
  } yield DropIndexOnName(name, ifExists, use)(pos)

  def _indexCommandsOldSyntax: Gen[SchemaCommand] = for {
    labelName <- _labelName
    props     <- oneOrMore(_propertyKeyName)
    use       <- option(_use)
    command   <- oneOf(CreateIndexOldSyntax(labelName, props, use)(pos), DropIndex(labelName, props, use)(pos))
  } yield command

  def _createConstraint: Gen[SchemaCommand] = for {
    variable            <- _variable
    labelName           <- _labelName
    relTypeName         <- _relTypeName
    props               <- _listOfProperties
    prop                <- _variableProperty
    name                <- option(_identifier)
    ifExistsDo          <- _ifExistsDo
    containsOn          <- boolean
    constraintVersion   <- _constraintVersion
    constraintVersion2  <- _constraintVersionZeroOrTwo
    options             <- _optionsMapAsEither
    use                 <- option(_use)
    nodeKey             = CreateNodeKeyConstraint(variable, labelName, props, name, ifExistsDo, options, containsOn, constraintVersion2, use)(pos)
    uniqueness          = CreateUniquePropertyConstraint(variable, labelName, Seq(prop), name, ifExistsDo, options, containsOn, constraintVersion2, use)(pos)
    compositeUniqueness = CreateUniquePropertyConstraint(variable, labelName, props, name, ifExistsDo, options, containsOn, constraintVersion2, use)(pos)
    nodeExistence       = CreateNodePropertyExistenceConstraint(variable, labelName, prop, name, ifExistsDo, options, containsOn, constraintVersion, use)(pos)
    relExistence        = CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, prop, name, ifExistsDo, options, containsOn, constraintVersion, use)(pos)
    command             <- oneOf(nodeKey, uniqueness, compositeUniqueness, nodeExistence, relExistence)
  } yield command

  def _dropConstraintOldSyntax: Gen[SchemaCommand] = for {
    variable            <- _variable
    labelName           <- _labelName
    relTypeName         <- _relTypeName
    props               <- _listOfProperties
    prop                <- _variableProperty
    use                 <- option(_use)
    nodeKey             = DropNodeKeyConstraint(variable, labelName, props, use)(pos)
    uniqueness          = DropUniquePropertyConstraint(variable, labelName, Seq(prop), use)(pos)
    compositeUniqueness = DropUniquePropertyConstraint(variable, labelName, props, use)(pos)
    nodeExistence       = DropNodePropertyExistenceConstraint(variable, labelName, prop, use)(pos)
    relExistence        = DropRelationshipPropertyExistenceConstraint(variable, relTypeName, prop, use)(pos)
    command             <- oneOf(nodeKey, uniqueness, compositeUniqueness, nodeExistence, relExistence)
  } yield command

  def _dropConstraint: Gen[DropConstraintOnName] = for {
    name     <- _identifier
    ifExists <- boolean
    use      <- option(_use)
  } yield DropConstraintOnName(name, ifExists, use)(pos)

  def _indexCommand: Gen[SchemaCommand] = oneOf(_createIndex, _dropIndex, _indexCommandsOldSyntax)

  def _constraintCommand: Gen[SchemaCommand] = oneOf(_createConstraint, _dropConstraint, _dropConstraintOldSyntax)

  def _schemaCommand: Gen[SchemaCommand] = oneOf(_indexCommand, _constraintCommand)

  // Administration commands
  // ----------------------------------

  def _nameAsEither: Gen[Either[String, Parameter]] = for {
    name  <- _identifier
    param <- _stringParameter
    finalName <- oneOf(Left(name), Right(param))
  } yield finalName

  def _optionsMapAsEither: Gen[Options] = for {
    map  <- oneOrMore(tuple(_identifier, _expression)).map(_.toMap)
    param <- _mapParameter
    finalMap <- oneOf(OptionsMap(map), OptionsParam(param), NoOptions)
  } yield finalMap

  def _listOfNameOfEither: Gen[List[Either[String, Parameter]]] = for {
    names <- oneOrMore(_nameAsEither)
  } yield names

  def _password: Gen[Expression] = oneOf(_sensitiveStringParameter, _sensitiveAutoStringParameter, _sensitiveStringLiteral)

  def _ifExistsDo: Gen[IfExistsDo] = oneOf(IfExistsReplace, IfExistsDoNothing, IfExistsThrowError, IfExistsInvalidSyntax)

  // User commands

  def _showUsers: Gen[ShowUsers] = for {
    yields <- _eitherYieldOrWhere
  } yield ShowUsers(yields)(pos)

  def _showCurrentUser: Gen[ShowCurrentUser] = for {
    yields <- _eitherYieldOrWhere
  } yield ShowCurrentUser(yields)(pos)

  def _eitherYieldOrWhere: Gen[YieldOrWhere] = for {
    yields  <- _yield
    where   <- _where
    returns <- option(_return)
    eyw     <- oneOf(Seq(Left((yields, returns)), Right(where)))
    oeyw    <- option(eyw)
  } yield oeyw

  def _createUser: Gen[CreateUser] = for {
    userName              <- _nameAsEither
    isEncryptedPassword   <- boolean
    password              <- _password
    requirePasswordChange <- boolean
    suspended             <- option(boolean)
    homeDatabase          <- option(_setHomeDatabaseAction)
    ifExistsDo            <- _ifExistsDo
    // requirePasswordChange is parsed as 'Some(true)' if omitted in query,
    // prettifier explicitly adds it so 'None' would be prettified and re-parsed to 'Some(true)'
    // hence the explicit 'Some(requirePasswordChange)'
  } yield CreateUser(userName, isEncryptedPassword, password, UserOptions(Some(requirePasswordChange), suspended, homeDatabase), ifExistsDo)(pos)

  def _renameUser: Gen[RenameUser] = for {
    fromUserName <- _nameAsEither
    toUserName   <- _nameAsEither
    ifExists     <- boolean
  } yield RenameUser(fromUserName, toUserName, ifExists)(pos)

  def _dropUser: Gen[DropUser] = for {
    userName <- _nameAsEither
    ifExists <- boolean
  } yield DropUser(userName, ifExists)(pos)

  def _alterUser: Gen[AlterUser] = for {
    userName              <- _nameAsEither
    ifExists              <- boolean
    password              <- option(_password)
    requirePasswordChange <- option(boolean)
    isEncryptedPassword   <- if (password.isEmpty) const(None) else some(boolean)
    suspended             <- option(boolean)
    // All four are not allowed to be None and REMOVE HOME DATABASE is only valid by itself
    homeDatabase          <- if (password.isEmpty && requirePasswordChange.isEmpty && suspended.isEmpty) oneOf(some(_setHomeDatabaseAction), some(RemoveHomeDatabaseAction)) else option(_setHomeDatabaseAction)
  } yield AlterUser(userName, isEncryptedPassword, password, UserOptions(requirePasswordChange, suspended, homeDatabase), ifExists)(pos)

  def _setHomeDatabaseAction: Gen[SetHomeDatabaseAction] = _nameAsEither.map(db => SetHomeDatabaseAction(db))

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
    showAll   <- boolean
    yields <- _eitherYieldOrWhere
  } yield ShowRoles(withUsers, showAll, yields)(pos)

  def _createRole: Gen[CreateRole] = for {
    roleName     <- _nameAsEither
    fromRoleName <- option(_nameAsEither)
    ifExistsDo   <- _ifExistsDo
  } yield CreateRole(roleName, fromRoleName, ifExistsDo)(pos)

  def _renameRole: Gen[RenameRole] = for {
    fromRoleName <- _nameAsEither
    toRoleName   <- _nameAsEither
    ifExists     <- boolean
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
    ExecuteProcedureAction, ExecuteBoostedProcedureAction, ExecuteAdminProcedureAction,
    ExecuteFunctionAction, ExecuteBoostedFunctionAction,
    ImpersonateUserAction,
    AllUserActions, ShowUserAction, CreateUserAction, RenameUserAction, SetUserStatusAction, SetUserHomeDatabaseAction, SetPasswordsAction, AlterUserAction, DropUserAction,
    AllRoleActions, ShowRoleAction, CreateRoleAction, RenameRoleAction, DropRoleAction, AssignRoleAction, RemoveRoleAction,
    AllDatabaseManagementActions, CreateDatabaseAction, DropDatabaseAction, AlterDatabaseAction , SetDatabaseAccessAction,
    AllPrivilegeActions, ShowPrivilegeAction, AssignPrivilegeAction, RemovePrivilegeAction
  )

  def _databaseAction: Gen[DatabaseAction] = oneOf(
    StartDatabaseAction, StopDatabaseAction,
    AllDatabaseAction, AccessDatabaseAction,
    AllIndexActions, CreateIndexAction, DropIndexAction, ShowIndexAction,
    AllConstraintActions, CreateConstraintAction, DropConstraintAction, ShowConstraintAction,
    AllTokenActions, CreateNodeLabelAction, CreateRelationshipTypeAction, CreatePropertyKeyAction,
    AllTransactionActions, ShowTransactionAction, TerminateTransactionAction
  )

  def _graphAction: Gen[GraphAction] = oneOf(
    TraverseAction, ReadAction, MatchAction, MergeAdminAction, CreateElementAction, DeleteElementAction, WriteAction, RemoveLabelAction, SetLabelAction, SetPropertyAction, AllGraphAction
  )

  def _dbmsQualifier(dbmsAction: DbmsAction): Gen[List[PrivilegeQualifier]] =
    if (dbmsAction == ExecuteProcedureAction || dbmsAction == ExecuteBoostedProcedureAction) {
      // Procedures
      for {
        procedureNamespace <- _namespace
        procedureName <- _procedureName
        procedures <- oneOrMore(ProcedureQualifier(procedureNamespace, procedureName)(pos))
        qualifier <- frequency(7 -> procedures, 3 -> List(ProcedureQualifier(Namespace()(pos), ProcedureName("*")(pos))(pos)))
      } yield qualifier
    } else if (dbmsAction == ExecuteFunctionAction || dbmsAction == ExecuteBoostedFunctionAction) {
      // Functions
      for {
        functionNamespace <- _namespace
        functionName <- _functionName
        functions <- oneOrMore(FunctionQualifier(functionNamespace, functionName)(pos))
        qualifier <- frequency(7 -> functions, 3 -> List(FunctionQualifier(Namespace()(pos), FunctionName("*")(pos))(pos)))
      } yield qualifier
    } else if (dbmsAction == ImpersonateUserAction) {
      // impersonation
      for {
        userNames <- _listOfNameOfEither
        qualifier <- frequency( 7 -> userNames.map(UserQualifier(_)(pos)), 3 -> List(UserAllQualifier()(pos)))
      } yield qualifier
    } else {
      // All other dbms privileges have AllQualifier
      List(AllQualifier()(pos))
    }

  def _databaseQualifier(haveUserQualifier: Boolean): Gen[List[DatabasePrivilegeQualifier]] =
    if (haveUserQualifier) {
      for {
        userNames <- _listOfNameOfEither
        qualifier <- frequency( 7 -> userNames.map(UserQualifier(_)(pos)), 3 -> List(UserAllQualifier()(pos)))
      } yield qualifier
    } else {
      List(AllDatabasesQualifier()(pos))
    }

  def _graphQualifier: Gen[List[GraphPrivilegeQualifier]] = for {
    qualifierNames <- oneOrMore(_identifier)
    qualifier <- oneOf(qualifierNames.map(RelationshipQualifier(_)(pos)), List(RelationshipAllQualifier()(pos)),
                       qualifierNames.map(LabelQualifier(_)(pos)), List(LabelAllQualifier()(pos)),
                       qualifierNames.map(ElementQualifier(_)(pos)), List(ElementsAllQualifier()(pos)))
  } yield qualifier

  def _graphQualifierAndResource(graphAction: GraphAction): Gen[(List[GraphPrivilegeQualifier], Option[ActionResource])] =
    if (graphAction == AllGraphAction) {
      // ALL GRAPH PRIVILEGES has AllQualifier and no resource
      (List(AllQualifier()(pos)), None)
    } else if (graphAction == WriteAction) {
      // WRITE has AllElementsQualifier and no resource
      (List(ElementsAllQualifier()(pos)), None)
    } else if (graphAction == SetLabelAction || graphAction == RemoveLabelAction) {
      // SET/REMOVE LABEL have AllLabelQualifier and label resource
      for {
        resourceNames  <- oneOrMore(_identifier)
        resource       <- oneOf(LabelsResource(resourceNames)(pos), AllLabelResource()(pos))
      } yield (List(LabelAllQualifier()(pos)), Some(resource))
    } else if (graphAction == TraverseAction || graphAction == CreateElementAction || graphAction == DeleteElementAction) {
      // TRAVERSE, CREATE/DELETE ELEMENT have any graph qualifier and no resource
      for {
        qualifier      <- _graphQualifier
      } yield (qualifier, None)
    } else {
      // READ, MATCH, MERGE, SET PROPERTY have any graph qualifier and property resource
      for {
        qualifier      <- _graphQualifier
        resourceNames  <- oneOrMore(_identifier)
        resource       <- oneOf(PropertiesResource(resourceNames)(pos), AllPropertyResource()(pos))
      } yield (qualifier, Some(resource))
    }

  def _showPrivileges: Gen[ShowPrivileges] = for {
    names      <- _listOfNameOfEither
    showRole   = ShowRolesPrivileges(names)(pos)
    showUser1  = ShowUsersPrivileges(names)(pos)
    showUser2  = ShowUserPrivileges(None)(pos)
    showAll    = ShowAllPrivileges()(pos)
    scope      <- oneOf(showRole, showUser1, showUser2, showAll)
    yields     <- _eitherYieldOrWhere
  } yield ShowPrivileges(scope, yields)(pos)

  def _showPrivilegeCommands: Gen[ShowPrivilegeCommands] = for {
    names      <- _listOfNameOfEither
    showRole   = ShowRolesPrivileges(names)(pos)
    showUser1  = ShowUsersPrivileges(names)(pos)
    showUser2  = ShowUserPrivileges(None)(pos)
    showAll    = ShowAllPrivileges()(pos)
    scope      <- oneOf(showRole, showUser1, showUser2, showAll)
    asRevoke   <- boolean
    yields     <- _eitherYieldOrWhere
  } yield ShowPrivilegeCommands(scope, asRevoke, yields)(pos)

  def _dbmsPrivilege: Gen[PrivilegeCommand] = for {
    dbmsAction      <- _dbmsAction
    qualifier       <- _dbmsQualifier(dbmsAction)
    roleNames       <- _listOfNameOfEither
    revokeType      <- _revokeType
    dbmsGrant       = GrantPrivilege.dbmsAction(dbmsAction, roleNames, qualifier)(pos)
    dbmsDeny        = DenyPrivilege.dbmsAction(dbmsAction, roleNames, qualifier)(pos)
    dbmsRevoke      = RevokePrivilege.dbmsAction(dbmsAction, roleNames, revokeType, qualifier)(pos)
    dbms            <- oneOf(dbmsGrant, dbmsDeny, dbmsRevoke)
  } yield dbms

  def _databasePrivilege: Gen[PrivilegeCommand] = for {
    databaseAction      <- _databaseAction
    namedScope          <- _listOfNameOfEither.map(_.map(n => NamedDatabaseScope(n)(pos)))
    databaseScope       <- oneOf(namedScope, List(AllDatabasesScope()(pos)), List(DefaultDatabaseScope()(pos)), List(HomeDatabaseScope()(pos)))
    databaseQualifier   <- _databaseQualifier(databaseAction.isInstanceOf[TransactionManagementAction])
    roleNames           <- _listOfNameOfEither
    revokeType          <- _revokeType
    databaseGrant       = GrantPrivilege.databaseAction(databaseAction, databaseScope, roleNames, databaseQualifier)(pos)
    databaseDeny        = DenyPrivilege.databaseAction(databaseAction, databaseScope, roleNames, databaseQualifier)(pos)
    databaseRevoke      = RevokePrivilege.databaseAction(databaseAction, databaseScope, roleNames, revokeType, databaseQualifier)(pos)
    database            <- oneOf(databaseGrant, databaseDeny, databaseRevoke)
  } yield database

  def _graphPrivilege: Gen[PrivilegeCommand] = for {
    graphAction                 <- _graphAction
    namedScope                  <- _listOfNameOfEither.map(_.map(n => NamedGraphScope(n)(pos)))
    graphScope                  <- oneOf(namedScope, List(AllGraphsScope()(pos)), List(DefaultGraphScope()(pos)), List(HomeGraphScope()(pos)))
    (qualifier, maybeResource)  <- _graphQualifierAndResource(graphAction)
    roleNames                   <- _listOfNameOfEither
    revokeType                  <- _revokeType
    graphGrant                  = GrantPrivilege.graphAction(graphAction, maybeResource, graphScope, qualifier, roleNames)(pos)
    graphDeny                   = DenyPrivilege.graphAction(graphAction, maybeResource, graphScope, qualifier, roleNames)(pos)
    graphRevoke                 = RevokePrivilege.graphAction(graphAction, maybeResource, graphScope, qualifier, roleNames, revokeType)(pos)
    graph                       <- oneOf(graphGrant, graphDeny, graphRevoke)
  } yield graph

  def _privilegeCommand: Gen[AdministrationCommand] = oneOf(
    _showPrivileges,
    _showPrivilegeCommands,
    _dbmsPrivilege,
    _databasePrivilege,
    _graphPrivilege
  )

  // Database commands

  def _showDatabase: Gen[ShowDatabase] = for {
    dbName <- _nameAsEither
    scope  <- oneOf(NamedDatabaseScope(dbName)(pos), AllDatabasesScope()(pos), DefaultDatabaseScope()(pos), HomeDatabaseScope()(pos))
    yields <- _eitherYieldOrWhere
  } yield ShowDatabase(scope, yields)(pos)

  def _createDatabase: Gen[CreateDatabase] = for {
    dbName <- _nameAsEither
    ifExistsDo <- _ifExistsDo
    wait <- _waitUntilComplete
    options <- _optionsMapAsEither
  } yield CreateDatabase(dbName, ifExistsDo, options, wait)(pos)

  def _dropDatabase: Gen[DropDatabase] = for {
    dbName <- _nameAsEither
    ifExists <- boolean
    additionalAction <- Gen.oneOf( DumpData, DestroyData )
    wait <- _waitUntilComplete
  } yield DropDatabase(dbName, ifExists, additionalAction, wait)(pos)

  def _alterDatabase: Gen[AlterDatabase] = for {
    dbName <- _nameAsEither
    ifExists <- boolean
    access <- _access
  } yield AlterDatabase(dbName, ifExists, access)(pos)

  def _startDatabase: Gen[StartDatabase] = for {
    dbName <- _nameAsEither
    wait <- _waitUntilComplete
  } yield StartDatabase(dbName, wait)(pos)

  def _stopDatabase: Gen[StopDatabase] = for {
    dbName <- _nameAsEither
    wait <- _waitUntilComplete
  } yield StopDatabase(dbName, wait)(pos)

  def _multiDatabaseCommand: Gen[AdministrationCommand] = oneOf(
    _showDatabase,
    _createDatabase,
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

  def _createAlias: Gen[CreateDatabaseAlias] = for {
    aliasName <- _nameAsEither
    targetName <- _nameAsEither
    ifExistsDo <- _ifExistsDo
  } yield CreateDatabaseAlias(aliasName, targetName, ifExistsDo)(pos)

  def _dropAlias: Gen[DropDatabaseAlias] = for {
    aliasName <- _nameAsEither
    ifExists <- boolean
  } yield DropDatabaseAlias(aliasName, ifExists)(pos)

  def _alterAlias: Gen[AlterDatabaseAlias] = for {
    aliasName <- _nameAsEither
    targetName <- _nameAsEither
    ifExists <- boolean
  } yield AlterDatabaseAlias(aliasName, targetName, ifExists)(pos)

  def _aliasCommands: Gen[AdministrationCommand] = oneOf(
    _createAlias,
    _dropAlias,
    _alterAlias
  )

  // Top level administration command

  def _adminCommand: Gen[AdministrationCommand] = for {
    command <- oneOf(_userCommand, _roleCommand, _privilegeCommand, _multiDatabaseCommand, _aliasCommands)
    use     <- frequency(1 -> some(_use), 9 -> const(None))
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
