/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AccessDatabaseAction
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdministrationAction
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
import org.neo4j.cypher.internal.ast.AlterUser
import org.neo4j.cypher.internal.ast.AlterUserAction
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.AssignPrivilegeAction
import org.neo4j.cypher.internal.ast.AssignRoleAction
import org.neo4j.cypher.internal.ast.BtreeIndexes
import org.neo4j.cypher.internal.ast.BuiltInFunctions
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.ConstraintVersion0
import org.neo4j.cypher.internal.ast.ConstraintVersion1
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.CreateBtreeNodeIndex
import org.neo4j.cypher.internal.ast.CreateBtreeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateConstraintAction
import org.neo4j.cypher.internal.ast.CreateDatabase
import org.neo4j.cypher.internal.ast.CreateDatabaseAction
import org.neo4j.cypher.internal.ast.CreateElementAction
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateIndex
import org.neo4j.cypher.internal.ast.CreateIndexAction
import org.neo4j.cypher.internal.ast.CreateIndexOldSyntax
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeLabelAction
import org.neo4j.cypher.internal.ast.CreatePointNodeIndex
import org.neo4j.cypher.internal.ast.CreatePointRelationshipIndex
import org.neo4j.cypher.internal.ast.CreatePropertyKeyAction
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRangeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRelationshipTypeAction
import org.neo4j.cypher.internal.ast.CreateRole
import org.neo4j.cypher.internal.ast.CreateRoleAction
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateUser
import org.neo4j.cypher.internal.ast.CreateUserAction
import org.neo4j.cypher.internal.ast.CurrentUser
import org.neo4j.cypher.internal.ast.DatabaseAction
import org.neo4j.cypher.internal.ast.DatabasePrivilege
import org.neo4j.cypher.internal.ast.DatabaseResource
import org.neo4j.cypher.internal.ast.DatabaseScope
import org.neo4j.cypher.internal.ast.DbmsAction
import org.neo4j.cypher.internal.ast.DbmsPrivilege
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
import org.neo4j.cypher.internal.ast.DropDatabaseAdditionalAction
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
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FulltextIndexes
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.GrantRolesToUsers
import org.neo4j.cypher.internal.ast.GraphAction
import org.neo4j.cypher.internal.ast.GraphPrivilege
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.HasCatalog
import org.neo4j.cypher.internal.ast.HomeDatabaseScope
import org.neo4j.cypher.internal.ast.HomeGraphScope
import org.neo4j.cypher.internal.ast.IfExistsDo
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
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
import org.neo4j.cypher.internal.ast.MergeAdminAction
import org.neo4j.cypher.internal.ast.NamedDatabaseScope
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.NewSyntax
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.NoResource
import org.neo4j.cypher.internal.ast.NoWait
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.OldValidSyntax
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.PeriodicCommitHint
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PrivilegeType
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.PropertiesResource
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.ReadAction
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
import org.neo4j.cypher.internal.ast.SchemaCommand
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.SeekOrScan
import org.neo4j.cypher.internal.ast.SetClause
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
import org.neo4j.cypher.internal.ast.ShowConstraintAction
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexAction
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowPrivilegeAction
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowRoleAction
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowTransactionAction
import org.neo4j.cypher.internal.ast.ShowUserAction
import org.neo4j.cypher.internal.ast.ShowUsers
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
import org.neo4j.cypher.internal.ast.TerminateTransactionAction
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.TimeoutAfter
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
import org.neo4j.cypher.internal.ast.UsingHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.WaitUntilComplete
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.WriteAction
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.ASTFactory
import org.neo4j.cypher.internal.ast.factory.ASTFactory.MergeActionType
import org.neo4j.cypher.internal.ast.factory.ASTFactory.StringPos
import org.neo4j.cypher.internal.ast.factory.ActionType
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.ast.factory.ConstraintVersion
import org.neo4j.cypher.internal.ast.factory.CreateIndexTypes
import org.neo4j.cypher.internal.ast.factory.ParameterType
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
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractExpression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FilterExpression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.In
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
import org.neo4j.cypher.internal.expressions.ParameterWithOldSyntax
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
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
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
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString

import java.lang
import java.nio.charset.StandardCharsets
import java.util
import java.util.stream.Collectors
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.mapAsScalaMap
import scala.util.Either

final case class Privilege(privilegeType: PrivilegeType, resource: ActionResource, qualifier: util.List[PrivilegeQualifier])

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

class Neo4jASTFactory(query: String, anonymousVariableNameGenerator: AnonymousVariableNameGenerator)
  extends ASTFactory[Statement,
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
    InputPosition] {

  override def newSingleQuery(p: InputPosition, clauses: util.List[Clause]): Query = {
    if (clauses.isEmpty) {
      throw new Neo4jASTConstructionException("A valid Cypher query has to contain at least 1 clause")
    }
    Query(None, SingleQuery(clauses.asScala.toList)(p))(p)
  }

  override def newSingleQuery(clauses: util.List[Clause]): Query = {
    if (clauses.isEmpty) {
      throw new Neo4jASTConstructionException("A valid Cypher query has to contain at least 1 clause")
    }
    val pos = clauses.get(0).position
    Query(None, SingleQuery(clauses.asScala.toList)(pos))(pos)
  }

  override def newUnion(p: InputPosition,
                        lhs: Query,
                        rhs: Query,
                        all: Boolean): Query = {
    val rhsQuery =
      rhs.part match {
        case x: SingleQuery => x
        case other =>
          throw new Neo4jASTConstructionException(
            s"The Neo4j AST encodes Unions as a left-deep tree, so the rhs query must always be a SingleQuery. Got `$other`")
      }

    val union =
      if (all) UnionAll(lhs.part, rhsQuery)(p)
      else UnionDistinct(lhs.part, rhsQuery)(p)
    Query(None, union)(lhs.position)
  }

  override def periodicCommitQuery(p: InputPosition,
                                   periodicCommitPosition: InputPosition,
                                   batchSize: String,
                                   loadCsv: Clause,
                                   queryBody: util.List[Clause]): Query =
    Query(Some(PeriodicCommitHint(Option(batchSize).map(SignedDecimalIntegerLiteral(_)(periodicCommitPosition)))(periodicCommitPosition)),
      SingleQuery(loadCsv +: queryBody.asScala)(loadCsv.position)
    )(p)

  override def useClause(p: InputPosition,
                         e: Expression): UseGraph = UseGraph(e)(p)

  override def newReturnClause(p: InputPosition,
                               distinct: Boolean,
                               returnItems: ReturnItems,
                               order: util.List[SortItem],
                               orderPosition: InputPosition,
                               skip: Expression,
                               skipPosition: InputPosition,
                               limit: Expression,
                               limitPosition: InputPosition): Return = {
    val orderList = order.asScala.toList
    Return(distinct,
      returnItems,
      if (order.isEmpty) None else Some(OrderBy(orderList)(orderPosition)),
      Option(skip).map(e => Skip(e)(skipPosition)),
      Option(limit).map(e => Limit(e)(limitPosition)))(p)
  }

  override def newReturnItems(p: InputPosition, returnAll: Boolean, returnItems: util.List[ReturnItem]): ReturnItems = {
    ReturnItems(returnAll, returnItems.asScala.toList)(p)
  }

  override def newReturnItem(p: InputPosition, e: Expression, v: Variable): ReturnItem = {
    AliasedReturnItem(e, v)(p)
  }

  override def newReturnItem(p: InputPosition,
                             e: Expression,
                             eStartOffset: Int,
                             eEndOffset: Int): ReturnItem = {

    val name = query.substring(eStartOffset, eEndOffset + 1)
    UnaliasedReturnItem(e, name)(p)
  }

  override def orderDesc(p: InputPosition, e: Expression): SortItem = DescSortItem(e)(p)

  override def orderAsc(p: InputPosition, e: Expression): SortItem = AscSortItem(e)(p)

  override def createClause(p: InputPosition, patterns: util.List[PatternPart]): Clause =
    Create(Pattern(patterns.asScala.toList)(patterns.asScala.map(_.position).minBy(_.offset)))(p)

  override def matchClause(p: InputPosition,
                           optional: Boolean,
                           patterns: util.List[PatternPart],
                           patternPos: InputPosition,
                           hints: util.List[UsingHint],
                           where: Where): Clause = {
    val patternList = patterns.asScala.toList
    Match(optional, Pattern(patternList)(patternPos), if (hints == null) Nil else hints.asScala.toList, Option(where))(p)
  }

  override def usingIndexHint(p: InputPosition,
                              v: Variable,
                              labelOrRelType: String,
                              properties: util.List[String],
                              seekOnly: Boolean): UsingHint =
    ast.UsingIndexHint(v,
      LabelOrRelTypeName(labelOrRelType)(p),
      properties.asScala.toList.map(PropertyKeyName(_)(p)),
      if (seekOnly) SeekOnly else SeekOrScan)(p)

  override def usingJoin(p: InputPosition, joinVariables: util.List[Variable]): UsingHint =
    UsingJoinHint(joinVariables.asScala.toList)(p)

  override def usingScan(p: InputPosition,
                         v: Variable,
                         labelOrRelType: String): UsingHint =
    UsingScanHint(v, LabelOrRelTypeName(labelOrRelType)(p))(p)

  override def withClause(p: InputPosition,
                          r: Return,
                          where: Where): Clause =
    With(r.distinct,
      r.returnItems,
      r.orderBy,
      r.skip,
      r.limit,
      Option(where))(p)

  override def whereClause(p: InputPosition,
                           where: Expression): Where =
    Where(where)(p)

  override def setClause(p: InputPosition, setItems: util.List[SetItem]): SetClause =
    SetClause(setItems.asScala.toList)(p)

  override def setProperty(property: Property,
                           value: Expression): SetItem = SetPropertyItem(property, value)(property.position)

  override def setVariable(variable: Variable,
                           value: Expression): SetItem = SetExactPropertiesFromMapItem(variable, value)(variable.position)

  override def addAndSetVariable(variable: Variable,
                                 value: Expression): SetItem = SetIncludingPropertiesFromMapItem(variable, value)(variable.position)

  override def setLabels(variable: Variable,
                         labels: util.List[StringPos[InputPosition]]): SetItem =
    SetLabelItem(variable, labels.asScala.toList.map(sp => LabelName(sp.string)(sp.pos)))(variable.position)

  override def removeClause(p: InputPosition, removeItems: util.List[RemoveItem]): Clause =
    Remove(removeItems.asScala.toList)(p)

  override def removeProperty(property: Property): RemoveItem = RemovePropertyItem(property)

  override def removeLabels(variable: Variable, labels: util.List[StringPos[InputPosition]]): RemoveItem =
    RemoveLabelItem(variable, labels.asScala.toList.map(sp => LabelName(sp.string)(sp.pos)))(variable.position)

  override def deleteClause(p: InputPosition,
                            detach: Boolean,
                            expressions: util.List[Expression]): Clause = Delete(expressions.asScala.toList, detach)(p)

  override def unwindClause(p: InputPosition,
                            e: Expression,
                            v: Variable): Clause = Unwind(e, v)(p)

  override def mergeClause(p: InputPosition,
                           pattern: PatternPart,
                           setClauses: util.List[SetClause],
                           actionTypes: util.List[MergeActionType],
                           positions: util.List[InputPosition]): Clause = {
    val clausesIter = setClauses.iterator()
    val positionItr = positions.iterator()
    val actions = actionTypes.asScala.toList.map {
      case MergeActionType.OnMatch =>
        OnMatch(clausesIter.next())(positionItr.next)
      case MergeActionType.OnCreate =>
        OnCreate(clausesIter.next())(positionItr.next)
    }

    Merge(Pattern(Seq(pattern))(p), actions)(p)
  }

  override def callClause(p: InputPosition,
                          namespacePosition: InputPosition,
                          procedureNamePosition: InputPosition,
                          procedureResultPosition: InputPosition,
                          namespace: util.List[String],
                          name: String,
                          arguments: util.List[Expression],
                          yieldAll: Boolean,
                          resultItems: util.List[ProcedureResultItem],
                          where: Where): Clause =
    UnresolvedCall(
      Namespace(namespace.asScala.toList)(namespacePosition),
      ProcedureName(name)(procedureNamePosition),
      if (arguments == null) None else Some(arguments.asScala.toList),
      Option(resultItems).map(items => ProcedureResult(items.asScala.toList.toIndexedSeq, Option(where))(procedureResultPosition)),
      yieldAll
    )(p)

  override def callResultItem(p: InputPosition,
                              name: String,
                              v: Variable): ProcedureResultItem =
    if (v == null) ProcedureResultItem(Variable(name)(p))(p)
    else ProcedureResultItem(ProcedureOutput(name)(v.position), v)(p)

  override def loadCsvClause(p: InputPosition,
                             headers: Boolean,
                             source: Expression,
                             v: Variable,
                             fieldTerminator: String): Clause =
    LoadCSV(headers, source, v, Option(fieldTerminator).map(StringLiteral(_)(p)))(p)

  override def foreachClause(p: InputPosition,
                             v: Variable,
                             list: Expression,
                             clauses: util.List[Clause]): Clause =
    Foreach(v, list, clauses.asScala.toList)(p)

  override def subqueryInTransactionsParams(p: InputPosition): SubqueryCall.InTransactionsParameters =
    SubqueryCall.InTransactionsParameters()(p)

  override def subqueryClause(p: InputPosition, subquery: Query, inTransactions: SubqueryCall.InTransactionsParameters): Clause =
    SubqueryCall(subquery.part, Option(inTransactions))(p)

  // PATTERNS

  override def namedPattern(v: Variable,
                            pattern: PatternPart): PatternPart =
    NamedPatternPart(v, pattern.asInstanceOf[AnonymousPatternPart])(v.position)

  override def shortestPathPattern(p: InputPosition, pattern: PatternPart): PatternPart = ShortestPaths(pattern.element, single = true)(p)

  override def allShortestPathsPattern(p: InputPosition, pattern: PatternPart): PatternPart = ShortestPaths(pattern.element, single = false)(p)

  override def everyPathPattern(nodes: util.List[NodePattern],
                                relationships: util.List[RelationshipPattern]): PatternPart = {

    val nodeIter = nodes.iterator()
    val relIter = relationships.iterator()

    var patternElement: PatternElement = nodeIter.next()
    while (relIter.hasNext) {
      val relPattern = relIter.next()
      val rightNodePattern = nodeIter.next()
      patternElement = RelationshipChain(patternElement, relPattern, rightNodePattern)(patternElement.position)
    }
    EveryPath(patternElement)
  }

  override def nodePattern(p: InputPosition,
                           v: Variable,
                           labels: util.List[StringPos[InputPosition]],
                           properties: Expression,
                           predicate: Expression): NodePattern =
    NodePattern(Option(v), labels.asScala.toList.map(sp => LabelName(sp.string)(sp.pos)), Option(properties), Option(predicate))(p)

  override def relationshipPattern(p: InputPosition,
                                   left: Boolean,
                                   right: Boolean,
                                   v: Variable,
                                   relTypes: util.List[StringPos[InputPosition]],
                                   pathLength: Option[Range],
                                   properties: Expression,
                                   legacyTypeSeparator: Boolean): RelationshipPattern = {
    val direction =
      if (left && !right) SemanticDirection.INCOMING
      else if (!left && right) SemanticDirection.OUTGOING
      else SemanticDirection.BOTH

    val range =
      pathLength match {
        case null => None
        case None => Some(None)
        case Some(r) => Some(Some(r))
      }

    RelationshipPattern(
      Option(v),
      relTypes.asScala.toList.map(sp => RelTypeName(sp.string)(sp.pos)),
      range,
      Option(properties),
      direction,
      legacyTypeSeparator)(p)
  }

  override def pathLength(p: InputPosition, pMin: InputPosition, pMax: InputPosition, minLength: String, maxLength: String): Option[Range] = {
    if (minLength == null && maxLength == null) {
      None
    } else {
      val min = if (minLength == "") None else Some(UnsignedDecimalIntegerLiteral(minLength)(pMin))
      val max = if (maxLength == "") None else Some(UnsignedDecimalIntegerLiteral(maxLength)(pMax))
      Some(Range(min, max)(if (pMin != null) pMin else p))
    }
  }

  // EXPRESSIONS

  override def newVariable(p: InputPosition, name: String): Variable = Variable(name)(p)

  override def newParameter(p: InputPosition, v: Variable, t: ParameterType): Parameter = {
    Parameter(v.name, transformParameterType(t))(p)
  }

  override def newParameter(p: InputPosition, offset: String, t: ParameterType): Parameter = {
    Parameter(offset, transformParameterType(t))(p)
  }

  private def transformParameterType(t: ParameterType) = {
    t match {
      case ParameterType.ANY => CTAny
      case ParameterType.STRING => CTString
      case ParameterType.MAP => CTMap
      case _ => throw new IllegalArgumentException("unknown parameter type: " + t.toString)
    }
  }
  override def newSensitiveStringParameter(p: InputPosition, v: Variable): Parameter = new ExplicitParameter(v.name, CTString)(p) with SensitiveParameter

  override def newSensitiveStringParameter(p: InputPosition, offset: String): Parameter = new ExplicitParameter(offset, CTString)(p) with SensitiveParameter

  override def oldParameter(p: InputPosition, v: Variable): Expression = ParameterWithOldSyntax(v.name, CTAny)(p)

  override def newDouble(p: InputPosition, image: String): Expression = DecimalDoubleLiteral(image)(p)

  override def newDecimalInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedDecimalIntegerLiteral("-"+image)(p)
    else SignedDecimalIntegerLiteral(image)(p)

  override def newHexInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedHexIntegerLiteral("-"+image)(p)
    else SignedHexIntegerLiteral(image)(p)

  override def newOctalInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedOctalIntegerLiteral("-"+image)(p)
    else SignedOctalIntegerLiteral(image)(p)

  override def newString(p: InputPosition, image: String): Expression = StringLiteral(image)(p)

  override def newTrueLiteral(p: InputPosition): Expression = True()(p)

  override def newFalseLiteral(p: InputPosition): Expression = False()(p)

  override def newNullLiteral(p: InputPosition): Expression = Null()(p)

  override def listLiteral(p: InputPosition, values: util.List[Expression]): Expression = {
    ListLiteral(values.asScala.toList)(p)
  }

  override def mapLiteral(p: InputPosition,
                          keys: util.List[StringPos[InputPosition]],
                          values: util.List[Expression]): Expression = {

    if (keys.size() != values.size()) {
      throw new Neo4jASTConstructionException(
        s"Map have the same number of keys and values, but got keys `${pretty(keys)}` and values `${pretty(values)}`")
    }

    var i = 0
    val pairs = new Array[(PropertyKeyName, Expression)](keys.size())

    while (i < keys.size()) {
      val key = keys.get(i)
      pairs(i) = PropertyKeyName(key.string)(key.pos) -> values.get(i)
      i += 1
    }

    MapExpression(pairs)(p)
  }

  override def hasLabelsOrTypes(subject: Expression, labels: util.List[StringPos[InputPosition]]): Expression =
    HasLabelsOrTypes(subject, labels.asScala.toList.map(sp => LabelOrRelTypeName(sp.string)(sp.pos)))(subject.position)

  override def property(subject: Expression, propertyKeyName: StringPos[InputPosition]): Property =
    Property(subject, PropertyKeyName(propertyKeyName.string)(propertyKeyName.pos))(subject.position)

  override def or(p: InputPosition,
                  lhs: Expression,
                  rhs: Expression): Expression = Or(lhs, rhs)(p)

  override def xor(p: InputPosition,
                   lhs: Expression,
                   rhs: Expression): Expression = Xor(lhs, rhs)(p)

  override def and(p: InputPosition,
                   lhs: Expression,
                   rhs: Expression): Expression = And(lhs, rhs)(p)

  override def ands(exprs: util.List[Expression]): Expression = Ands(exprs.asScala.toList)(exprs.get(0).position)

  override def not(p: InputPosition, e: Expression): Expression =
    e match {
      case IsNull(e) => IsNotNull(e)(p)
      case _ => Not(e)(p)
    }

  override def plus(p: InputPosition,
                    lhs: Expression,
                    rhs: Expression): Expression = Add(lhs, rhs)(p)

  override def minus(p: InputPosition,
                     lhs: Expression,
                     rhs: Expression): Expression = Subtract(lhs, rhs)(p)

  override def multiply(p: InputPosition,
                        lhs: Expression,
                        rhs: Expression): Expression = Multiply(lhs, rhs)(p)

  override def divide(p: InputPosition,
                      lhs: Expression,
                      rhs: Expression): Expression = Divide(lhs, rhs)(p)

  override def modulo(p: InputPosition,
                      lhs: Expression,
                      rhs: Expression): Expression = Modulo(lhs, rhs)(p)

  override def pow(p: InputPosition,
                   lhs: Expression,
                   rhs: Expression): Expression = Pow(lhs, rhs)(p)

  override def unaryPlus(e: Expression): Expression = unaryPlus(e.position, e)

  override def unaryPlus(p: InputPosition, e: Expression): Expression = UnaryAdd(e)(p)

  override def unaryMinus(p: InputPosition, e: Expression): Expression = UnarySubtract(e)(p)

  override def eq(p: InputPosition,
                  lhs: Expression,
                  rhs: Expression): Expression = Equals(lhs, rhs)(p)

  override def neq(p: InputPosition,
                   lhs: Expression,
                   rhs: Expression): Expression = InvalidNotEquals(lhs, rhs)(p)

  override def neq2(p: InputPosition,
                    lhs: Expression,
                    rhs: Expression): Expression = NotEquals(lhs, rhs)(p)

  override def lte(p: InputPosition,
                   lhs: Expression,
                   rhs: Expression): Expression = LessThanOrEqual(lhs, rhs)(p)

  override def gte(p: InputPosition,
                   lhs: Expression,
                   rhs: Expression): Expression = GreaterThanOrEqual(lhs, rhs)(p)

  override def lt(p: InputPosition,
                  lhs: Expression,
                  rhs: Expression): Expression = LessThan(lhs, rhs)(p)

  override def gt(p: InputPosition,
                  lhs: Expression,
                  rhs: Expression): Expression = GreaterThan(lhs, rhs)(p)

  override def regeq(p: InputPosition,
                     lhs: Expression,
                     rhs: Expression): Expression = RegexMatch(lhs, rhs)(p)

  override def startsWith(p: InputPosition,
                          lhs: Expression,
                          rhs: Expression): Expression = StartsWith(lhs, rhs)(p)

  override def endsWith(p: InputPosition,
                        lhs: Expression,
                        rhs: Expression): Expression = EndsWith(lhs, rhs)(p)

  override def contains(p: InputPosition,
                        lhs: Expression,
                        rhs: Expression): Expression = Contains(lhs, rhs)(p)

  override def in(p: InputPosition,
                  lhs: Expression,
                  rhs: Expression): Expression = In(lhs, rhs)(p)

  override def isNull(p: InputPosition, e: Expression): Expression = IsNull(e)(p)

  override def listLookup(list: Expression,
                          index: Expression): Expression = ContainerIndex(list, index)(index.position)

  override def listSlice(p: InputPosition,
                         list: Expression,
                         start: Expression,
                         end: Expression): Expression = {
    ListSlice(list, Option(start), Option(end))(p)
  }

  override def newCountStar(p: InputPosition): Expression = CountStar()(p)

  override def functionInvocation(p: InputPosition,
                                  functionNamePosition: InputPosition,
                                  namespace: util.List[String],
                                  name: String,
                                  distinct: Boolean,
                                  arguments: util.List[Expression]): Expression = {
    FunctionInvocation(Namespace(namespace.asScala.toList)(p),
      FunctionName(name)(functionNamePosition),
      distinct,
      arguments.asScala.toIndexedSeq)(p)
  }

  override def listComprehension(p: InputPosition,
                                 v: Variable,
                                 list: Expression,
                                 where: Expression,
                                 projection: Expression): Expression =
    ListComprehension(v, list, Option(where), Option(projection))(p)

  override def patternComprehension(p: InputPosition,
                                    relationshipPatternPosition: InputPosition,
                                    v: Variable,
                                    pattern: PatternPart,
                                    where: Expression,
                                    projection: Expression): Expression =
    PatternComprehension(Option(v),
      RelationshipsPattern(pattern.element.asInstanceOf[RelationshipChain])(relationshipPatternPosition),
      Option(where),
      projection)(p, Set.empty, anonymousVariableNameGenerator.nextName, anonymousVariableNameGenerator.nextName)

  override def filterExpression(p: InputPosition,
                                v: Variable,
                                list: Expression,
                                where: Expression): Expression =
    FilterExpression(v, list, Option(where))(p)

  override def extractExpression(p: InputPosition,
                                 v: Variable,
                                 list: Expression,
                                 where: Expression,
                                 projection: Expression): Expression =
    ExtractExpression(v, list, Option(where), Option(projection))(p)

  override def reduceExpression(p: InputPosition,
                                acc: Variable,
                                accExpr: Expression,
                                v: Variable,
                                list: Expression,
                                innerExpr: Expression): Expression =
    ReduceExpression(acc, accExpr, v, list, innerExpr)(p)

  override def allExpression(p: InputPosition,
                             v: Variable,
                             list: Expression,
                             where: Expression): Expression =
    AllIterablePredicate(v, list, Option(where))(p)

  override def anyExpression(p: InputPosition,
                             v: Variable,
                             list: Expression,
                             where: Expression): Expression =
    AnyIterablePredicate(v, list, Option(where))(p)

  override def noneExpression(p: InputPosition,
                              v: Variable,
                              list: Expression,
                              where: Expression): Expression =
    NoneIterablePredicate(v, list, Option(where))(p)

  override def singleExpression(p: InputPosition,
                                v: Variable,
                                list: Expression,
                                where: Expression): Expression =
    SingleIterablePredicate(v, list, Option(where))(p)

  override def patternExpression(p: InputPosition, pattern: PatternPart): Expression =
    pattern match {
      case paths: ShortestPaths =>
        ShortestPathExpression(paths)
      case _ =>
        PatternExpression(RelationshipsPattern(pattern.element.asInstanceOf[RelationshipChain])(p))(Set.empty, anonymousVariableNameGenerator.nextName, anonymousVariableNameGenerator.nextName)
    }

  override def existsSubQuery(p: InputPosition,
                              patterns: util.List[PatternPart],
                              where: Expression): Expression =
    ExistsSubClause(Pattern(patterns.asScala.toList)(p), Option(where))(p, Set.empty)

  override def mapProjection(p: InputPosition,
                             v: Variable,
                             items: util.List[MapProjectionElement]): Expression =
    MapProjection(v, items.asScala.toList)(p)

  override def mapProjectionLiteralEntry(property: StringPos[InputPosition],
                                         value: Expression): MapProjectionElement =
    LiteralEntry(PropertyKeyName(property.string)(property.pos), value)(value.position)

  override def mapProjectionProperty(property: StringPos[InputPosition]): MapProjectionElement =
    PropertySelector(Variable(property.string)(property.pos))(property.pos)

  override def mapProjectionVariable(v: Variable): MapProjectionElement =
    VariableSelector(v)(v.position)

  override def mapProjectionAll(p: InputPosition): MapProjectionElement =
    AllPropertiesSelector()(p)

  override def caseExpression(p: InputPosition,
                              e: Expression,
                              whens: util.List[Expression],
                              thens: util.List[Expression],
                              elze: Expression): Expression = {

    if (whens.size() != thens.size()) {
      throw new Neo4jASTConstructionException(
        s"Case expressions have the same number of whens and thens, but got whens `${pretty(whens)}` and thens `${pretty(thens)}`")
    }

    val alternatives = new Array[(Expression, Expression)](whens.size())
    var i = 0
    while (i < whens.size()) {
      alternatives(i) = whens.get(i) -> thens.get(i)
      i += 1
    }
    CaseExpression(Option(e), alternatives, Option(elze))(p)
  }

  override def inputPosition(offset: Int, line: Int, column: Int): InputPosition = InputPosition(offset, line, column)

  // Commands

  override def useGraph(command: StatementWithGraph, graph: UseGraph): StatementWithGraph = {
    command.withGraph(Option(graph))
  }

  override def hasCatalog(statement: Statement): AdministrationCommand = {
    statement match {
      case command: AdministrationCommand => HasCatalog(command)
      case _ => throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidCatalogStatement)
    }
  }

  // Show Commands

  override def yieldClause(p: InputPosition,
                           returnAll: Boolean,
                           returnItemList: util.List[ReturnItem],
                           returnItemsP: InputPosition,
                           order: util.List[SortItem],
                           orderPos: InputPosition,
                           skip: Expression,
                           skipPosition: InputPosition,
                           limit: Expression,
                           limitPosition: InputPosition,
                           where: Where): Yield = {

    val returnItems = ReturnItems(returnAll, returnItemList.asScala.toList)(returnItemsP)

    Yield(returnItems,
      Option(order.asScala.toList).filter(_.nonEmpty).map(o => OrderBy(o)(orderPos)),
      Option(skip).map(s => Skip(s)(skipPosition)),
      Option(limit).map(l => Limit(l)(limitPosition)),
      Option(where)
    )(p)
  }

  override def showIndexClause(p: InputPosition,
                               initialIndexType: ShowCommandFilterTypes,
                               brief: Boolean,
                               verbose: Boolean,
                               where: Where,
                               hasYield: Boolean): Clause = {
    val indexType = initialIndexType match {
      case ShowCommandFilterTypes.ALL => AllIndexes
      case ShowCommandFilterTypes.BTREE => BtreeIndexes
      case ShowCommandFilterTypes.RANGE => RangeIndexes
      case ShowCommandFilterTypes.FULLTEXT => FulltextIndexes
      case ShowCommandFilterTypes.TEXT => TextIndexes
      case ShowCommandFilterTypes.POINT => PointIndexes
      case ShowCommandFilterTypes.LOOKUP => LookupIndexes
      case t => throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidShowFilterType("indexes", t))
    }
    ShowIndexesClause(indexType, brief, verbose, Option(where), hasYield)(p)
  }

  override def showConstraintClause(p: InputPosition,
                                    initialConstraintType: ShowCommandFilterTypes,
                                    brief: Boolean,
                                    verbose: Boolean,
                                    where: Where,
                                    hasYield: Boolean): Clause = {
    val constraintType: ShowConstraintType = initialConstraintType match {
      case ShowCommandFilterTypes.ALL => AllConstraints
      case ShowCommandFilterTypes.UNIQUE => UniqueConstraints
      case ShowCommandFilterTypes.NODE_KEY => NodeKeyConstraints
      case ShowCommandFilterTypes.EXIST  => ExistsConstraints(NewSyntax)
      case ShowCommandFilterTypes.OLD_EXISTS => ExistsConstraints(DeprecatedSyntax)
      case ShowCommandFilterTypes.OLD_EXIST => ExistsConstraints(OldValidSyntax)
      case ShowCommandFilterTypes.NODE_EXIST => NodeExistsConstraints(NewSyntax)
      case ShowCommandFilterTypes.NODE_OLD_EXISTS => NodeExistsConstraints(DeprecatedSyntax)
      case ShowCommandFilterTypes.NODE_OLD_EXIST => NodeExistsConstraints(OldValidSyntax)
      case ShowCommandFilterTypes.RELATIONSHIP_EXIST => RelExistsConstraints(NewSyntax)
      case ShowCommandFilterTypes.RELATIONSHIP_OLD_EXISTS => RelExistsConstraints(DeprecatedSyntax)
      case ShowCommandFilterTypes.RELATIONSHIP_OLD_EXIST => RelExistsConstraints(OldValidSyntax)
      case t => throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidShowFilterType("constraints", t))
    }
    ShowConstraintsClause(constraintType, brief, verbose, Option(where), hasYield)(p)
  }

  override def showProcedureClause(p: InputPosition,
                                   currentUser: Boolean,
                                   user: String,
                                   where: Where,
                                   hasYield: Boolean): Clause = {
    // either we have 'EXECUTABLE BY user', 'EXECUTABLE [BY CURRENT USER]' or nothing
    val executableBy = if (user != null) Some(User(user)) else if (currentUser) Some(CurrentUser) else None
    ShowProceduresClause(executableBy, Option(where), hasYield)(p)
  }

  override def showFunctionClause(p: InputPosition,
                                  initialFunctionType: ShowCommandFilterTypes,
                                  currentUser: Boolean,
                                  user: String,
                                  where: Where,
                                  hasYield: Boolean): Clause = {
    val functionType = initialFunctionType match {
      case ShowCommandFilterTypes.ALL   => AllFunctions
      case ShowCommandFilterTypes.BUILT_IN => BuiltInFunctions
      case ShowCommandFilterTypes.USER_DEFINED  => UserDefinedFunctions
      case t => throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidShowFilterType("functions", t))
    }

    // either we have 'EXECUTABLE BY user', 'EXECUTABLE [BY CURRENT USER]' or nothing
    val executableBy = if (user != null) Some(User(user)) else if (currentUser) Some(CurrentUser) else None
    ShowFunctionsClause(functionType, executableBy, Option(where), hasYield)(p)
  }

  // Schema Commands
  // Constraint Commands

  override def createConstraint(p: InputPosition,
                                constraintType: ConstraintType,
                                replace: Boolean,
                                ifNotExists: Boolean,
                                name: String,
                                variable: Variable,
                                label: StringPos[InputPosition],
                                javaProperties: util.List[Property],
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

    val properties = javaProperties.asScala
    constraintType match {
      case ConstraintType.UNIQUE => ast.CreateUniquePropertyConstraint(variable, LabelName(label.string)(label.pos), properties, Option(name),
        ifExistsDo(replace, ifNotExists), asOptionsAst(options), containsOn, constraintVersionScala)(p)
      case ConstraintType.NODE_KEY => ast.CreateNodeKeyConstraint(variable, LabelName(label.string)(label.pos), properties, Option(name),
        ifExistsDo(replace, ifNotExists), asOptionsAst(options), containsOn, constraintVersionScala)(p)
      case ConstraintType.NODE_EXISTS | ConstraintType.NODE_IS_NOT_NULL =>
        validateSingleProperty(properties, constraintType)
        ast.CreateNodePropertyExistenceConstraint(variable, LabelName(label.string)(label.pos), properties.head, Option(name), ifExistsDo(replace, ifNotExists), asOptionsAst(options), containsOn, constraintVersionScala)(p)
      case ConstraintType.REL_EXISTS | ConstraintType.REL_IS_NOT_NULL =>
        validateSingleProperty(properties, constraintType)
        ast.CreateRelationshipPropertyExistenceConstraint(variable, RelTypeName(label.string)(label.pos), properties.head, Option(name), ifExistsDo(replace, ifNotExists), asOptionsAst(options), containsOn, constraintVersionScala)(p)
    }
  }

  override def dropConstraint(p: InputPosition, name: String, ifExists: Boolean): DropConstraintOnName = DropConstraintOnName(name, ifExists)(p)

  override def dropConstraint(p: InputPosition,
                              constraintType: ConstraintType,
                              variable: Variable,
                              label: StringPos[InputPosition],
                              javaProperties: util.List[Property]): SchemaCommand = {
    val properties = javaProperties.asScala
    constraintType match {
      case ConstraintType.UNIQUE => DropUniquePropertyConstraint(variable, LabelName(label.string)(label.pos), properties)(p)
      case ConstraintType.NODE_KEY => DropNodeKeyConstraint(variable, LabelName(label.string)(label.pos), properties)(p)
      case ConstraintType.NODE_EXISTS =>
        validateSingleProperty(properties, constraintType)
        DropNodePropertyExistenceConstraint(variable, LabelName(label.string)(label.pos), properties.head)(p)
      case ConstraintType.NODE_IS_NOT_NULL =>
        throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand)
      case ConstraintType.REL_EXISTS =>
        validateSingleProperty(properties, constraintType)
        DropRelationshipPropertyExistenceConstraint(variable, RelTypeName(label.string)(label.pos), properties.head)(p)
      case ConstraintType.REL_IS_NOT_NULL =>
        throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand)
    }
  }

  private def validateSingleProperty(seq: Seq[_], constraintType:ConstraintType): Unit = {
    if (seq.size != 1) throw new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(constraintType))
  }

  // Index Commands

  override def createLookupIndex(p: InputPosition,
                                 replace: Boolean,
                                 ifNotExists:Boolean,
                                 isNode: Boolean,
                                 indexName: String,
                                 variable: Variable,
                                 functionName: StringPos[InputPosition],
                                 functionParameter: Variable,
                                 options: SimpleEither[util.Map[String, Expression], Parameter]
                                ): CreateLookupIndex = {
    val function = FunctionInvocation(FunctionName(functionName.string) (functionName.pos), distinct = false, IndexedSeq(functionParameter))(functionName.pos)
    CreateLookupIndex(variable, isNode, function, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
  }

  override def createIndexWithOldSyntax(p: InputPosition,
                                        label: StringPos[InputPosition],
                                        properties: util.List[StringPos[InputPosition]]): CreateIndexOldSyntax = {
    CreateIndexOldSyntax(LabelName(label.string)(label.pos), properties.asScala.toList.map(prop => PropertyKeyName(prop.string)(prop.pos)))(p)
  }

  override def createIndex(p: InputPosition,
                           replace: Boolean,
                           ifNotExists: Boolean,
                           isNode: Boolean,
                           indexName: String,
                           variable: Variable,
                           label: StringPos[InputPosition],
                           javaProperties: util.List[Property],
                           options: SimpleEither[util.Map[String, Expression], Parameter],
                           indexType: CreateIndexTypes): CreateIndex = {
    val properties = javaProperties.asScala.toList
    (indexType, isNode) match {
      case (CreateIndexTypes.DEFAULT, true)  =>
        CreateRangeNodeIndex(variable, LabelName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options), fromDefault = true)(p)
      case (CreateIndexTypes.DEFAULT, false) =>
        CreateRangeRelationshipIndex(variable, RelTypeName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options), fromDefault = true)(p)
      case (CreateIndexTypes.RANGE, true)    =>
        CreateRangeNodeIndex(variable, LabelName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options), fromDefault = false)(p)
      case (CreateIndexTypes.RANGE, false)   =>
        CreateRangeRelationshipIndex(variable, RelTypeName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options), fromDefault = false)(p)
      case (CreateIndexTypes.BTREE, true)    =>
        CreateBtreeNodeIndex(variable, LabelName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
      case (CreateIndexTypes.BTREE, false)   =>
        CreateBtreeRelationshipIndex(variable, RelTypeName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
      case (CreateIndexTypes.TEXT, true)     =>
        CreateTextNodeIndex(variable, LabelName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
      case (CreateIndexTypes.TEXT, false)    =>
        CreateTextRelationshipIndex(variable, RelTypeName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
      case (CreateIndexTypes.POINT, true)     =>
        CreatePointNodeIndex(variable, LabelName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
      case (CreateIndexTypes.POINT, false)    =>
        CreatePointRelationshipIndex(variable, RelTypeName(label.string)(label.pos), properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
      case (t, _) =>
        throw new Neo4jASTConstructionException(ASTExceptionFactory.invalidCreateIndexType(t))
    }
  }

  override def createFulltextIndex(p: InputPosition,
                                   replace: Boolean,
                                   ifNotExists: Boolean,
                                   isNode: Boolean,
                                   indexName: String,
                                   variable: Variable,
                                   labels: util.List[StringPos[InputPosition]],
                                   javaProperties: util.List[Property],
                                   options: SimpleEither[util.Map[String, Expression], Parameter]): CreateIndex = {
    val properties = javaProperties.asScala.toList
    if (isNode) {
      val labelNames = labels.asScala.toList.map(stringPos => LabelName(stringPos.string)(stringPos.pos))
      CreateFulltextNodeIndex(variable, labelNames, properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
    } else {
      val relTypeNames = labels.asScala.toList.map(stringPos => RelTypeName(stringPos.string)(stringPos.pos))
      CreateFulltextRelationshipIndex(variable, relTypeNames, properties, Option(indexName), ifExistsDo(replace, ifNotExists), asOptionsAst(options))(p)
    }
  }

  override def dropIndex(p: InputPosition, name: String, ifExists: Boolean): DropIndexOnName = DropIndexOnName(name, ifExists)(p)

  override def dropIndex(p: InputPosition,
                         label: StringPos[InputPosition],
                         javaProperties: util.List[StringPos[InputPosition]]): DropIndex = {
    val properties = javaProperties.asScala.map(property => PropertyKeyName(property.string)(property.pos)).toList
    DropIndex(LabelName(label.string)(label.pos), properties)(p)
  }

  // Administration Commands
  // Role commands

  override def createRole(p: InputPosition,
                          replace: Boolean,
                          roleName: SimpleEither[String, Parameter],
                          from: SimpleEither[String, Parameter],
                          ifNotExists: Boolean): CreateRole = {
    CreateRole(roleName.asScala, Option(from).map(_.asScala), ifExistsDo(replace, ifNotExists))(p)
  }

  override def dropRole(p: InputPosition, roleName: SimpleEither[String, Parameter], ifExists: Boolean): DropRole = {
    DropRole(roleName.asScala, ifExists)(p)
  }

  override def renameRole(p: InputPosition, fromRoleName: SimpleEither[String, Parameter], toRoleName: SimpleEither[String, Parameter], ifExists: Boolean): RenameRole = {
    RenameRole(fromRoleName.asScala, toRoleName.asScala, ifExists)(p)
  }

  override def showRoles(p: InputPosition,
                         WithUsers: Boolean,
                         showAll: Boolean,
                         yieldExpr: Yield,
                         returnWithoutGraph: Return,
                         where: Where): ShowRoles = {
    ShowRoles(WithUsers, showAll, yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
  }

  override def grantRoles(p: InputPosition,
                          roles: util.List[SimpleEither[String, Parameter]],
                          users: util.List[SimpleEither[String, Parameter]]): GrantRolesToUsers = {
    GrantRolesToUsers(roles.asScala.map(_.asScala), users.asScala.map(_.asScala))(p)
  }

  override def revokeRoles(p: InputPosition,
                           roles: util.List[SimpleEither[String, Parameter]],
                           users: util.List[SimpleEither[String, Parameter]]): RevokeRolesFromUsers = {
    RevokeRolesFromUsers(roles.asScala.map(_.asScala), users.asScala.map(_.asScala))(p)
  }

  // User commands

  override def createUser(p: InputPosition,
                          replace: Boolean,
                          ifNotExists: Boolean,
                          username: SimpleEither[String, Parameter],
                          password: Expression,
                          encrypted: Boolean,
                          changeRequired: Boolean,
                          suspended: lang.Boolean,
                          homeDatabase: SimpleEither[String, Parameter]): AdministrationCommand = {
    val homeAction = if (homeDatabase == null) None else Some(SetHomeDatabaseAction(homeDatabase.asScala))
    val userOptions = UserOptions(Some(changeRequired), asBooleanOption(suspended), homeAction)
    CreateUser(username.asScala, encrypted, password, userOptions, ifExistsDo(replace, ifNotExists))(p)
  }

  override def dropUser(p: InputPosition, ifExists: Boolean, username: SimpleEither[String, Parameter]): DropUser = {
    DropUser(username.asScala, ifExists)(p)
  }

  override def renameUser(p: InputPosition, fromUserName: SimpleEither[String, Parameter], toUserName: SimpleEither[String, Parameter], ifExists: Boolean): RenameUser = {
    RenameUser(fromUserName.asScala, toUserName.asScala, ifExists)(p)
  }

  override def setOwnPassword(p: InputPosition, currentPassword: Expression, newPassword: Expression): SetOwnPassword = {
    SetOwnPassword(newPassword, currentPassword)(p)
  }

  override def alterUser(p: InputPosition,
                         ifExists: Boolean,
                         username: SimpleEither[String, Parameter],
                         password: Expression,
                         encrypted: Boolean,
                         changeRequired: lang.Boolean,
                         suspended: lang.Boolean,
                         homeDatabase: SimpleEither[String, Parameter],
                         removeHome: Boolean): AlterUser = {
    val maybePassword = Option(password)
    val isEncrypted = if (maybePassword.isDefined) Some(encrypted) else None
    val homeAction = if (removeHome) Some(RemoveHomeDatabaseAction) else if (homeDatabase == null) None else Some(SetHomeDatabaseAction(homeDatabase.asScala))
    val userOptions = UserOptions(asBooleanOption(changeRequired), asBooleanOption(suspended), homeAction)
    AlterUser(username.asScala, isEncrypted, maybePassword, userOptions, ifExists)(p)
  }

  override def passwordExpression(password: Parameter): Expression = new ExplicitParameter(password.name, CTString)(password.position) with SensitiveParameter

  override def passwordExpression(p: InputPosition, password: String): Expression = SensitiveStringLiteral(password.getBytes(StandardCharsets.UTF_8))(p)

  override def showUsers(p: InputPosition, yieldExpr: Yield, returnWithoutGraph: Return, where: Where): ShowUsers = {
    ShowUsers(yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
  }

  override def showCurrentUser(p: InputPosition, yieldExpr: Yield, returnWithoutGraph: Return, where: Where): ShowCurrentUser = {
    ShowCurrentUser(yieldOrWhere(yieldExpr, returnWithoutGraph, where))(p)
  }

  // Privilege commands

  override def grantPrivilege(p: InputPosition,
                              roles: util.List[SimpleEither[String, Parameter]],
                              privilege: Privilege): AdministrationCommand =
    GrantPrivilege(privilege.privilegeType, Option(privilege.resource), privilege.qualifier.asScala.toList, roles.asScala.map(_.asScala))(p)

  override def denyPrivilege(p: InputPosition, roles: util.List[SimpleEither[String, Parameter]], privilege: Privilege): AdministrationCommand =
    DenyPrivilege(privilege.privilegeType, Option(privilege.resource), privilege.qualifier.asScala.toList, roles.asScala.map(_.asScala))(p)

  override def revokePrivilege(p: InputPosition,
                               roles: util.List[SimpleEither[String, Parameter]],
                               privilege: Privilege,
                               revokeGrant: Boolean,
                               revokeDeny: Boolean): AdministrationCommand = (revokeGrant, revokeDeny) match {
    case (true, false) => RevokePrivilege(privilege.privilegeType, Option(privilege.resource), privilege.qualifier.asScala.toList, roles.asScala.map(_.asScala), RevokeGrantType()(p))(p)
    case (false, true) => RevokePrivilege(privilege.privilegeType, Option(privilege.resource), privilege.qualifier.asScala.toList, roles.asScala.map(_.asScala), RevokeDenyType()(p))(p)
    case _             => RevokePrivilege(privilege.privilegeType, Option(privilege.resource), privilege.qualifier.asScala.toList, roles.asScala.map(_.asScala), RevokeBothType()(p))(p)
  }

  override def databasePrivilege(p: InputPosition, action: AdministrationAction, scope: util.List[DatabaseScope], qualifier: util.List[PrivilegeQualifier]): Privilege =
    Privilege(DatabasePrivilege(action.asInstanceOf[DatabaseAction], scope.asScala.toList)(p), null, qualifier)

  override def dbmsPrivilege(p: InputPosition, action: AdministrationAction, qualifier: util.List[PrivilegeQualifier]): Privilege =
    Privilege(DbmsPrivilege(action.asInstanceOf[DbmsAction])(p), null, qualifier)

  override def graphPrivilege(p: InputPosition, action: AdministrationAction, scope: util.List[GraphScope], resource: ActionResource, qualifier: util.List[PrivilegeQualifier]): Privilege =
    Privilege(GraphPrivilege(action.asInstanceOf[GraphAction], scope.asScala.toList)(p), resource, qualifier)

  override def privilegeAction(action: ActionType): AdministrationAction = action match {
    case ActionType.DATABASE_ALL => AllDatabaseAction
    case ActionType.ACCESS => AccessDatabaseAction
    case ActionType.DATABASE_START => StartDatabaseAction
    case ActionType.DATABASE_STOP => StopDatabaseAction
    case ActionType.INDEX_ALL => AllIndexActions
    case ActionType.INDEX_CREATE => CreateIndexAction
    case ActionType.INDEX_DROP => DropIndexAction
    case ActionType.INDEX_SHOW => ShowIndexAction
    case ActionType.CONSTRAINT_ALL => AllConstraintActions
    case ActionType.CONSTRAINT_CREATE => CreateConstraintAction
    case ActionType.CONSTRAINT_DROP => DropConstraintAction
    case ActionType.CONSTRAINT_SHOW => ShowConstraintAction
    case ActionType.CREATE_TOKEN =>AllTokenActions
    case ActionType.CREATE_PROPERTYKEY => CreatePropertyKeyAction
    case ActionType.CREATE_LABEL => CreateNodeLabelAction
    case ActionType.CREATE_RELTYPE => CreateRelationshipTypeAction
    case ActionType.TRANSACTION_ALL => AllTransactionActions
    case ActionType.TRANSACTION_SHOW => ShowTransactionAction
    case ActionType.TRANSACTION_TERMINATE => TerminateTransactionAction

    case ActionType.DBMS_ALL => AllDbmsAction
    case ActionType.USER_ALL => AllUserActions
    case ActionType.USER_SHOW => ShowUserAction
    case ActionType.USER_ALTER => AlterUserAction
    case ActionType.USER_CREATE => CreateUserAction
    case ActionType.USER_DROP => DropUserAction
    case ActionType.USER_RENAME => RenameUserAction
    case ActionType.USER_PASSWORD => SetPasswordsAction
    case ActionType.USER_STATUS => SetUserStatusAction
    case ActionType.USER_HOME   => SetUserHomeDatabaseAction
    case ActionType.ROLE_ALL    => AllRoleActions
    case ActionType.ROLE_SHOW => ShowRoleAction
    case ActionType.ROLE_CREATE => CreateRoleAction
    case ActionType.ROLE_DROP => DropRoleAction
    case ActionType.ROLE_RENAME => RenameRoleAction
    case ActionType.ROLE_ASSIGN => AssignRoleAction
    case ActionType.ROLE_REMOVE => RemoveRoleAction
    case ActionType.DATABASE_MANAGEMENT => AllDatabaseManagementActions
    case ActionType.DATABASE_CREATE => CreateDatabaseAction
    case ActionType.DATABASE_DROP => DropDatabaseAction
    case ActionType.PRIVILEGE_ALL => AllPrivilegeActions
    case ActionType.PRIVILEGE_ASSIGN => AssignPrivilegeAction
    case ActionType.PRIVILEGE_REMOVE => RemovePrivilegeAction
    case ActionType.PRIVILEGE_SHOW => ShowPrivilegeAction

    case ActionType.GRAPH_ALL => AllGraphAction
    case ActionType.GRAPH_WRITE => WriteAction
    case ActionType.GRAPH_CREATE => CreateElementAction
    case ActionType.GRAPH_MERGE => MergeAdminAction
    case ActionType.GRAPH_DELETE => DeleteElementAction
    case ActionType.GRAPH_LABEL_SET => SetLabelAction
    case ActionType.GRAPH_LABEL_REMOVE => RemoveLabelAction
    case ActionType.GRAPH_PROPERTY_SET => SetPropertyAction
    case ActionType.GRAPH_MATCH => MatchAction
    case ActionType.GRAPH_READ => ReadAction
    case ActionType.GRAPH_TRAVERSE => TraverseAction
  }

  // Resources

  override def propertiesResource(p: InputPosition, properties: util.List[String]): ActionResource = PropertiesResource(properties.asScala)(p)

  override def allPropertiesResource(p: InputPosition): ActionResource = AllPropertyResource()(p)

  override def labelsResource(p: InputPosition, labels: util.List[String]): ActionResource = LabelsResource(labels.asScala)(p)

  override def allLabelsResource(p: InputPosition): ActionResource = AllLabelResource()(p)

  override def databaseResource(p: InputPosition): ActionResource = DatabaseResource()(p)

  override def noResource(p: InputPosition): ActionResource = NoResource()(p)

  override def labelQualifier(p: InputPosition, label: String): PrivilegeQualifier = LabelQualifier(label)(p)

  override def relationshipQualifier(p: InputPosition, relationshipType: String): PrivilegeQualifier = RelationshipQualifier(relationshipType)(p)

  override def elementQualifier(p: InputPosition, name: String): PrivilegeQualifier = ElementQualifier(name)(p)

  override def allElementsQualifier(p: InputPosition): PrivilegeQualifier = ElementsAllQualifier()(p)

  override def allLabelsQualifier(p: InputPosition): PrivilegeQualifier = LabelAllQualifier()(p)

  override def allRelationshipsQualifier(p: InputPosition): PrivilegeQualifier = RelationshipAllQualifier()(p)

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

  override def graphScopes(p: InputPosition, graphNames: util.List[SimpleEither[String, Parameter]], scopeType: ScopeType): util.List[GraphScope] = {
    val list = new util.ArrayList[GraphScope]()
    scopeType match {
      case ScopeType.ALL     => list.add(AllGraphsScope()(p))
      case ScopeType.HOME    => list.add(HomeGraphScope()(p))
      case ScopeType.DEFAULT => list.add(DefaultGraphScope()(p))
      case ScopeType.NAMED   => graphNames.asScala.foreach(db => list.add(NamedGraphScope(db.asScala)(p)))
    }
    list
  }

  override def databaseScopes(p: InputPosition, databaseNames: util.List[SimpleEither[String, Parameter]], scopeType: ScopeType): util.List[DatabaseScope] = {
    val list = new util.ArrayList[DatabaseScope]()
    scopeType match {
      case ScopeType.ALL     => list.add(AllDatabasesScope()(p))
      case ScopeType.HOME    => list.add(HomeDatabaseScope()(p))
      case ScopeType.DEFAULT => list.add(DefaultDatabaseScope()(p))
      case ScopeType.NAMED   => databaseNames.asScala.foreach(db => list.add(NamedDatabaseScope(db.asScala)(p)))
    }
    list
  }

  // Database commands

  override def createDatabase(p: InputPosition,
                              replace: Boolean,
                              databaseName: SimpleEither[String, Parameter],
                              ifNotExists: Boolean,
                              wait: WaitUntilComplete,
                              options: SimpleEither[util.Map[String, Expression], Parameter]): CreateDatabase = {
    CreateDatabase(databaseName.asScala, ifExistsDo(replace, ifNotExists), asOptionsAst(options), wait)(p)
  }

  override def dropDatabase(p:InputPosition, databaseName: SimpleEither[String, Parameter], ifExists: Boolean, dumpData: Boolean, wait: WaitUntilComplete): DropDatabase = {
    val action: DropDatabaseAdditionalAction = if (dumpData) {
      DumpData
    } else {
      DestroyData
    }

    DropDatabase(databaseName.asScala, ifExists, action, wait)(p)
  }

  override def showDatabase(p: InputPosition,
                            scope: DatabaseScope,
                            yieldExpr: Yield,
                            returnWithoutGraph: Return,
                            where: Where): ShowDatabase = {
    if (yieldExpr != null) {
      ShowDatabase(scope, Some(Left((yieldExpr, Option(returnWithoutGraph)))))(p)
    } else {
      ShowDatabase(scope, Option(where).map(e => Right(where)))(p)
    }
  }

  override def databaseScope(p: InputPosition, databaseName: SimpleEither[String, Parameter], isDefault: Boolean, isHome: Boolean): DatabaseScope = {
    if (databaseName != null) {
      NamedDatabaseScope(databaseName.asScala)(p)
    } else if (isDefault) {
      DefaultDatabaseScope()(p)
    } else if (isHome) {
      HomeDatabaseScope()(p)
    } else {
      AllDatabasesScope()(p)
    }
  }

  override def startDatabase(p: InputPosition,
                             databaseName: SimpleEither[String, Parameter],
                             wait: WaitUntilComplete): StartDatabase = {
    StartDatabase(databaseName.asScala, wait)(p)
  }

  override def stopDatabase(p: InputPosition,
                            databaseName: SimpleEither[String, Parameter],
                            wait: WaitUntilComplete): StopDatabase = {
    StopDatabase(databaseName.asScala, wait)(p)
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

  private def ifExistsDo(replace: Boolean, ifNotExists: Boolean): IfExistsDo = {
    (replace, ifNotExists) match {
      case (true, true) => IfExistsInvalidSyntax
      case (true, false) => IfExistsReplace
      case (false, true) => IfExistsDoNothing
      case (false, false) => IfExistsThrowError
    }
  }

  private def yieldOrWhere(yieldExpr: Yield,
                           returnWithoutGraph: Return,
                           where: Where): Option[Either[(Yield, Option[Return]), Where]] = {
    if (yieldExpr != null) {
      Some(Left(yieldExpr, Option(returnWithoutGraph)))
    } else if (where != null) {
      Some(Right(where))
    } else {
      None
    }
  }

  private def asBooleanOption(bool: lang.Boolean): Option[Boolean] = if (bool == null) None else Some(bool.booleanValue())

  private def asOptionsAst(options: SimpleEither[util.Map[String, Expression], Parameter]) =
    Option(options).map(_.asScala) match {
      case Some(Left(map)) => OptionsMap(mapAsScalaMap(map).toMap)
      case Some(Right(param)) => OptionsParam(param)
      case None => NoOptions
    }

  private def pretty[T <: AnyRef](ts: util.List[T]): String = {
    ts.stream().map[String](t => t.toString).collect(Collectors.joining(","))
  }
}

