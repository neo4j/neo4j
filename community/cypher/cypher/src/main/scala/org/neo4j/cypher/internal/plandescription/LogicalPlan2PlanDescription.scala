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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.ActionResource
import org.neo4j.cypher.internal.ast.AdminAction
import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.AllLabelResource
import org.neo4j.cypher.internal.ast.AllPropertyResource
import org.neo4j.cypher.internal.ast.DefaultDatabaseScope
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.LabelResource
import org.neo4j.cypher.internal.ast.NamedGraphScope
import org.neo4j.cypher.internal.ast.PrivilegeQualifier
import org.neo4j.cypher.internal.ast.PropertyResource
import org.neo4j.cypher.internal.ast.UserAllQualifier
import org.neo4j.cypher.internal.ast.UserQualifier
import org.neo4j.cypher.internal.ast.UsersQualifier
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AssertDatabaseAdmin
import org.neo4j.cypher.internal.logical.plans.AssertDbmsAdmin
import org.neo4j.cypher.internal.logical.plans.AssertDbmsAdminOrSelf
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.Bound
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.CopyRolePrivileges
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.CreateDatabase
import org.neo4j.cypher.internal.logical.plans.CreateIndex
import org.neo4j.cypher.internal.logical.plans.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.logical.plans.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.CreateRole
import org.neo4j.cypher.internal.logical.plans.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.logical.plans.CreateUser
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DenyDatabaseAction
import org.neo4j.cypher.internal.logical.plans.DenyDbmsAction
import org.neo4j.cypher.internal.logical.plans.DenyGraphAction
import org.neo4j.cypher.internal.logical.plans.DenyMatch
import org.neo4j.cypher.internal.logical.plans.DenyRead
import org.neo4j.cypher.internal.logical.plans.DenyRemoveLabel
import org.neo4j.cypher.internal.logical.plans.DenySetLabel
import org.neo4j.cypher.internal.logical.plans.DenyTraverse
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNothingIfExists
import org.neo4j.cypher.internal.logical.plans.DoNothingIfNotExists
import org.neo4j.cypher.internal.logical.plans.DropConstraintOnName
import org.neo4j.cypher.internal.logical.plans.DropDatabase
import org.neo4j.cypher.internal.logical.plans.DropIndex
import org.neo4j.cypher.internal.logical.plans.DropIndexOnName
import org.neo4j.cypher.internal.logical.plans.DropNodeKeyConstraint
import org.neo4j.cypher.internal.logical.plans.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.logical.plans.DropResult
import org.neo4j.cypher.internal.logical.plans.DropRole
import org.neo4j.cypher.internal.logical.plans.DropUniquePropertyConstraint
import org.neo4j.cypher.internal.logical.plans.DropUser
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.EnsureNodeExists
import org.neo4j.cypher.internal.logical.plans.EnsureValidNonSystemDatabase
import org.neo4j.cypher.internal.logical.plans.EnsureValidNumberOfDatabases
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.GrantDatabaseAction
import org.neo4j.cypher.internal.logical.plans.GrantDbmsAction
import org.neo4j.cypher.internal.logical.plans.GrantGraphAction
import org.neo4j.cypher.internal.logical.plans.GrantMatch
import org.neo4j.cypher.internal.logical.plans.GrantRead
import org.neo4j.cypher.internal.logical.plans.GrantRemoveLabel
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.GrantSetLabel
import org.neo4j.cypher.internal.logical.plans.GrantTraverse
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.LogSystemCommand
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.MergeCreateNode
import org.neo4j.cypher.internal.logical.plans.MergeCreateRelationship
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RequireRole
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeGraphAction
import org.neo4j.cypher.internal.logical.plans.RevokeMatch
import org.neo4j.cypher.internal.logical.plans.RevokeRead
import org.neo4j.cypher.internal.logical.plans.RevokeRemoveLabel
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.RevokeSetLabel
import org.neo4j.cypher.internal.logical.plans.RevokeTraverse
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SeekableArgs
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetOwnPassword
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.ShowDatabase
import org.neo4j.cypher.internal.logical.plans.ShowDatabases
import org.neo4j.cypher.internal.logical.plans.ShowDefaultDatabase
import org.neo4j.cypher.internal.logical.plans.ShowPrivileges
import org.neo4j.cypher.internal.logical.plans.ShowRoles
import org.neo4j.cypher.internal.logical.plans.ShowUsers
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StartDatabase
import org.neo4j.cypher.internal.logical.plans.StopDatabase
import org.neo4j.cypher.internal.logical.plans.SystemProcedureCall
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.plandescription.Arguments.Details
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.plandescription.PlanDescriptionArgumentSerializer.asPrettyString
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.UnNamedNameGenerator.NameString
import org.neo4j.exceptions.InternalException

object LogicalPlan2PlanDescription {

  def apply(input: LogicalPlan,
            plannerName: PlannerName,
            cypherVersion: CypherVersion,
            readOnly: Boolean,
            cardinalities: Cardinalities,
            providedOrders: ProvidedOrders,
            executionPlan: ExecutionPlan): InternalPlanDescription = {
    new LogicalPlan2PlanDescription(readOnly, cardinalities, providedOrders, executionPlan).create(input)
      .addArgument(Version("CYPHER " + cypherVersion.name))
      .addArgument(RuntimeVersion("4.1"))
      .addArgument(Planner(plannerName.toTextOutput))
      .addArgument(PlannerImpl(plannerName.name))
      .addArgument(PlannerVersion("4.1"))
  }
}

case class LogicalPlan2PlanDescription(readOnly: Boolean, cardinalities: Cardinalities, providedOrders: ProvidedOrders, executionPlan: ExecutionPlan)
  extends LogicalPlans.Mapper[InternalPlanDescription] {
  private val SEPARATOR = ", "

  def create(plan: LogicalPlan): InternalPlanDescription =
    LogicalPlans.map(plan, this)

  override def onLeaf(plan: LogicalPlan): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.isLeaf)

    val id = plan.id
    val variables = plan.availableSymbols

    val result: InternalPlanDescription = plan match {
      case AllNodesScan(idName, _) =>
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq(Details(idName)), variables)

      case NodeByLabelScan(idName, label, _) =>
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(Details(s"$idName:${label.name}")), variables)

      case NodeByIdSeek(idName, nodeIds: SeekableArgs, _) =>
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(Details(s"$idName WHERE id($idName) ${seekableArgsInfo(nodeIds)}")), variables)

      case p@NodeIndexSeek(idName, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getDescriptions(idName, label, properties.map(_.propertyKeyToken), valueExpr, unique = false, readOnly, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables)

      case p@NodeUniqueIndexSeek(idName, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getDescriptions(idName, label, properties.map(_.propertyKeyToken), valueExpr, unique = true, readOnly, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(Details(indexDesc)), variables)

      case p@MultiNodeIndexSeek(indexLeafPlans) =>
        val (_, indexDescs) = indexLeafPlans.map(l => getDescriptions(l.idName, l.label, l.properties.map(_.propertyKeyToken), l.valueExpr, unique = l.isInstanceOf[NodeUniqueIndexSeek], readOnly, p.cachedProperties)).unzip
        PlanDescriptionImpl(id = plan.id, "MultiNodeIndexSeek", NoChildren, Seq(Details(indexDescs)), variables)

      case plans.Argument(argumentIds) if argumentIds.nonEmpty =>
        val details = if (argumentIds.nonEmpty) Seq(Details(argumentIds.mkString(", "))) else Seq.empty
        PlanDescriptionImpl(id, "Argument", NoChildren, details, variables)

      case _: plans.Argument =>
        ArgumentPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, true))
        PlanDescriptionImpl(id, "DirectedRelationshipByIdSeek", NoChildren, Seq(details), variables)

      case UndirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, _) =>
        val details = Details(relationshipByIdSeekInfo(idName, relIds, startNode, endNode, false))
        PlanDescriptionImpl(id, "UndirectedRelationshipByIdSeek", NoChildren, Seq(details), variables)

      case Input(nodes, rels, inputVars, _) =>
        PlanDescriptionImpl(id, "Input", NoChildren, Seq(Details(nodes ++ rels ++ inputVars)), variables)

      case NodeCountFromCountStore(ident, labelNames, _) =>
        val info = nodeCountFromCountStoreInfo(ident, labelNames)
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren, Seq(Details(info)), variables)

      case p@NodeIndexContainsScan(idName, label, property, valueExpr, _, _) =>
        val predicate = s"${property.propertyKeyToken.name} CONTAINS ${PlanDescriptionArgumentSerializer.asPrettyString(valueExpr)}"
        val info = indexInfoString(idName, unique = false, label, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "NodeIndexContainsScan", NoChildren, Seq(Details(info)), variables)

      case p@NodeIndexEndsWithScan(idName, label, property, valueExpr, _, _) =>
        val predicate = s"${property.propertyKeyToken.name} ENDS WITH ${PlanDescriptionArgumentSerializer.asPrettyString(valueExpr)}"
        val info = indexInfoString(idName, unique = false, label, Seq(property.propertyKeyToken), predicate, p.cachedProperties)
        PlanDescriptionImpl(id, "NodeIndexEndsWithScan", NoChildren, Seq(Details(info)), variables)

      case p@NodeIndexScan(idName, label, properties, _, _) =>
        val tokens = properties.map(_.propertyKeyToken)
        val props = tokens.map(_.name)
        val predicates = props.map(p => s"exists($p)").mkString(" AND ")
        val info = indexInfoString(idName, unique = false, label, tokens, predicates, p.cachedProperties)
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren, Seq(Details(info)), variables)

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(id, "ProcedureCall", NoChildren, Seq(Details(signatureInfo(call))), variables)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        val info = relationshipCountFromCountStoreInfo(ident, startLabel, typeNames, endLabel)
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(Details(info)), variables)

      case CreateIndex(labelName, propertyKeyNames, nameOption) =>
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq(Details(indexSchemaInfo(nameOption, labelName, propertyKeyNames))), variables)

      case DropIndex(labelName, propertyKeyNames) =>
        PlanDescriptionImpl(id, "DropIndex", NoChildren, Seq(Details(indexSchemaInfo(None, labelName, propertyKeyNames))), variables)

      case DropIndexOnName(name) =>
        PlanDescriptionImpl(id, "DropIndex", NoChildren, Seq(Details(s"INDEX ${ExpressionStringifier.backtick(name)}")), variables)

      case CreateUniquePropertyConstraint(node, label, properties: Seq[Property], nameOption) =>
        val details = Details(constraintInfo(nameOption, Some(node), scala.util.Left(label), properties, scala.util.Right("IS UNIQUE")))
        PlanDescriptionImpl(id, "CreateUniquePropertyConstraint", NoChildren, Seq(details), variables)

      case CreateNodeKeyConstraint(node, label, properties: Seq[Property], nameOption) =>
        val details = Details(constraintInfo(nameOption, Some(node), scala.util.Left(label), properties, scala.util.Right("IS NODE KEY")))
        PlanDescriptionImpl(id, "CreateNodeKeyConstraint", NoChildren, Seq(details), variables)

      case CreateNodePropertyExistenceConstraint(label, prop, nameOption) =>
        val node = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(nameOption, Some(node), scala.util.Left(label), Seq(prop), scala.util.Left("exists")))
        PlanDescriptionImpl(id, "CreateNodePropertyExistenceConstraint", NoChildren, Seq(details), variables)

      case CreateRelationshipPropertyExistenceConstraint(relTypeName, prop, nameOption) =>
        val relationship = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(nameOption, Some(relationship),scala.util.Right(relTypeName), Seq(prop), scala.util.Left("exists")))
        PlanDescriptionImpl(id, "CreateRelationshipPropertyExistenceConstraint", NoChildren, Seq(details), variables)

      case DropUniquePropertyConstraint(label, props) =>
        val node = props.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, Some(node), scala.util.Left(label), props, scala.util.Right("IS UNIQUE")))
        PlanDescriptionImpl(id, "DropUniquePropertyConstraint", NoChildren, Seq(details), variables)

      case DropNodeKeyConstraint(label, props) =>
        val node = props.head.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, Some(node), scala.util.Left(label), props, scala.util.Right("IS NODE KEY")))
        PlanDescriptionImpl(id, "DropNodeKeyConstraint", NoChildren, Seq(details), variables)

      case DropNodePropertyExistenceConstraint(label, prop) =>
        val node = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, Some(node), scala.util.Left(label), Seq(prop), scala.util.Left("exists")))
        PlanDescriptionImpl(id, "DropNodePropertyExistenceConstraint", NoChildren, Seq(details), variables)

      case DropRelationshipPropertyExistenceConstraint(relTypeName, prop) =>
        val relationship = prop.map.asCanonicalStringVal
        val details = Details(constraintInfo(None, Some(relationship),scala.util.Right(relTypeName), Seq(prop), scala.util.Left("exists")))
        PlanDescriptionImpl(id, "DropRelationshipPropertyExistenceConstraint", NoChildren, Seq(details), variables)

      case DropConstraintOnName(name) =>
        val constraintName = Details(s"CONSTRAINT ${ExpressionStringifier.backtick(name)}")
        PlanDescriptionImpl(id, "DropConstraint", NoChildren, Seq(constraintName), variables)

      case SetOwnPassword(_, _) =>
        PlanDescriptionImpl(id, "AlterCurrentUserSetPassword", NoChildren, Seq.empty, variables)

      case ShowPrivileges(_, scope) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "ShowPrivileges", NoChildren, Seq(Details(Prettifier.extractScope(scope))), variables)

      case ShowDatabase(dbName) =>
        PlanDescriptionImpl(id, "ShowDatabase", NoChildren, Seq(Details(Prettifier.escapeName(dbName))), variables)

      case ShowDatabases() =>
        PlanDescriptionImpl(id, "ShowDatabases", NoChildren, Seq.empty, variables)

      case ShowDefaultDatabase() =>
        PlanDescriptionImpl(id, "ShowDefaultDatabase", NoChildren, Seq.empty, variables)

      case SystemProcedureCall(procedureName, _, _, _) =>
        PlanDescriptionImpl(id, procedureName, NoChildren, Seq.empty, variables)

      case AssertDbmsAdmin(actions) =>
        PlanDescriptionImpl(id, "AssertDbmsAdmin", NoChildren, Seq(Details(actions.map(_.name))), variables)

      case AssertDbmsAdminOrSelf(user, actions) =>
        PlanDescriptionImpl(id, "AssertDbmsAdminOrSelf", NoChildren, Seq(Details(actions.map(_.name) ++ getUserInfo(user).info)), variables)

      case AssertDatabaseAdmin(action, dbName) =>
        val arguments = Seq(Details(Seq(action.name, Prettifier.escapeName(dbName))))
        PlanDescriptionImpl(id, "AssertDatabaseAdmin", NoChildren, arguments, variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  override def onOneChildPlan(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.isEmpty)

    val id = plan.id
    val variables = plan.availableSymbols
    val children = if (source.isInstanceOf[ArgumentPlanDescription]) NoChildren else SingleChild(source)

    val result: InternalPlanDescription = plan match {
      case Distinct(_, groupingExpressions) =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(Details(aggregationInfo(groupingExpressions, Map.empty))), variables)

      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, Map.empty, orderToLeverage)
        PlanDescriptionImpl(id, "OrderedDistinct", children, Seq(Details(details)), variables)

      case Aggregation(_, groupingExpressions, aggregationExpressions) =>
        val details = aggregationInfo(groupingExpressions, aggregationExpressions)
        PlanDescriptionImpl(id, "EagerAggregation", children, Seq(Details(details)), variables)

      case OrderedAggregation(_, groupingExpressions, aggregationExpressions, orderToLeverage) =>
        val details = aggregationInfo(groupingExpressions, aggregationExpressions, orderToLeverage)
        PlanDescriptionImpl(id, "OrderedAggregation", children, Seq(Details(details)), variables)

      case Create(_, nodes, relationships) =>
        val relationshipDetails = relationships
          .map { case CreateRelationship(idName, leftNode, relType, rightNode, direction, _) =>
            expandExpressionDescription(leftNode, Some(idName), Seq(relType.name), rightNode, direction, 1, Some(1))
          }
        val nodeDetails = nodes.map { case CreateNode(idName, labels, _) => s"($idName${if (labels.nonEmpty) labels.map(_.name).mkString(":", ":", "") else ""})"}
        PlanDescriptionImpl(id, "Create", children, Seq(Details(nodeDetails ++ relationshipDetails)), variables)

      case DeleteExpression(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables)

      case DeleteNode(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables)

      case DeletePath(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables)

      case DeleteRelationship(_, expression) =>
        PlanDescriptionImpl(id, "Delete", children, Seq(Details(asPrettyString(expression))), variables)

      case DetachDeleteExpression(_, expression) =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq(Details(asPrettyString(expression))), variables)

      case DetachDeleteNode(_, expression) =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq(Details(asPrettyString(expression))), variables)

      case DetachDeletePath(_, expression) =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq(Details(asPrettyString(expression))), variables)

      case _: Eager =>
        PlanDescriptionImpl(id, "Eager", children, Seq.empty, variables)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables)

      case _: DropResult =>
        PlanDescriptionImpl(id, "DropResult", children, Seq.empty, variables)

      case NodeCountFromCountStore(idName, labelName, _) =>
        val info = nodeCountFromCountStoreInfo(idName, labelName)
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren, Seq(Details(info)), variables)

      case RelationshipCountFromCountStore(idName, start, types, end, _) =>
        val info = relationshipCountFromCountStoreInfo(idName, start, types, end)
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(Details(info)), variables)

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables)

      case Expand(_, fromName, dir, typeNames, toName, relName, mode, _) =>
        val expression = Details(expandExpressionDescription(fromName, Some(relName), typeNames.map(_.name), toName, dir, 1, Some(1)))
        val modeText = mode match {
          case ExpandAll => "Expand(All)"
          case ExpandInto => "Expand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(expression), variables)

      case Limit(_, count, _) =>
        PlanDescriptionImpl(id, "Limit", children, Seq(Details(PlanDescriptionArgumentSerializer.asPrettyString(count))), variables)

      case CacheProperties(_, properties) =>
        PlanDescriptionImpl(id, "CacheProperties", children, Seq(Details(properties.toSeq.map(PlanDescriptionArgumentSerializer.asPrettyString).mkString(", "))), variables)

      case LockNodes(_, nodesToLock) =>
        PlanDescriptionImpl(id, name = "LockNodes", children, Seq(Details(keyNamesInfo(nodesToLock.toSeq))), variables)

      case OptionalExpand(_, fromName, dir, typeNames, toName, relName, mode, predicates, _) =>
        val predicate = predicates.map(p => s" WHERE ${PlanDescriptionArgumentSerializer.asPrettyString(p)}").getOrElse("")
        val details = Details(expandExpressionDescription(fromName, Some(relName), typeNames.map(_.name), toName, dir, 1, Some(1)) + predicate)
        val modeText = mode match {
          case ExpandAll => "OptionalExpand(All)"
          case ExpandInto => "OptionalExpand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(details), variables)

      case ProduceResult(_, columns) =>
        PlanDescriptionImpl(id, "ProduceResults", children, Seq(Details(columns)), variables)

      case Projection(_, expr) =>
        val expressions = Details(projectedExpressionInfo(expr).mkString(SEPARATOR))
        PlanDescriptionImpl(id, "Projection", children, Seq(expressions), variables)

      case Selection(predicate, _) =>
        val details = Details(predicate.exprs.map(PlanDescriptionArgumentSerializer.asPrettyString).mkString(" AND "))
        PlanDescriptionImpl(id, "Filter", children, Seq(details), variables)

      case Skip(_, count) =>
        PlanDescriptionImpl(id, name = "Skip", children, Seq(Details(PlanDescriptionArgumentSerializer.asPrettyString(count))), variables)

      case FindShortestPaths(_, ShortestPathPattern(maybePathName, PatternRelationship(relName, (fromName, toName), dir, relTypes, patternLength: PatternLength), isSingle), predicates, _, _) =>
        val patternRelationshipInfo = expandExpressionDescription(fromName, Some(relName), relTypes.map(_.name), toName, dir, patternLength)

        val predicatesInfo = if (predicates.isEmpty) "" else s" WHERE ${predicates.map(PlanDescriptionArgumentSerializer.asPrettyString).mkString(" AND ")}"

        val pathName = maybePathName match {
          case Some(p) if !p.unnamed => s"$p = "
          case _ => ""
        }

        PlanDescriptionImpl(id, "ShortestPath", children, Seq(Details(s"$pathName$patternRelationshipInfo$predicatesInfo")), variables)

      case LoadCSV(_, _, variableName, _, _, _, _) =>
        PlanDescriptionImpl(id, "LoadCSV", children, Seq(Details(variableName)), variables)

      case MergeCreateNode(_, idName, _, _) =>
        PlanDescriptionImpl(id, "MergeCreateNode", children, Seq(Details(idName)), variables)

      case MergeCreateRelationship(_, idName, startNode, relTypeName, endNode, _) =>
        val details = expandExpressionDescription(startNode, Some(idName), Seq(relTypeName.name), endNode, SemanticDirection.OUTGOING, 1, Some(1))
        PlanDescriptionImpl(id, "MergeCreateRelationship", children, Seq(Details(details)), variables)

      case Optional(_, protectedSymbols) =>
        PlanDescriptionImpl(id, "Optional", children, Seq(Details(keyNamesInfo(protectedSymbols.toSeq))), variables)

      case _: Anti =>
        PlanDescriptionImpl(id, "Anti", children, Seq.empty, variables)

      case ProcedureCall(_, call) =>
        PlanDescriptionImpl(id, "ProcedureCall", children, Seq(Details(signatureInfo(call))), variables)

      case ProjectEndpoints(_, relName, start, _, end, _, relTypes, directed, patternLength) =>
        val name = if (directed) "ProjectEndpoints" else "ProjectEndpoints(BOTH)"
        val direction = if (directed) SemanticDirection.OUTGOING else SemanticDirection.BOTH
        val relTypeNames = relTypes.toSeq.flatten.map(_.name)
        val details = expandExpressionDescription(start, Some(relName), relTypeNames, end, direction, patternLength)
        PlanDescriptionImpl(id, name, children, Seq(Details(details)), variables)

      case PruningVarExpand(_, fromName, dir, types, toName, min, max, maybeNodePredicate, maybeRelationshipPredicate) =>
        val maybeRelName = maybeRelationshipPredicate.map(_.variable.name)
        val expandInfo = expandExpressionDescription(fromName, maybeRelName, types.map(_.name), toName, dir, minLength = min, maxLength = Some(max))
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate) match {
          case Some(predicateInfo) => s" WHERE $predicateInfo"
          case _ => ""
        }
        PlanDescriptionImpl(id, s"VarLengthExpand(Pruning)", children, Seq(Details(s"$expandInfo$predicatesDescription")), variables)

      case RemoveLabels(_, idName, labelNames) =>
        val details = Details(s"$idName${labelNames.map(_.name).mkString(":", ":", "")}")
        PlanDescriptionImpl(id, "RemoveLabels", children, Seq(details), variables)

      case SetLabels(_, idName, labelNames) =>
        val details = Details(s"$idName${labelNames.map(_.name).mkString(":", ":", "")}")
        PlanDescriptionImpl(id, "SetLabels", children, Seq(details), variables)

      case SetNodePropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(idName, expression, removeOtherProps))
        PlanDescriptionImpl(id, "SetNodePropertiesFromMap", children, Seq(details), variables)

      case SetPropertiesFromMap(_, entity, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(asPrettyString(entity), expression, removeOtherProps))
        PlanDescriptionImpl(id, "SetPropertiesFromMap", children, Seq(details), variables)

      case SetProperty(_, entity, propertyKey, expression) =>
        val entityString = s"${PlanDescriptionArgumentSerializer.asPrettyString(entity)}.${propertyKey.name}"
        val details = Details(setPropertyInfo(entityString, expression, true))
        PlanDescriptionImpl(id, "SetProperty", children, Seq(details), variables)

      case SetNodeProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(s"$idName.${propertyKey.name}", expression, true))
        PlanDescriptionImpl(id, "SetProperty", children, Seq(details), variables)

      case SetRelationshipProperty(_, idName, propertyKey, expression) =>
        val details = Details(setPropertyInfo(s"$idName.${propertyKey.name}", expression, true))
        PlanDescriptionImpl(id, "SetProperty", children, Seq(details), variables)

      case SetRelationshipPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        val details = Details(setPropertyInfo(idName, expression, removeOtherProps))
        PlanDescriptionImpl(id, "SetRelationshipPropertiesFromMap", children, Seq(details), variables)

      case Sort(_, orderBy) =>
        PlanDescriptionImpl(id, "Sort", children, Seq(Details(orderInfo(orderBy))), variables)

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix) =>
        PlanDescriptionImpl(id, "PartialSort", children, Seq(Details(orderInfo(alreadySortedPrefix ++ stillToSortSuffix))), variables)

      case Top(_, orderBy, limit) =>
        val details = s"${orderInfo(orderBy)} LIMIT ${PlanDescriptionArgumentSerializer.asPrettyString(limit)}"
        PlanDescriptionImpl(id, "Top", children, Seq(Details(details)), variables)

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit) =>
        val details = s"${orderInfo(alreadySortedPrefix ++ stillToSortSuffix)} LIMIT ${PlanDescriptionArgumentSerializer.asPrettyString(limit)}"
        PlanDescriptionImpl(id, "PartialTop", children, Seq(Details(details)), variables)

      case UnwindCollection(_, variable, expression) =>
        val details = Details(projectedExpressionInfo(Map(variable -> expression)).mkString(SEPARATOR))
        PlanDescriptionImpl(id, "Unwind", children, Seq(details), variables)

      case VarExpand(_, fromName, dir, _, types, toName, relName, length, mode, maybeNodePredicate, maybeRelationshipPredicate) =>
        val expandDescription = expandExpressionDescription(fromName, Some(relName), types.map(_.name), toName, dir, minLength = length.min, maxLength = length.max)
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate) match {
          case Some(predicateInfo) => s" WHERE $predicateInfo"
          case _ => ""
        }
        val modeDescr = mode match {
          case ExpandAll => "All"
          case ExpandInto => "Into"
        }
        PlanDescriptionImpl(id, s"VarLengthExpand($modeDescr)", children, Seq(Details(s"$expandDescription$predicatesDescription")), variables)

      case ShowUsers(_) =>
      PlanDescriptionImpl(id, "ShowUsers", children, Seq.empty, variables)

      case CreateUser(_, name, _, _, _) =>
        PlanDescriptionImpl(id, "CreateUser", children, Seq(getUserInfo(name)), variables)

      case DropUser(_, name) =>
        PlanDescriptionImpl(id, "DropUser", children, Seq(getUserInfo(name)), variables)

      case AlterUser(_, name, _, _, _) =>
        PlanDescriptionImpl(id, "AlterUser", children, Seq(getUserInfo(name)), variables)

      case ShowRoles(_, _, _) =>
        PlanDescriptionImpl(id, "ShowRoles", children, Seq.empty, variables)

      case DropRole(_, name) =>
        PlanDescriptionImpl(id, "DropRole", children, Seq(getRoleInfo(name)), variables)

      case CreateRole(_, name) =>
        PlanDescriptionImpl(id, "CreateRole", children, Seq(getRoleInfo(name)), variables)

      case RequireRole(_, name) =>
        PlanDescriptionImpl(id, "RequireRole", children, Seq(getRoleInfo(name)), variables)

      case CopyRolePrivileges(_, to, from, grantDeny) =>
        val details = Details(s"FROM ROLE ${Prettifier.escapeName(from)} TO ROLE ${Prettifier.escapeName(to)}")
        PlanDescriptionImpl(id, s"CopyRolePrivileges($grantDeny)", children, Seq(details), variables)

      case GrantRoleToUser(_, roleName, userName) =>
        PlanDescriptionImpl(id, "GrantRoleToUser", children, Seq(Details(getRoleInfo(roleName).info ++ getUserInfo(userName).info)), variables)

      case RevokeRoleFromUser(_, roleName, userName) =>
        PlanDescriptionImpl(id, "RevokeRoleFromUser", children, Seq(Details(getRoleInfo(roleName).info ++ getUserInfo(userName).info)), variables)

      case GrantDbmsAction(_, action, roleName) =>
        PlanDescriptionImpl(id, "GrantDbmsAction", children, Seq(Details(action.name +: getRoleInfo(roleName).info)), variables)

      case DenyDbmsAction(_, action, roleName) =>
        PlanDescriptionImpl(id, "DenyDbmsAction", children, Seq(Details(action.name +: getRoleInfo(roleName).info)), variables)

      case RevokeDbmsAction(_, action, roleName, revokeType) =>
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeDbmsAction", revokeType), children, Seq(Details(action.name +: getRoleInfo(roleName).info)), variables)

      case GrantDatabaseAction(_, action, database, qualifier, roleName) =>
        val details = extractDatabaseArguments(action, database, qualifier, roleName)
        PlanDescriptionImpl(id, "GrantDatabaseAction", children, Seq(details), variables)

      case DenyDatabaseAction(_, action, database, qualifier, roleName) =>
        val details = extractDatabaseArguments(action, database, qualifier, roleName)
        PlanDescriptionImpl(id, "DenyDatabaseAction", children, Seq(details), variables)

      case RevokeDatabaseAction(_, action, database, qualifier, roleName, revokeType) =>
        val details = extractDatabaseArguments(action, database, qualifier, roleName)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeDatabaseAction", revokeType), children, Seq(details), variables)

      case GrantGraphAction(_, action, database, qualifier, roleName) =>
        val dbName = extractGraphScope(database)
        val qualifierText = Prettifier.extractQualifierPart(qualifier)
        PlanDescriptionImpl(id, s"Grant${action.planName}", children, Seq(Details(Seq(dbName, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case DenyGraphAction(_, action, database, qualifier, roleName) =>
        val dbName = extractGraphScope(database)
        val qualifierText = Prettifier.extractQualifierPart(qualifier)
        PlanDescriptionImpl(id, s"Deny${action.planName}", children, Seq(Details(Seq(dbName, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case RevokeGraphAction(_, action, database, qualifier, roleName, revokeType) =>
        val dbName = extractGraphScope(database)
        val qualifierText = Prettifier.extractQualifierPart(qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation(s"Revoke${action.planName}", revokeType), children, Seq(Details(Seq(dbName, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case GrantTraverse(_, database, qualifier, roleName) =>
        val dbName = extractGraphScope(database)
        val qualifierText = Prettifier.extractQualifierPart(qualifier)
        PlanDescriptionImpl(id, "GrantTraverse", children, Seq(Details(Seq(dbName, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case DenyTraverse(_, database, qualifier, roleName) =>
        val dbName = extractGraphScope(database)
        val qualifierText = Prettifier.extractQualifierPart(qualifier)
        PlanDescriptionImpl(id, "DenyTraverse", children, Seq(Details(Seq(dbName, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case RevokeTraverse(_, database, qualifier, roleName, revokeType) =>
        val dbName = extractGraphScope(database)
        val qualifierText = Prettifier.extractQualifierPart(qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeTraverse", revokeType), children, Seq(Details(Seq(dbName, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case GrantRead(_, resource, database, qualifier, roleName) =>
        val (dbName, qualifierText, resourceText) = extractGraphScope(database, qualifier, resource)
        PlanDescriptionImpl(id, "GrantRead", children, Seq(Details(Seq(dbName, resourceText, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case DenyRead(_, resource, database, qualifier, roleName) =>
        val (dbName, qualifierText, resourceText) = extractGraphScope(database, qualifier, resource)
        PlanDescriptionImpl(id, "DenyRead", children, Seq(Details(Seq(dbName, resourceText, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case RevokeRead(_, resource, database, qualifier, roleName, revokeType) =>
        val (dbName, qualifierText, resourceText) = extractGraphScope(database, qualifier, resource)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeRead", revokeType), children,
          Seq(Details(Seq(dbName, resourceText, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case GrantMatch(_, resource, database, qualifier, roleName) =>
        val (dbName, qualifierText, resourceText) = extractGraphScope(database, qualifier, resource)
        PlanDescriptionImpl(id, "GrantMatch", children, Seq(Details(Seq(dbName, resourceText, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case DenyMatch(_, resource, database, qualifier, roleName) =>
        val (dbName, qualifierText, resourceText) = extractGraphScope(database, qualifier, resource)
        PlanDescriptionImpl(id, "DenyMatch", children, Seq(Details(Seq(dbName, resourceText, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case RevokeMatch(_, resource, database, qualifier, roleName, revokeType) =>
        val (dbName, qualifierText, resourceText) = extractGraphScope(database, qualifier, resource)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeMatch", revokeType), children, Seq(Details(Seq(dbName, resourceText, qualifierText) ++ getRoleInfo(roleName).info)), variables)

      case GrantSetLabel(_, resource, database, _, roleName) =>
        val dbName = extractGraphScope(database)
        val labelText = extractLabelPart(resource)
        PlanDescriptionImpl(id, "GrantSetLabel", children, Seq(Details(Seq(dbName, labelText) ++ getRoleInfo(roleName).info)), variables)

      case DenySetLabel(_, resource, database, _, roleName) =>
        val dbName = extractGraphScope(database)
        val labelText = extractLabelPart(resource)
        PlanDescriptionImpl(id, "DenySetLabel", children, Seq(Details(Seq(dbName, labelText) ++ getRoleInfo(roleName).info)), variables)

      case RevokeSetLabel(_, resource, database, _, roleName, revokeType) =>
        val dbName = extractGraphScope(database)
        val labelText = extractLabelPart(resource)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeSetLabel", revokeType), children, Seq(Details(Seq(dbName, labelText) ++ getRoleInfo(roleName).info)), variables)

      case GrantRemoveLabel(_, resource, database, _, roleName) =>
        val dbName = extractGraphScope(database)
        val labelText = extractLabelPart(resource)
        PlanDescriptionImpl(id, "GrantRemoveLabel", children, Seq(Details(Seq(dbName, labelText) ++ getRoleInfo(roleName).info)), variables)

      case DenyRemoveLabel(_, resource, database, _, roleName) =>
        val dbName = extractGraphScope(database)
        val labelText = extractLabelPart(resource)
        PlanDescriptionImpl(id, "DenyRemoveLabel", children, Seq(Details(Seq(dbName, labelText) ++ getRoleInfo(roleName).info)), variables)

      case RevokeRemoveLabel(_, resource, database, _, roleName, revokeType) =>
        val dbName = extractGraphScope(database)
        val labelText = extractLabelPart(resource)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeRemoveLabel", revokeType), children, Seq(Details(Seq(dbName, labelText) ++ getRoleInfo(roleName).info)), variables)

      case ShowPrivileges(_, scope) => // Can be both a leaf plan and a middle plan so need to be in both places
        PlanDescriptionImpl(id, "ShowPrivileges", children, Seq(Details(Prettifier.extractScope(scope))), variables)

      case CreateDatabase(_, dbName) =>
        PlanDescriptionImpl(id, "CreateDatabase", children, Seq(Details(Prettifier.escapeName(dbName))), variables)

      case DropDatabase(_, dbName) =>
        PlanDescriptionImpl(id, "DropDatabase", children, Seq(Details(Prettifier.escapeName(dbName))), variables)

      case StartDatabase(_, dbName) =>
        PlanDescriptionImpl(id, "StartDatabase", children, Seq(Details(Prettifier.escapeName(dbName))), variables)

      case StopDatabase(_, dbName) =>
        PlanDescriptionImpl(id, "StopDatabase", children, Seq(Details(Prettifier.escapeName(dbName))), variables)

      case EnsureValidNonSystemDatabase(_, dbName, _) =>
        PlanDescriptionImpl(id, "EnsureValidNonSystemDatabase", children, Seq(Details(Prettifier.escapeName(dbName))), variables)

      case EnsureValidNumberOfDatabases(_) =>
        PlanDescriptionImpl(id, "EnsureValidNumberOfDatabases", children, Seq.empty, variables)

      case LogSystemCommand(_, _) =>
        PlanDescriptionImpl(id, "LogSystemCommand", children, Seq.empty, variables)

      case DoNothingIfNotExists(_, label, name, _) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"DoNothingIfNotExists($label)", children, Seq(nameArgument), variables)

      case DoNothingIfExists(_, label, name, _) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"DoNothingIfExists($label)", children, Seq(nameArgument), variables)

      case EnsureNodeExists(_, label, name, _) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"EnsureNodeExists($label)", children, Seq(nameArgument), variables)

      case AssertNotCurrentUser(_, userName, _, _) =>
        PlanDescriptionImpl(id, "AssertNotCurrentUser", children, Seq(getUserInfo(userName)), variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  override def onTwoChildPlan(plan: LogicalPlan, lhs: InternalPlanDescription,
                              rhs: InternalPlanDescription): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.nonEmpty)

    val id = plan.id
    val variables = plan.availableSymbols
    val children = TwoChildren(lhs, rhs)

    val result: InternalPlanDescription = plan match {
      case _: AntiConditionalApply =>
        PlanDescriptionImpl(id, "AntiConditionalApply", children, Seq.empty, variables)

      case _: AntiSemiApply =>
        PlanDescriptionImpl(id, "AntiSemiApply", children, Seq.empty, variables)

      case _: ConditionalApply =>
        PlanDescriptionImpl(id, "ConditionalApply", children, Seq.empty, variables)

      case _: Apply =>
        PlanDescriptionImpl(id, "Apply", children, Seq.empty, variables)

      case AssertSameNode(node, _, _) =>
        PlanDescriptionImpl(id, "AssertSameNode", children, Seq(Details(node)), variables)

      case CartesianProduct(_, _) =>
        PlanDescriptionImpl(id, "CartesianProduct", children, Seq.empty, variables)

      case NodeHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeHashJoin", children, Seq(Details(keyNamesInfo(nodes.toSeq))), variables)

      case _: ForeachApply =>
        PlanDescriptionImpl(id, "Foreach", children, Seq.empty, variables)

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrSemiApply", children, Seq(Details(PlanDescriptionArgumentSerializer.asPrettyString(predicate))), variables)

      case row: plans.Argument =>
        ArgumentPlanDescription(id = plan.id, Seq.empty, row.argumentIds)

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrAntiSemiApply", children, Seq(Details(PlanDescriptionArgumentSerializer.asPrettyString(predicate))), variables)

      case _: LetSemiApply =>
        PlanDescriptionImpl(id, "LetSemiApply", children, Seq.empty, variables)

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(id, "LetAntiSemiApply", children, Seq.empty, variables)

      case LeftOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeLeftOuterHashJoin", children, Seq(Details(keyNamesInfo(nodes.toSeq))), variables)

      case RightOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeRightOuterHashJoin", children, Seq(Details(keyNamesInfo(nodes.toSeq))), variables)

      case RollUpApply(_, _, collectionName, variableToCollect, _) =>
        val detailsList = Seq(collectionName, variableToCollect).map(e => keyNamesInfo(Seq(e)))
        PlanDescriptionImpl(id, "RollUpApply", children, Seq(Details(detailsList)), variables)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrAntiSemiApply", children, Seq(Details(PlanDescriptionArgumentSerializer.asPrettyString(predicate))), variables)

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrSemiApply", children, Seq(Details(PlanDescriptionArgumentSerializer.asPrettyString(predicate))), variables)

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables)

      case TriadicSelection(_, _, positivePredicate, source, seen, target) =>
        val positivePredicateString = if (positivePredicate) "" else "NOT "
        val prettifiedSource = PlanDescriptionArgumentSerializer.removeGeneratedNames(source)
        val prettifiedTarget = PlanDescriptionArgumentSerializer.removeGeneratedNames(target)
        val details = Details(s"WHERE $positivePredicateString($prettifiedSource)--($prettifiedTarget)")
        PlanDescriptionImpl(id, "TriadicSelection", children, Seq(details), variables)

      case _: Union =>
        PlanDescriptionImpl(id, "Union", children, Seq.empty, variables)

      case ValueHashJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueHashJoin",
          children = children,
          arguments = Seq(Details(PlanDescriptionArgumentSerializer.asPrettyString(predicate))),
          variables
        )

      case _: MultiNodeIndexSeek =>
        PlanDescriptionImpl(id = plan.id, "MultiNodeIndexSeek", children, Seq.empty, variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addRuntimeAttributes(addPlanningAttributes(result, plan), plan)
  }

  private def addPlanningAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    val withEstRows = if (cardinalities.isDefinedAt(plan.id)) {
      description.addArgument(EstimatedRows(cardinalities.get(plan.id).amount))
    } else {
      description
    }
    if (providedOrders.isDefinedAt(plan.id) && !providedOrders(plan.id).isEmpty) {
      withEstRows.addArgument(Order(providedOrders(plan.id)))
    } else {
      withEstRows
    }
  }

  private def addRuntimeAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    executionPlan.operatorMetadata(plan.id).foldLeft(description)((acc, x) => acc.addArgument(x))
  }

  private def buildPredicatesDescription(maybeNodePredicate: Option[VariablePredicate],
                                         maybeRelationshipPredicate: Option[VariablePredicate]): Option[String] = {
    val nodePredicateInfo = maybeNodePredicate.map(_.predicate).map(PlanDescriptionArgumentSerializer.asPrettyString)
    val relationshipPredicateInfo = maybeRelationshipPredicate.map(_.predicate).map(PlanDescriptionArgumentSerializer.asPrettyString)

    (nodePredicateInfo ++ relationshipPredicateInfo) match {
      case predicates if predicates.nonEmpty => Some(predicates.mkString(" AND "))
      case _ => None
    }
  }

  private def getDescriptions(idName: String,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[expressions.Expression],
                              unique: Boolean,
                              readOnly: Boolean,
                              caches: Seq[expressions.Expression]): (String, String) = {

    val name = indexOperatorName(valueExpr, unique, readOnly)
    val predicate = indexPredicateString(propertyKeys, valueExpr)
    val info = indexInfoString(idName, unique, label, propertyKeys, predicate, caches)

    (name, info)
  }

  private def indexOperatorName(valueExpr: QueryExpression[expressions.Expression],
                                unique: Boolean,
                                readOnly: Boolean): String = {
    def findName(exactOnly: Boolean = true) =
      if (unique && !readOnly && exactOnly) {
        "NodeUniqueIndexSeek(Locking)"
      } else if (unique) {
        "NodeUniqueIndexSeek"
      } else {
        "NodeIndexSeek"
      }
    valueExpr match {
      case _: ExistenceQueryExpression[expressions.Expression] => "NodeIndexScan"
      case _: RangeQueryExpression[expressions.Expression] =>
        if (unique) "NodeUniqueIndexSeekByRange" else "NodeIndexSeekByRange"
      case e: CompositeQueryExpression[expressions.Expression] =>
        findName(e.exactOnly)
      case _: SingleQueryExpression[org.neo4j.cypher.internal.expressions.Expression] =>
        findName()
      case _: ManyQueryExpression[org.neo4j.cypher.internal.expressions.Expression] =>
        findName()
    }
  }

  private def indexPredicateString(propertyKeys: Seq[PropertyKeyToken],
                                   valueExpr: QueryExpression[expressions.Expression]): String = valueExpr match {
    case _: ExistenceQueryExpression[expressions.Expression] =>
      s"exists(${propertyKeys.head.name})"

    case e: RangeQueryExpression[expressions.Expression] =>
      checkOnlyWhenAssertionsAreEnabled(propertyKeys.size == 1)
      e.expression match {
        case PrefixSeekRangeWrapper(range) =>
          val propertyKeyName = propertyKeys.head.name
          s"$propertyKeyName STARTS WITH ${PlanDescriptionArgumentSerializer.asPrettyString(range.prefix)}"

        case InequalitySeekRangeWrapper(RangeLessThan(bounds)) =>
          bounds.map(rangeBoundString(propertyKeys.head, _, '<')).toIndexedSeq.mkString(" AND ")

        case InequalitySeekRangeWrapper(RangeGreaterThan(bounds)) =>
          bounds.map(rangeBoundString(propertyKeys.head, _, '>')).toIndexedSeq.mkString(" AND ")

        case InequalitySeekRangeWrapper(RangeBetween(greaterThanBounds, lessThanBounds)) =>
          val gtBoundString = greaterThanBounds.bounds.map(rangeBoundString(propertyKeys.head, _, '>'))
          val ltBoundStrings = lessThanBounds.bounds.map(rangeBoundString(propertyKeys.head, _, '<'))
          (gtBoundString ++ ltBoundStrings).toIndexedSeq.mkString(" AND ")

        case PointDistanceSeekRangeWrapper(PointDistanceRange(point, distance, inclusive)) =>
          val funcName = Point.name
          val poi = point match {
            case FunctionInvocation(Namespace(List()), FunctionName(`funcName`), _, Seq(MapExpression(args))) =>
              s"point(${args.map(_._2).map(PlanDescriptionArgumentSerializer.asPrettyString).mkString(", ")})"
            case _ => PlanDescriptionArgumentSerializer.asPrettyString(point)
          }
          val propertyKeyName = propertyKeys.head.name
          val distanceStr = PlanDescriptionArgumentSerializer.asPrettyString(distance)
          s"distance($propertyKeyName, $poi) <${if (inclusive) "=" else ""} $distanceStr"
      }

    case e: SingleQueryExpression[expressions.Expression] =>
      val propertyKeyName = propertyKeys.head.name
      s"$propertyKeyName = ${PlanDescriptionArgumentSerializer.asPrettyString(e.expression)}"

    case e: ManyQueryExpression[expressions.Expression] =>
      val (eqOp, innerExp) = e.expression match {
        case ll@ListLiteral(es) =>
          if (es.size == 1) ("=", es.head) else ("IN", ll)
        // This case is used for example when the expression in a parameter
        case x => ("IN", x)
      }
      val propertyKeyName = propertyKeys.head.name
      s"$propertyKeyName $eqOp ${PlanDescriptionArgumentSerializer.asPrettyString(innerExp)}"

    case e: CompositeQueryExpression[expressions.Expression] =>
      val predicates = e.inner.zipWithIndex.map {
        case (exp, i) => indexPredicateString(Seq(propertyKeys(i)), exp)
      }
      predicates.mkString(" AND ")
  }

  private def rangeBoundString(propertyKey: PropertyKeyToken, bound: Bound[expressions.Expression], sign: Char): String = {
    s"${propertyKey.name} $sign${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}"
  }

  private def nodeCountFromCountStoreInfo(ident: String, labelNames: List[Option[LabelName]]): String = {
    val labels = labelNames.flatten.map(_.name)
    val node = labels.map(":" + _).mkString
    s"count( ($node) )" + (if (ident.unnamed) "" else s" AS $ident")
  }

  private def relationshipCountFromCountStoreInfo(ident: String,
                                                  startLabel: Option[LabelName],
                                                  typeNames: Seq[RelTypeName],
                                                  endLabel: Option[LabelName]): String = {
    val start = startLabel.map(_.name).map(l => ":" + l).mkString
    val end = endLabel.map(_.name).map(l => ":" + l).mkString
    val types = typeNames.map(_.name).mkString(":", "|", "")
    s"count( ($start)-[$types]->($end) )" + (if (ident.unnamed) "" else s" AS $ident")
  }

  private def relationshipByIdSeekInfo(idName: String, relIds: SeekableArgs, startNode: String, endNode: String, isDirectional: Boolean): String = {
    val predicate = seekableArgsInfo(relIds)
    val directionString = if (isDirectional) ">" else ""
    s"($startNode)-[$idName]-$directionString($endNode) WHERE id($idName) $predicate"
  }

  private def seekableArgsInfo(seekableArgs: SeekableArgs) = seekableArgs match {
    case ManySeekableArgs(ListLiteral(exprs)) if exprs.size > 1 =>
      s"IN ${exprs.map(PlanDescriptionArgumentSerializer.asPrettyString).mkString("[", ",", "]")}"
    case ManySeekableArgs(ListLiteral(exprs)) =>
      s"= ${PlanDescriptionArgumentSerializer.asPrettyString(exprs.head)}"
    case _ =>
      s"= ${PlanDescriptionArgumentSerializer.asPrettyString(seekableArgs.expr)}"
  }

  private def signatureInfo(call: ResolvedCall): String = {
    val argString = call.callArguments.map(PlanDescriptionArgumentSerializer.asPrettyString).mkString(", ")
    val resultString = call.callResultTypes.map { case (name, typ) => s"$name :: ${typ.toNeoTypeString}" }.mkString(", ")
    s"${call.qualifiedName}($argString) :: ($resultString)"
  }

  private def orderInfo(orderBy: Seq[ColumnOrder]): String = {
    orderBy.map {
      case Ascending(id) => s"$id ASC"
      case Descending(id) => s"$id DESC"
    }.mkString(SEPARATOR)
  }

  private def expandExpressionDescription(from: String,
                                          maybeRelName: Option[String],
                                          relTypes: Seq[String],
                                          to: String,
                                          direction: SemanticDirection,
                                          patternLength: PatternLength): String = {
    val (min, maybeMax) = patternLength match {
      case SimplePatternLength => (1, None)
      case VarPatternLength(min, maybeMax) => (min, maybeMax)
    }

    expandExpressionDescription(from, maybeRelName, relTypes, to, direction, min, maybeMax)
  }

  private def expandExpressionDescription(from: String,
                                          maybeRelName: Option[String],
                                          relTypes: Seq[String],
                                          to: String,
                                          direction: SemanticDirection,
                                          minLength: Int,
                                          maxLength: Option[Int]): String = {
    val left = if (direction == SemanticDirection.INCOMING) "<-" else "-"
    val right = if (direction == SemanticDirection.OUTGOING) "->" else "-"
    val types = if (relTypes.isEmpty) "" else relTypes.mkString(":", "|", "")
    val lengthDescr = (minLength, maxLength) match {
      case (1, Some(1)) => ""
      case (1, None) => "*"
      case (1, Some(m)) => s"*..$m"
      case _ => s"*$minLength..${maxLength.getOrElse("")}"
    }

    val relName = maybeRelName match {
      case Some(name) if !name.unnamed => name
      case _ => ""
    }
    val relInfo = if (lengthDescr == "" && relTypes.isEmpty && relName.isEmpty) "" else s"[$relName$types$lengthDescr]"
    s"(${if (from.unnamed) "" else from})$left$relInfo$right(${if (to.unnamed) "" else to})"
  }

  private def indexInfoString(idName: String,
                              unique: Boolean,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              predicate: String,
                              caches: Seq[expressions.Expression]): String = {
    val uniqueStr = if (unique) "UNIQUE " else ""
    val propertyKeyString = propertyKeys.map(_.name).mkString(", ")
    s"$uniqueStr$idName:${label.name}($propertyKeyString) WHERE $predicate${cachesSuffix(caches)}"
  }

  private def aggregationInfo(groupingExpressions: Map[String, Expression],
                              aggregationExpressions: Map[String, Expression],
                              ordered: Seq[Expression] = Seq.empty): String = {
    val orderedStrings = ordered.map(e => PlanDescriptionArgumentSerializer.asPrettyString(e))
    val sanitizedOrdered = orderedStrings.map(PlanDescriptionArgumentSerializer.removeGeneratedNames).toIndexedSeq
    val groupingInfo = projectedExpressionInfo(groupingExpressions, sanitizedOrdered)
    val aggregatingInfo = projectedExpressionInfo(aggregationExpressions)
    (groupingInfo ++ aggregatingInfo).mkString(SEPARATOR)
  }

  private def projectedExpressionInfo(expressions: Map[String, Expression], ordered: IndexedSeq[String] = IndexedSeq.empty): Seq[String] = {
    expressions.toList.map { case (k, v) =>
      val key = PlanDescriptionArgumentSerializer.removeGeneratedNames(k)
      val value = PlanDescriptionArgumentSerializer.removeGeneratedNames(PlanDescriptionArgumentSerializer.asPrettyString(v))
      (key, value)
    }.sortBy {
      case (key, _) if ordered.contains(key) => ordered.indexOf(key)
      case (_, value) if ordered.contains(value) => ordered.indexOf(value)
      case _ => Int.MaxValue
    }.map { case (key, value) => if (key == value) key else s"$value AS $key" }
  }

  private def keyNamesInfo(keys: Seq[String]): String = {
    keys
      .map(PlanDescriptionArgumentSerializer.removeGeneratedNames)
      .mkString(SEPARATOR)
  }

  private def cachesSuffix(caches: Seq[expressions.Expression]): String = {
    if (caches.isEmpty) "" else caches.map(asPrettyString).mkString(", ", ", ", "")
  }

  private def extractDatabaseArguments(action: AdminAction,
                                       database: GraphScope,
                                       qualifier: PrivilegeQualifier,
                                       roleName: Either[String, Parameter]): Details =
    Details(Seq(action.name, extractDbScope(database)) ++ extractUserQualifier(qualifier).toSeq ++ getRoleInfo(roleName).info)

  private def getNameArgumentForLabelInAdministrationCommand(label: String, name: Either[String, Parameter]) = {
    label match {
      case "User" => getUserInfo(name)
      case "Role" => getRoleInfo(name)
      case "Database" => Details(Prettifier.escapeName(name))
    }
  }

  private def extractGraphScope(dbScope: GraphScope, qualifier: PrivilegeQualifier, resource: ActionResource): (String, String, String) = {
    val dbName = extractGraphScope(dbScope)
    val qualifierText = Prettifier.extractQualifierPart(qualifier)
    val resourceText = resource match {
      case PropertyResource(name) => s"PROPERTY ${ExpressionStringifier.backtick(name)}"
      case AllPropertyResource() => "ALL PROPERTIES"
    }
    (dbName, qualifierText, resourceText)
  }

  private def extractGraphScope(dbScope: GraphScope): String = {
   dbScope match {
      case NamedGraphScope(name) => s"GRAPH ${Prettifier.escapeName(name)}"
      case AllGraphsScope() => "ALL GRAPHS"
    }
  }

  private def extractLabelPart(resource: ActionResource): String = resource match {
    case LabelResource(name)        => s"LABEL ${ExpressionStringifier.backtick(name)}"
    case AllLabelResource()         => "ALL LABELS"
    case _                           => "<unknown>"
  }

  private def extractUserQualifier(qualifier: PrivilegeQualifier): Option[String] = qualifier match {
    case UsersQualifier(names) => Some(s"USERS ${names.map(Prettifier.escapeName).mkString(", ")}")
    case UserQualifier(name) => Some(s"USER ${Prettifier.escapeName(name)}")
    case UserAllQualifier() => Some("ALL USERS")
    case _ => None
  }

  def extractDbScope(dbScope: GraphScope): String = dbScope match {
    case NamedGraphScope(name) => s"DATABASE ${Prettifier.escapeName(name)}"
    case AllGraphsScope() => "ALL DATABASES"
    case DefaultDatabaseScope() => "DEFAULT DATABASE"
  }

  private def getUserInfo(user: Either[String, Parameter]): Details = Details(s"USER ${Prettifier.escapeName(user)}")

  private def getRoleInfo(role: Either[String, Parameter]): Details = Details(s"ROLE ${Prettifier.escapeName(role)}")

  private def indexSchemaInfo(nameOption: Option[String], label: LabelName, properties: Seq[PropertyKeyName]) = {
    val name = nameOption match {
      case Some(n) => s" ${ExpressionStringifier.backtick(n)}"
      case _ => ""
    }
    val propertyString = properties.map(asPrettyString).mkString("(", SEPARATOR, ")")
    s"INDEX$name FOR (:${label.name}) ON $propertyString"
  }

  private def constraintInfo(nameOption: Option[String], entityOption: Option[String], entityType: Either[LabelName, RelTypeName], properties: Seq[Property], assertion: Either[String, String]) = {
    val name = nameOption match {
      case Some(n) => s" ${ExpressionStringifier.backtick(n)}"
      case _ => ""
    }
    val (leftAssertion, rightAssertion) = assertion match {
      case scala.util.Left(a) => (a, "")
      case scala.util.Right(a) => ("", s" $a")
    }
    val propertyString = properties.map(asPrettyString).mkString("(", SEPARATOR, ")")
    val entity = entityOption.map(ExpressionStringifier.backtick(_)).getOrElse("")

    val entityInfo = entityType match {
      case scala.util.Left(label) => s"($entity:${asPrettyString(label)})"
      case scala.util.Right(relType) => s"()-[$entity:${asPrettyString(relType)}]-()"
    }
    s"CONSTRAINT$name ON $entityInfo ASSERT $leftAssertion$propertyString$rightAssertion"
  }


  private def setPropertyInfo(idName: String,
                              expression: Expression,
                              removeOtherProps: Boolean) = {
    val setString = if (removeOtherProps) "=" else "+="

    s"$idName $setString ${asPrettyString(expression)}"
  }
}
