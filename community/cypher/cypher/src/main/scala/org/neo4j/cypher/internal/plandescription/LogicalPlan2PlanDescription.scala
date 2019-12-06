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
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AlterUser
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.AssertDatabaseAdmin
import org.neo4j.cypher.internal.logical.plans.AssertDbmsAdmin
import org.neo4j.cypher.internal.logical.plans.AssertNotCurrentUser
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertValidRevoke
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
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
import org.neo4j.cypher.internal.logical.plans.CrossApply
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DenyDatabaseAction
import org.neo4j.cypher.internal.logical.plans.DenyDbmsAction
import org.neo4j.cypher.internal.logical.plans.DenyRead
import org.neo4j.cypher.internal.logical.plans.DenyTraverse
import org.neo4j.cypher.internal.logical.plans.DenyWrite
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
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
import org.neo4j.cypher.internal.logical.plans.GrantRead
import org.neo4j.cypher.internal.logical.plans.GrantRoleToUser
import org.neo4j.cypher.internal.logical.plans.GrantTraverse
import org.neo4j.cypher.internal.logical.plans.GrantWrite
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
import org.neo4j.cypher.internal.logical.plans.MergeCreateNode
import org.neo4j.cypher.internal.logical.plans.MergeCreateRelationship
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
import org.neo4j.cypher.internal.logical.plans.RevokeDatabaseAction
import org.neo4j.cypher.internal.logical.plans.RevokeDbmsAction
import org.neo4j.cypher.internal.logical.plans.RevokeRead
import org.neo4j.cypher.internal.logical.plans.RevokeRoleFromUser
import org.neo4j.cypher.internal.logical.plans.RevokeTraverse
import org.neo4j.cypher.internal.logical.plans.RevokeWrite
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
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
import org.neo4j.cypher.internal.plandescription.Arguments.ConstraintName
import org.neo4j.cypher.internal.plandescription.Arguments.CountNodesExpression
import org.neo4j.cypher.internal.plandescription.Arguments.CountRelationshipsExpression
import org.neo4j.cypher.internal.plandescription.Arguments.Database
import org.neo4j.cypher.internal.plandescription.Arguments.DatabaseAction
import org.neo4j.cypher.internal.plandescription.Arguments.DbmsAction
import org.neo4j.cypher.internal.plandescription.Arguments.EntityByIdRhs
import org.neo4j.cypher.internal.plandescription.Arguments.EstimatedRows
import org.neo4j.cypher.internal.plandescription.Arguments.ExpandExpression
import org.neo4j.cypher.internal.plandescription.Arguments.Expression
import org.neo4j.cypher.internal.plandescription.Arguments.Expressions
import org.neo4j.cypher.internal.plandescription.Arguments.Index
import org.neo4j.cypher.internal.plandescription.Arguments.IndexName
import org.neo4j.cypher.internal.plandescription.Arguments.InequalityIndex
import org.neo4j.cypher.internal.plandescription.Arguments.KeyNames
import org.neo4j.cypher.internal.plandescription.Arguments.LabelName
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerImpl
import org.neo4j.cypher.internal.plandescription.Arguments.PlannerVersion
import org.neo4j.cypher.internal.plandescription.Arguments.PointDistanceIndex
import org.neo4j.cypher.internal.plandescription.Arguments.PrefixIndex
import org.neo4j.cypher.internal.plandescription.Arguments.Qualifier
import org.neo4j.cypher.internal.plandescription.Arguments.Role
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Scope
import org.neo4j.cypher.internal.plandescription.Arguments.Signature
import org.neo4j.cypher.internal.plandescription.Arguments.User
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.exceptions.InternalException

object LogicalPlan2PlanDescription {

  def apply(input: LogicalPlan,
            plannerName: PlannerName,
            cypherVersion: CypherVersion,
            readOnly: Boolean,
            cardinalities: Cardinalities,
            providedOrders: ProvidedOrders): InternalPlanDescription = {
    new LogicalPlan2PlanDescription(readOnly, cardinalities, providedOrders).create(input)
      .addArgument(Version("CYPHER " + cypherVersion.name))
      .addArgument(RuntimeVersion("4.1"))
      .addArgument(Planner(plannerName.toTextOutput))
      .addArgument(PlannerImpl(plannerName.name))
      .addArgument(PlannerVersion("4.1"))
  }
}

case class LogicalPlan2PlanDescription(readOnly: Boolean, cardinalities: Cardinalities, providedOrders: ProvidedOrders)
  extends LogicalPlans.Mapper[InternalPlanDescription] {

  def create(plan: LogicalPlan): InternalPlanDescription =
    LogicalPlans.map(plan, this)

  override def onLeaf(plan: LogicalPlan): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.isLeaf)

    val id = plan.id
    val variables = plan.availableSymbols

    val result: InternalPlanDescription = plan match {
      case _: AllNodesScan =>
        PlanDescriptionImpl(id, "AllNodesScan", NoChildren, Seq.empty, variables)

      case NodeByLabelScan(_, label, _) =>
        PlanDescriptionImpl(id, "NodeByLabelScan", NoChildren, Seq(LabelName(label.name)), variables)

      case NodeByIdSeek(_, _, _) =>
        PlanDescriptionImpl(id, "NodeByIdSeek", NoChildren, Seq(), variables)

      case p@NodeIndexSeek(_, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getDescriptions(label, properties.map(_.propertyKeyToken), valueExpr, unique = false, readOnly, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(indexDesc), variables)

      case p@NodeUniqueIndexSeek(_, label, properties, valueExpr, _, _) =>
        val (indexMode, indexDesc) = getDescriptions(label, properties.map(_.propertyKeyToken), valueExpr, unique = true, readOnly, p.cachedProperties)
        PlanDescriptionImpl(id, indexMode, NoChildren, Seq(indexDesc), variables)

      case p@MultiNodeIndexSeek(indexLeafPlans) =>
        val (indexModes, indexDescs) = indexLeafPlans.map { l =>
          getDescriptions(l.label, l.properties.map(_.propertyKeyToken), l.valueExpr, unique = true, readOnly, p.cachedProperties)
        }.unzip

        // TODO: Convey the uniqueness and index mode information (locking etc.) in a user friendly way
        PlanDescriptionImpl(id = plan.id, "MultiNodeIndexSeek", NoChildren,
                            indexDescs, variables)

      case ProduceResult(_, _) =>
        PlanDescriptionImpl(id, "ProduceResults", NoChildren, Seq(), variables)

      case _: plans.Argument if variables.nonEmpty =>
        PlanDescriptionImpl(id, "Argument", NoChildren, Seq.empty, variables)

      case _: plans.Argument =>
        ArgumentPlanDescription(id, Seq.empty, variables)

      case DirectedRelationshipByIdSeek(_, relIds, _, _, _) =>
        val entityByIdRhs = EntityByIdRhs(relIds)
        PlanDescriptionImpl(id, "DirectedRelationshipByIdSeek", NoChildren, Seq(entityByIdRhs), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", NoChildren, Seq.empty, variables)

      case _: Input =>
        PlanDescriptionImpl(id, "Input", NoChildren, Seq.empty, variables)

      case NodeCountFromCountStore(variable, labelNames, _) =>
        val arguments = Seq(CountNodesExpression(variable, labelNames.map(l => l.map(_.name))))
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren, arguments, variables)

      case p@NodeIndexContainsScan(_, label, property, valueExpr, _, _) =>
        val arguments = Seq(Index(label.name, Seq(property.propertyKeyToken.name), p.cachedProperties), Expression(valueExpr))
        PlanDescriptionImpl(id, "NodeIndexContainsScan", NoChildren, arguments, variables)

      case p@NodeIndexEndsWithScan(_, label, property, valueExpr, _, _) =>
        val arguments = Seq(Index(label.name, Seq(property.propertyKeyToken.name), p.cachedProperties), Expression(valueExpr))
        PlanDescriptionImpl(id, "NodeIndexEndsWithScan", NoChildren, arguments, variables)

      case p@NodeIndexScan(_, label, properties, _, _) =>
        PlanDescriptionImpl(id, "NodeIndexScan", NoChildren, Seq(Index(label.name, properties.map(_.propertyKeyToken.name), p.cachedProperties)), variables)

      case ProcedureCall(_, call) =>
        val signature = Signature(call.qualifiedName, call.callArguments, call.callResultTypes)
        PlanDescriptionImpl(id, "ProcedureCall", NoChildren, Seq(signature), variables)

      case RelationshipCountFromCountStore(ident, startLabel, typeNames, endLabel, _) =>
        val exp = CountRelationshipsExpression(ident, startLabel.map(_.name), typeNames.map(_.name),
          endLabel.map(_.name))
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren, Seq(exp), variables)

      case _: UndirectedRelationshipByIdSeek =>
        PlanDescriptionImpl(id, "UndirectedRelationshipByIdSeek", NoChildren, Seq.empty, variables)

      case CreateIndex(_ , _, None) =>
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq.empty, variables)

      case CreateIndex(_ , _, Some(name)) =>
        val indexName = IndexName(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateIndex", NoChildren, Seq(indexName), variables)

      case _: DropIndex =>
        PlanDescriptionImpl(id, "DropIndex", NoChildren, Seq.empty, variables)

      case DropIndexOnName(name) =>
        val indexName = IndexName(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "DropIndex", NoChildren, Seq(indexName), variables)

      case CreateUniquePropertyConstraint(_, _, _, None) =>
        PlanDescriptionImpl(id, "CreateUniquePropertyConstraint", NoChildren, Seq.empty, variables)

      case CreateUniquePropertyConstraint(_, _, _, Some(name)) =>
        val constraintName = ConstraintName(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateUniquePropertyConstraint", NoChildren, Seq(constraintName), variables)

      case CreateNodeKeyConstraint(_, _, _, None) =>
        PlanDescriptionImpl(id, "CreateNodeKeyConstraint", NoChildren, Seq.empty, variables)

      case CreateNodeKeyConstraint(_, _, _, Some(name)) =>
        val constraintName = ConstraintName(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateNodeKeyConstraint", NoChildren, Seq(constraintName), variables)

      case CreateNodePropertyExistenceConstraint(_, _, None) =>
        PlanDescriptionImpl(id, "CreateNodePropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case CreateNodePropertyExistenceConstraint(_, _, Some(name)) =>
        val constraintName = ConstraintName(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateNodePropertyExistenceConstraint", NoChildren, Seq(constraintName), variables)

      case CreateRelationshipPropertyExistenceConstraint(_, _, None) =>
        PlanDescriptionImpl(id, "CreateRelationshipPropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case CreateRelationshipPropertyExistenceConstraint(_, _, Some(name)) =>
        val constraintName = ConstraintName(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateRelationshipPropertyExistenceConstraint", NoChildren, Seq(constraintName), variables)

      case _: DropUniquePropertyConstraint =>
        PlanDescriptionImpl(id, "DropUniquePropertyConstraint", NoChildren, Seq.empty, variables)

      case _: DropNodeKeyConstraint =>
        PlanDescriptionImpl(id, "DropNodeKeyConstraint", NoChildren, Seq.empty, variables)

      case _: DropNodePropertyExistenceConstraint =>
        PlanDescriptionImpl(id, "DropNodePropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case _: DropRelationshipPropertyExistenceConstraint =>
        PlanDescriptionImpl(id, "DropRelationshipPropertyExistenceConstraint", NoChildren, Seq.empty, variables)

      case DropConstraintOnName(name) =>
        val constraintName = ConstraintName(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "DropConstraint", NoChildren, Seq(constraintName), variables)

      // TODO: Some of these (those with a source) are currently required in both leaf and one-child code paths,
      //  surely there is a way to not require that?
      case ShowUsers(_) =>
        PlanDescriptionImpl(id, "ShowUsers", NoChildren, Seq.empty, variables)

      case CreateUser(_, name, _, _, _, _) =>
        val userName = User(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateUser", NoChildren, Seq(userName), variables)

      case DropUser(_, name) =>
        val userName = User(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "DropUser", NoChildren, Seq(userName), variables)

      case AlterUser(_, name, _, _, _, _) =>
        val userName = User(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "AlterUser", NoChildren, Seq(userName), variables)

      case SetOwnPassword(_, _, _, _) =>
        PlanDescriptionImpl(id, "AlterCurrentUserSetPassword", NoChildren, Seq.empty, variables)

      case ShowRoles(_, _,_) =>
        PlanDescriptionImpl(id, "ShowRoles", NoChildren, Seq.empty, variables)

      case DropRole(_, name) =>
        val roleName = Role(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "DropRole", NoChildren, Seq(roleName), variables)

      case CreateRole(_, name) =>
        val roleName = Role(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateRole", NoChildren, Seq(roleName), variables)

      case RequireRole(_, name) =>
        val roleName = Role(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "RequireRole", NoChildren, Seq(roleName), variables)

      case CopyRolePrivileges(_, to, from, grantDeny) =>
        val fromRole = Role(Prettifier.escapeName(from))
        val toRole = Role(Prettifier.escapeName(to))
        PlanDescriptionImpl(id, s"CopyRolePrivileges($grantDeny)", NoChildren, Seq(fromRole, toRole), variables)

      case GrantRoleToUser(_, roleName, userName) =>
        PlanDescriptionImpl(id, "GrantRoleToUser", NoChildren, Seq(Role(Prettifier.escapeName(roleName)), User(Prettifier.escapeName(userName))), variables)

      case RevokeRoleFromUser(_, roleName, userName) =>
        PlanDescriptionImpl(id, "RevokeRoleFromUser", NoChildren, Seq(Role(Prettifier.escapeName(roleName)), User(Prettifier.escapeName(userName))), variables)

      case AssertValidRevoke(_, action, scope, roleName) =>
        val args = action match {
          case _: DatabaseAction => Seq(DatabaseAction(action.name), Database(Prettifier.extractDbScope(scope)._1))
          case _ => Seq(DbmsAction(action.name))
        }
        PlanDescriptionImpl(id, "AssertValidRevoke", NoChildren, args :+ Role(Prettifier.escapeName(roleName)), variables)

      case GrantDbmsAction(_, action, roleName) =>
        PlanDescriptionImpl(id, "GrantDbmsAction", NoChildren, Seq(DbmsAction(action.name), Role(Prettifier.escapeName(roleName))), variables)

      case DenyDbmsAction(_, action, roleName) =>
        PlanDescriptionImpl(id, "DenyDbmsAction", NoChildren, Seq(DbmsAction(action.name), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeDbmsAction(_, action, roleName, revokeType) =>
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeDbmsAction", revokeType), NoChildren, Seq(DbmsAction(action.name), Role(Prettifier.escapeName(roleName))), variables)

      case GrantDatabaseAction(_, action, database, roleName) =>
        val dbName = Prettifier.extractDbScope(database)._1
        PlanDescriptionImpl(id, "GrantDatabaseAction", NoChildren, Seq(DatabaseAction(action.name), Database(dbName), Role(Prettifier.escapeName(roleName))), variables)

      case DenyDatabaseAction(_, action, database, roleName) =>
        val dbName = Prettifier.extractDbScope(database)._1
        PlanDescriptionImpl(id, "DenyDatabaseAction", NoChildren, Seq(DatabaseAction(action.name), Database(dbName), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeDatabaseAction(_, action, database, roleName, revokeType) =>
        val dbName = Prettifier.extractDbScope(database)._1
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeDatabaseAction", revokeType), NoChildren,
          Seq(DatabaseAction(action.name), Database(dbName), Role(Prettifier.escapeName(roleName))), variables)

      case GrantTraverse(_, database, qualifier, roleName) =>
        val (dbName, qualifierText) = Prettifier.extractScope(database, qualifier)
        PlanDescriptionImpl(id, "GrantTraverse", NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case DenyTraverse(_, database, qualifier, roleName) =>
        val (dbName, qualifierText) = Prettifier.extractScope(database, qualifier)
        PlanDescriptionImpl(id, "DenyTraverse", NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeTraverse(_, database, qualifier, roleName, revokeType) =>
        val (dbName, qualifierText) = Prettifier.extractScope(database, qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeTraverse", revokeType), NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case GrantRead(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "GrantRead", NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case DenyRead(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "DenyRead", NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeRead(_, resource, database, qualifier, roleName, revokeType) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeRead", revokeType), NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case GrantWrite(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "GrantWrite", NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case DenyWrite(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "DenyWrite", NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeWrite(_, resource, database, qualifier, roleName, revokeType) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeWrite", revokeType), NoChildren, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case ShowPrivileges(_, scope) =>
        PlanDescriptionImpl(id, "ShowPrivileges", NoChildren, Seq(Scope(Prettifier.extractScope(scope))), variables)

      case ShowDatabase(normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "ShowDatabase", NoChildren, Seq(dbName), variables)

      case ShowDatabases() =>
        PlanDescriptionImpl(id, "ShowDatabases", NoChildren, Seq.empty, variables)

      case ShowDefaultDatabase() =>
        PlanDescriptionImpl(id, "ShowDefaultDatabase", NoChildren, Seq.empty, variables)

      case CreateDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "CreateDatabase", NoChildren, Seq(dbName), variables)

      case DropDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "DropDatabase", NoChildren, Seq(dbName), variables)

      case StartDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "StartDatabase", NoChildren, Seq(dbName), variables)

      case StopDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "StopDatabase", NoChildren, Seq(dbName), variables)

      case EnsureValidNonSystemDatabase(_, normalizedName, _) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "EnsureValidNonSystemDatabase", NoChildren, Seq(dbName), variables)

      case EnsureValidNumberOfDatabases(_) =>
        PlanDescriptionImpl(id, "EnsureValidNumberOfDatabases", NoChildren, Seq.empty, variables)

      case LogSystemCommand(_, _) =>
        PlanDescriptionImpl(id, "LogSystemCommand", NoChildren, Seq.empty, variables)

      case SystemProcedureCall(procedureName, _, _, _) =>
        PlanDescriptionImpl(id, procedureName, NoChildren, Seq.empty, variables)

      case DoNothingIfNotExists(_, label, name) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"DoNothingIfNotExists($label)", NoChildren, Seq(nameArgument), variables)

      case DoNothingIfExists(_, label, name) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"DoNothingIfExists($label)", NoChildren, Seq(nameArgument), variables)

      case EnsureNodeExists(_, label, name) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"EnsureNodeExists($label)", NoChildren, Seq(nameArgument), variables)

      case AssertDbmsAdmin(action) =>
        PlanDescriptionImpl(id, "AssertDbmsAdmin", NoChildren, Seq(DbmsAction(action.name)), variables)

      case AssertDatabaseAdmin(action, normalizedName) =>
        val arguments = Seq(DatabaseAction(action.name), Database(Prettifier.escapeName(normalizedName.name)))
        PlanDescriptionImpl(id, "AssertDatabaseAdmin", NoChildren, arguments, variables)

      case AssertNotCurrentUser(_, userName, _) =>
        PlanDescriptionImpl(id, "AssertNotCurrentUser", NoChildren, Seq(User(Prettifier.escapeName(userName))), variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addPlanningAttributes(result, plan)
  }

  override def onOneChildPlan(plan: LogicalPlan, source: InternalPlanDescription): InternalPlanDescription = {
    checkOnlyWhenAssertionsAreEnabled(plan.lhs.nonEmpty)
    checkOnlyWhenAssertionsAreEnabled(plan.rhs.isEmpty)

    val id = plan.id
    val variables = plan.availableSymbols
    val children = if (source.isInstanceOf[ArgumentPlanDescription]) NoChildren else SingleChild(source)

    val result: InternalPlanDescription = plan match {
      case Aggregation(_, groupingExpressions, aggregationExpressions) if aggregationExpressions.isEmpty =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case Distinct(_, groupingExpressions) =>
        PlanDescriptionImpl(id, "Distinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case OrderedDistinct(_, groupingExpressions, _) =>
        PlanDescriptionImpl(id, "OrderedDistinct", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)), variables)

      case Aggregation(_, groupingExpressions, _) =>
        PlanDescriptionImpl(id, "EagerAggregation", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)),
          variables)

      case OrderedAggregation(_, groupingExpressions, _, _) =>
        PlanDescriptionImpl(id, "OrderedAggregation", children, Seq(KeyNames(groupingExpressions.keySet.toIndexedSeq)),
          variables)

      case _: Create =>
        PlanDescriptionImpl(id, "Create", children, Seq.empty, variables)

      case _: DeleteExpression | _: DeleteNode | _: DeletePath | _: DeleteRelationship =>
        PlanDescriptionImpl(id, "Delete", children, Seq.empty, variables)

      case _: DetachDeleteExpression | _: DetachDeleteNode | _: DetachDeletePath =>
        PlanDescriptionImpl(id, "DetachDelete", children, Seq.empty, variables)

      case _: Eager =>
        PlanDescriptionImpl(id, "Eager", children, Seq.empty, variables)

      case _: EmptyResult =>
        PlanDescriptionImpl(id, "EmptyResult", children, Seq.empty, variables)

      case _: DropResult =>
        PlanDescriptionImpl(id, "DropResult", children, Seq.empty, variables)

      case NodeCountFromCountStore(idName, labelName, _) =>
        PlanDescriptionImpl(id, "NodeCountFromCountStore", NoChildren,
          Seq(CountNodesExpression(idName, labelName.map(l => l.map(_.name)))), variables)

      case RelationshipCountFromCountStore(idName, start, types, end, _) =>
        PlanDescriptionImpl(id, "RelationshipCountFromCountStore", NoChildren,
          Seq(
            CountRelationshipsExpression(idName, start.map(_.name), types.map(_.name), end.map(_.name))),
          variables)

//      case p@NodeUniqueIndexSeek(_, label, properties, _, _, _) =>
//        PlanDescriptionImpl(id = plan.id, "NodeUniqueIndexSeek", NoChildren,
//          Seq(Index(label.name, properties.map(_.propertyKeyToken.name), p.cachedProperties)), variables)

      case _: ErrorPlan =>
        PlanDescriptionImpl(id, "Error", children, Seq.empty, variables)

      case Expand(_, fromName, dir, typeNames, toName, relName, mode) =>
        val expression = ExpandExpression(fromName, relName, typeNames.map(_.name), toName, dir, 1, Some(1))
        val modeText = mode match {
          case ExpandAll => "Expand(All)"
          case ExpandInto => "Expand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, Seq(expression), variables)

      case Limit(_, count, DoNotIncludeTies) =>
        PlanDescriptionImpl(id, name = "Limit", children, Seq(Expression(count)), variables)

      case CacheProperties(_, properties) =>
        PlanDescriptionImpl(id, "CacheProperties", children, properties.toSeq.map(Expression(_)), variables)

      case LockNodes(_, nodesToLock) =>
        PlanDescriptionImpl(id, name = "LockNodes", children, Seq(KeyNames(nodesToLock.toSeq)), variables)

      case OptionalExpand(_, fromName, dir, typeNames, toName, relName, mode, predicates) =>
        val expressions = predicates.map(Expression.apply).toSeq :+
          ExpandExpression(fromName, relName, typeNames.map(_.name), toName, dir, 1, Some(1))
        val modeText = mode match {
          case ExpandAll => "OptionalExpand(All)"
          case ExpandInto => "OptionalExpand(Into)"
        }
        PlanDescriptionImpl(id, modeText, children, expressions, variables)

      case ProduceResult(_, _) =>
        PlanDescriptionImpl(id, "ProduceResults", children, Seq(), variables)

      case Projection(_, expr) =>
        val expressions = Expressions(expr)
        PlanDescriptionImpl(id, "Projection", children, Seq(expressions), variables)

      case Selection(predicate, _) =>
        PlanDescriptionImpl(id, "Filter", children, predicate.exprs.map(Expression).toSeq, variables)

      case Skip(_, count) =>
        PlanDescriptionImpl(id, name = "Skip", children, Seq(Expression(count)), variables)

      case FindShortestPaths(_, _, predicates, _, _) =>
        val args = predicates.zipWithIndex.map { case (p, idx) => s"p$idx" -> p }
        PlanDescriptionImpl(id, "ShortestPath", children, Seq(Expressions(args.toMap)), variables)

      case Limit(_, count, _) =>
        PlanDescriptionImpl(id, "Limit", children, Seq(Expression(count)), variables)

      case _: LoadCSV =>
        PlanDescriptionImpl(id, "LoadCSV", children, Seq.empty, variables)

      case _: MergeCreateNode =>
        PlanDescriptionImpl(id, "MergeCreateNode", children, Seq.empty, variables)

      case _: MergeCreateRelationship =>
        PlanDescriptionImpl(id, "MergeCreateRelationship", children, Seq.empty, variables)

      case _: Optional =>
        PlanDescriptionImpl(id, "Optional", children, Seq.empty, variables)

      case ProcedureCall(_, call) =>
        val signature = Signature(call.qualifiedName, call.callArguments, call.callResultTypes)
        PlanDescriptionImpl(id, "ProcedureCall", children, Seq(signature), variables)

      case ProjectEndpoints(_, relName, start, _, end, _, _, directed, _) =>
        val name = if (directed) "ProjectEndpoints" else "ProjectEndpoints(BOTH)"
        PlanDescriptionImpl(id, name, children, Seq(KeyNames(Seq(relName, start, end))), variables)

      case PruningVarExpand(_, fromName, dir, types, toName, min, max, maybeNodePredicate, maybeRelationshipPredicate) =>
        val expandSpec = ExpandExpression(fromName, "", types.map(_.name), toName, dir, minLength = min,
          maxLength = Some(max))
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate)
        PlanDescriptionImpl(id, s"VarLengthExpand(Pruning)", children, Seq(expandSpec) ++ predicatesDescription, variables)

      case _: RemoveLabels =>
        PlanDescriptionImpl(id, "RemoveLabels", children, Seq.empty, variables)

      case _: SetLabels =>
        PlanDescriptionImpl(id, "SetLabels", children, Seq.empty, variables)

      case _: SetNodePropertiesFromMap =>
        PlanDescriptionImpl(id, "SetNodePropertiesFromMap", children, Seq.empty, variables)

      case _: SetPropertiesFromMap =>
        PlanDescriptionImpl(id, "SetPropertiesFromMap", children, Seq.empty, variables)

      case _: SetProperty |
           _: SetNodeProperty |
           _: SetRelationshipProperty =>
        PlanDescriptionImpl(id, "SetProperty", children, Seq.empty, variables)

      case _: SetRelationshipPropertiesFromMap =>
        PlanDescriptionImpl(id, "SetRelationshipPropertiesFromMap", children, Seq.empty, variables)

      case Sort(_, orderBy) =>
        PlanDescriptionImpl(id, "Sort", children, Seq(KeyNames(orderBy.map(_.id))), variables)

      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix) =>
        PlanDescriptionImpl(id, "PartialSort", children, Seq(KeyNames(alreadySortedPrefix.map(_.id)), KeyNames(stillToSortSuffix.map(_.id))), variables)

      case Top(_, orderBy, limit) =>
        PlanDescriptionImpl(id, "Top", children, Seq(KeyNames(orderBy.map(_.id)), Expression(limit)), variables)

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit) =>
        PlanDescriptionImpl(id, "PartialTop", children, Seq(KeyNames(alreadySortedPrefix.map(_.id)), KeyNames(stillToSortSuffix.map(_.id)), Expression(limit)), variables)

      case UnwindCollection(_, _, expression) =>
        PlanDescriptionImpl(id, "Unwind", children, Seq(Expression(expression)), variables)

      case VarExpand(_, fromName, dir, _, types, toName, relName, length, mode, maybeNodePredicate, maybeRelationshipPredicate) =>
        val expandDescription = ExpandExpression(fromName, relName, types.map(_.name), toName, dir,
          minLength = length.min, maxLength = length.max)
        val predicatesDescription = buildPredicatesDescription(maybeNodePredicate, maybeRelationshipPredicate)
        val modeDescr = mode match {
          case ExpandAll => "All"
          case ExpandInto => "Into"
        }
        PlanDescriptionImpl(id, s"VarLengthExpand($modeDescr)", children,
          Seq(expandDescription) ++ predicatesDescription, variables)

      // TODO: These are currently required in both leaf and one-child code paths, surely there is a way to not require that?
      case ShowUsers(_) =>
        PlanDescriptionImpl(id, "ShowUsers", children, Seq.empty, variables)

      case CreateUser(_, name, _, _, _, _) =>
        val userName = User(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateUser", children, Seq(userName), variables)

      case DropUser(_, name) =>
        val userName = User(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "DropUser", children, Seq(userName), variables)

      case AlterUser(_, name, _, _, _, _) =>
        val userName = User(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "AlterUser", children, Seq(userName), variables)

      case ShowRoles(_, _,_) =>
        PlanDescriptionImpl(id, "ShowRoles", children, Seq.empty, variables)

      case DropRole(_, name) =>
        val roleName = Role(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "DropRole", children, Seq(roleName), variables)

      case CreateRole(_, name) =>
        val roleName = Role(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "CreateRole", children, Seq(roleName), variables)

      case RequireRole(_, name) =>
        val roleName = Role(Prettifier.escapeName(name))
        PlanDescriptionImpl(id, "RequireRole", children, Seq(roleName), variables)

      case CopyRolePrivileges(_, to, from, grantDeny) =>
        val fromRole = Role(Prettifier.escapeName(from))
        val toRole = Role(Prettifier.escapeName(to))
        PlanDescriptionImpl(id, s"CopyRolePrivileges($grantDeny)", children, Seq(fromRole, toRole), variables)

      case GrantRoleToUser(_, roleName, userName) =>
        PlanDescriptionImpl(id, "GrantRoleToUser", children, Seq(Role(Prettifier.escapeName(roleName)), User(Prettifier.escapeName(userName))), variables)

      case RevokeRoleFromUser(_, roleName, userName) =>
        PlanDescriptionImpl(id, "RevokeRoleFromUser", children, Seq(Role(Prettifier.escapeName(roleName)), User(Prettifier.escapeName(userName))), variables)

      case AssertValidRevoke(_, action, scope, roleName) =>
        val args = action match {
          case _: DatabaseAction => Seq(DatabaseAction(action.name), Database(Prettifier.extractDbScope(scope)._1))
          case _ => Seq(DbmsAction(action.name))
        }
        PlanDescriptionImpl(id, "AssertValidRevoke", children, args :+ Role(Prettifier.escapeName(roleName)), variables)

      case GrantDbmsAction(_, action, roleName) =>
        PlanDescriptionImpl(id, "GrantDbmsAction", children, Seq(DbmsAction(action.name), Role(Prettifier.escapeName(roleName))), variables)

      case DenyDbmsAction(_, action, roleName) =>
        PlanDescriptionImpl(id, "DenyDbmsAction", children, Seq(DbmsAction(action.name), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeDbmsAction(_, action, roleName, revokeType) =>
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeDbmsAction", revokeType), children, Seq(DbmsAction(action.name), Role(Prettifier.escapeName(roleName))), variables)

      case GrantDatabaseAction(_, action, database, roleName) =>
        val dbName = Prettifier.extractDbScope(database)._1
        PlanDescriptionImpl(id, "GrantDatabaseAction", children, Seq(DatabaseAction(action.name), Database(dbName), Role(Prettifier.escapeName(roleName))), variables)

      case DenyDatabaseAction(_, action, database, roleName) =>
        val dbName = Prettifier.extractDbScope(database)._1
        PlanDescriptionImpl(id, "DenyDatabaseAction", children, Seq(DatabaseAction(action.name), Database(dbName), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeDatabaseAction(_, action, database, roleName, revokeType) =>
        val dbName = Prettifier.extractDbScope(database)._1
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeDatabaseAction", revokeType), children,
          Seq(DatabaseAction(action.name), Database(dbName), Role(Prettifier.escapeName(roleName))), variables)

      case GrantTraverse(_, database, qualifier, roleName) =>
        val (dbName, qualifierText) = Prettifier.extractScope(database, qualifier)
        PlanDescriptionImpl(id, "GrantTraverse", children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case DenyTraverse(_, database, qualifier, roleName) =>
        val (dbName, qualifierText) = Prettifier.extractScope(database, qualifier)
        PlanDescriptionImpl(id, "DenyTraverse", children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeTraverse(_, database, qualifier, roleName, revokeType) =>
        val (dbName, qualifierText) = Prettifier.extractScope(database, qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeTraverse", revokeType), children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case GrantRead(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "GrantRead", children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case DenyRead(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "DenyRead", children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeRead(_, resource, database, qualifier, roleName, revokeType) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeRead", revokeType), children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case GrantWrite(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "GrantWrite", children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case DenyWrite(_, resource, database, qualifier, roleName) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, "DenyWrite", children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case RevokeWrite(_, resource, database, qualifier, roleName, revokeType) =>
        val (_, dbName, qualifierText) = Prettifier.extractScope(resource, database, qualifier)
        PlanDescriptionImpl(id, Prettifier.revokeOperation("RevokeWrite", revokeType), children, Seq(Database(dbName), Qualifier(qualifierText), Role(Prettifier.escapeName(roleName))), variables)

      case ShowPrivileges(_, scope) =>
        PlanDescriptionImpl(id, "ShowPrivileges", children, Seq(Scope(Prettifier.extractScope(scope))), variables)

      case CreateDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "CreateDatabase", children, Seq(dbName), variables)

      case DropDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "DropDatabase", children, Seq(dbName), variables)

      case StartDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "StartDatabase", children, Seq(dbName), variables)

      case StopDatabase(_, normalizedName) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "StopDatabase", children, Seq(dbName), variables)

      case EnsureValidNonSystemDatabase(_, normalizedName, _) =>
        val dbName = Database(Prettifier.escapeName(normalizedName.name))
        PlanDescriptionImpl(id, "EnsureValidNonSystemDatabase", children, Seq(dbName), variables)

      case EnsureValidNumberOfDatabases(_) =>
        PlanDescriptionImpl(id, "EnsureValidNumberOfDatabases", children, Seq.empty, variables)

      case LogSystemCommand(_, _) =>
        PlanDescriptionImpl(id, "LogSystemCommand", children, Seq.empty, variables)

      case DoNothingIfNotExists(_, label, name) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"DoNothingIfNotExists($label)", children, Seq(nameArgument), variables)

      case DoNothingIfExists(_, label, name) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"DoNothingIfExists($label)", children, Seq(nameArgument), variables)

      case EnsureNodeExists(_, label, name) =>
        val nameArgument = getNameArgumentForLabelInAdministrationCommand(label, name)
        PlanDescriptionImpl(id, s"EnsureNodeExists($label)", children, Seq(nameArgument), variables)

      case AssertNotCurrentUser(_, userName, _) =>
        PlanDescriptionImpl(id, "AssertNotCurrentUser", children, Seq(User(Prettifier.escapeName(userName))), variables)

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addPlanningAttributes(result, plan)
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

      case _: CrossApply =>
        PlanDescriptionImpl(id, "CrossApply", children, Seq.empty, variables)

      case _: AssertSameNode =>
        PlanDescriptionImpl(id, "AssertSameNode", children, Seq.empty, variables)

      case CartesianProduct(_, _) =>
        PlanDescriptionImpl(id, "CartesianProduct", children, Seq.empty, variables)

      case NodeHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeHashJoin", children, Seq(KeyNames(nodes.toIndexedSeq)), variables)

      case _: ForeachApply =>
        PlanDescriptionImpl(id, "Foreach", children, Seq.empty, variables)

      case LetSelectOrSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case row: plans.Argument =>
        ArgumentPlanDescription(id = plan.id, Seq.empty, row.argumentIds)

      case LetSelectOrAntiSemiApply(_, _, _, predicate) =>
        PlanDescriptionImpl(id, "LetSelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case _: LetSemiApply =>
        PlanDescriptionImpl(id, "LetSemiApply", children, Seq.empty, variables)

      case _: LetAntiSemiApply =>
        PlanDescriptionImpl(id, "LetAntiSemiApply", children, Seq.empty, variables)

      case LeftOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeLeftOuterHashJoin", children, Seq(KeyNames(nodes.toSeq)), variables)

      case RightOuterHashJoin(nodes, _, _) =>
        PlanDescriptionImpl(id, "NodeRightOuterHashJoin", children, Seq(KeyNames(nodes.toSeq)), variables)

      case RollUpApply(_, _, collectionName, _, _) =>
        PlanDescriptionImpl(id, "RollUpApply", children, Seq(KeyNames(Seq(collectionName))), variables)

      case SelectOrAntiSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrAntiSemiApply", children, Seq(Expression(predicate)), variables)

      case SelectOrSemiApply(_, _, predicate) =>
        PlanDescriptionImpl(id, "SelectOrSemiApply", children, Seq(Expression(predicate)), variables)

      case _: SemiApply =>
        PlanDescriptionImpl(id, "SemiApply", children, Seq.empty, variables)

      case TriadicSelection(_, _, _, source, seen, target) =>
        PlanDescriptionImpl(id, "TriadicSelection", children, Seq(KeyNames(Seq(source, seen, target))), variables)

      case _: Union =>
        PlanDescriptionImpl(id, "Union", children, Seq.empty, variables)

      case ValueHashJoin(_, _, predicate) =>
        PlanDescriptionImpl(
          id = id,
          name = "ValueHashJoin",
          children = children,
          arguments = Seq(Expression(predicate)),
          variables
        )

      case x => throw new InternalException(s"Unknown plan type: ${x.getClass.getSimpleName}. Missing a case?")
    }

    addPlanningAttributes(result, plan)
  }

  private def addPlanningAttributes(description: InternalPlanDescription, plan: LogicalPlan): InternalPlanDescription = {
    val withEstRows = if (cardinalities.isDefinedAt(plan.id))
      description.addArgument(EstimatedRows(cardinalities.get(plan.id).amount))
    else
      description

    if (providedOrders.isDefinedAt(plan.id) && !providedOrders(plan.id).isEmpty)
      withEstRows.addArgument(Order(providedOrders(plan.id)))
    else
      withEstRows
  }

  private def buildPredicatesDescription(maybeNodePredicate: Option[VariablePredicate],
                                         maybeRelationshipPredicate: Option[VariablePredicate]): Option[Expressions] = {
    val predicatesMap =
      (maybeNodePredicate.map(variablePredicate => "node" -> variablePredicate.predicate) ++
        maybeRelationshipPredicate.map(variablePredicate => "relationship" -> variablePredicate.predicate)).toMap

    if (predicatesMap.isEmpty)
      None
    else
      Some(Expressions(predicatesMap))
  }

  private def getDescriptions(label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[expressions.Expression],
                              unique: Boolean,
                              readOnly: Boolean,
                              caches: Seq[expressions.Expression]): (String, Argument) = {

    def findName(exactOnly: Boolean =  true) =
      if (unique && !readOnly && exactOnly) "NodeUniqueIndexSeek(Locking)"
      else if (unique) "NodeUniqueIndexSeek"
      else "NodeIndexSeek"

    val (name, indexDesc) = valueExpr match {
      case e: RangeQueryExpression[_] =>
        checkOnlyWhenAssertionsAreEnabled(propertyKeys.size == 1)
        val propertyKey = propertyKeys.head.name
        val name = if (unique) "NodeUniqueIndexSeekByRange" else "NodeIndexSeekByRange"
        e.expression match {
          case PrefixSeekRangeWrapper(range) =>
            (name, PrefixIndex(label.name, propertyKey, range.prefix, caches))
          case InequalitySeekRangeWrapper(RangeLessThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey,
              bounds.map(bound => s"<${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq, caches))
          case InequalitySeekRangeWrapper(RangeGreaterThan(bounds)) =>
            (name, InequalityIndex(label.name, propertyKey,
              bounds.map(bound => s">${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq, caches))
          case InequalitySeekRangeWrapper(RangeBetween(greaterThanBounds, lessThanBounds)) =>
            val greaterThanBoundsText = greaterThanBounds.bounds.map(bound =>
              s">${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq
            val lessThanBoundsText = lessThanBounds.bounds.map(bound =>
              s"<${bound.inequalitySignSuffix} ${bound.endPoint.asCanonicalStringVal}").toIndexedSeq
            (name, InequalityIndex(label.name, propertyKey, greaterThanBoundsText ++ lessThanBoundsText, caches))
          case PointDistanceSeekRangeWrapper(PointDistanceRange(point, distance, inclusive)) =>
            val funcName = Point.name
            val poi = point match {
              case FunctionInvocation(Namespace(List()), FunctionName(`funcName`), _, Seq(MapExpression(args))) =>
                s"point(${args.map(_._1.name).mkString(",")})"
              case _ => point.toString
            }
            (name, PointDistanceIndex(label.name, propertyKey, poi, distance.toString, inclusive, caches))
          case _ => throw new InternalException("This should never happen. Missing a case?")
        }
      case e: CompositeQueryExpression[_] =>
        val predicates = e.inner.map {
          case _: ExistenceQueryExpression[Expression] => "exists"
          case _: RangeQueryExpression[Expression] => "range"
          case _: CompositeQueryExpression[Expression] =>
            throw new InternalException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")
          case _ => "equality"
        }
        (s"${findName(e.exactOnly)}(${predicates.mkString(",")})", Index(label.name, propertyKeys.map(_.name), caches))
      case _ => (findName(), Index(label.name, propertyKeys.map(_.name), caches))
    }

    (name, indexDesc)
  }

  private def getNameArgumentForLabelInAdministrationCommand(label: String, name: String) = {
    label match {
      case "User" => User(Prettifier.escapeName(name))
      case "Role" => Role(Prettifier.escapeName(name))
      case "Database" => Database(Prettifier.escapeName(name))
    }
  }
}
