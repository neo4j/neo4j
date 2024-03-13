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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.ir.CreateCommand
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.DeleteMutatingPattern
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertiesPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.AssertingMultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.AssertingMultiRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NestedPlanGetByNameExpression
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedIntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PathPropagatingBFS
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.ProjectingPlan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipLogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedNodeScan
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.runtime.ast.VariableRef
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

/**
 * Rewrites some variables to VariableRef that might fail SlottedRewriter.
 *
 * Sorry that you have to witness this mess,
 * it's a compromise between planning and physical planning that led us here.
 */
object VariableRefRewriter extends Rewriter {
  override def apply(value: AnyRef): AnyRef = instance.apply(value)

  private val rewriter = Rewriter.lift {
    case p: LogicalPlan => p match {
        case projecting: ProjectingPlan => projecting match {
            case a @ Aggregation(_, grouping, aggregation) =>
              a.copy(groupingExpressions = varMap(grouping), aggregationExpressions = varMap(aggregation))(SameId(a.id))
            case d @ Distinct(_, grouping) =>
              d.copy(groupingExpressions = varMap(grouping))(SameId(d.id))
            case a @ OrderedAggregation(_, grouping, aggregation, _) =>
              a.copy(groupingExpressions = varMap(grouping), aggregationExpressions = varMap(aggregation))(SameId(a.id))
            case d @ OrderedDistinct(_, grouping, _) =>
              d.copy(groupingExpressions = varMap(grouping))(SameId(d.id))
            case p @ Projection(_, project) =>
              p.copy(projectExpressions = varMap(project))(SameId(p.id))

          }
        case leaf: NodeLogicalLeafPlan => leaf match {
            case plan: NodeIndexLeafPlan => plan match {
                case plan: NodeIndexSeekLeafPlan => plan match {
                    case s @ NodeIndexSeek(node, _, _, _, args, _, _, _) =>
                      s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
                    case s @ PartitionedNodeIndexSeek(node, _, _, _, args, _) =>
                      s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
                    case s @ NodeUniqueIndexSeek(node, _, _, _, args, _, _, _) =>
                      s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
                  }
                case s @ NodeIndexContainsScan(node, _, _, _, args, _, _) =>
                  s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
                case s @ NodeIndexEndsWithScan(node, _, _, _, args, _, _) =>
                  s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
                case s @ NodeIndexScan(node, _, _, args, _, _, _) =>
                  s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
                case s @ PartitionedNodeIndexScan(node, _, _, args, _) =>
                  s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
              }
            case s @ AllNodesScan(node, args) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ PartitionedAllNodesScan(node, args) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ IntersectionNodeByLabelsScan(node, _, args, _) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ PartitionedIntersectionNodeByLabelsScan(node, _, args) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ NodeByElementIdSeek(node, _, args) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ NodeByIdSeek(node, _, args) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ NodeByLabelScan(node, _, args, _) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ PartitionedNodeByLabelScan(node, _, args) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ UnionNodeByLabelsScan(node, _, args, _) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ PartitionedUnionNodeByLabelsScan(node, _, args) =>
              s.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(s.id))
            case s @ SimulatedNodeScan(node, _) =>
              s.copy(idName = varRef(node))(SameId(s.id))
          }
        case leaf: RelationshipLogicalLeafPlan => leaf match {
            case plan: RelationshipIndexLeafPlan => plan match {
                case plan: RelationshipIndexSeekLeafPlan => plan match {
                    case s @ DirectedRelationshipIndexSeek(rel, start, end, _, _, _, args, _, _, _) =>
                      s.copy(
                        idName = varRef(rel),
                        startNode = varRef(start),
                        endNode = varRef(end),
                        argumentIds = args.map(varRef)
                      )(SameId(s.id))
                    case s @ PartitionedDirectedRelationshipIndexSeek(rel, start, end, _, _, _, args, _) =>
                      s.copy(
                        idName = varRef(rel),
                        startNode = varRef(start),
                        endNode = varRef(end),
                        argumentIds = args.map(varRef)
                      )(SameId(s.id))
                    case s @ DirectedRelationshipUniqueIndexSeek(rel, start, end, _, _, _, args, _, _) =>
                      s.copy(
                        idName = varRef(rel),
                        startNode = varRef(start),
                        endNode = varRef(end),
                        argumentIds = args.map(varRef)
                      )(SameId(s.id))
                    case s @ UndirectedRelationshipIndexSeek(rel, left, right, _, _, _, args, _, _, _) =>
                      s.copy(
                        idName = varRef(rel),
                        leftNode = varRef(left),
                        rightNode = varRef(right),
                        argumentIds = args.map(varRef)
                      )(SameId(s.id))
                    case s @ PartitionedUndirectedRelationshipIndexSeek(rel, left, right, _, _, _, args, _) =>
                      s.copy(
                        idName = varRef(rel),
                        leftNode = varRef(left),
                        rightNode = varRef(right),
                        argumentIds = args.map(varRef)
                      )(SameId(s.id))
                    case s @ UndirectedRelationshipUniqueIndexSeek(rel, left, right, _, _, _, args, _, _) =>
                      s.copy(
                        idName = varRef(rel),
                        leftNode = varRef(left),
                        rightNode = varRef(right),
                        argumentIds = args.map(varRef)
                      )(SameId(s.id))
                  }
                case s @ DirectedRelationshipIndexContainsScan(rel, start, end, _, _, _, args, _, _) =>
                  s.copy(
                    idName = varRef(rel),
                    startNode = varRef(start),
                    endNode = varRef(end),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
                case s @ DirectedRelationshipIndexEndsWithScan(rel, start, end, _, _, _, args, _, _) =>
                  s.copy(
                    idName = varRef(rel),
                    startNode = varRef(start),
                    endNode = varRef(end),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
                case s @ DirectedRelationshipIndexScan(rel, start, end, _, _, args, _, _, _) =>
                  s.copy(
                    idName = varRef(rel),
                    startNode = varRef(start),
                    endNode = varRef(end),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
                case s @ PartitionedDirectedRelationshipIndexScan(rel, start, end, _, _, args, _) =>
                  s.copy(
                    idName = varRef(rel),
                    startNode = varRef(start),
                    endNode = varRef(end),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
                case s @ UndirectedRelationshipIndexContainsScan(rel, left, right, _, _, _, args, _, _) =>
                  s.copy(
                    idName = varRef(rel),
                    leftNode = varRef(left),
                    rightNode = varRef(right),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
                case s @ UndirectedRelationshipIndexEndsWithScan(rel, left, right, _, _, _, args, _, _) =>
                  s.copy(
                    idName = varRef(rel),
                    leftNode = varRef(left),
                    rightNode = varRef(right),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
                case s @ UndirectedRelationshipIndexScan(rel, left, right, _, _, args, _, _, _) =>
                  s.copy(
                    idName = varRef(rel),
                    leftNode = varRef(left),
                    rightNode = varRef(right),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
                case s @ PartitionedUndirectedRelationshipIndexScan(rel, left, right, _, _, args, _) =>
                  s.copy(
                    idName = varRef(rel),
                    leftNode = varRef(left),
                    rightNode = varRef(right),
                    argumentIds = args.map(varRef)
                  )(SameId(s.id))
              }
            case s @ AssertingMultiRelationshipIndexSeek(rel, left, right, _, _) =>
              s.copy(
                relationship = varRef(rel),
                leftNode = varRef(left),
                rightNode = varRef(right)
              )(SameId(s.id))
            case s @ DirectedAllRelationshipsScan(rel, start, end, args) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ PartitionedDirectedAllRelationshipsScan(rel, start, end, args) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ DirectedRelationshipByElementIdSeek(rel, _, start, end, args) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ DirectedRelationshipByIdSeek(rel, _, start, end, args) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ DirectedRelationshipTypeScan(rel, start, _, end, args, _) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ PartitionedDirectedRelationshipTypeScan(rel, start, _, end, args) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ DirectedUnionRelationshipTypesScan(rel, start, _, end, args, _) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ PartitionedDirectedUnionRelationshipTypesScan(rel, start, _, end, args) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ UndirectedAllRelationshipsScan(rel, left, right, args) =>
              s.copy(
                idName = varRef(rel),
                leftNode = varRef(left),
                rightNode = varRef(right),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ PartitionedUndirectedAllRelationshipsScan(rel, left, right, args) =>
              s.copy(
                idName = varRef(rel),
                leftNode = varRef(left),
                rightNode = varRef(right),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ UndirectedRelationshipByElementIdSeek(rel, _, left, right, args) =>
              s.copy(
                idName = varRef(rel),
                leftNode = varRef(left),
                rightNode = varRef(right),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ UndirectedRelationshipByIdSeek(rel, _, left, right, args) =>
              s.copy(
                idName = varRef(rel),
                leftNode = varRef(left),
                rightNode = varRef(right),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ UndirectedRelationshipTypeScan(rel, left, _, right, args, _) =>
              s.copy(
                idName = varRef(rel),
                leftNode = varRef(left),
                rightNode = varRef(right),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ PartitionedUndirectedRelationshipTypeScan(rel, left, _, right, args) =>
              s.copy(
                idName = varRef(rel),
                leftNode = varRef(left),
                rightNode = varRef(right),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ UndirectedUnionRelationshipTypesScan(rel, start, _, end, args, _) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
            case s @ PartitionedUndirectedUnionRelationshipTypesScan(rel, start, _, end, args) =>
              s.copy(
                idName = varRef(rel),
                startNode = varRef(start),
                endNode = varRef(end),
                argumentIds = args.map(varRef)
              )(SameId(s.id))
          }
        case p @ AllNodesScan(node, args) =>
          p.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(p.id))
        case p @ PartitionedAllNodesScan(node, args) =>
          p.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(p.id))
        case p @ AntiConditionalApply(_, _, items) =>
          p.copy(items = items.map(varRef))(SameId(p.id))
        case p @ Argument(args) =>
          p.copy(argumentIds = args.map(varRef))(SameId(p.id))
        case p @ AssertingMultiNodeIndexSeek(node, _) =>
          p.copy(node = varRef(node))(SameId(p.id))
        case p @ AssertSameNode(node, _, _) =>
          p.copy(node = varRef(node))(SameId(p.id))
        case p @ AssertSameRelationship(rel, _, _) =>
          p.copy(idName = varRef(rel))(SameId(p.id))
        case p @ BFSPruningVarExpand(_, from, _, _, to, _, _, depthName, _, _, _) =>
          p.copy(from = varRef(from), to = varRef(to), depthName = depthName.map(varRef))(SameId(p.id))
        case p @ BidirectionalRepeatTrail(_, _, _, start, end, iStart, iEnd, _, _, iRel, pRel, pRelGr, _) =>
          p.copy(
            start = varRef(start),
            end = varRef(end),
            innerStart = varRef(iStart),
            innerEnd = varRef(iEnd),
            innerRelationships = iRel.map(varRef),
            previouslyBoundRelationships = pRel.map(varRef),
            previouslyBoundRelationshipGroups = pRelGr.map(varRef)
          )(SameId(p.id))
        case p @ ConditionalApply(_, _, items) =>
          p.copy(items = items.map(varRef))(SameId(p.id))
        case p @ Expand(_, from, _, _, to, rel, _) =>
          p.copy(from = varRef(from), to = varRef(to), relName = varRef(rel))(SameId(p.id))
        case p @ Foreach(_, variable, _, _) =>
          p.copy(variable = varRef(variable))(SameId(p.id))
        case p @ ForeachApply(_, _, variable, _) =>
          p.copy(variable = varRef(variable))(SameId(p.id))
        case p @ Input(nodes, relationships, variables, nullable) =>
          Input(nodes.map(varRef), relationships.map(varRef), variables.map(varRef), nullable)(SameId(p.id))
        case p @ LeftOuterHashJoin(nodes, _, _) =>
          p.copy(nodes = nodes.map(varRef))(SameId(p.id))
        case p @ LetAntiSemiApply(_, _, idName) =>
          p.copy(idName = varRef(idName))(SameId(p.id))
        case p @ LetSelectOrAntiSemiApply(_, _, idName, _) =>
          p.copy(idName = varRef(idName))(SameId(p.id))
        case p @ LetSelectOrSemiApply(_, _, idName, _) =>
          p.copy(idName = varRef(idName))(SameId(p.id))
        case p @ LetSemiApply(_, _, idName) =>
          p.copy(idName = varRef(idName))(SameId(p.id))
        case p @ LoadCSV(_, _, variable, _, _, _, _) =>
          p.copy(variableName = varRef(variable))(SameId(p.id))
        case p @ NodeCountFromCountStore(node, _, args) =>
          p.copy(idName = varRef(node), argumentIds = args.map(varRef))(SameId(p.id))
        case p @ NodeHashJoin(nodes, _, _) =>
          p.copy(nodes = nodes.map(varRef))(SameId(p.id))
        case p @ Optional(_, protectedVars) =>
          p.copy(protectedSymbols = protectedVars.map(varRef))(SameId(p.id))
        case p @ OptionalExpand(_, from, _, _, to, rel, _, _) =>
          p.copy(from = varRef(from), to = varRef(to), relName = varRef(rel))(SameId(p.id))
        case p @ PathPropagatingBFS(_, _, from, _, _, _, to, rel, _, _, _) =>
          p.copy(from = varRef(from), to = varRef(to), relName = varRef(rel))(SameId(p.id))
        case p @ ProduceResult(_, columns) =>
          p.copy(columns = columns.map(varRef))(SameId(p.id))
        case p @ ProjectEndpoints(_, rels, start, _, end, _, _, _, _) =>
          p.copy(rels = varRef(rels), start = varRef(start), end = varRef(end))(SameId(p.id))
        case p @ PruningVarExpand(_, from, _, _, to, _, _, _, _) =>
          p.copy(from = varRef(from), to = varRef(to))(SameId(p.id))
        case p @ RelationshipCountFromCountStore(rel, _, _, _, args) =>
          p.copy(idName = varRef(rel), argumentIds = args.map(varRef))(SameId(p.id))
        case p @ RemoveLabels(_, variable, _) =>
          p.copy(idName = varRef(variable))(SameId(p.id))
        case p @ RightOuterHashJoin(nodes, _, _) =>
          p.copy(nodes = nodes.map(varRef))(SameId(p.id))
        case p @ RollUpApply(_, _, collectionName, variableToCollect) =>
          p.copy(collectionName = varRef(collectionName), variableToCollect = varRef(variableToCollect))(SameId(p.id))
        case p @ SetLabels(_, variable, _) =>
          p.copy(idName = varRef(variable))(SameId(p.id))
        case p @ SetNodePropertiesFromMap(_, node, _, _) =>
          p.copy(idName = varRef(node))(SameId(p.id))
        case p @ SetNodeProperty(_, node, _, _) =>
          p.copy(idName = varRef(node))(SameId(p.id))
        case p @ SetRelationshipProperties(_, rel, _) =>
          p.copy(idName = varRef(rel))(SameId(p.id))
        case p @ SetRelationshipPropertiesFromMap(_, rel, _, _) =>
          p.copy(idName = varRef(rel))(SameId(p.id))
        case p @ SetRelationshipProperty(_, rel, _, _) =>
          p.copy(idName = varRef(rel))(SameId(p.id))
        case p @ SimulatedExpand(_, from, rel, to, _) =>
          p.copy(fromNode = varRef(from), relName = varRef(rel), toNode = varRef(to))(SameId(p.id))
        case p @ SimulatedNodeScan(node, _) =>
          p.copy(idName = varRef(node))(SameId(p.id))
        case p @ StatefulShortestPath(_, source, target, _, _, _, _, _, _, _, _, _, _) =>
          p.copy(sourceNode = varRef(source), targetNode = varRef(target))(SameId(p.id))
        case p @ Trail(_, _, _, start, end, iStart, iEnd, _, _, iRels, pRels, pRelGr, _) =>
          p.copy(
            start = varRef(start),
            end = varRef(end),
            innerStart = varRef(iStart),
            innerEnd = varRef(iEnd),
            innerRelationships = iRels.map(varRef),
            previouslyBoundRelationships = pRels.map(varRef),
            previouslyBoundRelationshipGroups = pRelGr.map(varRef)
          )(SameId(p.id))
        case p @ TransactionApply(_, _, _, _, _, maybeReportAs) =>
          p.copy(maybeReportAs = maybeReportAs.map(varRef))(SameId(p.id))
        case p @ TransactionForeach(_, _, _, _, _, maybeReportAs) =>
          p.copy(maybeReportAs = maybeReportAs.map(varRef))(SameId(p.id))
        case p @ TriadicBuild(_, source, seen, _) =>
          p.copy(sourceId = varRef(source), seenId = varRef(seen))(SameId(p.id))
        case p @ TriadicFilter(_, _, source, target, _) =>
          p.copy(sourceId = varRef(source), targetId = varRef(target))(SameId(p.id))
        case p @ TriadicSelection(_, _, _, source, seen, target) =>
          p.copy(sourceId = varRef(source), seenId = varRef(seen), targetId = varRef(target))(SameId(p.id))
        case p @ UnwindCollection(_, variable, _) =>
          p.copy(variable = varRef(variable))(SameId(p.id))
        case p @ PartitionedUnwindCollection(_, variable, _) =>
          p.copy(variable = varRef(variable))(SameId(p.id))
        case p @ VarExpand(_, from, _, _, _, to, rel, _, _, _, _) =>
          p.copy(from = varRef(from), to = varRef(to), relName = varRef(rel))(SameId(p.id))
        case other => other
      }
    case e: Expression => e match {
        case npe @ NestedPlanGetByNameExpression(_, column, _) =>
          npe.copy(columnNameToGet = varRef(column))(npe.position)
        case other => other
      }

    case p: SimpleMutatingPattern                      => rewrite(p)
    case c: CreateCommand                              => rewrite(c)
    case p @ ShortestRelationshipPattern(name, _, _)   => p.copy(maybePathVar = name.map(varRef))(p.expr)
    case c: ColumnOrder                                => rewrite(c)
    case g @ VariableGrouping(left, right)             => g.copy(varRef(left), varRef(right))(g.position)
    case m @ StatefulShortestPath.Mapping(left, right) => m.copy(varRef(left), varRef(right))
    case p @ PatternRelationship(name, (left, right), _, _, _) =>
      p.copy(variable = varRef(name), boundaryNodes = (varRef(left), varRef(right)))
    case NFA.State(id, variable)                => NFA.State(id, varRef(variable))
    case re: NFA.RelationshipExpansionPredicate => re.copy(relationshipVariable = varRef(re.relationshipVariable))
  }

  private val instance = topDown(rewriter)

  private def varRef(v: LogicalVariable): VariableRef = VariableRef.apply(v)

  private def varMap(map: Map[LogicalVariable, Expression]): Map[LogicalVariable, Expression] = {
    map.map { case (v, e) => VariableRef(v) -> e }
  }

  private def rewrite(p: SimpleMutatingPattern): SimpleMutatingPattern = p match {
    case m: SetMutatingPattern => m match {
        case s: SetPropertyPattern                      => s
        case s: SetPropertiesPattern                    => s
        case s: SetPropertiesFromMapPattern             => s
        case s: SetRelationshipPropertyPattern          => s.copy(variable = VariableRef(s.variable))
        case s: SetRelationshipPropertiesPattern        => s.copy(variable = VariableRef(s.variable))
        case s: SetNodePropertiesFromMapPattern         => s.copy(variable = VariableRef(s.variable))
        case s: SetRelationshipPropertiesFromMapPattern => s.copy(variable = VariableRef(s.variable))
        case s: SetNodePropertyPattern                  => s.copy(variable = VariableRef(s.variable))
        case s: SetNodePropertiesPattern                => s.copy(variable = VariableRef(s.variable))
        case s: SetLabelPattern                         => s.copy(variable = VariableRef(s.variable))
        case s: RemoveLabelPattern                      => s.copy(variable = VariableRef(s.variable))
      }
    case d: DeleteMutatingPattern => d match {
        case d: DeleteExpression => d
      }
    case c: CreatePattern => c.copy(commands = c.commands.map(rewrite))
  }

  private def rewrite(c: CreateCommand): CreateCommand = c match {
    case n: CreateNode         => n.copy(variable = VariableRef(n.variable))
    case r: CreateRelationship => r.copy(variable = VariableRef(r.variable))
  }

  private def rewrite(c: ColumnOrder): AnyRef = c match {
    case a @ Ascending(id)  => a.copy(varRef(id))
    case d @ Descending(id) => d.copy(varRef(id))
  }
}
