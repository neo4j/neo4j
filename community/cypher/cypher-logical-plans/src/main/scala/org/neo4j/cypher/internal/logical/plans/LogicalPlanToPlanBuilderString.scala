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
package org.neo4j.cypher.internal.logical.plans

import org.apache.commons.text.StringEscapeUtils
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier.Extension
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.CreateCommand
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetDynamicPropertyPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
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
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.DynamicLabel.SetOperator
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.Empty
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.Value
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.call
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.concat
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.conditional
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.convertableToParam
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.multilineParams
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.optional
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.params
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.seqParam
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.setParam
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.spread
import org.neo4j.cypher.internal.logical.plans.NFA.MultiRelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.NodeExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.graphdb.schema.IndexType

import scala.collection.mutable
import scala.language.implicitConversions

object LogicalPlanToPlanBuilderString {

  private val expressionStringifier =
    ExpressionStringifier(
      Extension.simple(expressionStringifierExtension),
      alwaysParens = false,
      alwaysBacktick = false,
      preferSingleQuotes = true,
      sensitiveParamsAsParams = false,
      javaCompatible = true
    )

  /**
   * Generates a string that plays nicely together with `AbstractLogicalPlanBuilder`.
   */
  def apply(logicalPlan: LogicalPlan): String = render(logicalPlan, None, None)

  def apply(logicalPlan: LogicalPlan, extra: LogicalPlan => String): String = render(logicalPlan, Some(extra), None)

  def apply(logicalPlan: LogicalPlan, extra: LogicalPlan => String, planPrefixDot: LogicalPlan => String): String =
    render(logicalPlan, Some(extra), Some(planPrefixDot))

  /**
   * To be used as parameter `extra` on {LogicalPlanToPlanBuilderString#apply} to print the ids of the plan operators.
   *
   * E.g. `LogicalPlanToPlanBuilderString(logicalPlan, formatId)`
   */
  def formatId(plan: LogicalPlan): String =
    s" // id ${plan.id.x}"

  private def expressionStringifierExtension(expression: Expression): String = {
    expression match {
      case p @ CachedHasProperty(_, _, _, NODE_TYPE, false)         => s"cacheNHasProperty[${p.propertyAccessString}]"
      case p @ CachedHasProperty(_, _, _, RELATIONSHIP_TYPE, false) => s"cacheRHasProperty[${p.propertyAccessString}]"
      case p @ CachedHasProperty(_, _, _, NODE_TYPE, true) => s"cacheNHasPropertyFromStore[${p.propertyAccessString}]"
      case p @ CachedHasProperty(_, _, _, RELATIONSHIP_TYPE, true) =>
        s"cacheRHasPropertyFromStore[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, NODE_TYPE, false, _)         => s"cacheN[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, RELATIONSHIP_TYPE, false, _) => s"cacheR[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, NODE_TYPE, true, _)          => s"cacheNFromStore[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, RELATIONSHIP_TYPE, true, _)  => s"cacheRFromStore[${p.propertyAccessString}]"
      case e                                                        => e.asCanonicalStringVal
    }
  }

  private def render(
    logicalPlan: LogicalPlan,
    extra: Option[LogicalPlan => String],
    planPrefixDot: Option[LogicalPlan => String]
  ) = {
    def planRepresentation(plan: LogicalPlan): String = {
      val sb = new mutable.StringBuilder()
      sb ++= planPrefixDot.fold(".")(_.apply(plan))
      sb ++= pre(plan)
      sb += '('
      sb ++= par(plan).toString
      sb += ')'
      extra.foreach(e => sb ++= e.apply(plan))

      sb.toString()
    }

    val treeString = LogicalPlanTreeRenderer.render(logicalPlan, ".|", planRepresentation)

    if (extra.isEmpty) {
      s"""$treeString
         |.build()""".stripMargin
    } else {
      treeString
    }
  }

  /**
   * Formats the plan's name as method name.
   */
  private def pre(logicalPlan: LogicalPlan): String = {
    val specialCases: PartialFunction[LogicalPlan, String] = {
      case _: ProduceResult                 => "produceResults"
      case _: AllNodesScan                  => "allNodeScan"
      case _: PartitionedAllNodesScan       => "partitionedAllNodeScan"
      case e: Expand                        => if (e.mode == ExpandAll) "expandAll" else "expandInto"
      case _: VarExpand                     => "expand"
      case _: BFSPruningVarExpand           => "bfsPruningVarExpand"
      case _: PathPropagatingBFS            => "pathPropagatingBFS"
      case e: OptionalExpand                => if (e.mode == ExpandAll) "optionalExpandAll" else "optionalExpandInto"
      case _: Selection                     => "filter"
      case _: SimulatedSelection            => "simulatedFilter"
      case _: UnwindCollection              => "unwind"
      case _: PartitionedUnwindCollection   => "partitionedUnwind"
      case _: FindShortestPaths             => "shortestPath"
      case _: NodeIndexScan                 => "nodeIndexOperator"
      case _: PartitionedNodeIndexScan      => "partitionedNodeIndexOperator"
      case _: DirectedRelationshipIndexScan => "relationshipIndexOperator"
      case _: PartitionedDirectedRelationshipIndexScan => "partitionedRelationshipIndexOperator"
      case NodeIndexSeek(_, _, _, RangeQueryExpression(PointDistanceSeekRangeWrapper(_)), _, _, _, _) =>
        "pointDistanceNodeIndexSeek"
      case NodeIndexSeek(_, _, _, RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(_)), _, _, _, _) =>
        "pointBoundingBoxNodeIndexSeek"
      case _: NodeIndexSeek            => "nodeIndexOperator"
      case _: PartitionedNodeIndexSeek => "partitionedNodeIndexOperator"
      case _: NodeUniqueIndexSeek      => "nodeIndexOperator"
      case _: NodeIndexContainsScan    => "nodeIndexOperator"
      case _: NodeIndexEndsWithScan    => "nodeIndexOperator"
      case _: MultiNodeIndexSeek       => "multiNodeIndexSeekOperator"
      case DirectedRelationshipIndexSeek(
          _,
          _,
          _,
          _,
          _,
          RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(_)),
          _,
          _,
          _,
          _
        ) =>
        "pointBoundingBoxRelationshipIndexSeek"
      case DirectedRelationshipIndexSeek(
          _,
          _,
          _,
          _,
          _,
          RangeQueryExpression(PointDistanceSeekRangeWrapper(_)),
          _,
          _,
          _,
          _
        ) =>
        "pointDistanceRelationshipIndexSeek"
      case UndirectedRelationshipIndexSeek(
          _,
          _,
          _,
          _,
          _,
          RangeQueryExpression(PointDistanceSeekRangeWrapper(_)),
          _,
          _,
          _,
          _
        ) =>
        "pointDistanceRelationshipIndexSeek"
      case _: DirectedRelationshipIndexSeek            => "relationshipIndexOperator"
      case _: PartitionedDirectedRelationshipIndexSeek => "partitionedRelationshipIndexOperator"
      case UndirectedRelationshipIndexSeek(
          _,
          _,
          _,
          _,
          _,
          RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(_)),
          _,
          _,
          _,
          _
        ) =>
        "pointBoundingBoxRelationshipIndexSeek"
      case _: UndirectedRelationshipIndexSeek                 => "relationshipIndexOperator"
      case _: PartitionedUndirectedRelationshipIndexSeek      => "partitionedRelationshipIndexOperator"
      case _: DirectedRelationshipIndexContainsScan           => "relationshipIndexOperator"
      case _: UndirectedRelationshipIndexContainsScan         => "relationshipIndexOperator"
      case _: DirectedRelationshipIndexEndsWithScan           => "relationshipIndexOperator"
      case _: UndirectedRelationshipIndexEndsWithScan         => "relationshipIndexOperator"
      case _: UndirectedRelationshipIndexScan                 => "relationshipIndexOperator"
      case _: PartitionedUndirectedRelationshipIndexScan      => "partitionedRelationshipIndexOperator"
      case _: UndirectedRelationshipUniqueIndexSeek           => "relationshipIndexOperator"
      case _: DirectedRelationshipUniqueIndexSeek             => "relationshipIndexOperator"
      case _: DirectedRelationshipTypeScan                    => "relationshipTypeScan"
      case _: UndirectedRelationshipTypeScan                  => "relationshipTypeScan"
      case _: PartitionedDirectedRelationshipTypeScan         => "partitionedRelationshipTypeScan"
      case _: PartitionedUndirectedRelationshipTypeScan       => "partitionedRelationshipTypeScan"
      case _: DirectedAllRelationshipsScan                    => "allRelationshipsScan"
      case _: UndirectedAllRelationshipsScan                  => "allRelationshipsScan"
      case _: PartitionedDirectedAllRelationshipsScan         => "partitionedAllRelationshipsScan"
      case _: PartitionedUndirectedAllRelationshipsScan       => "partitionedAllRelationshipsScan"
      case _: DirectedUnionRelationshipTypesScan              => "unionRelationshipTypesScan"
      case _: UndirectedUnionRelationshipTypesScan            => "unionRelationshipTypesScan"
      case _: PartitionedDirectedUnionRelationshipTypesScan   => "partitionedUnionRelationshipTypesScan"
      case _: PartitionedUndirectedUnionRelationshipTypesScan => "partitionedUnionRelationshipTypesScan"
      case _: DirectedRelationshipByIdSeek                    => "relationshipByIdSeek"
      case _: UndirectedRelationshipByIdSeek                  => "relationshipByIdSeek"
      case _: DirectedRelationshipByElementIdSeek             => "relationshipByElementIdSeek"
      case _: UndirectedRelationshipByElementIdSeek           => "relationshipByElementIdSeek"
    }
    specialCases.applyOrElse(logicalPlan, classNameFormat)
  }

  private def classNameFormat(logicalPlan: LogicalPlan): String = {
    val className = logicalPlan.getClass.getSimpleName
    val head = Character.toLowerCase(className.head)
    head +: className.tail
  }

  /**
   * Formats the plan's parameters to be represented inside the parameters' parentheses.
   */
  private def par(logicalPlan: LogicalPlan): Param =
    logicalPlan match {
      case Aggregation(_, groupingExpressions, aggregationExpression) =>
        params(seqParam(projectVars(groupingExpressions)), seqParam(projectVars(aggregationExpression)))
      case OrderedAggregation(_, groupingExpressions, aggregationExpression, orderToLeverage) =>
        params(
          seqParam(projectVars(groupingExpressions)),
          seqParam(projectVars(aggregationExpression)),
          seqParam(orderToLeverage)(_.quoted)
        )
      case Distinct(_, groupingExpressions) =>
        spread(projectVars(groupingExpressions))
      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        params(
          seqParam(orderToLeverage)(_.quoted),
          spread(projectVars(groupingExpressions))
        )
      case Projection(_, projectExpressions)                    => spread(projectVars(projectExpressions))
      case UnwindCollection(_, variable, expression)            => spread(projectVars(Map(variable -> expression)))
      case PartitionedUnwindCollection(_, variable, expression) => spread(projectVars(Map(variable -> expression)))
      case AllNodesScan(idName, argumentIds)                    => params(idName, spread(argumentIds))
      case PartitionedAllNodesScan(idName, argumentIds)         => params(idName, spread(argumentIds))
      case Argument(argumentIds)                                => spread(argumentIds)
      case CacheProperties(_, properties)                       => spread(properties)(_.quoted)
      case RemoteBatchProperties(_, properties)                 => spread(properties)(_.quoted)
      case RemoteBatchPropertiesWithFilter(_, predicates, properties) =>
        concat(
          spread(properties)(_.quoted),
          ")(",
          spread(predicates)(_.quoted)
        )
      case Create(_, commands) => spread(commands)
      case Merge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        params(createNodes, createRelationships, onMatch, onCreate, nodesToLock)

      case Foreach(_, variable, list, mutations) =>
        params(
          variable,
          list.quoted,
          seqParam(mutations)
        )

      case Expand(_, from, dir, types, to, rel, _) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val fromName = escapeIdentifier(from.name)
        val relName = escapeIdentifier(rel.name)
        val toName = escapeIdentifier(to.name)
        s"($fromName)$dirStrA[$relName$typeStr]$dirStrB($toName)".quoted

      case VarExpand(
          _,
          from,
          dir,
          pDir,
          types,
          to,
          relName,
          length,
          mode,
          nodePredicates,
          relationshipPredicates,
          pathMode
        ) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"${length.min}..${length.max.getOrElse("")}"

        params(
          s"(${from.name})$dirStrA[${relName.name}$typeStr*$lenStr]$dirStrB(${to.name})".quoted,
          "expandMode" -> objectName(mode),
          "projectedDir" -> objectName(pDir),
          "nodePredicates" -> nodePredicates,
          "relationshipPredicates" -> relationshipPredicates,
          "pathMode" -> pathMode
        )

      case PathPropagatingBFS(
          _,
          _,
          from,
          dir,
          projectedDir,
          types,
          to,
          relName,
          length,
          nodePredicates,
          relationshipPredicates
        ) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"${length.min}..${length.max.getOrElse("")}"
        params(
          s"(${from.name})$dirStrA[${relName.name}$typeStr*$lenStr]$dirStrB(${to.name})".quoted,
          "projectedDir" -> objectName(projectedDir),
          "nodePredicates" -> nodePredicates,
          "relationshipPredicates" -> relationshipPredicates
        )

      case FindShortestPaths(
          _,
          ShortestRelationshipPattern(
            maybePathName,
            PatternRelationship(relName, (from, to), dir, types, length),
            single
          ),
          nodePredicates,
          relationshipPredicates,
          pathPredicates,
          withFallBack,
          sameNodeMode
        ) =>
        val lenStr = length match {
          case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
          case SimplePatternLength        => ""
        }
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        params(
          s"(${from.name})$dirStrA[${relName.name}$typeStr$lenStr]$dirStrB(${to.name})".quoted,
          "pathName" -> optional(maybePathName.map(_.some)),
          "all" -> conditional(!single)(true),
          "nodePredicates" -> nodePredicates,
          "relationshipPredicates" -> relationshipPredicates,
          "pathPredicates" -> conditional(pathPredicates.nonEmpty)(seqParam(pathPredicates)(_.quoted)),
          "withFallback" -> conditional(withFallBack)(true),
          "sameNodeMode" -> objectName(sameNodeMode)
        )

      case StatefulShortestPath(
          _,
          from,
          to,
          nfa,
          mode,
          nonInlinedPreFilters,
          nodeVariableGroupings,
          relationshipVariableGroupings,
          singletonNodeVariables,
          singletonRelationshipVariables,
          selector,
          solvedExpressionString,
          reverseGroupVariableProjections,
          lengthBounds,
          pathMode
        ) =>
        multilineParams(
          2,
          from,
          to,
          solvedExpressionString.quoted,
          nonInlinedPreFilters.map(_.quoted),
          nodeVariableGroupings,
          relationshipVariableGroupings,
          singletonNodeVariables,
          singletonRelationshipVariables,
          objectName(StatefulShortestPath) + "." + objectName(StatefulShortestPath.Selector) + "." + selector.toString,
          nfaString(nfa),
          mode.toString,
          reverseGroupVariableProjections,
          lengthBounds.min,
          lengthBounds.max,
          pathMode
        )

      case PruningVarExpand(
          _,
          from,
          dir,
          types,
          to,
          minLength,
          maxLength,
          nodePredicates,
          relationshipPredicates,
          pathMode
        ) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"$minLength..$maxLength"
        params(
          s"(${from.name})$dirStrA[$typeStr*$lenStr]$dirStrB(${to.name})".quoted,
          "nodePredicates" -> nodePredicates,
          "relationshipPredicates" -> relationshipPredicates,
          "pathMode" -> pathMode
        )

      case BFSPruningVarExpand(
          _,
          from,
          dir,
          types,
          to,
          includeStartNode,
          maxLength,
          depthName,
          mode,
          nodePredicates,
          relationshipPredicates,
          pathMode
        ) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val minLength = if (includeStartNode) 0 else 1
        val lenStr = s"$minLength..$maxLength"
        params(
          s"(${from.name})$dirStrA[$typeStr*$lenStr]$dirStrB(${to.name})".quoted,
          "nodePredicates" -> nodePredicates,
          "relationshipPredicates" -> relationshipPredicates,
          "depthName" -> optional(depthName.map(_.some)),
          "mode" -> mode.toString,
          "pathMode" -> pathMode
        )

      case Limit(_, count)           => integerString(count)
      case ExhaustiveLimit(_, count) => integerString(count)
      case Skip(_, count)            => integerString(count)
      case NodeByLabelScan(idName, label, argumentIds, indexOrder) =>
        params(idName, label, indexOrder, spread(argumentIds))
      case DynamicNodeByLabelsScan(idName, labelExpr, argumentIds, indexOrder) =>
        labelExpr match {
          case DynamicLabel.Simple(expr, operator) =>
            params(idName, expr.quoted, operator, indexOrder, spread(argumentIds))
        }
      case PartitionedNodeByLabelScan(idName, label, argumentIds) =>
        params(idName, label, spread(argumentIds))
      case UnionNodeByLabelsScan(idName, labels, argumentIds, indexOrder) =>
        params(idName, labels, indexOrder, spread(argumentIds))
      case PartitionedUnionNodeByLabelsScan(idName, labels, argumentIds) =>
        params(idName, labels, spread(argumentIds))
      case IntersectionNodeByLabelsScan(idName, labels, argumentIds, indexOrder) =>
        params(idName, labels, indexOrder, spread(argumentIds))
      case PartitionedIntersectionNodeByLabelsScan(idName, labels, argumentIds) =>
        params(idName, labels, spread(argumentIds))
      case SubtractionNodeByLabelsScan(idName, ps, ns, argumentIds, indexOrder) =>
        params(idName, ps, ns, indexOrder, spread(argumentIds))
      case PartitionedSubtractionNodeByLabelsScan(idName, ps, ns, argumentIds) =>
        params(idName, ps, ns, spread(argumentIds))

      case DirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds, indexOrder) =>
        val typeNames = types.map(l => l.name).mkString("|")
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:$typeNames]->(${end.map(_.name).getOrElse("")})".quoted,
          indexOrder,
          spread(argumentIds)
        )

      case UndirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds, indexOrder) =>
        val typeNames = types.map(l => l.name).mkString("|")
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:$typeNames]-(${end.map(_.name).getOrElse("")})".quoted,
          indexOrder,
          spread(argumentIds)
        )

      case PartitionedDirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds) =>
        val typeNames = types.map(l => l.name).mkString("|")
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:$typeNames]->(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )

      case PartitionedUndirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds) =>
        val typeNames = types.map(l => l.name).mkString("|")
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:$typeNames]-(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )

      case Optional(_, protectedSymbols) =>
        spread(protectedSymbols)
      case OptionalExpand(_, from, dir, types, to, relName, _, predicate) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        params(
          s"(${from.name})$dirStrA[${relName.name}$typeStr]$dirStrB(${to.name})".quoted,
          optional(predicate.map(_.quoted.some))
        )

      case ProcedureCall(
          _,
          ResolvedCall(
            ProcedureSignature(QualifiedName(namespace, name), _, _, _, _, _, _, _, _, _, _, _),
            callArguments,
            callResults,
            _,
            _,
            yieldAll,
            _
          )
        ) =>
        val yielding =
          if (yieldAll) {
            " YIELD *"
          } else if (callResults.isEmpty) {
            ""
          } else {
            callResults.map(i => expressionStringifier(i.variable)).mkString(" YIELD ", ",", "")
          }

        val func = namespace.mkString(".") + "." + name
        val invocation = call(func, spread(callArguments))
        s"$invocation$yielding".quoted

      case ProduceResult(_, columns) if columns.exists(_.cachedProperties.nonEmpty) =>
        spread(columns) { col =>
          call("column", col.variable, spread(col.cachedProperties)(expressionStringifierExtension(_).quoted))
        }

      case ProduceResult(_, columns) =>
        spread(columns)(_.variable)

      case ProjectEndpoints(_, relName, start, startInScope, end, endInScope, types, direction, length) =>
        val (dirStrA, dirStrB) = arrows(direction)
        val typeStr = relTypeStr(types)
        val lenStr = length match {
          case SimplePatternLength        => ""
          case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
        }
        params(
          s"(${start.name})$dirStrA[${relName.name}$typeStr$lenStr]$dirStrB(${end.name})".quoted,
          "startInScope" -> startInScope,
          "endInScope" -> endInScope
        )

      case ValueHashJoin(_, _, join)       => join.quoted
      case NodeHashJoin(nodes, _, _)       => spread(nodes)
      case RightOuterHashJoin(nodes, _, _) => spread(nodes)
      case LeftOuterHashJoin(nodes, _, _)  => spread(nodes)
      case Sort(_, sortItems)              => spread(sortItems)
      case Top(_, sortItems, limit)        => params(integerString(limit), spread(sortItems))
      case Top1WithTies(_, sortItems)      => spread(sortItems)
      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, skipSortingPrefixLength) =>
        params(
          alreadySortedPrefix,
          stillToSortSuffix,
          optional(skipSortingPrefixLength.map(integerString))
        )

      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, skipSortingPrefixLength) =>
        params(
          integerString(limit),
          optional(skipSortingPrefixLength.map(integerString)),
          alreadySortedPrefix,
          stillToSortSuffix
        )

      case OrderedUnion(_, _, sortedColumns) =>
        spread(sortedColumns)
      case ErrorPlan(_, exception) =>
        // This is by no means complete, but the best we can do.
        s"new ${exception.getClass.getSimpleName}()"
      case Input(nodes, rels, vars, nullable) =>
        params(nodes, rels, vars, nullable)
      case RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, argumentIds) =>
        params(idName, startLabel, typeNames, endLabel, spread(argumentIds))
      case NodeCountFromCountStore(idName, labelNames, argumentIds) =>
        params(idName, seqParam(labelNames), spread(argumentIds))
      case DetachDeleteNode(_, expression)                   => expression.quoted
      case DeleteRelationship(_, expression)                 => expression.quoted
      case DeleteNode(_, expression)                         => expression.quoted
      case DeletePath(_, expression)                         => expression.quoted
      case DetachDeletePath(_, expression)                   => expression.quoted
      case DeleteExpression(_, expression)                   => expression.quoted
      case DetachDeleteExpression(_, expression)             => expression.quoted
      case SetProperty(_, entity, propertyKey, value)        => params(entity.quoted, propertyKey, value.quoted)
      case SetDynamicProperty(_, entity, propertyKey, value) => params(entity.quoted, propertyKey.quoted, value.quoted)
      case SetNodeProperty(_, idName, propertyKey, value)    => params(idName, propertyKey, value.quoted)
      case SetRelationshipProperty(_, idName, propertyKey, value) => params(idName, propertyKey, value.quoted)
      case SetProperties(_, entity, items)                        => params(entity.quoted, setPropertiesParam(items))
      case SetNodeProperties(_, entity, items)                    => params(entity, setPropertiesParam(items))
      case SetRelationshipProperties(_, entity, items)            => params(entity, setPropertiesParam(items))
      case SetPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        params(idName.quoted, expression.quoted, removeOtherProps)
      case SetNodePropertiesFromMap(_, idName, expression, removeOtherProps) =>
        params(idName, expression.quoted, removeOtherProps)
      case SetRelationshipPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        params(idName, expression.quoted, removeOtherProps)
      case Selection(ands, _)                                => spread(ands.exprs)(_.quoted)
      case SelectOrSemiApply(_, _, predicate)                => predicate.quoted
      case LetSelectOrSemiApply(_, _, idName, predicate)     => params(idName, predicate.quoted)
      case SelectOrAntiSemiApply(_, _, predicate)            => predicate.quoted
      case LetSelectOrAntiSemiApply(_, _, idName, predicate) => params(idName, predicate.quoted)
      case RepeatTrail(
          _,
          _,
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          groupRelationships,
          innerRelationships,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections,
          expansionMode
        ) =>
        trailParametersString(
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          groupRelationships,
          innerRelationships,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections,
          expansionMode
        )
      case BidirectionalRepeatTrail(
          _,
          _,
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          groupRelationships,
          innerRelationships,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections
        ) =>
        trailParametersString(
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          groupRelationships,
          innerRelationships,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections,
          ExpandAll
        )
      case RepeatWalk(
          _,
          _,
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          groupRelationships,
          reverseGroupVariableProjections,
          innerRelationships,
          expansionMode
        ) =>
        walkParametersString(
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          groupRelationships,
          reverseGroupVariableProjections,
          innerRelationships,
          expansionMode
        )

      case NodeByIdSeek(idName, ids, argumentIds) =>
        params(idName, argumentIds, ids)
      case NodeByElementIdSeek(idName, ids, argumentIds) =>
        params(idName, argumentIds, ids)
      case UndirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          s"(${leftNode.map(_.name).getOrElse("")})-[${idName.name}]-(${rightNode.map(_.name).getOrElse("")})".quoted,
          argumentIds,
          ids
        )
      case UndirectedRelationshipByElementIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          s"(${leftNode.map(_.name).getOrElse("")})-[${idName.name}]-(${rightNode.map(_.name).getOrElse("")})".quoted,
          argumentIds,
          ids
        )
      case DirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          s"(${leftNode.map(_.name).getOrElse("")})-[${idName.name}]->(${rightNode.map(_.name).getOrElse("")})".quoted,
          argumentIds,
          ids
        )
      case DirectedRelationshipByElementIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          s"(${leftNode.map(_.name).getOrElse("")})-[${idName.name}]->(${rightNode.map(_.name).getOrElse("")})".quoted,
          argumentIds,
          ids
        )
      case DirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}]->(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )
      case UndirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}]-(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )
      case PartitionedDirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}]->(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )
      case PartitionedUndirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}]-(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )
      case DirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typ.name}]->(${end.map(_.name).getOrElse("")})".quoted,
          indexOrder,
          spread(argumentIds)
        )
      case UndirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typ.name}]-(${end.map(_.name).getOrElse("")})".quoted,
          indexOrder,
          spread(argumentIds)
        )
      case PartitionedDirectedRelationshipTypeScan(idName, start, typ, end, argumentIds) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typ.name}]->(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )
      case PartitionedUndirectedRelationshipTypeScan(idName, start, typ, end, argumentIds) =>
        params(
          s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typ.name}]-(${end.map(_.name).getOrElse("")})".quoted,
          spread(argumentIds)
        )
      case NodeIndexScan(idName, labelToken, properties, argumentIds, indexOrder, indexType, supportPartitionedScan) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        nodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          indexOrder,
          Seq.empty,
          unique = false,
          propNames.mkString(", "),
          indexType,
          supportPartitionedScan
        )
      case PartitionedNodeIndexScan(idName, labelToken, properties, argumentIds, indexType) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        partitionedNodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          propNames.mkString(", "),
          indexType
        )
      case NodeIndexContainsScan(idName, labelToken, property, valueExpr, argumentIds, indexOrder, indexType) =>
        val propName = property.propertyKeyToken.name
        val paramExpr = getParamExpr(valueExpr)
        nodeIndexOperator(
          idName,
          labelToken,
          Seq(property),
          argumentIds,
          indexOrder,
          paramExpr,
          unique = false,
          s"$propName CONTAINS ${stringifyValueInIndexOperator(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case NodeIndexEndsWithScan(idName, labelToken, property, valueExpr, argumentIds, indexOrder, indexType) =>
        val propName = property.propertyKeyToken.name
        val paramExpr = getParamExpr(valueExpr)
        nodeIndexOperator(
          idName,
          labelToken,
          Seq(property),
          argumentIds,
          indexOrder,
          paramExpr,
          unique = false,
          s"$propName ENDS WITH ${stringifyValueInIndexOperator(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case NodeIndexSeek(
          idName,
          labelToken,
          properties,
          RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(
            PointExpression(arg),
            distance,
            inclusive
          ))),
          argumentIds,
          indexOrder,
          indexType,
          _
        ) =>
        pointDistanceNodeIndexSeek(
          idName,
          labelToken,
          properties,
          arg,
          distance,
          argumentIds,
          indexOrder,
          inclusive = inclusive,
          indexType
        )
      case NodeIndexSeek(
          idName,
          labelToken,
          properties,
          RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
            PointBoundingBoxRange(PointExpression(lowerLeft), PointExpression(upperRight))
          )),
          argumentIds,
          indexOrder,
          indexType,
          _
        ) =>
        pointBoundingBoxNodeIndexSeek(
          idName,
          labelToken,
          properties,
          lowerLeft,
          upperRight,
          argumentIds,
          indexOrder,
          indexType
        )
      case NodeIndexSeek(
          idName,
          labelToken,
          properties,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType,
          supportPartitionedScan
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        val paramExpr = getParamExpr(valueExpr)
        nodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          indexOrder,
          paramExpr,
          unique = false,
          queryStr,
          indexType,
          supportPartitionedScan
        )
      case PartitionedNodeIndexSeek(idName, labelToken, properties, valueExpr, argumentIds, indexType) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        partitionedNodeIndexOperator(idName, labelToken, properties, argumentIds, queryStr, indexType)
      case NodeUniqueIndexSeek(
          idName,
          labelToken,
          properties,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType,
          supportPartitionedScan
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        val paramExpr = getParamExpr(valueExpr)
        nodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          indexOrder,
          paramExpr,
          unique = true,
          queryStr,
          indexType,
          supportPartitionedScan = supportPartitionedScan
        )
      case DirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
            PointBoundingBoxRange(PointExpression(lowerLeft), PointExpression(upperRight))
          )),
          argumentIds,
          indexOrder,
          indexType,
          _
        ) =>
        pointBoundingBoxRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          lowerLeft,
          upperRight,
          argumentIds,
          indexOrder,
          indexType,
          directed = true
        )
      case UndirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
            PointBoundingBoxRange(PointExpression(lowerLeft), PointExpression(upperRight))
          )),
          argumentIds,
          indexOrder,
          indexType,
          _
        ) =>
        pointBoundingBoxRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          lowerLeft,
          upperRight,
          argumentIds,
          indexOrder,
          indexType,
          directed = false
        )
      case DirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          RangeQueryExpression(PointDistanceSeekRangeWrapper(
            PointDistanceRange(PointExpression(point), distance, inclusive)
          )),
          argumentIds,
          indexOrder,
          indexType,
          _
        ) =>
        pointDistanceRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          point,
          distance,
          argumentIds,
          indexOrder,
          indexType,
          directed = true,
          inclusive
        )
      case UndirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          RangeQueryExpression(PointDistanceSeekRangeWrapper(
            PointDistanceRange(PointExpression(point), distance, inclusive)
          )),
          argumentIds,
          indexOrder,
          indexType,
          _
        ) =>
        pointDistanceRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          point,
          distance,
          argumentIds,
          indexOrder,
          indexType,
          directed = false,
          inclusive
        )
      case DirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType,
          supportPartitionedScan
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          paramExpr,
          directed = true,
          unique = false,
          queryStr,
          indexType,
          supportPartitionedScan
        )
      case UndirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType,
          supportPartitionedScan
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          paramExpr,
          directed = false,
          unique = false,
          queryStr,
          indexType,
          supportPartitionedScan
        )
      case PartitionedDirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          valueExpr,
          argumentIds,
          indexType
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        partitionedRelationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          directed = true,
          queryStr,
          indexType
        )
      case PartitionedUndirectedRelationshipIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          valueExpr,
          argumentIds,
          indexType
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        partitionedRelationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          directed = false,
          queryStr,
          indexType
        )
      case DirectedRelationshipIndexScan(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          indexType,
          supportPartitionedScan
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          Seq.empty,
          directed = true,
          unique = false,
          propNames.mkString(", "),
          indexType,
          supportPartitionedScan
        )
      case UndirectedRelationshipIndexScan(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          indexType,
          supportPartitionedScan
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          Seq.empty,
          directed = false,
          unique = false,
          propNames.mkString(", "),
          indexType,
          supportPartitionedScan
        )
      case PartitionedDirectedRelationshipIndexScan(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexType
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        partitionedRelationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          directed = true,
          propNames.mkString(", "),
          indexType
        )
      case PartitionedUndirectedRelationshipIndexScan(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexType
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        partitionedRelationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          directed = false,
          propNames.mkString(", "),
          indexType
        )
      case DirectedRelationshipIndexContainsScan(
          idName,
          start,
          end,
          typeToken,
          property,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType
        ) =>
        val propName = property.propertyKeyToken.name
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          paramExpr,
          directed = true,
          unique = false,
          s"$propName CONTAINS ${stringifyValueInIndexOperator(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case UndirectedRelationshipIndexContainsScan(
          idName,
          start,
          end,
          typeToken,
          property,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType
        ) =>
        val propName = property.propertyKeyToken.name
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          paramExpr,
          directed = false,
          unique = false,
          s"$propName CONTAINS ${stringifyValueInIndexOperator(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case DirectedRelationshipIndexEndsWithScan(
          idName,
          start,
          end,
          typeToken,
          property,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType
        ) =>
        val propName = property.propertyKeyToken.name
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          paramExpr,
          directed = true,
          unique = false,
          s"$propName ENDS WITH ${stringifyValueInIndexOperator(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case UndirectedRelationshipIndexEndsWithScan(
          idName,
          start,
          end,
          typeToken,
          property,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType
        ) =>
        val propName = property.propertyKeyToken.name
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          paramExpr,
          directed = false,
          unique = false,
          s"$propName ENDS WITH ${stringifyValueInIndexOperator(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case DirectedRelationshipUniqueIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          paramExpr,
          directed = true,
          unique = true,
          queryStr,
          indexType,
          supportPartitionedScan = false
        )
      case UndirectedRelationshipUniqueIndexSeek(
          idName,
          start,
          end,
          typeToken,
          properties,
          valueExpr,
          argumentIds,
          indexOrder,
          indexType
        ) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        val paramExpr = getParamExpr(valueExpr)
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          paramExpr,
          directed = false,
          unique = true,
          queryStr,
          indexType,
          supportPartitionedScan = false
        )
      case RollUpApply(_, _, collectionName, variableToCollect) => params(collectionName, variableToCollect)
      case ForeachApply(_, _, variable, expression)             => params(variable, expression.quoted)
      case ConditionalApply(_, _, items)                        => spread(items)
      case AntiConditionalApply(_, _, items)                    => spread(items)
      case LetSemiApply(_, _, idName)                           => idName
      case LetAntiSemiApply(_, _, idName)                       => idName
      case TriadicSelection(_, _, positivePredicate, sourceId, seenId, targetId) =>
        params(positivePredicate, sourceId, seenId, targetId)
      case TriadicBuild(_, sourceId, seenId, triadicSelectionId) =>
        params(triadicSelectionId.value.x, sourceId, seenId)
      case TriadicFilter(_, positivePredicate, sourceId, targetId, triadicSelectionId) =>
        params(triadicSelectionId.value.x, positivePredicate, sourceId, targetId)
      case AssertSameNode(idName, _, _)         => idName
      case AssertSameRelationship(idName, _, _) => idName
      case Prober(_, _) =>
        "Prober.NoopProbe" // We do not preserve the object reference through the string transformation
      case RemoveLabels(_, idName, labelNames, dynamicLabels) =>
        params(
          idName,
          labelNames.toSeq,
          seqParam(dynamicLabels)(_.quoted)
        )
      case SetLabels(_, idName, labelNames, dynamicLabels) =>
        params(
          idName,
          labelNames.toSeq,
          seqParam(dynamicLabels)(_.quoted)
        )
      case LoadCSV(_, url, variableName, format, fieldTerminator, _, _) =>
        params(
          url.quoted,
          variableName,
          format.toString,
          fieldTerminator.map(_.quoted)
        )
      case Eager(_, reasons) =>
        Param.collection("ListSet", reasons)(r => eagernessReasonStr(r))
      case TransactionForeach(_, _, batchSize, concurrency, onErrorBehaviour, maybeReportAs, maybeRetryParameters) =>
        callInTxParams(batchSize, concurrency, onErrorBehaviour, maybeReportAs, maybeRetryParameters)
      case TransactionApply(_, _, batchSize, concurrency, onErrorBehaviour, maybeReportAs, maybeRetryParameters) =>
        callInTxParams(batchSize, concurrency, onErrorBehaviour, maybeReportAs, maybeRetryParameters)
      case RunQueryAt(_, query, graphReference, parameters, importsAsParameters, columns) =>
        params(
          "query" -> StringEscapeUtils.escapeJava(query).quoted,
          "graphReference" -> graphReference.print.quoted,
          "parameters" -> conditional(parameters.nonEmpty)(parameters),
          "importsAsParameters" -> conditional(importsAsParameters.nonEmpty)(importsAsParameters),
          "columns" -> conditional(columns.nonEmpty)(columns)
        )

      case SimulatedNodeScan(idName, numberOfRows) =>
        params(idName, numberOfRows)
      case SimulatedExpand(_, from, rel, to, factor) =>
        params(from, rel, to, factor)
      case SimulatedSelection(_, selectivity) =>
        s"$selectivity"
      case MultiNodeIndexSeek(indexSeekLeafPlans: Seq[NodeIndexSeekLeafPlan]) =>
        indexSeekLeafPlans.map(p => s"_.nodeIndexSeek(${par(p)})").mkString(", ")
      case _ => ""
    }

  /**
   * NFAs cause stateful shortest path operators to spill over several lines. It is then confusing if the NFA is
   * rendered on the same indentation as the stateful shortest path operator.
   */
  val indent = "  "

  private def nfaString(nfa: NFA): String = {
    val start = nfa.startState
    val constructor =
      s"${indent}new TestNFABuilder(${start.id}, ${wrapInQuotations(start.variable.name)})"
    val transitions = nfa.transitions.toSeq.sortBy(_._1).flatMap {
      case (from, transitions) =>
        transitions.toSeq.sortBy(_.endId).map(t => transitionString(nfa, nfa.states(from), t))
    }
    val finalState = s"$indent$indent.setFinalState(${nfa.finalState.id})"
    val build = s"$indent$indent.build()"

    val lines = Seq(constructor) ++ transitions :+ finalState :+ build
    lines.mkString("", "\n", "\n")
  }

  private def transitionString(nfa: NFA, from: State, transition: Transition): String = {
    val (patternString, maybeCompoundPredicate) = transition match {
      case NodeJuxtapositionTransition(endId) =>
        val to = nfa.states(endId)
        val whereString =
          to.variablePredicate.map(vp =>
            s" WHERE ${expressionStringifier(vp.predicate)}"
          ).getOrElse("")
        (
          s""" "(${escapeIdentifier(from.variable.name)}) (${escapeIdentifier(to.variable.name)}$whereString)" """.trim,
          None
        )
      case RelationshipExpansionTransition(RelationshipExpansionPredicate(relName, relPred, types, dir), endId) =>
        val to = nfa.states(endId)
        val relWhereString =
          relPred.map(vp =>
            s" WHERE ${expressionStringifier(vp.predicate)}"
          ).getOrElse("")
        val nodeWhereString =
          to.variablePredicate.map(vp =>
            s" WHERE ${expressionStringifier(vp.predicate)}"
          ).getOrElse("")
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        (
          s""" "(${escapeIdentifier(from.variable.name)})$dirStrA[${escapeIdentifier(
              relName.name
            )}$typeStr$relWhereString]$dirStrB(${escapeIdentifier(to.variable.name)}$nodeWhereString)" """.trim,
          None
        )

      case MultiRelationshipExpansionTransition(relPredicates, nodePredicates, compoundPredicate, endId) =>
        val pattern =
          (NodeExpansionPredicate(from.variable, from.variablePredicate) +: nodePredicates).zip(relPredicates).map {
            case (
                NodeExpansionPredicate(nodeVariable, nodePred),
                RelationshipExpansionPredicate(relName, relPred, types, dir)
              ) =>
              val nodeWhereString =
                nodePred.map(vp =>
                  s" WHERE ${expressionStringifier(vp.predicate)}"
                ).getOrElse("")
              val relWhereString =
                relPred.map(vp =>
                  s" WHERE ${expressionStringifier(vp.predicate)}"
                ).getOrElse("")
              val (dirStrA, dirStrB) = arrows(dir)
              val typeStr = relTypeStr(types)
              s"(${escapeIdentifier(nodeVariable.name)}$nodeWhereString)$dirStrA[${escapeIdentifier(relName.name)}$typeStr$relWhereString]$dirStrB"
          }.mkString("")
        val to = nfa.states(endId)
        val nodeWhereString =
          to.variablePredicate.map(vp =>
            s" WHERE ${expressionStringifier(vp.predicate)}"
          ).getOrElse("")
        (
          s"""  "$pattern(${escapeIdentifier(to.variable.name)}$nodeWhereString)" """.trim,
          compoundPredicate.map(c => wrapInQuotations(expressionStringifier(c)))
        )
    }

    val compoundString = maybeCompoundPredicate.map(cp => s", compoundPredicate = $cp").getOrElse("")
    s"$indent$indent.addTransition(${from.id}, ${transition.endId}, $patternString$compoundString)"
  }

  private def trailParametersString(
    repetition: Repetition,
    start: LogicalVariable,
    end: LogicalVariable,
    innerStart: LogicalVariable,
    innerEnd: LogicalVariable,
    groupNodes: Set[VariableGrouping],
    groupRelationships: Set[VariableGrouping],
    innerRelationships: Set[LogicalVariable],
    previouslyBoundRelationships: Set[LogicalVariable],
    previouslyBoundRelationshipGroups: Set[LogicalVariable],
    reverseGroupVariableProjections: Boolean,
    expansionMode: ExpansionMode
  ) =
    call(
      "TrailParameters",
      repetition.min,
      repetition.max.toString,
      start,
      end,
      innerStart,
      innerEnd,
      groupNodes,
      groupRelationships,
      innerRelationships,
      previouslyBoundRelationships,
      previouslyBoundRelationshipGroups,
      reverseGroupVariableProjections,
      expansionMode
    )

  private def walkParametersString(
    repetition: Repetition,
    start: LogicalVariable,
    end: LogicalVariable,
    innerStart: LogicalVariable,
    innerEnd: LogicalVariable,
    groupNodes: Set[VariableGrouping],
    groupRelationships: Set[VariableGrouping],
    reverseGroupVariableProjections: Boolean,
    innerRelationships: Set[LogicalVariable],
    expansionMode: ExpansionMode
  ) =
    call(
      "WalkParameters",
      repetition.min,
      repetition.max.toString,
      start,
      end,
      innerStart,
      innerEnd,
      groupNodes,
      groupRelationships,
      reverseGroupVariableProjections,
      innerRelationships,
      expansionMode
    )

  private def setPropertiesParam(items: Seq[(PropertyKeyName, Expression)]): Param =
    spread(
      items.map {
        case (p, e) => Param.tuple(p, e.quoted)
      }
    )

  private def queryExpressionStr(valueExpr: QueryExpression[Expression], propNames: Seq[String]): String = {
    valueExpr match {
      case SingleQueryExpression(expression) =>
        s"${propNames.head} = ${stringifyValueInIndexOperator(expression)}"
      case ManyQueryExpression(ListLiteral(expressions)) =>
        s"${propNames.head} = ${expressions.map(stringifyValueInIndexOperator).mkString(" OR ")}"
      case ManyQueryExpression(expr) =>
        s"${propNames.head} IN ${stringifyValueInIndexOperator(expr)}"
      case ExistenceQueryExpression() => propNames.head
      case RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(expression))) =>
        s"${propNames.head} STARTS WITH ${stringifyValueInIndexOperator(expression)}"
      case RangeQueryExpression(InequalitySeekRangeWrapper(range)) => rangeStr(range, propNames.head).toString
      case CompositeQueryExpression(inner) => inner.zip(propNames).map { case (qe, propName) =>
          queryExpressionStr(qe, Seq(propName))
        }.mkString(", ")
      case _ => ""
    }
  }

  private case class RangeStr(pre: Option[(String, String)], expr: String, post: (String, String)) {

    override def toString: String = {
      val preStr = pre match {
        case Some((vl, sign)) => s"$vl $sign "
        case None             => ""
      }
      val postStr = s" ${post._1} ${post._2}"
      s"$preStr$expr$postStr"
    }
  }

  private def rangeStr(range: InequalitySeekRange[Expression], propName: String): RangeStr = {
    range match {
      case RangeGreaterThan(NonEmptyList(ExclusiveBound(expression))) =>
        RangeStr(None, propName, (">", stringifyValueInIndexOperator(expression)))
      case RangeGreaterThan(NonEmptyList(InclusiveBound(expression))) =>
        RangeStr(None, propName, (">=", stringifyValueInIndexOperator(expression)))
      case RangeGreaterThan(NonEmptyList(preBound, postBound)) =>
        val pre = boundStringifier(preBound, "<")
        val post = boundStringifier(postBound, ">")
        RangeStr(Some(pre.swap), propName, post)
      case RangeLessThan(NonEmptyList(ExclusiveBound(expression))) =>
        RangeStr(None, propName, ("<", stringifyValueInIndexOperator(expression)))
      case RangeLessThan(NonEmptyList(preBound, postBound)) =>
        val pre = boundStringifier(preBound, ">")
        val post = boundStringifier(postBound, "<")
        RangeStr(Some(pre.swap), propName, post)
      case RangeLessThan(NonEmptyList(InclusiveBound(expression))) =>
        RangeStr(None, propName, ("<=", stringifyValueInIndexOperator(expression)))
      case RangeBetween(greaterThan, lessThan) =>
        val gt = rangeStr(greaterThan, propName)
        val lt = rangeStr(lessThan, propName)
        val pre = (gt.post._2, switchInequalitySign(gt.post._1))
        RangeStr(Some(pre), propName, lt.post)
      case _ =>
        // Should never come here
        throw new IllegalStateException(s"Unknown range expression: $range")
    }
  }

  private def boundStringifier(expression: Bound[Expression], exclusiveSign: String) = {
    expression match {
      case InclusiveBound(endPoint) => (exclusiveSign + "=", stringifyValueInIndexOperator(endPoint))
      case ExclusiveBound(endPoint) => (exclusiveSign, stringifyValueInIndexOperator(endPoint))
    }
  }

  // we support parameters in index operators, if they are top-level values.
  // That is, something like "n.prop = $param" is supported, whereas "n.prop = $param + 1" is not.
  private def stringifyValueInIndexOperator(expression: Expression): String = expression match {
    case _: ExplicitParameter => "???"
    case other                => expressionStringifier(other)
  }

  private def getParamExpr(valueExpr: QueryExpression[Expression]): Seq[String] = {
    valueExpr.folder.treeCollect {
      case ExplicitParameter(param, parameterType, _) =>
        stringifyParameter(param, parameterType)
    }
  }

  private def getParamExpr(valueExpr: Expression): Seq[String] = {
    Seq(valueExpr).collect {
      case ExplicitParameter(param, parameterType, _) =>
        stringifyParameter(param, parameterType)
    }
  }

  private def stringifyParameter(param: String, parameterType: CypherType) =
    s"""parameter("$param", ${typeValue(parameterType)})"""

  private def typeValue(cypherType: CypherType): String = {
    cypherType match {
      case CTBoolean       => "CTBoolean"
      case CTInteger       => "CTInteger"
      case CTFloat         => "CTFloat"
      case CTString        => "CTString"
      case CTDate          => "CTDate"
      case CTTime          => "CTTime"
      case CTLocalTime     => "CTLocalTime"
      case CTDateTime      => "CTDateTime"
      case CTLocalDateTime => "CTLocalDateTime"
      case CTPoint         => "CTPoint"
      case ListType(underlying, _) =>
        s"CTList(${typeValue(underlying)})"
      case _ =>
        // While this may not be correct in all cases, this function is only used for making it easier for developers to write tests
        s"CT$cypherType"
    }
  }

  private def switchInequalitySign(s: String): String = switchInequalitySign(s.head) +: s.tail

  private def switchInequalitySign(c: Char): Char = c match {
    case '>' => '<'
    case '<' => '>'
  }

  private def nodeIndexOperator(
    idName: LogicalVariable,
    labelToken: LabelToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    paramExpr: Seq[String],
    unique: Boolean,
    parenthesesContent: String,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ) =
    params(
      s"${idName.name}:${labelToken.name}($parenthesesContent)".quoted,
      "indexOrder" -> indexOrder,
      "paramExpr" -> paramExpr,
      "argumentIds" -> argumentIds,
      "getValue" -> Param.mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
      "unique" -> unique,
      "indexType" -> indexType,
      "supportPartitionedScan" -> supportPartitionedScan
    )

  private def partitionedNodeIndexOperator(
    idName: LogicalVariable,
    labelToken: LabelToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    parenthesesContent: String,
    indexType: IndexType
  ) =
    params(
      s"${idName.name}:${labelToken.name}($parenthesesContent)".quoted,
      "argumentIds" -> argumentIds,
      "getValue" -> Param.mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
      "indexType" -> indexType
    )

  private def relationshipIndexOperator(
    idName: LogicalVariable,
    start: Option[LogicalVariable],
    end: Option[LogicalVariable],
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    paramExpr: Seq[String],
    directed: Boolean,
    unique: Boolean,
    parenthesesContent: String,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ) = {
    val rarrow = if (directed) "->" else "-"
    params(
      s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typeToken.name}($parenthesesContent)]$rarrow(${end.map(_.name).getOrElse("")})".quoted,
      "indexOrder" -> indexOrder,
      "paramExpr" -> paramExpr,
      "argumentIds" -> argumentIds,
      "getValue" -> Param.mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
      "unique" -> unique,
      "indexType" -> indexType,
      "supportPartitionedScan" -> supportPartitionedScan
    )
  }

  private def partitionedRelationshipIndexOperator(
    idName: LogicalVariable,
    start: Option[LogicalVariable],
    end: Option[LogicalVariable],
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    directed: Boolean,
    parenthesesContent: String,
    indexType: IndexType
  ) = {
    val rarrow = if (directed) "->" else "-"
    params(
      s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typeToken.name}($parenthesesContent)]$rarrow(${end.map(_.name).getOrElse("")})".quoted,
      "argumentIds" -> argumentIds,
      "getValue" -> Param.mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
      "indexType" -> indexType
    )
  }

  private def pointDistanceNodeIndexSeek(
    idName: LogicalVariable,
    labelToken: LabelToken,
    properties: Seq[IndexedProperty],
    point: Expression,
    distance: Expression,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    inclusive: Boolean,
    indexType: IndexType
  ) =
    params(
      idName,
      labelToken,
      properties.head.propertyKeyToken,
      point.quoted,
      distance,
      "indexOrder" -> indexOrder,
      "argumentIds" -> argumentIds,
      "getValue" -> getSingleIndexBehavior(properties),
      "inclusive" -> inclusive,
      "indexType" -> indexType
    )

  private def pointBoundingBoxNodeIndexSeek(
    idName: LogicalVariable,
    labelToken: LabelToken,
    properties: Seq[IndexedProperty],
    lowerLeft: Expression,
    upperRight: Expression,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    indexType: IndexType
  ) =
    params(
      idName,
      labelToken,
      properties.head.propertyKeyToken,
      lowerLeft.quoted,
      upperRight.quoted,
      "indexOrder" -> indexOrder,
      "argumentIds" -> argumentIds,
      "getValue" -> getSingleIndexBehavior(properties),
      "indexType" -> indexType
    )

  private def pointBoundingBoxRelationshipIndexSeek(
    idName: LogicalVariable,
    start: Option[LogicalVariable],
    end: Option[LogicalVariable],
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    lowerLeft: Expression,
    upperRight: Expression,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    indexType: IndexType,
    directed: Boolean
  ) = {
    val propName = properties.map(_.propertyKeyToken.name).head
    val rarrow = if (directed) "->" else "-"
    params(
      s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typeToken.name}($propName)]$rarrow(${end.map(_.name).getOrElse("")})".quoted,
      lowerLeft.quoted,
      upperRight.quoted,
      "indexOrder" -> indexOrder,
      "argumentIds" -> argumentIds,
      "getValue" -> getSingleIndexBehavior(properties),
      "indexType" -> indexType
    )
  }

  private def pointDistanceRelationshipIndexSeek(
    idName: LogicalVariable,
    start: Option[LogicalVariable],
    end: Option[LogicalVariable],
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    point: Expression,
    distance: Expression,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    indexType: IndexType,
    directed: Boolean,
    inclusive: Boolean
  ) = {
    val propName = properties.map(_.propertyKeyToken.name).head
    val rarrow = if (directed) "->" else "-"
    params(
      s"(${start.map(_.name).getOrElse("")})-[${idName.name}:${typeToken.name}($propName)]$rarrow(${end.map(_.name).getOrElse("")})".quoted,
      point.quoted,
      distance,
      "inclusive" -> inclusive,
      "getValue" -> getSingleIndexBehavior(properties),
      "indexOrder" -> indexOrder,
      "argumentIds" -> argumentIds,
      "indexType" -> indexType
    )
  }

  private def getSingleIndexBehavior(properties: Seq[IndexedProperty]): Param =
    properties.map(_.getValueFromIndex).reduce[GetValueFromIndexBehavior] {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException(
            "Index operators with different getValueFromIndex behaviors not supported."
          )
        }
    }

  private def integerString(count: Expression) = {
    count match {
      case SignedDecimalIntegerLiteral(i) => i
      case _                              => "/* " + count + "*/"
    }
  }

  private def conflictStr(conflict: EagernessReason.Conflict): String =
    s"EagernessReason.Conflict(${conflict.first}, ${conflict.second})"

  private def eagernessReasonStr(reason: EagernessReason): String = {
    val prefix = objectName(EagernessReason)
    val suffix = reason match {
      case EagernessReason.Unknown                      => objectName(EagernessReason.Unknown)
      case EagernessReason.UpdateStrategyEager          => objectName(EagernessReason.UpdateStrategyEager)
      case EagernessReason.WriteAfterCallInTransactions => objectName(EagernessReason.WriteAfterCallInTransactions)
      case EagernessReason.ProcedureCallEager           => objectName(EagernessReason.ProcedureCallEager)
      case r: EagernessReason.NonUnique                 => nonUniqueEagernessReasonStr(r)
      case EagernessReason.ReasonWithConflict(reason, conflict) =>
        s"${nonUniqueEagernessReasonStr(reason)}.withConflict(${conflictStr(conflict)})"
      case EagernessReason.Summarized(summary) =>
        val entryPrefix = s"$prefix.${objectName(EagernessReason.SummaryEntry)}"
        val summaryStr = summary.map {
          case (reason, EagernessReason.SummaryEntry(conflict, count)) =>
            s"${eagernessReasonStr(reason)} -> $entryPrefix(${conflictStr(conflict)}, $count)"
        }.mkString("Map(", ", ", ")")
        s"${objectName(EagernessReason.Summarized)}($summaryStr)"
    }
    s"$prefix.$suffix"
  }

  private def nonUniqueEagernessReasonStr(reason: EagernessReason.NonUnique): String = reason match {
    case EagernessReason.LabelReadSetConflict(label) =>
      s"${objectName(EagernessReason.LabelReadSetConflict)}(LabelName(${wrapInQuotations(label.name)})(InputPosition.NONE))"
    case EagernessReason.TypeReadSetConflict(relType) =>
      s"${objectName(EagernessReason.TypeReadSetConflict)}(RelTypeName(${wrapInQuotations(relType.name)})(InputPosition.NONE))"
    case EagernessReason.LabelReadRemoveConflict(label) =>
      s"${objectName(EagernessReason.LabelReadRemoveConflict)}(LabelName(${wrapInQuotations(label.name)})(InputPosition.NONE))"
    case EagernessReason.UnknownLabelReadSetConflict =>
      s"${objectName(EagernessReason.UnknownLabelReadSetConflict)}"
    case EagernessReason.UnknownLabelReadRemoveConflict =>
      s"${objectName(EagernessReason.UnknownLabelReadRemoveConflict)}"
    case EagernessReason.ReadDeleteConflict(identifier) =>
      s"${objectName(EagernessReason.ReadDeleteConflict)}(${wrapInQuotations(identifier)})"
    case EagernessReason.ReadCreateConflict =>
      s"${objectName(EagernessReason.ReadCreateConflict)}"
    case EagernessReason.PropertyReadSetConflict(property) =>
      s"${objectName(EagernessReason.PropertyReadSetConflict)}(PropertyKeyName(${wrapInQuotations(property.name)})(InputPosition.NONE))"
    case EagernessReason.UnknownPropertyReadSetConflict =>
      s"${objectName(EagernessReason.UnknownPropertyReadSetConflict)}"
  }

  private[plans] def relTypeStr(types: Seq[RelTypeName]) = {
    types match {
      case head +: tail => s":${head.name}${tail.map(t => s"|${t.name}").mkString("")}"
      case _            => ""
    }
  }

  private def projectVars(map: Map[LogicalVariable, Expression]) =
    map.view.map { case (key, e) => concat(e, " AS ", escapeIdentifier(key.name)).quoted }

  private def escapeIdentifier(alias: String) = {
    if (alias.matches("\\w+")) alias else s"`$alias`"
  }

  private def wrapInQuotations(c: String): String = "\"" + c + "\""

  private def objectName(obj: AnyRef): String = {
    val str = obj.getClass.getSimpleName
    str.substring(0, str.length - 1)
  }

  private[plans] def arrows(dir: SemanticDirection): (String, String) = dir match {
    case SemanticDirection.OUTGOING => ("-", "->")
    case SemanticDirection.INCOMING => ("<-", "-")
    case SemanticDirection.BOTH     => ("-", "-")
  }

  private def callInTxParams(
    batchSize: Expression,
    concurrency: TransactionConcurrency,
    onErrorBehaviour: InTransactionsOnErrorBehaviour,
    maybeReportAs: Option[LogicalVariable],
    maybeRetryParams: Option[InTransactionsRetryParameters]
  ) =
    params(
      batchSize,
      concurrency match {
        case TransactionConcurrency.Concurrent(Some(concurrency)) => s"Concurrent(Some($concurrency))"
        case c                                                    => c.toString
      },
      onErrorBehaviour.toString,
      optional(maybeReportAs.map(_.name.some)),
      "maybeRetryParameters" -> (maybeRetryParams match {
        case Some(InTransactionsRetryParameters(Some(timeoutExpr))) =>
          s"Some(InTransactionsRetryParameters(Some(DecimalDoubleLiteral(\"${expressionStringifier(timeoutExpr)}\")(InputPosition.NONE)))(InputPosition.NONE))"
        case _ =>
          "None"
      })
    )

  /** Typeclass providing a standardised way to encode particular types as parameter strings.
   *  Instances are used to provide values for the [[Param]] magnet object */
  trait ToParam[A] {
    def convert(value: A): Param
  }

  private object ToParam {
    private def str[A](f: A => String): ToParam[A] = v => f(v)
    implicit def fromParam: ToParam[Param] = identity(_)
    implicit def fromString: ToParam[String] = Param(_)
    implicit def fromBoolean: ToParam[Boolean] = x => Param(_.append(x))
    implicit def fromLong: ToParam[Long] = x => Param(_.append(x))
    implicit def fromInt: ToParam[Int] = x => Param(_.append(x))
    implicit def fromDouble: ToParam[Double] = x => Param(_.append(x))

    implicit def fromOption[A: ToParam]: ToParam[Option[A]] = {
      case Some(a) => call("Some", a)
      case None    => Param("None")
    }

    implicit def fromStringToNamedTuple[A: ToParam]: ToParam[(String, A)] = {
      case (name, value) =>
        convertableToParam(value) match {
          case v: Value =>
            Param.Named(name, v)
          case _ =>
            Empty
        }
    }

    implicit def fromSet[A: ToParam]: ToParam[Set[A]] = setParam(_)
    implicit def fromSeq[A: ToParam]: ToParam[Seq[A]] = seqParam(_)

    implicit def fromMap[K: ToParam, V: ToParam]: ToParam[Map[K, V]] = map =>
      call("Map", spread(map) { case (k, v) => Param.tuple(k, v) })

    implicit def fromExpression[E <: Expression]: ToParam[E] = str(expressionStringifier(_))

    implicit def fromIndexType: ToParam[IndexType] = i =>
      Param(_
        .append(i.getDeclaringClass.getSimpleName)
        .append('.')
        .append(i.name))

    implicit def fromColumnOrder: ToParam[ColumnOrder] = co =>
      Param(_
        .append(escapeIdentifier(co.id.name))
        .append(' ')
        .append(if (co.isAscending) "ASC" else "DESC")).quoted

    implicit def fromExpansionMode: ToParam[ExpansionMode] = str(objectName)
    implicit def fromVariable: ToParam[LogicalVariable] = v => escapeIdentifier(v.name).quoted
    implicit def fromLabelName: ToParam[LabelName] = _.name.quoted
    implicit def fromLabelToken: ToParam[LabelToken] = _.name.quoted
    implicit def fromPropertyKeyName: ToParam[PropertyKeyName] = _.name.quoted
    implicit def fromPropertyKeyToken: ToParam[PropertyKeyToken] = _.name.quoted
    implicit def fromRelTypeName: ToParam[RelTypeName] = _.name.quoted
    implicit def fromRelTypeToken: ToParam[RelationshipTypeToken] = _.name.quoted
    implicit def fromSetOperator: ToParam[SetOperator] = str(x => s"DynamicLabel.$x")

    implicit def fromIndexOrder: ToParam[IndexOrder] = str(objectName)
    implicit def fromTraversalPathMode: ToParam[TraversalPathMode] = str(objectName)
    implicit def fromGetValueFromIndexBehavior: ToParam[GetValueFromIndexBehavior] = str(objectName)

    implicit def fromParameter: ToParam[Parameter] = _.asCanonicalStringVal.quoted

    implicit def fromVariablePredicate: ToParam[VariablePredicate] =
      vp => call("Predicate", vp.variable, vp.predicate.quoted)

    implicit def fromVariableGroupingSet: ToParam[Set[VariableGrouping]] =
      setParam(_)(x => Param.tuple(x.singleton, x.group))

    implicit def fromNfaMappingSet: ToParam[Set[Mapping]] =
      setParam(_)(x => Param.tuple(x.nfaExprVar, x.rowVar))

    implicit def fromSeekableArgs: ToParam[SeekableArgs] = { ids =>
      def stringify(expr: Expression): Param = expr match {
        case literal: NumberLiteral => literal
        case expr                   => expr.quoted
      }

      ids match {
        case SingleSeekableArg(expr)                    => stringify(expr)
        case ManySeekableArgs(ListLiteral(expressions)) => spread(expressions)(e => stringify(e))
        case ManySeekableArgs(expr)                     => stringify(expr)
      }
    }

    implicit def fromSimpleMutatingPattern[A <: SimpleMutatingPattern]: ToParam[A] = {
      case c: CreatePattern =>
        call("createPattern", c.nodes, c.relationships)
      case org.neo4j.cypher.internal.ir.DeleteExpression(expression, forced) =>
        call("delete", expression.quoted, forced)
      case SetLabelPattern(node, labelNames, dynamicLabels) =>
        call("setLabel", node, labelNames, seqParam(dynamicLabels)(_.quoted))
      case RemoveLabelPattern(node, labelNames, dynamicLabels) =>
        call("removeLabel", node, labelNames, seqParam(dynamicLabels)(_.quoted))
      case SetNodePropertyPattern(node, propertyKey, value) =>
        call("setNodeProperty", node, propertyKey, value.quoted)
      case SetRelationshipPropertyPattern(relationship, propertyKey, value) =>
        call("setRelationshipProperty", relationship, propertyKey, value.quoted)
      case SetNodePropertiesFromMapPattern(idName, expression, removeOtherProps) =>
        call("setNodePropertiesFromMap", idName, expression.quoted, removeOtherProps)
      case SetRelationshipPropertiesFromMapPattern(idName, expression, removeOtherProps) =>
        call("setRelationshipPropertiesFromMap", idName, expression.quoted, removeOtherProps)
      case SetPropertyPattern(entityExpression, propertyKey, value) =>
        call("setProperty", entityExpression.quoted, propertyKey, value.quoted)
      case SetDynamicPropertyPattern(entityExpression, propertyKey, value) =>
        call("setDynamicProperty", entityExpression.quoted, propertyKey.quoted, value.quoted)
      case SetPropertiesFromMapPattern(entityExpression, map, removeOtherProps) =>
        call("setPropertyFromMap", entityExpression.quoted, map.quoted, removeOtherProps)
      case SetPropertiesPattern(entity, items) =>
        call("setProperties", entity.quoted, setPropertiesParam(items)).toString
      case SetNodePropertiesPattern(entity, items) =>
        call("setNodeProperties", entity, setPropertiesParam(items)).toString
      case SetRelationshipPropertiesPattern(entity, items) =>
        call("setRelationshipProperties", entity, setPropertiesParam(items)).toString
    }

    implicit def fromCreateCommand[A <: CreateCommand]: ToParam[A] = {
      case createNode: CreateNode =>
        call(
          "createNodeFull",
          createNode.variable,
          "labels" -> conditional(createNode.labels.nonEmpty)(createNode.labels.toSeq),
          "dynamicLabels" -> conditional(createNode.labelExpressions.nonEmpty)(
            seqParam(createNode.labelExpressions.toSeq)(_.quoted)
          ),
          "properties" -> optional(createNode.properties.map(_.quoted.some))
        )

      case rel: CreateRelationship =>
        rel.relType match {
          case staticName: RelTypeName =>
            call(
              "createRelationship",
              rel.variable,
              rel.leftNode,
              staticName,
              rel.rightNode,
              rel.direction.toString,
              optional(rel.properties.map(_.quoted.some))
            )

          case dynamicExpr: DynamicRelTypeExpression =>
            call(
              "createRelationshipWithDynamicType",
              rel.variable,
              rel.leftNode,
              dynamicExpr.expression.quoted,
              rel.rightNode,
              rel.direction.toString,
              optional(rel.properties.map(_.quoted.some))
            )
        }
    }
  }

  /** Magnet pattern object to provide string representation of a parameter. See [[ToParam]] */
  sealed trait Param {
    def write(sb: StringBuilder): Unit

    def wrap(before: String, after: String): Param =
      Param { sb =>
        sb.append(before)
        write(sb)
        sb.append(after)
      }

    /** Wraps this parameter in "double quotes" */
    def quoted: Param =
      wrap("\"", "\"")

    /** Wraps this parameter in Some() */
    def some: Param =
      wrap("Some(", ")")

    override def toString: String = {
      val sb = new StringBuilder
      write(sb)
      sb.toString
    }

  }

  object Param {

    def apply(f: StringBuilder => Unit): Param = Value(f)
    def apply(s: String): Param = apply(_.append(s))

    // enables the magnet pattern
    implicit def convertableToParam[A](value: A)(implicit toParam: ToParam[A]): Param =
      toParam.convert(value)

    /** Includes the optional parameter directly if it is Some */
    def optional(param: Option[Param]): Param =
      param match {
        case Some(value) => value
        case None        => Empty
      }

    /** Includes the parameter if the predicate is True */
    def conditional(predicate: Boolean)(param: Param): Param =
      if (predicate) param else Empty

    def seqParam[A](seq: Iterable[A])(implicit toParam: ToParam[A]): Param =
      collection("Seq", seq)

    def setParam[A](set: Iterable[A])(implicit toParam: ToParam[A]): Param =
      collection("Set", set)

    def mapParam[A, K: ToParam, V: ToParam](map: Iterable[A])(key: A => K, value: A => V): Param =
      collection("Map", map)(x => tupleArrow(key(x), value(x)))

    def collection[A](name: String, coll: Iterable[A])(implicit toParam: ToParam[A]): Param =
      call(name, spread(coll))

    /** Writes a function invocation with the given name and parameters */
    def call(name: String, params: Param*): Param =
      Param { sb =>
        sb.append(name).append("(")
        commaSeparated(params.iterator, sb)
        sb.append(")")
      }

    /** Writes a tuple of two parameters with comma syntax (a,b) */
    def tuple(a: Param, b: Param): Param =
      Param { sb =>
        sb.append('(')
        a.write(sb)
        sb.append(", ")
        b.write(sb)
        sb.append(')')
      }

    /** Writes a tuple of two parameters with arrow syntax a -> b */
    private def tupleArrow(a: Param, b: Param): Param =
      Param { sb =>
        a.write(sb)
        sb.append(" -> ")
        b.write(sb)
      }

    // plain parameter value
    case class Value(f: StringBuilder => Unit) extends Param {
      def write(sb: StringBuilder): Unit = f(sb)
    }

    // appears in the parameter list as a parameter with name
    case class Named(name: String, value: Value) extends Param {

      def write(sb: StringBuilder): Unit = {
        sb.append(name).append(" = ")
        value.write(sb)
      }
    }

    // will not appear in the parameter list
    case object Empty extends Param {
      def write(sb: StringBuilder): Unit = ()
    }

    private def commaSeparated(params: Iterator[Param], sb: StringBuilder): Unit = {
      var initial = true
      params.foreach {
        case Empty => ()
        case p =>
          if (!initial) {
            sb.append(", ")
          }
          initial = false
          p.write(sb)
      }
    }

    /** Joins the parameters as a comma-separated list.
     *  Returns [[Empty]] if the iterable is empty or it only contains [[Empty]] parameters.
     *  The name is a reference to the
     *  [[https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/Spread_syntax javascript operator]] */
    def spread[A](params: Iterable[A])(implicit toParam: ToParam[A]): Param = {
      val iter = params.iterator.map(toParam.convert)
        .filter(_ != Empty)

      if (!iter.hasNext) Empty
      else Param(commaSeparated(iter, _))
    }

    /** Builds a comma-separated list of parameter values */
    def params(params: Param*): Param = {
      spread(params)
    }

    /** Concatenates a series of values with no separator */
    def concat(params: Param*): Param =
      Param { sb =>
        params.foreach(_.write(sb))
      }

    /** Writes a list of parameters on individual lines with indentation */
    def multilineParams(indent: Int, params: Param*): Param = {
      val idt = " ".repeat(indent)
      Param { sb =>
        var initial = true
        params.foreach {
          case Empty => ()
          case p =>
            if (!initial) {
              sb.append(", ")
            }
            initial = false
            sb.append('\n').append(idt)
            p.write(sb)
        }
      }
    }
  }
}

object PointExpression {

  def unapply(point: Expression): Option[Expression] = point match {
    case FunctionInvocation(FunctionName(_, "point"), _, args, _, _) => Some(args.head)
    case parameter: ExplicitParameter                                => Some(parameter)
    case _                                                           => None
  }
}
