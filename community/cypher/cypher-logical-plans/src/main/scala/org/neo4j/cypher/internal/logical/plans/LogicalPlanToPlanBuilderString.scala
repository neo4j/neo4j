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
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByMode
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsDisjointByParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsRetryParameters
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier.Extension
import org.neo4j.cypher.internal.expressions.AllReduceAccumulator
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
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
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
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
import org.neo4j.cypher.internal.logical.plans.DynamicElement.SetOperator
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.Empty
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.EscapeableVariable
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.Value
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.call
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.chain
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.concat
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.conditional
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.convertableToParam
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.mapParam
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.multilineParams
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.optional
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.params
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.seqParam
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.setParam
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString.Param.spread
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.symbols.CTAny
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
      new Extension {
        override def apply(ctx: ExpressionStringifier)(expression: Expression): String =
          expressionStringifierExtension(expression)
      },
      alwaysParens = false,
      alwaysBacktick = false,
      preferSingleQuotes = true,
      sensitiveParamsAsParams = false,
      javaCompatible = true
    )

  private val queryExpressionStringifier =
    new QueryExpressionStringifier(expressionStringifier, Some(stringifyValueInIndexOperator))

  /**
   * Generates a string that plays nicely together with `AbstractLogicalPlanBuilder`.
   */
  def apply(logicalPlan: LogicalPlan): String = render(logicalPlan, None, None)

  def apply(logicalPlan: LogicalPlan, extra: LogicalPlan => String): String = render(logicalPlan, Some(extra), None)

  def apply(logicalPlan: LogicalPlan, extra: LogicalPlan => String, planPrefixDot: LogicalPlan => String): String =
    render(logicalPlan, Some(extra), Some(planPrefixDot))

  def applyWith(logicalPlan: LogicalPlan, customParam: LogicalPlan => Option[Param]): String =
    render(logicalPlan, None, None, customParam)

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
      // RuntimeConstant lives in cypher-runtime-util, which again depends on this module.
      // Therefore, we match on the class name to avoid a dependency cycle.
      case rc if rc.getClass.getName == "org.neo4j.cypher.internal.runtime.ast.RuntimeConstant" =>
        val v = rc.productElement(0).asInstanceOf[Expression]
        val inner = rc.productElement(1).asInstanceOf[Expression]
        s"RuntimeConstant(${expressionStringifier(v)}, ${expressionStringifier(inner)})"
      case e => e.asCanonicalStringVal
    }
  }

  private def stringifyValueInIndexOperator(expression: Expression): String = expression match {
    case _: ExplicitParameter => "???"
    case other                => expressionStringifier(other)
  }

  private def render(
    logicalPlan: LogicalPlan,
    extra: Option[LogicalPlan => String],
    planPrefixDot: Option[LogicalPlan => String],
    customParam: LogicalPlan => Option[Param] = _ => None
  ) = {
    def planRepresentation(plan: LogicalPlan): String = {
      val sb = new mutable.StringBuilder()
      sb ++= planPrefixDot.fold(".")(_.apply(plan))
      sb ++= pre(plan)
      sb += '('
      sb ++= customParam(plan).getOrElse(par(plan)).toString
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
      case NodeIndexSeek(
          _,
          _,
          _,
          RangeQueryExpression(
            PointDistanceSeekRangeWrapper(
              PointDistanceRange(CachedProperty(_, _, _, _, _, _), _, _)
            )
          ),
          _,
          _,
          _,
          _
        ) =>
        "cachedPropertyPointDistanceNodeIndexSeek"
      case NodeIndexSeek(_, _, _, RangeQueryExpression(PointDistanceSeekRangeWrapper(_)), _, _, _, _) =>
        "pointDistanceNodeIndexSeek"
      case NodeIndexSeek(_, _, _, RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(_)), _, _, _, _) =>
        "pointBoundingBoxNodeIndexSeek"
      case _: NodeIndexSeek             => "nodeIndexOperator"
      case _: RemoteNodeIndexSeek       => "remoteNodeIndexOperator"
      case _: RemoteNodeUniqueIndexSeek => "remoteNodeIndexOperator"
      case _: PartitionedNodeIndexSeek  => "partitionedNodeIndexOperator"
      case _: NodeUniqueIndexSeek       => "nodeIndexOperator"
      case _: NodeIndexContainsScan     => "nodeIndexOperator"
      case _: NodeIndexEndsWithScan     => "nodeIndexOperator"
      case _: MultiNodeIndexSeek        => "multiNodeIndexSeekOperator"
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
      case _: DynamicDirectedRelationshipTypeLookup           => "dynamicRelationshipTypeLookup"
      case _: DynamicUndirectedRelationshipTypeLookup         => "dynamicRelationshipTypeLookup"
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
      case _: DirectedRelationshipVectorIndexSearch           => "relationshipVectorIndexSearch"
      case _: UndirectedRelationshipVectorIndexSearch         => "relationshipVectorIndexSearch"
      case _: DirectedRelationshipFulltextIndexSearch         => "relationshipFulltextIndexSearch"
      case _: UndirectedRelationshipFulltextIndexSearch       => "relationshipFulltextIndexSearch"
      case RemoteBatchPropertiesWithPushdownOperators(_, _, NODE_TYPE, _, _, _, _, _, _, _) =>
        "remoteBatchPropertiesWithPushdownOperatorsOnNode"
      case RemoteBatchPropertiesWithPushdownOperators(
          _,
          _,
          RELATIONSHIP_TYPE,
          _,
          _,
          _,
          _,
          _,
          _,
          _
        ) =>
        "remoteBatchPropertiesWithPushdownOperatorsOnRelationship"
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
      case Projection(_, projectExpressions) => spread(projectVars(projectExpressions))
      case UnwindCollection(_, maybeVariable, expression) =>
        spread(projectVars(Map(maybeVariable.getOrElse(varFor("_")) -> expression)))
      case PartitionedUnwindCollection(_, variable, expression) =>
        spread(projectVars(Map(variable.getOrElse(varFor("_")) -> expression)))
      case AllNodesScan(idName, argumentIds)            => params(idName.escaped, spread(argumentIds.map(_.escaped)))
      case PartitionedAllNodesScan(idName, argumentIds) => params(idName.escaped, spread(argumentIds.map(_.escaped)))
      case Argument(argumentIds)                        => spread(argumentIds)
      case CacheProperties(_, properties)               => spread(properties)(_.quoted)
      case RemoteBatchProperties(_, properties)         => spread(properties)(_.quoted)
      case RemoteBatchPropertiesWithFilter(_, predicates, properties) =>
        concat(
          spread(properties)(_.quoted),
          ")(",
          spread(predicates)(_.quoted)
        )
      case remoteBatchPropertiesWithPushdownOperators: RemoteBatchPropertiesWithPushdownOperators => concat(
          params(
            "variable" -> remoteBatchPropertiesWithPushdownOperators.variable,
            "properties" -> spread(remoteBatchPropertiesWithPushdownOperators.properties)
          ),
          ")(",
          pushdownOperatorsString(remoteBatchPropertiesWithPushdownOperators)
        )

      case Create(_, commands) => spread(commands)
      case Merge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        params(createNodes, createRelationships, onMatch, onCreate, nodesToLock)

      case FusedMerge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        params(createNodes, createRelationships, onMatch, onCreate, nodesToLock)

      case MergeUniqueNode(node, label, props, seekExpressions, args, _, indexType, onMatch, onCreate) =>
        val combined = props.zip(seekExpressions).map {
          case (p, e) => Param.tupleArrow(p.propertyKeyToken, e.quoted)
        }
        params(
          node.escaped,
          label,
          combined,
          s"Seq(${setPropertiesParam(onMatch)})",
          s"Seq(${setPropertiesParam(onCreate)})",
          args.map(_.escaped),
          indexType
        )

      case MergeInto(_, rel, from, dir, relTYpe, to, onMatch, onCreate) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(Seq(relTYpe))
        val fromName = escapeIdentifier(from.name)
        val relName = escapeIdentifier(rel.name)
        val toName = escapeIdentifier(to.name)
        params(
          s"($fromName)$dirStrA[$relName$typeStr]$dirStrB($toName)".quoted,
          s"Seq(${setPropertiesParam(onMatch)})",
          s"Seq(${setPropertiesParam(onCreate)})"
        )

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
        val relName = rel.map(n => escapeIdentifier(n.name)).getOrElse("")
        val toName = to.map(n => escapeIdentifier(n.name)).getOrElse("")
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
          s"(${name(from)})$dirStrA[${name(relName)}$typeStr*$lenStr]$dirStrB(${name(to)})".quoted,
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
          s"(${name(from)})$dirStrA[${name(relName)}$typeStr*$lenStr]$dirStrB(${name(to)})".quoted,
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
          sameNodeMode,
          traversalPathMode
        ) =>
        val lenStr = length match {
          case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
          case SimplePatternLength        => ""
        }
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        params(
          s"(${name(from)})$dirStrA[${name(relName)}$typeStr$lenStr]$dirStrB(${name(to)})".quoted,
          "pathName" -> optional(maybePathName.map(_.some)),
          "all" -> conditional(!single)(true),
          "nodePredicates" -> nodePredicates,
          "relationshipPredicates" -> relationshipPredicates,
          "pathPredicates" -> conditional(pathPredicates.nonEmpty)(seqParam(pathPredicates)(_.quoted)),
          "withFallback" -> conditional(withFallBack)(true),
          "sameNodeMode" -> objectName(sameNodeMode),
          "traversalPathMode" -> objectName(traversalPathMode)
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
          s"(${name(from)})$dirStrA[$typeStr*$lenStr]$dirStrB(${name(to)})".quoted,
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
          s"(${name(from)})$dirStrA[$typeStr*$lenStr]$dirStrB(${name(to)})".quoted,
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
        params(idName.escaped, label, indexOrder, spread(argumentIds.map(_.escaped)))

      case DynamicLabelNodeLookup(
          idName,
          DynamicElement.Simple(expr, operator),
          argumentIds,
          propertyConstraints
        ) =>
        val props = propertyConstraints.view.mapValues(_.quoted).toMap
        params(
          idName.escaped,
          expr.quoted,
          operator,
          Param.conditional(props.nonEmpty)(props),
          spread(argumentIds.map(_.escaped))
        )

      case DynamicDirectedRelationshipTypeLookup(
          idName,
          start,
          typeExpr,
          end,
          argumentIds,
          indexOrder,
          propertyPredicates
        ) =>
        typeExpr match {
          case DynamicElement.Simple(expr, operator) =>
            val op = operator match {
              case DynamicElement.All => "all"
              case DynamicElement.Any => "any"
            }
            val relExpr = s"$$$op(${expressionStringifier(expr)})"

            val props = propertyPredicates.view.mapValues(_.quoted).toMap
            params(
              renderSimplePath(idName, start, Seq.empty, end),
              relExpr.quoted,
              indexOrder,
              props,
              argumentIds
            )
        }
      case DynamicUndirectedRelationshipTypeLookup(
          idName,
          start,
          typeExpr,
          end,
          argumentIds,
          indexOrder,
          propertyPredicates
        ) =>
        typeExpr match {
          case DynamicElement.Simple(expr, operator) =>
            val op = operator match {
              case DynamicElement.All => "all"
              case DynamicElement.Any => "any"
            }
            val relExpr = s"$$$op(${expressionStringifier(expr)})"

            val props = propertyPredicates.view.mapValues(_.quoted).toMap
            params(
              renderSimplePath(idName, start, Seq.empty, end, BOTH),
              relExpr.quoted,
              indexOrder,
              props,
              argumentIds
            )
        }
      case PartitionedNodeByLabelScan(idName, label, argumentIds) =>
        params(idName.escaped, label, spread(argumentIds.map(_.escaped)))
      case UnionNodeByLabelsScan(idName, labels, argumentIds, indexOrder) =>
        params(idName.escaped, labels, indexOrder, spread(argumentIds.map(_.escaped)))
      case PartitionedUnionNodeByLabelsScan(idName, labels, argumentIds) =>
        params(idName.escaped, labels, spread(argumentIds.map(_.escaped)))
      case IntersectionNodeByLabelsScan(idName, labels, argumentIds, indexOrder) =>
        params(idName.escaped, labels, indexOrder, spread(argumentIds.map(_.escaped)))
      case PartitionedIntersectionNodeByLabelsScan(idName, labels, argumentIds) =>
        params(idName.escaped, labels, spread(argumentIds.map(_.escaped)))
      case SubtractionNodeByLabelsScan(idName, ps, ns, argumentIds, indexOrder) =>
        params(idName.escaped, ps, ns, indexOrder, spread(argumentIds.map(_.escaped)))
      case PartitionedSubtractionNodeByLabelsScan(idName, ps, ns, argumentIds) =>
        params(idName.escaped, ps, ns, spread(argumentIds.map(_.escaped)))

      case DirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds, indexOrder) =>
        params(
          renderSimplePath(idName, start, types.map(_.name), end, OUTGOING),
          indexOrder,
          spread(argumentIds)
        )

      case UndirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds, indexOrder) =>
        params(
          renderSimplePath(idName, start, types.map(_.name), end, BOTH),
          indexOrder,
          spread(argumentIds)
        )

      case PartitionedDirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, types.map(_.name), end, OUTGOING),
          spread(argumentIds)
        )

      case PartitionedUndirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, types.map(_.name), end, BOTH),
          spread(argumentIds)
        )

      case LockNodes(_, nodesToLock) =>
        spread(nodesToLock)
      case Optional(_, protectedSymbols) =>
        spread(protectedSymbols)
      case OptionalExpand(_, from, dir, types, to, relName, _, predicate) =>
        params(
          renderSimplePath(relName, Some(from), types.map(_.name), to, dir),
          optional(predicate.map(_.quoted.some))
        )

      case ProcedureCall(
          _,
          ResolvedNonLocalCall(
            ProcedureSignature(procedureName, _, _, _, _, _, _, _, _, _, _, _),
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

        val invocation = call(procedureName.fullName, spread(callArguments))
        s"$invocation$yielding".quoted

      case ProduceResult(_, columns) if columns.exists(_.cachedProperties.nonEmpty) =>
        spread(columns)(using { col =>
            call("column", col.variable, spread(col.cachedProperties)(expressionStringifierExtension(_).quoted))
          })

      case ProduceResult(_, columns) =>
        spread(columns)(_.variable.escaped)

      case ProjectEndpoints(_, relName, start, startInScope, end, endInScope, types, direction, length) =>
        val (dirStrA, dirStrB) = arrows(direction)
        val typeStr = relTypeStr(types)
        val lenStr = length match {
          case SimplePatternLength        => ""
          case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
        }
        params(
          s"(${name(start)})$dirStrA[${name(relName)}$typeStr$lenStr]$dirStrB(${name(end)})".quoted,
          "startInScope" -> startInScope,
          "endInScope" -> endInScope
        )

      case ValueHashJoin(_, _, join)       => join.quoted
      case ValueMergeJoin(_, _, join)      => join.quoted
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
          expansionMode,
          accumulators
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
          expansionMode,
          accumulators
        )
      case RepeatAcyclic(
          _,
          _,
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          innerNodes,
          previouslyBoundNodes,
          previouslyBoundNodeGroups,
          groupRelationships,
          innerRelationships,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections,
          expansionMode,
          accumulators
        ) =>
        acyclicParameterString(
          repetition,
          start,
          end,
          innerStart,
          innerEnd,
          groupNodes,
          innerNodes,
          previouslyBoundNodes,
          previouslyBoundNodeGroups,
          groupRelationships,
          innerRelationships,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          reverseGroupVariableProjections,
          expansionMode,
          accumulators
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
          ExpandAll,
          Set.empty
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
          expansionMode,
          accumulators
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
          expansionMode,
          accumulators
        )

      case NodeByIdSeek(idName, ids, argumentIds) =>
        params(idName.escaped, argumentIds, ids)
      case NodeByElementIdSeek(idName, ids, argumentIds) =>
        params(idName.escaped, argumentIds, ids)
      case UndirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          s"(${name(leftNode)})-[${name(idName)}]-(${name(rightNode)})".quoted,
          argumentIds,
          ids
        )
      case UndirectedRelationshipByElementIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          s"(${name(leftNode)})-[${name(idName)}]-(${name(rightNode)})".quoted,
          argumentIds,
          ids
        )
      case DirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          renderSimplePath(idName, leftNode, Seq.empty, rightNode),
          argumentIds,
          ids
        )
      case DirectedRelationshipByElementIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        params(
          renderSimplePath(idName, leftNode, Seq.empty, rightNode),
          argumentIds,
          ids
        )
      case DirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end),
          spread(argumentIds)
        )
      case UndirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end, BOTH),
          spread(argumentIds)
        )
      case PartitionedDirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end, OUTGOING),
          spread(argumentIds)
        )
      case PartitionedUndirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end, BOTH),
          spread(argumentIds)
        )
      case DirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        params(
          renderSimplePath(idName, start, Seq(typ.name), end, OUTGOING),
          indexOrder,
          spread(argumentIds)
        )
      case UndirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        params(
          renderSimplePath(idName, start, Seq(typ.name), end, BOTH),
          indexOrder,
          spread(argumentIds)
        )
      case PartitionedDirectedRelationshipTypeScan(idName, start, typ, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, Seq(typ.name), end, OUTGOING),
          spread(argumentIds)
        )
      case PartitionedUndirectedRelationshipTypeScan(idName, start, typ, end, argumentIds) =>
        params(
          renderSimplePath(idName, start, Seq(typ.name), end, BOTH),
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
          arg.quoted,
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
          RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(
            CachedProperty(_, v, PropertyKeyName(propKeyName), _, _, _),
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
          s"cachedNodePropFromStore(${v.name.quoted}, ${propKeyName.quoted})",
          distance match {
            case SignedDecimalIntegerLiteral(x) => s"literalInt($x)"
            case DecimalDoubleLiteral(x)        => s"literalFloat($x)"
            case _                              => distance
          },
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
      case RemoteNodeIndexSeek(
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
        remoteNodeIndexOperator(
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
      case RemoteNodeUniqueIndexSeek(
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
        remoteNodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          indexOrder,
          paramExpr,
          unique = true,
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
      case NodeVectorIndexSearch(
          idName,
          labelTokens,
          properties,
          score,
          indexName,
          vector,
          limit,
          entityFilter,
          maybePropertyFilter,
          argumentIds
        ) =>
        params(
          idName,
          seqParam(labelTokens.map(_.name.quoted)),
          seqParam(properties.map(_.propertyKeyToken.name.quoted)),
          indexName.quoted,
          vector.quoted,
          limit.quoted,
          score.map(_.name.quoted).getOrElse("".quoted),
          argumentIds,
          mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
          entityFilter,
          maybePropertyFilter
        )

      case DirectedRelationshipVectorIndexSearch(
          idName,
          start,
          end,
          labelTokens,
          properties,
          score,
          indexName,
          vector,
          limit,
          entityFilter,
          maybePropertyFilter,
          argumentIds
        ) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end),
          seqParam(labelTokens.map(_.name.quoted)),
          seqParam(properties.map(_.propertyKeyToken.name.quoted)),
          indexName.quoted,
          vector.quoted,
          limit.quoted,
          score.map(_.name.quoted).getOrElse("".quoted),
          argumentIds,
          mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
          entityFilter,
          maybePropertyFilter
        )

      case UndirectedRelationshipVectorIndexSearch(
          idName,
          start,
          end,
          labelTokens,
          properties,
          score,
          indexName,
          vector,
          limit,
          entityFilter,
          maybePropertyFilter,
          argumentIds
        ) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end, BOTH),
          seqParam(labelTokens.map(_.name.quoted)),
          seqParam(properties.map(_.propertyKeyToken.name.quoted)),
          indexName.quoted,
          vector.quoted,
          limit.quoted,
          score.map(_.name.quoted).getOrElse("".quoted),
          argumentIds,
          mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
          entityFilter,
          maybePropertyFilter
        )

      case NodeFulltextIndexSearch(
          idName,
          labelTokens,
          properties,
          score,
          indexName,
          queryString,
          analyzer,
          skip,
          limit,
          argumentIds
        ) =>
        params(
          idName,
          seqParam(labelTokens.map(_.name.quoted)),
          seqParam(properties.map(_.propertyKeyToken.name.quoted)),
          indexName.quoted,
          queryString.quoted,
          limit.quoted,
          analyzer.map(_.quoted.some).getOrElse(Param("None")),
          skip.map(_.quoted.some).getOrElse(Param("None")),
          score.map(_.name.quoted).getOrElse("".quoted),
          argumentIds,
          mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex)
        )

      case DirectedRelationshipFulltextIndexSearch(
          idName,
          start,
          end,
          labelTokens,
          properties,
          score,
          indexName,
          queryString,
          limit,
          analyzer,
          skip,
          argumentIds
        ) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end),
          seqParam(labelTokens.map(_.name.quoted)),
          seqParam(properties.map(_.propertyKeyToken.name.quoted)),
          indexName.quoted,
          queryString.quoted,
          limit.quoted,
          analyzer.map(_.quoted.some).getOrElse(Param("None")),
          skip.map(_.quoted.some).getOrElse(Param("None")),
          score.map(_.name.quoted).getOrElse("".quoted),
          argumentIds,
          mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex)
        )

      case UndirectedRelationshipFulltextIndexSearch(
          idName,
          start,
          end,
          labelTokens,
          properties,
          score,
          indexName,
          queryString,
          limit,
          analyzer,
          skip,
          argumentIds
        ) =>
        params(
          renderSimplePath(idName, start, Seq.empty, end, BOTH),
          seqParam(labelTokens.map(_.name.quoted)),
          seqParam(properties.map(_.propertyKeyToken.name.quoted)),
          indexName.quoted,
          queryString.quoted,
          limit.quoted,
          analyzer.map(_.quoted.some).getOrElse(Param("None")),
          skip.map(_.quoted.some).getOrElse(Param("None")),
          score.map(_.name.quoted).getOrElse("".quoted),
          argumentIds,
          mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex)
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
      case TransactionForeach(
          _,
          _,
          batchSize,
          concurrency,
          onErrorBehaviour,
          maybeReportAs,
          maybeRetryParameters,
          maybeDisjointByParameters,
          effectiveDisjointBy
        ) =>
        callInTxParams(
          batchSize,
          concurrency,
          onErrorBehaviour,
          maybeReportAs,
          maybeRetryParameters,
          maybeDisjointByParameters,
          effectiveDisjointBy
        )
      case TransactionApply(
          _,
          _,
          batchSize,
          concurrency,
          onErrorBehaviour,
          maybeReportAs,
          maybeRetryParameters,
          maybeDisjointByParameters,
          effectiveDisjointBy
        ) =>
        callInTxParams(
          batchSize,
          concurrency,
          onErrorBehaviour,
          maybeReportAs,
          maybeRetryParameters,
          maybeDisjointByParameters,
          effectiveDisjointBy
        )
      case RunQueryAt(_, query, graphReference, parameters, importsAsParameters, columns) =>
        params(
          "query" -> StringEscapeUtils.escapeJava(query).quoted,
          "graphReference" -> graphReference.print.quoted,
          "parameters" -> conditional(parameters.nonEmpty)(parameters),
          "importsAsParameters" -> conditional(importsAsParameters.nonEmpty)(importsAsParameters),
          "columns" -> conditional(columns.nonEmpty)(columns)
        )

      case SimulatedNodeScan(idName, numberOfRows) =>
        params(idName.escaped, numberOfRows)
      case SimulatedExpand(_, from, rel, to, factor) =>
        params(from, rel, to, factor)
      case SimulatedSelection(_, selectivity) =>
        s"$selectivity"
      case MultiNodeIndexSeek(indexSeekLeafPlans: Seq[NodeIndexSeekLeafPlan]) =>
        indexSeekLeafPlans.map(p => s"_.nodeIndexSeek(${par(p)})").mkString(", ")
      case _ => ""
    }

  private def renderSimplePath(
    idName: Option[LogicalVariable],
    start: Option[LogicalVariable],
    relType: Seq[String],
    end: Option[LogicalVariable],
    direction: SemanticDirection = OUTGOING
  ): Param = {
    val (dirA, dirB) = arrows(direction)
    val relTypes = relType match {
      case Seq()    => ""
      case typeList => typeList.mkString(":", "|", "")
    }

    s"(${name(start)})$dirA[${name(idName)}$relTypes]$dirB(${name(end)})".quoted
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
    val patternString = transition match {
      case NodeJuxtapositionTransition(endId) =>
        val to = nfa.states(endId)
        val whereString =
          to.variablePredicate.map(vp =>
            s" WHERE ${expressionStringifier(vp.predicate)}"
          ).getOrElse("")
        s""" "(${escapeIdentifier(from.variable.name)}) (${escapeIdentifier(to.variable.name)}$whereString)" """.trim
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
        s""" "(${escapeIdentifier(from.variable.name)})$dirStrA[${escapeIdentifier(
            relName.name
          )}$typeStr$relWhereString]$dirStrB(${escapeIdentifier(to.variable.name)}$nodeWhereString)" """.trim
    }

    s"$indent$indent.addTransition(${from.id}, ${transition.endId}, $patternString)"
  }

  private def acyclicParameterString(
    repetition: Repetition,
    start: LogicalVariable,
    end: LogicalVariable,
    innerStart: LogicalVariable,
    innerEnd: LogicalVariable,
    groupNodes: Set[VariableGrouping],
    innerNodes: Set[LogicalVariable],
    previouslyBoundNodes: Set[LogicalVariable],
    previouslyBoundNodeGroups: Set[LogicalVariable],
    groupRelationships: Set[VariableGrouping],
    innerRelationships: Set[LogicalVariable],
    previouslyBoundRelationships: Set[LogicalVariable],
    previouslyBoundRelationshipGroups: Set[LogicalVariable],
    reverseGroupVariableProjections: Boolean,
    expansionMode: ExpansionMode,
    accumulators: Set[AllReduceAccumulator]
  ) =
    call(
      "AcyclicParameters",
      repetition.min,
      repetition.max.toString,
      start,
      end,
      innerStart,
      innerEnd,
      groupNodes,
      innerNodes,
      previouslyBoundNodes,
      previouslyBoundNodeGroups,
      groupRelationships,
      innerRelationships,
      previouslyBoundRelationships,
      previouslyBoundRelationshipGroups,
      reverseGroupVariableProjections,
      expansionMode,
      accumulators
    )

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
    expansionMode: ExpansionMode,
    accumulators: Set[AllReduceAccumulator]
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
      expansionMode,
      accumulators
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
    expansionMode: ExpansionMode,
    accumulators: Set[AllReduceAccumulator]
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
      expansionMode,
      accumulators
    )

  private def pushdownOperatorsString(
    remoteBatchPropertiesWithPushdownOperators: RemoteBatchPropertiesWithPushdownOperators
  ): Param = {
    chain(
      "PushdownOperators",
      conditional(remoteBatchPropertiesWithPushdownOperators.limit.nonEmpty)(call(
        "limit",
        spread(remoteBatchPropertiesWithPushdownOperators.limit.map(_.quoted))
      )),
      conditional(remoteBatchPropertiesWithPushdownOperators.orderBy.nonEmpty)(call(
        "orderBy",
        spread(remoteBatchPropertiesWithPushdownOperators.orderBy.map(order =>
          propertyKeyNameOrderString(remoteBatchPropertiesWithPushdownOperators.variable, order)
        ))
      )),
      conditional(remoteBatchPropertiesWithPushdownOperators.distinctBy.nonEmpty)(call(
        "distinct",
        spread(remoteBatchPropertiesWithPushdownOperators.distinctBy.map(_.quoted))
      )),
      conditional(remoteBatchPropertiesWithPushdownOperators.predicates.nonEmpty)(call(
        "filter",
        spread(remoteBatchPropertiesWithPushdownOperators.predicates.map(_.quoted))
      )),
      conditional(remoteBatchPropertiesWithPushdownOperators.importedConstantValues.nonEmpty)(call(
        "importedConstantValues",
        spread(remoteBatchPropertiesWithPushdownOperators.importedConstantValues.map(_.quoted))
      )),
      conditional(remoteBatchPropertiesWithPushdownOperators.importedPerRowValues.nonEmpty)(
        call(
          "importedPerRowValues",
          mapParam(remoteBatchPropertiesWithPushdownOperators.importedPerRowValues)(_._1, _._2.quoted)
        )
      )
    )
  }

  private def propertyKeyNameOrderString(variable: LogicalVariable, propertyKeyNameOrder: PropertyKeyNameOrder): Param =
    Param(_
      .append(escapeIdentifier(variable.name))
      .append('.')
      .append(escapeIdentifier(propertyKeyNameOrder.propertyKeyName.name))
      .append(' ')
      .append(propertyKeyNameOrder.order match {
        case PropertyKeyNameOrder.Ascending  => "ASC"
        case PropertyKeyNameOrder.Descending => "DESC"
      })).quoted

  private def setPropertiesParam(items: Seq[(PropertyKeyName, Expression)]): Param =
    spread(
      items.map {
        case (p, e) => Param.tuple(p, e.quoted)
      }
    )

  private def queryExpressionStr(valueExpr: QueryExpression[Expression], propNames: Seq[String]): String =
    queryExpressionStringifier(valueExpr, propNames)

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
      case CTAny           => "CTAny"
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

  private def name(variable: LogicalVariable): String = escapeIdentifier(variable.name)

  private def name(variable: Option[LogicalVariable]): String =
    variable.map(v => escapeIdentifier(v.name)).getOrElse("")

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
      s"${name(idName)}:${labelToken.name}($parenthesesContent)".quoted,
      "indexOrder" -> indexOrder,
      "paramExpr" -> paramExpr,
      "argumentIds" -> argumentIds,
      "getValue" -> Param.mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
      "unique" -> unique,
      "indexType" -> indexType,
      "supportPartitionedScan" -> supportPartitionedScan
    )

  private def remoteNodeIndexOperator(
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
      s"${name(idName)}:${labelToken.name}($parenthesesContent)".quoted,
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
      s"${name(idName)}:${labelToken.name}($parenthesesContent)".quoted,
      "argumentIds" -> argumentIds,
      "getValue" -> Param.mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
      "indexType" -> indexType
    )

  private def relationshipIndexOperator(
    idName: Option[LogicalVariable],
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
      s"(${name(start)})-[${name(idName)}:${typeToken.name}($parenthesesContent)]$rarrow(${name(end)})".quoted,
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
    idName: Option[LogicalVariable],
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
      s"(${name(start)})-[${name(idName)}:${typeToken.name}($parenthesesContent)]$rarrow(${name(end)})".quoted,
      "argumentIds" -> argumentIds,
      "getValue" -> Param.mapParam(properties)(_.propertyKeyToken, _.getValueFromIndex),
      "indexType" -> indexType
    )
  }

  private def pointDistanceNodeIndexSeek(
    idName: LogicalVariable,
    labelToken: LabelToken,
    properties: Seq[IndexedProperty],
    point: Param,
    distance: Param,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    inclusive: Boolean,
    indexType: IndexType
  ) =
    params(
      idName,
      labelToken,
      properties.head.propertyKeyToken,
      point,
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
    idName: Option[LogicalVariable],
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
      s"(${name(start)})-[${name(idName)}:${typeToken.name}($propName)]$rarrow(${name(end)})".quoted,
      lowerLeft.quoted,
      upperRight.quoted,
      "indexOrder" -> indexOrder,
      "argumentIds" -> argumentIds,
      "getValue" -> getSingleIndexBehavior(properties),
      "indexType" -> indexType
    )
  }

  private def pointDistanceRelationshipIndexSeek(
    idName: Option[LogicalVariable],
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
      s"(${name(start)})-[${name(idName)}:${typeToken.name}($propName)]$rarrow(${name(end)})".quoted,
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
      case _                              => "/* " + expressionStringifier(count) + "*/"
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
      s"${objectName(EagernessReason.LabelReadSetConflict)}(labelName(${wrapInQuotations(label.name)}))"
    case EagernessReason.TypeReadSetConflict(relType) =>
      s"${objectName(EagernessReason.TypeReadSetConflict)}(relTypeName(${wrapInQuotations(relType.name)}))"
    case EagernessReason.LabelReadRemoveConflict(label) =>
      s"${objectName(EagernessReason.LabelReadRemoveConflict)}(labelName(${wrapInQuotations(label.name)}))"
    case EagernessReason.UnknownLabelReadSetConflict =>
      s"${objectName(EagernessReason.UnknownLabelReadSetConflict)}"
    case EagernessReason.UnknownLabelReadRemoveConflict =>
      s"${objectName(EagernessReason.UnknownLabelReadRemoveConflict)}"
    case EagernessReason.ReadDeleteConflict(identifier) =>
      s"${objectName(EagernessReason.ReadDeleteConflict)}(${wrapInQuotations(identifier)})"
    case EagernessReason.ReadCreateConflict =>
      s"${objectName(EagernessReason.ReadCreateConflict)}"
    case EagernessReason.PropertyReadSetConflict(property) =>
      s"${objectName(EagernessReason.PropertyReadSetConflict)}(propName(${wrapInQuotations(property.name)}))"
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

  /**
   * @see Stringifier.backtick
   */
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
    maybeRetryParams: Option[InTransactionsRetryParameters],
    maybeDisjointByParameters: Option[InTransactionsDisjointByParameters],
    effectiveDisjointBy: Seq[Expression]
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
      }),
      "maybeDisjointByParameters" -> (maybeDisjointByParameters match {
        case Some(InTransactionsDisjointByParameters(mode)) =>
          val disjointByString = mode match {
            case InTransactionsDisjointByMode.DisjointByAuto => "auto"
            case InTransactionsDisjointByMode.DisjointByNone => "none"
            case InTransactionsDisjointByMode.DisjointByExpressions(expressions) =>
              expressions.map(e => expressionStringifier(e)).mkString("(", ", ", ")")
          }
          Param(disjointByString).quoted.some
        case None =>
          Param("None")
      }),
      "effectiveDisjointBy" -> effectiveDisjointBy.map(_.quoted)
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

    /**
     * This is the inverse of [[QueryExpressionConstructionTestSupport]]
     */
    implicit def fromQueryExpression: ToParam[QueryExpression[Expression]] = {
      case RangeQueryExpression(InequalitySeekRangeWrapper(RangeBetween(gt, lt))) =>
        call(
          "between",
          gt.asInstanceOf[InequalitySeekRange[Expression]],
          lt.asInstanceOf[InequalitySeekRange[Expression]]
        )
      case RangeQueryExpression(InequalitySeekRangeWrapper(range)) =>
        call("rangeExpression", range)
      case SingleQueryExpression(expression) =>
        call("single", expression)
      case queryExpression: QueryExpression[Expression] =>
        // Ideally, we should not get here, but as QueryExpression is an extensive trait, there is this fallback
        queryExpression.toString
    }

    implicit def fromEntityFilterQueryExpression: ToParam[EntityFilterQueryExpression[Expression]] = {
      case MatchAllQueryExpression => call("matchAll")
      case MatchEntitySetQueryExpression(expression) =>
        call("matchEntities", expression)
    }

    implicit def fromInequalitySeekRange: ToParam[InequalitySeekRange[Expression]] = {
      case RangeGreaterThan(NonEmptyList(ExclusiveBound(expression))) =>
        call("gt", expression)
      case RangeGreaterThan(NonEmptyList(InclusiveBound(expression))) =>
        call("gte", expression)
      case RangeLessThan(NonEmptyList(ExclusiveBound(expression))) =>
        call("lt", expression)
      case RangeLessThan(NonEmptyList(InclusiveBound(expression))) =>
        call("lte", expression)
      case other =>
        // Ideally, we should not get here, but as QueryExpression is an extensive trait, there is this fallback
        other.toString
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
      call("Map", spread(map)(using { case (k, v) => Param.tuple(k, v) }))

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
    implicit def fromVariable: ToParam[LogicalVariable] = _.name.quoted
    implicit def fromLabelName: ToParam[LabelName] = _.name.quoted
    implicit def fromLabelToken: ToParam[LabelToken] = _.name.quoted
    implicit def fromPropertyKeyName: ToParam[PropertyKeyName] = _.name.quoted
    implicit def fromPropertyKeyToken: ToParam[PropertyKeyToken] = _.name.quoted
    implicit def fromRelTypeName: ToParam[RelTypeName] = _.name.quoted
    implicit def fromRelTypeToken: ToParam[RelationshipTypeToken] = _.name.quoted

    implicit def fromSetOperator: ToParam[SetOperator] = _.name

    implicit def fromIndexOrder: ToParam[IndexOrder] = str(objectName)
    implicit def fromTraversalPathMode: ToParam[TraversalPathMode] = str(objectName)
    implicit def fromGetValueFromIndexBehavior: ToParam[GetValueFromIndexBehavior] = str(objectName)

    implicit def fromParameter: ToParam[Parameter] = _.asCanonicalStringVal.quoted

    implicit def fromVariablePredicate: ToParam[VariablePredicate] =
      vp => call("Predicate", vp.variable, vp.predicate.quoted)

    implicit def fromVariableGroupingSet: ToParam[Set[VariableGrouping]] =
      setParam(_)(x => Param.tuple(x.singleton, x.group))

    implicit def fromAllReduceAccumulator: ToParam[AllReduceAccumulator] =
      x => params(x.initial.quoted, x.previous, x.next).wrap("(", ")")

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

    /**
     * Most of the time, we do not want to escape single variables. (see `fromVariable`)
     * This method is for when we do.
     */
    implicit class EscapeableVariable(inner: LogicalVariable) {

      def escaped: Param =
        escapeIdentifier(inner.name).quoted
    }

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

    def optionalSetParam[A](set: Iterable[A])(implicit toParam: ToParam[A]): Param =
      if (set.isEmpty) Empty else setParam(set)

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

    /** Writes a function chain, for a builder new Builder().withA().withB().build()*/
    def chain(name: String, params: Param*): Param = Param { sb =>
      sb.append(s"$name()")
      params.foreach {
        case Empty => ()
        case param =>
          sb.append(s"\n$indent.")
          param.write(sb)
      }
    }

    /** Writes a tuple of two parameters with arrow syntax a -> b */
    def tupleArrow(a: Param, b: Param): Param =
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
    case FunctionInvocation(FunctionName(_, "point"), _, args, _, _, _, _) => Some(args.head)
    case parameter: ExplicitParameter                                      => Some(parameter)
    case _                                                                 => None
  }
}
