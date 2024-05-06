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
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NumberLiteral
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
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.graphdb.schema.IndexType

import scala.collection.mutable

object LogicalPlanToPlanBuilderString {
  private val expressionStringifier = ExpressionStringifier(expressionStringifierExtension, preferSingleQuotes = true)

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

  def expressionStringifierExtension(expression: Expression): String = {
    expression match {
      case p @ CachedHasProperty(_, _, _, NODE_TYPE, false)         => s"cacheNHasProperty[${p.propertyAccessString}]"
      case p @ CachedHasProperty(_, _, _, RELATIONSHIP_TYPE, false) => s"cacheRHasProperty[${p.propertyAccessString}]"
      case p @ CachedHasProperty(_, _, _, NODE_TYPE, true) => s"cacheNHasPropertyFromStore[${p.propertyAccessString}]"
      case p @ CachedHasProperty(_, _, _, RELATIONSHIP_TYPE, true) =>
        s"cacheRHasPropertyFromStore[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, NODE_TYPE, false)         => s"cacheN[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, RELATIONSHIP_TYPE, false) => s"cacheR[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, NODE_TYPE, true)          => s"cacheNFromStore[${p.propertyAccessString}]"
      case p @ CachedProperty(_, _, _, RELATIONSHIP_TYPE, true)  => s"cacheRFromStore[${p.propertyAccessString}]"
      case e                                                     => e.asCanonicalStringVal
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
      sb ++= par(plan)
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
  private def par(logicalPlan: LogicalPlan): String = {
    val plansWithContent: PartialFunction[LogicalPlan, String] = {
      case Aggregation(_, groupingExpressions, aggregationExpression) =>
        s"Seq(${projectVars(groupingExpressions)}), Seq(${projectVars(aggregationExpression)})"
      case OrderedAggregation(_, groupingExpressions, aggregationExpression, orderToLeverage) =>
        s"Seq(${projectVars(groupingExpressions)}), Seq(${projectVars(aggregationExpression)}), Seq(${wrapInQuotationsAndMkString(orderToLeverage.map(expressionStringifier(_)))})"
      case Distinct(_, groupingExpressions) =>
        projectVars(groupingExpressions)
      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        s""" Seq(${wrapInQuotationsAndMkString(orderToLeverage.map(expressionStringifier(_)))}), ${projectVars(
            groupingExpressions
          )} """.trim
      case Projection(_, projectExpressions) => projectVars(projectExpressions)
      case UnwindCollection(_, variable, expression) =>
        projectVars(Map(variable -> expression))
      case PartitionedUnwindCollection(_, variable, expression) =>
        projectVars(Map(variable -> expression))
      case AllNodesScan(idName, argumentIds) =>
        wrapVarsInQuotationsAndMkString(idName +: argumentIds.toSeq)
      case PartitionedAllNodesScan(idName, argumentIds) =>
        wrapVarsInQuotationsAndMkString(idName +: argumentIds.toSeq)
      case Argument(argumentIds) =>
        wrapVarsInQuotationsAndMkString(argumentIds.toSeq)
      case CacheProperties(_, properties) =>
        wrapInQuotationsAndMkString(properties.toSeq.map(expressionStringifier(_)))
      case RemoteBatchProperties(_, properties) =>
        wrapInQuotationsAndMkString(properties.toSeq.map(expressionStringifier(_)))
      case Create(_, commands) =>
        commands.map(createCreateCommandToString).mkString(", ")
      case Merge(_, createNodes, createRelationships, onMatch, onCreate, nodesToLock) =>
        val nodesToCreate = createNodes.map(createNodeToString)
        val relsToCreate = createRelationships.map(createRelationshipToString)

        val onMatchString = onMatch.map(mutationToString)
        val onCreateString = onCreate.map(mutationToString)

        s"Seq(${nodesToCreate.mkString(", ")}), Seq(${relsToCreate.mkString(", ")}), Seq(${onMatchString.mkString(
            ", "
          )}), Seq(${onCreateString.mkString(", ")}), Set(${wrapVarsInQuotationsAndMkString(nodesToLock)})"

      case Foreach(_, variable, list, mutations) =>
        s"${wrapInQuotations(variable)}, ${wrapInQuotations(expressionStringifier(list))}, Seq(${mutations.map(mutationToString).mkString(", ")})"

      case Expand(_, from, dir, types, to, rel, _) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val fromName = escapeIdentifier(from.name)
        val relName = escapeIdentifier(rel.name)
        val toName = escapeIdentifier(to.name)
        s""" "($fromName)$dirStrA[$relName$typeStr]$dirStrB($toName)" """.trim
      case VarExpand(_, from, dir, pDir, types, to, relName, length, mode, nodePredicates, relationshipPredicates) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"${length.min}..${length.max.getOrElse("")}"
        val modeStr = s", expandMode = ${objectName(mode)}"
        val pDirStr = s", projectedDir = ${objectName(pDir)}"
        val nPredStr = variablePredicates(nodePredicates, "nodePredicates")
        val rPredStr = variablePredicates(relationshipPredicates, "relationshipPredicates")
        s""" "(${from.name})$dirStrA[${relName.name}$typeStr*$lenStr]$dirStrB(${to.name})"$modeStr$pDirStr$nPredStr$rPredStr """.trim

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
        val pDirStr = s", projectedDir = ${objectName(projectedDir)}"
        val nPredStr = variablePredicates(nodePredicates, "nodePredicates")
        val rPredStr = variablePredicates(relationshipPredicates, "relationshipPredicates")
        s""" "(${from.name})$dirStrA[${relName.name}$typeStr*$lenStr]$dirStrB(${to.name})"$pDirStr$nPredStr$rPredStr """.trim

      case FindShortestPaths(
          _,
          shortestPath,
          nodePredicates,
          relationshipPredicates,
          pathPredicates,
          withFallBack,
          sameNodeMode
        ) =>
        val fbStr = if (withFallBack) ", withFallback = true" else ""
        val sameNodeStr = ", sameNodeMode = " + objectName(sameNodeMode)
        shortestPath match {
          case ShortestRelationshipPattern(
              maybePathName,
              PatternRelationship(relName, (from, to), dir, types, length),
              single
            ) =>
            val lenStr = length match {
              case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
              case SimplePatternLength        => ""
            }
            val (dirStrA, dirStrB) = arrows(dir)
            val typeStr = relTypeStr(types)
            val pNameStr = maybePathName.map(p => s", pathName = Some(${wrapInQuotations(p)})").getOrElse("")
            val allStr = if (single) "" else ", all = true"
            val nPredStr = variablePredicates(nodePredicates, "nodePredicates")
            val rPredStr = variablePredicates(relationshipPredicates, "relationshipPredicates")
            val pPredStr =
              if (pathPredicates.isEmpty) ""
              else ", pathPredicates = Seq(" + wrapInQuotationsAndMkString(
                pathPredicates.map(expressionStringifier(_))
              ) + ")"
            s""" "(${from.name})$dirStrA[${relName.name}$typeStr$lenStr]$dirStrB(${to.name})"$pNameStr$allStr$nPredStr$rPredStr$pPredStr$fbStr$sameNodeStr """.trim
        }

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
          lengthBounds
        ) =>
        Seq(
          wrapInQuotations(from),
          wrapInQuotations(to),
          wrapInQuotations(solvedExpressionString),
          nonInlinedPreFilters.map(e => wrapInQuotations(expressionStringifier(e))),
          s"Set(${groupEntitiesString(nodeVariableGroupings)})",
          s"Set(${groupEntitiesString(relationshipVariableGroupings)})",
          s"Set(${mappedEntitiesString(singletonNodeVariables)})",
          s"Set(${mappedEntitiesString(singletonRelationshipVariables)})",
          objectName(StatefulShortestPath) + "." + objectName(StatefulShortestPath.Selector) + "." + selector.toString,
          nfaString(nfa),
          mode.toString,
          reverseGroupVariableProjections.toString,
          lengthBounds.min.toString,
          lengthBounds.max.toString
        ).mkString(s"\n${indent}", s",\n${indent}", "")
      case PruningVarExpand(_, from, dir, types, to, minLength, maxLength, nodePredicates, relationshipPredicates) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"$minLength..$maxLength"
        val nPredStr = variablePredicates(nodePredicates, "nodePredicates")
        val rPredStr = variablePredicates(relationshipPredicates, "relationshipPredicates")
        s""" "(${from.name})$dirStrA[$typeStr*$lenStr]$dirStrB(${to.name})"$nPredStr$rPredStr """.trim
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
          relationshipPredicates
        ) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val minLength = if (includeStartNode) 0 else 1
        val lenStr = s"$minLength..$maxLength"
        val nPredStr = variablePredicates(nodePredicates, "nodePredicates")
        val rPredStr = variablePredicates(relationshipPredicates, "relationshipPredicates")
        val depthNameStr = depthName.map(d => s""", depthName = Some("${d.name}")""").getOrElse("")
        val modeStr = s", mode = $mode"
        s""" "(${from.name})$dirStrA[$typeStr*$lenStr]$dirStrB(${to.name})"$nPredStr$rPredStr$depthNameStr$modeStr """.trim
      case Limit(_, count) =>
        integerString(count)
      case ExhaustiveLimit(_, count) =>
        integerString(count)
      case Skip(_, count) =>
        integerString(count)
      case NodeByLabelScan(idName, label, argumentIds, indexOrder) =>
        val args = Seq(escapeIdentifier(idName.name), label.name).map(wrapInQuotations) ++
          Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        args.mkString(", ")
      case PartitionedNodeByLabelScan(idName, label, argumentIds) =>
        val args = Seq(escapeIdentifier(idName.name), label.name).map(wrapInQuotations) ++
          argumentIds.map(wrapInQuotations)
        args.mkString(", ")
      case UnionNodeByLabelsScan(idName, labels, argumentIds, indexOrder) =>
        val labelNames = labels.map(l => wrapInQuotations(l.name)).mkString(", ")
        val args = Seq(wrapInQuotations(idName), s"Seq($labelNames)") ++ Seq(objectName(indexOrder)) ++
          argumentIds.map(wrapInQuotations)
        args.mkString(", ")
      case PartitionedUnionNodeByLabelsScan(idName, labels, argumentIds) =>
        val labelNames = labels.map(l => wrapInQuotations(l.name)).mkString(", ")
        val args = Seq(wrapInQuotations(idName), s"Seq($labelNames)") ++ argumentIds.map(wrapInQuotations)
        args.mkString(", ")

      case IntersectionNodeByLabelsScan(idName, labels, argumentIds, indexOrder) =>
        val labelNames = labels.map(l => wrapInQuotations(l.name)).mkString(", ")
        val args = Seq(wrapInQuotations(idName), s"Seq($labelNames)") ++ Seq(objectName(indexOrder)) ++
          argumentIds.map(wrapInQuotations)
        args.mkString(", ")

      case PartitionedIntersectionNodeByLabelsScan(idName, labels, argumentIds) =>
        val labelNames = labels.map(l => wrapInQuotations(l.name)).mkString(", ")
        val args = Seq(wrapInQuotations(idName), s"Seq($labelNames)") ++ argumentIds.map(wrapInQuotations)
        args.mkString(", ")

      case SubtractionNodeByLabelsScan(idName, ps, ns, argumentIds, indexOrder) =>
        val positiveLabels = ps.map(l => wrapInQuotations(l.name)).mkString(", ")
        val negativeLabels = ns.map(l => wrapInQuotations(l.name)).mkString(", ")
        val args =
          Seq(wrapInQuotations(idName), s"Seq($positiveLabels)", s"Seq($negativeLabels)") ++ Seq(
            objectName(indexOrder)
          ) ++
            argumentIds.map(wrapInQuotations)
        args.mkString(", ")

      case PartitionedSubtractionNodeByLabelsScan(idName, ps, ns, argumentIds) =>
        val positiveLabels = ps.map(l => wrapInQuotations(l.name)).mkString(", ")
        val negativeLabels = ns.map(l => wrapInQuotations(l.name)).mkString(", ")
        val args =
          Seq(wrapInQuotations(idName), s"Seq($positiveLabels)", s"Seq($negativeLabels)") ++
            argumentIds.map(wrapInQuotations)
        args.mkString(", ")

      case DirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds, indexOrder) =>
        val typeNames = types.map(l => l.name).mkString("|")
        val args = Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        s""" "(${start.name})-[${idName.name}:$typeNames]->(${end.name})", ${args.mkString(", ")} """.trim

      case UndirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds, indexOrder) =>
        val typeNames = types.map(l => l.name).mkString("|")
        val args = Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        s""" "(${start.name})-[${idName.name}:$typeNames]-(${end.name})", ${args.mkString(", ")} """.trim

      case PartitionedDirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds) =>
        val typeNames = types.map(l => l.name).mkString("|")
        val args = if (argumentIds.isEmpty) "" else argumentIds.map(wrapInQuotations).mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}:$typeNames]->(${end.name})"$args """.trim

      case PartitionedUndirectedUnionRelationshipTypesScan(idName, start, types, end, argumentIds) =>
        val typeNames = types.map(l => l.name).mkString("|")
        val args = if (argumentIds.isEmpty) "" else argumentIds.map(wrapInQuotations).mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}:$typeNames]-(${end.name})"$args """.trim

      case Optional(_, protectedSymbols) =>
        wrapVarsInQuotationsAndMkString(protectedSymbols)
      case OptionalExpand(_, from, dir, types, to, relName, _, predicate) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val predStr = predicate.fold("")(p => s""", Some("${expressionStringifier(p)}")""")
        s""" "(${from.name})$dirStrA[${relName.name}$typeStr]$dirStrB(${to.name})"$predStr""".trim
      case ProcedureCall(
          _,
          ResolvedCall(
            ProcedureSignature(QualifiedName(namespace, name), _, _, _, _, _, _, _, _, _, _, _),
            callArguments,
            callResults,
            _,
            _,
            yieldAll
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
        s""" "${namespace.mkString(".")}.$name(${callArguments.map(expressionStringifier(_)).mkString(
            ", "
          )})$yielding" """.trim
      case ProduceResult(_, columns) if columns.exists(_.cachedProperties.nonEmpty) =>
        columns.map(c =>
          s"column(${wrapInQuotations(escapeIdentifier(c.variable.name))}, ${c.cachedProperties.map(cp => wrapInQuotations(expressionStringifierExtension(cp))).mkString(", ")})"
        ).mkString(", ")

      case ProduceResult(_, columns) =>
        wrapInQuotationsAndMkString(columns.map(c => escapeIdentifier(c.variable.name)))

      case ProjectEndpoints(_, relName, start, startInScope, end, endInScope, types, direction, length) =>
        val (dirStrA, dirStrB) = arrows(direction)
        val typeStr = relTypeStr(types)
        val lenStr = length match {
          case SimplePatternLength        => ""
          case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
        }
        s""" "(${start.name})$dirStrA[${relName.name}$typeStr$lenStr]$dirStrB(${end.name})", startInScope = $startInScope, endInScope = $endInScope """.trim
      case ValueHashJoin(_, _, join) =>
        wrapInQuotations(expressionStringifier(join))
      case NodeHashJoin(nodes, _, _) =>
        wrapVarsInQuotationsAndMkString(nodes)
      case RightOuterHashJoin(nodes, _, _) =>
        wrapVarsInQuotationsAndMkString(nodes)
      case LeftOuterHashJoin(nodes, _, _) =>
        wrapVarsInQuotationsAndMkString(nodes)
      case Sort(_, sortItems) =>
        sortItemsStr(sortItems)
      case Top(_, sortItems, limit) =>
        val siStr = sortItemsStr(sortItems)
        val lStr = integerString(limit)
        s""" $lStr, $siStr """.trim
      case Top1WithTies(_, sortItems) =>
        sortItemsStr(sortItems)
      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, skipSortingPrefixLength) =>
        val asStr = sortItemsStrSeq(alreadySortedPrefix)
        val stsStr = sortItemsStrSeq(stillToSortSuffix)
        val ssplStr = skipSortingPrefixLength.map(integerString) match {
          case Some(value) => s", $value"
          case None        => ""
        }
        s""" $asStr, $stsStr$ssplStr """.trim
      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, skipSortingPrefixLength) =>
        val asStr = sortItemsStrSeq(alreadySortedPrefix)
        val stsStr = sortItemsStrSeq(stillToSortSuffix)
        val lStr = integerString(limit)
        val ssplStr = skipSortingPrefixLength.map(integerString) match {
          case Some(value) => s", $value"
          case None        => ""
        }
        s""" $lStr$ssplStr, $asStr, $stsStr """.trim
      case OrderedUnion(_, _, sortedColumns) =>
        sortItemsStr(sortedColumns)
      case ErrorPlan(_, exception) =>
        // This is by no means complete, but the best we can do.
        s"new ${exception.getClass.getSimpleName}()"
      case Input(nodes, rels, vars, nullable) =>
        s""" Seq(${wrapVarsInQuotationsAndMkString(nodes)}), Seq(${wrapVarsInQuotationsAndMkString(
            rels
          )}), Seq(${wrapVarsInQuotationsAndMkString(vars)}), $nullable  """.trim
      case RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, argumentIds) =>
        val args = if (argumentIds.isEmpty) "" else ", " + wrapVarsInQuotationsAndMkString(argumentIds.toSeq)
        s""" "${idName.name}", ${startLabel.map(l => wrapInQuotations(l.name))}, Seq(${wrapInQuotationsAndMkString(
            typeNames.map(_.name)
          )}), ${endLabel.map(l => wrapInQuotations(l.name))}$args """.trim
      case NodeCountFromCountStore(idName, labelNames, argumentIds) =>
        val args = if (argumentIds.isEmpty) "" else ", " + wrapVarsInQuotationsAndMkString(argumentIds.toSeq)
        val labelStr = labelNames.map(_.map(l => wrapInQuotations(l.name)).toString).mkString(", ")
        s""" "${idName.name}", Seq($labelStr)$args """.trim
      case DetachDeleteNode(_, expression) =>
        wrapInQuotations(expressionStringifier(expression))
      case DeleteRelationship(_, expression) =>
        wrapInQuotations(expressionStringifier(expression))
      case DeleteNode(_, expression) =>
        wrapInQuotations(expressionStringifier(expression))
      case DeletePath(_, expression) =>
        wrapInQuotations(expressionStringifier(expression))
      case DetachDeletePath(_, expression) =>
        wrapInQuotations(expressionStringifier(expression))
      case DeleteExpression(_, expression) =>
        wrapInQuotations(expressionStringifier(expression))
      case DetachDeleteExpression(_, expression) =>
        wrapInQuotations(expressionStringifier(expression))
      case SetProperty(_, entity, propertyKey, value) =>
        wrapInQuotationsAndMkString(Seq(expressionStringifier(entity), propertyKey.name, expressionStringifier(value)))
      case SetNodeProperty(_, idName, propertyKey, value) =>
        wrapInQuotationsAndMkString(Seq(idName.name, propertyKey.name, expressionStringifier(value)))
      case SetRelationshipProperty(_, idName, propertyKey, value) =>
        wrapInQuotationsAndMkString(Seq(idName.name, propertyKey.name, expressionStringifier(value)))
      case SetProperties(_, entity, items)             => setPropertiesParam(expressionStringifier(entity), items)
      case SetNodeProperties(_, entity, items)         => setPropertiesParam(entity, items)
      case SetRelationshipProperties(_, entity, items) => setPropertiesParam(entity, items)
      case SetPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        s""" ${wrapInQuotationsAndMkString(
            Seq(expressionStringifier(idName), expressionStringifier(expression))
          )}, $removeOtherProps """.trim
      case SetNodePropertiesFromMap(_, idName, expression, removeOtherProps) =>
        s""" ${wrapInQuotationsAndMkString(
            Seq(idName.name, expressionStringifier(expression))
          )}, $removeOtherProps """.trim
      case SetRelationshipPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        s""" ${wrapInQuotationsAndMkString(
            Seq(idName.name, expressionStringifier(expression))
          )}, $removeOtherProps """.trim
      case Selection(ands, _) =>
        wrapInQuotationsAndMkString(ands.exprs.map(expressionStringifier(_)))
      case SelectOrSemiApply(_, _, predicate) => wrapInQuotations(expressionStringifier(predicate))
      case LetSelectOrSemiApply(_, _, idName, predicate) =>
        wrapInQuotationsAndMkString(Seq(idName.name, expressionStringifier(predicate)))
      case SelectOrAntiSemiApply(_, _, predicate) => wrapInQuotations(expressionStringifier(predicate))
      case LetSelectOrAntiSemiApply(_, _, idName, predicate) =>
        wrapInQuotationsAndMkString(Seq(idName.name, expressionStringifier(predicate)))
      case Trail(
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
          reverseGroupVariableProjections
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
          reverseGroupVariableProjections
        )

      case NodeByIdSeek(idName, ids, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapInQuotations(idName)}, Set(${wrapVarsInQuotationsAndMkString(argumentIds)}), $idsString """.trim
      case NodeByElementIdSeek(idName, ids, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapInQuotations(idName)}, Set(${wrapVarsInQuotationsAndMkString(argumentIds)}), $idsString """.trim
      case UndirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapVarsInQuotationsAndMkString(
            Seq(idName, leftNode, rightNode)
          )}, Set(${wrapVarsInQuotationsAndMkString(
            argumentIds
          )}), $idsString """.trim
      case UndirectedRelationshipByElementIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapVarsInQuotationsAndMkString(
            Seq(idName, leftNode, rightNode)
          )}, Set(${wrapVarsInQuotationsAndMkString(
            argumentIds
          )}), $idsString """.trim
      case DirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapVarsInQuotationsAndMkString(
            Seq(idName, leftNode, rightNode)
          )}, Set(${wrapVarsInQuotationsAndMkString(
            argumentIds
          )}), $idsString """.trim
      case DirectedRelationshipByElementIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapVarsInQuotationsAndMkString(
            Seq(idName, leftNode, rightNode)
          )}, Set(${wrapVarsInQuotationsAndMkString(
            argumentIds
          )}), $idsString """.trim
      case DirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        val args = argumentIds.map(wrapInQuotations)
        val argString = if (args.isEmpty) "" else args.mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}]->(${end.name})"$argString """.trim
      case UndirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        val args = argumentIds.map(wrapInQuotations)
        val argString = if (args.isEmpty) "" else args.mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}]-(${end.name})"$argString """.trim
      case PartitionedDirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        val args = argumentIds.map(wrapInQuotations)
        val argString = if (args.isEmpty) "" else args.mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}]->(${end.name})"$argString """.trim
      case PartitionedUndirectedAllRelationshipsScan(idName, start, end, argumentIds) =>
        val args = argumentIds.map(wrapInQuotations)
        val argString = if (args.isEmpty) "" else args.mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}]-(${end.name})"$argString """.trim
      case DirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        val args = Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        s""" "(${start.name})-[${idName.name}:${typ.name}]->(${end.name})", ${args.mkString(", ")} """.trim
      case UndirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        val args = Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        s""" "(${start.name})-[${idName.name}:${typ.name}]-(${end.name})", ${args.mkString(", ")} """.trim
      case PartitionedDirectedRelationshipTypeScan(idName, start, typ, end, argumentIds) =>
        val args = if (argumentIds.isEmpty) "" else argumentIds.map(wrapInQuotations).mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}:${typ.name}]->(${end.name})"${args} """.trim
      case PartitionedUndirectedRelationshipTypeScan(idName, start, typ, end, argumentIds) =>
        val args = if (argumentIds.isEmpty) "" else argumentIds.map(wrapInQuotations).mkString(", ", ", ", "")
        s""" "(${start.name})-[${idName.name}:${typ.name}]-(${end.name})"${args} """.trim
      case NodeIndexScan(idName, labelToken, properties, argumentIds, indexOrder, indexType, supportPartitionedScan) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        nodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          indexOrder,
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
        nodeIndexOperator(
          idName,
          labelToken,
          Seq(property),
          argumentIds,
          indexOrder,
          unique = false,
          s"$propName CONTAINS ${expressionStringifier(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case NodeIndexEndsWithScan(idName, labelToken, property, valueExpr, argumentIds, indexOrder, indexType) =>
        val propName = property.propertyKeyToken.name
        nodeIndexOperator(
          idName,
          labelToken,
          Seq(property),
          argumentIds,
          indexOrder,
          unique = false,
          s"$propName ENDS WITH ${expressionStringifier(valueExpr)}",
          indexType,
          supportPartitionedScan = false
        )
      case NodeIndexSeek(
          idName,
          labelToken,
          properties,
          RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(
            PointFunction(arg),
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
            PointBoundingBoxRange(PointFunction(lowerLeft), PointFunction(upperRight))
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
        nodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          indexOrder,
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
        nodeIndexOperator(
          idName,
          labelToken,
          properties,
          argumentIds,
          indexOrder,
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
            PointBoundingBoxRange(PointFunction(lowerLeft), PointFunction(upperRight))
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
            PointBoundingBoxRange(PointFunction(lowerLeft), PointFunction(upperRight))
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
            PointDistanceRange(PointFunction(point), distance, inclusive)
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
            PointDistanceRange(PointFunction(point), distance, inclusive)
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          directed = true,
          unique = false,
          s"$propName CONTAINS ${expressionStringifier(valueExpr)}",
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          directed = false,
          unique = false,
          s"$propName CONTAINS ${expressionStringifier(valueExpr)}",
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          directed = true,
          unique = false,
          s"$propName ENDS WITH ${expressionStringifier(valueExpr)}",
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          Seq(property),
          argumentIds,
          indexOrder,
          directed = false,
          unique = false,
          s"$propName ENDS WITH ${expressionStringifier(valueExpr)}",
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
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
        relationshipIndexOperator(
          idName,
          start,
          end,
          typeToken,
          properties,
          argumentIds,
          indexOrder,
          directed = false,
          unique = true,
          queryStr,
          indexType,
          supportPartitionedScan = false
        )
      case RollUpApply(_, _, collectionName, variableToCollect) =>
        s"""${wrapInQuotations(collectionName)}, ${wrapInQuotations(variableToCollect)}"""
      case ForeachApply(_, _, variable, expression) =>
        Seq(variable.name, expressionStringifier(expression)).map(wrapInQuotations).mkString(", ")
      case ConditionalApply(_, _, items)     => wrapVarsInQuotationsAndMkString(items)
      case AntiConditionalApply(_, _, items) => wrapVarsInQuotationsAndMkString(items)
      case LetSemiApply(_, _, idName)        => wrapInQuotations(idName)
      case LetAntiSemiApply(_, _, idName)    => wrapInQuotations(idName)
      case TriadicSelection(_, _, positivePredicate, sourceId, seenId, targetId) =>
        s"$positivePredicate, ${wrapVarsInQuotationsAndMkString(Seq(sourceId, seenId, targetId))}"
      case TriadicBuild(_, sourceId, seenId, triadicSelectionId) =>
        s"${triadicSelectionId.value.x}, ${wrapVarsInQuotationsAndMkString(Seq(sourceId, seenId))}"
      case TriadicFilter(_, positivePredicate, sourceId, targetId, triadicSelectionId) =>
        s"${triadicSelectionId.value.x}, $positivePredicate, ${wrapVarsInQuotationsAndMkString(Seq(sourceId, targetId))}"
      case AssertSameNode(idName, _, _) =>
        wrapInQuotations(idName)
      case AssertSameRelationship(idName, _, _) =>
        wrapInQuotations(idName)
      case Prober(_, _) =>
        "Prober.NoopProbe" // We do not preserve the object reference through the string transformation
      case RemoveLabels(_, idName, labelNames) =>
        wrapInQuotationsAndMkString(idName.name +: labelNames.map(_.name).toSeq)
      case SetLabels(_, idName, labelNames) => wrapInQuotationsAndMkString(idName.name +: labelNames.map(_.name).toSeq)
      case LoadCSV(_, url, variableName, format, fieldTerminator, _, _) =>
        val fieldTerminatorStr = fieldTerminator.fold("None")(ft => s"Some(${wrapInQuotations(ft)})")
        Seq(
          wrapInQuotations(expressionStringifier(url)),
          wrapInQuotations(variableName),
          format.toString,
          fieldTerminatorStr
        ).mkString(", ")
      case Eager(_, reasons) => reasons.map(eagernessReasonStr).mkString("ListSet(", ", ", ")")
      case TransactionForeach(_, _, batchSize, concurrency, onErrorBehaviour, maybeReportAs) =>
        callInTxParams(batchSize, concurrency, onErrorBehaviour, maybeReportAs)
      case TransactionApply(_, _, batchSize, concurrency, onErrorBehaviour, maybeReportAs) =>
        callInTxParams(batchSize, concurrency, onErrorBehaviour, maybeReportAs)
      case RunQueryAt(_, query, graphReference, parameters, importsAsParameters, columns) =>
        val escapedQuery = StringEscapeUtils.escapeJava(query)
        val parametersString =
          Option
            .when(parameters.nonEmpty) {
              parameters
                .iterator
                .map(parameter => wrapInQuotations(parameter.asCanonicalStringVal))
                .mkString(", parameters = Set(", ", ", ")")
            }.getOrElse("")
        val importsAsParametersString =
          Option
            .when(importsAsParameters.nonEmpty) {
              importsAsParameters.map { case (parameter, variable) =>
                val parameterName = wrapInQuotations(parameter.asCanonicalStringVal)
                val variableName = wrapInQuotations(variable)
                s"$parameterName -> $variableName"
              }.mkString(", importsAsParameters = Map(", ", ", ")")
            }.getOrElse("")
        val columnsString =
          Option
            .when(columns.nonEmpty)(s", columns = Set(${wrapInQuotationsAndMkString(columns.map(_.name))})")
            .getOrElse("")
        s"query = \"$escapedQuery\", graphReference = \"${graphReference.print}\"$parametersString$importsAsParametersString$columnsString"
      case SimulatedNodeScan(idName, numberOfRows) =>
        s"${wrapInQuotations(idName)}, $numberOfRows"
      case SimulatedExpand(_, from, rel, to, factor) =>
        s"${wrapInQuotationsAndMkString(Seq(from.name, rel.name, to.name))}, $factor"
      case SimulatedSelection(_, selectivity) =>
        s"$selectivity"
    }
    val plansWithContent2: PartialFunction[LogicalPlan, String] = {
      case MultiNodeIndexSeek(indexSeekLeafPlans: Seq[NodeIndexSeekLeafPlan]) =>
        indexSeekLeafPlans.map(p => s"_.nodeIndexSeek(${plansWithContent(p)})").mkString(", ")
    }
    plansWithContent.orElse(plansWithContent2).applyOrElse(logicalPlan, (_: LogicalPlan) => "")
  }

  private def groupEntitiesString(groupEntities: Set[VariableGrouping]): String =
    groupEntities.map(g => s"(${wrapInQuotations(g.singleton)}, ${wrapInQuotations(g.group)})").mkString(
      ", "
    )

  private def mappedEntitiesString(mappedEntities: Set[Mapping]): String =
    mappedEntities.map(g => s"(${wrapInQuotations(g.nfaExprVar)}, ${wrapInQuotations(g.rowVar)})").mkString(
      ", "
    )

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
    val finalState = s"${indent}${indent}.setFinalState(${nfa.finalState.id})"
    val build = s"${indent}${indent}.build()"

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
    s"${indent}${indent}.addTransition(${from.id}, ${transition.endId}, $patternString)"
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
    reverseGroupVariableProjections: Boolean
  ) = {

    val trailParameters =
      s"""${repetition.min}, ${repetition.max}, "${start.name}", "${end.name}", "${innerStart.name}", "${innerEnd.name}", """ +
        s"Set(${groupEntitiesString(groupNodes)}), Set(${groupEntitiesString(groupRelationships)}), " +
        s"Set(${wrapVarsInQuotationsAndMkString(innerRelationships)}), " +
        s"Set(${wrapVarsInQuotationsAndMkString(previouslyBoundRelationships)}), " +
        s"Set(${wrapVarsInQuotationsAndMkString(previouslyBoundRelationshipGroups)}), " +
        reverseGroupVariableProjections

    s"TrailParameters($trailParameters)"
  }

  private def setPropertiesParam(entity: String, items: Seq[(PropertyKeyName, Expression)]): String = {
    val args = items.map {
      case (p, e) => s"(${wrapInQuotations(p.name)}, ${wrapInQuotations(expressionStringifier(e))})"
    }.mkString(", ")
    Seq(wrapInQuotations(entity), args).mkString(", ")
  }

  private def setPropertiesParam(entity: LogicalVariable, items: Seq[(PropertyKeyName, Expression)]): String = {
    setPropertiesParam(entity.name, items)
  }

  private def queryExpressionStr(valueExpr: QueryExpression[Expression], propNames: Seq[String]): String = {
    valueExpr match {
      case SingleQueryExpression(expression) => s"${propNames.head} = ${expressionStringifier(expression)}"
      case ManyQueryExpression(ListLiteral(expressions)) =>
        s"${propNames.head} = ${expressions.map(expressionStringifier(_)).mkString(" OR ")}"
      case ManyQueryExpression(expr)  => s"${propNames.head} IN ${expressionStringifier(expr)}"
      case ExistenceQueryExpression() => propNames.head
      case RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(expression))) =>
        s"${propNames.head} STARTS WITH ${expressionStringifier(expression)}"
      case RangeQueryExpression(InequalitySeekRangeWrapper(range)) => rangeStr(range, propNames.head).toString
      case CompositeQueryExpression(inner) => inner.zip(propNames).map { case (qe, propName) =>
          queryExpressionStr(qe, Seq(propName))
        }.mkString(", ")
      case _ => ""
    }
  }

  case class RangeStr(pre: Option[(String, String)], expr: String, post: (String, String)) {

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
        RangeStr(None, propName, (">", expressionStringifier(expression)))
      case RangeGreaterThan(NonEmptyList(InclusiveBound(expression))) =>
        RangeStr(None, propName, (">=", expressionStringifier(expression)))
      case RangeGreaterThan(NonEmptyList(preBound, postBound)) =>
        val pre = boundStringifier(preBound, "<")
        val post = boundStringifier(postBound, ">")
        RangeStr(Some(pre.swap), propName, post)
      case RangeLessThan(NonEmptyList(ExclusiveBound(expression))) =>
        RangeStr(None, propName, ("<", expressionStringifier(expression)))
      case RangeLessThan(NonEmptyList(preBound, postBound)) =>
        val pre = boundStringifier(preBound, ">")
        val post = boundStringifier(postBound, "<")
        RangeStr(Some(pre.swap), propName, post)
      case RangeLessThan(NonEmptyList(InclusiveBound(expression))) =>
        RangeStr(None, propName, ("<=", expressionStringifier(expression)))
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
      case InclusiveBound(endPoint) => (exclusiveSign + "=", expressionStringifier(endPoint))
      case ExclusiveBound(endPoint) => (exclusiveSign, expressionStringifier(endPoint))
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
    unique: Boolean,
    parenthesesContent: String,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ): String = {
    val indexStr = s"${idName.name}:${labelToken.name}($parenthesesContent)"
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"
    val uniqueStr = s", unique = $unique"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)

    val getValueBehaviors = indexedPropertyGetValueBehaviors(properties)
    val getValueStr = s", getValue = $getValueBehaviors"
    val supportPartitionedScanString = s", supportPartitionedScan = $supportPartitionedScan"
    s""" "$indexStr"$indexOrderStr$argStr$getValueStr$uniqueStr$indexTypeStr$supportPartitionedScanString """.trim
  }

  private def partitionedNodeIndexOperator(
    idName: LogicalVariable,
    labelToken: LabelToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    parenthesesContent: String,
    indexType: IndexType
  ): String = {
    val indexStr = s"${idName.name}:${labelToken.name}($parenthesesContent)"
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)

    val getValueBehaviors = indexedPropertyGetValueBehaviors(properties)
    val getValueStr = s", getValue = $getValueBehaviors"
    s""" "$indexStr"$argStr$getValueStr$indexTypeStr """.trim
  }

  private def relationshipIndexOperator(
    idName: LogicalVariable,
    start: LogicalVariable,
    end: LogicalVariable,
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    directed: Boolean,
    unique: Boolean,
    parenthesesContent: String,
    indexType: IndexType,
    supportPartitionedScan: Boolean
  ): String = {
    val rarrow = if (directed) "->" else "-"
    val indexStr = s"(${start.name})-[${idName.name}:${typeToken.name}($parenthesesContent)]$rarrow(${end.name})"
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"
    val uniqueStr = s", unique = $unique"

    val getValueBehaviors = indexedPropertyGetValueBehaviors(properties)
    val getValueStr = s", getValue = $getValueBehaviors"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)
    val supportPartitionedScanString = s", supportPartitionedScan = $supportPartitionedScan"
    s""" "$indexStr"$indexOrderStr$argStr$getValueStr$uniqueStr$indexTypeStr$supportPartitionedScanString """.trim
  }

  private def partitionedRelationshipIndexOperator(
    idName: LogicalVariable,
    start: LogicalVariable,
    end: LogicalVariable,
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[LogicalVariable],
    directed: Boolean,
    parenthesesContent: String,
    indexType: IndexType
  ): String = {
    val rarrow = if (directed) "->" else "-"
    val indexStr = s"(${start.name})-[${idName.name}:${typeToken.name}($parenthesesContent)]$rarrow(${end.name})"
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"

    val getValueBehaviors = indexedPropertyGetValueBehaviors(properties)
    val getValueStr = s", getValue = $getValueBehaviors"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)
    s""" "$indexStr"$argStr$getValueStr$indexTypeStr """.trim
  }

  private def indexedPropertyGetValueBehaviors(properties: Seq[IndexedProperty]): String = {
    properties.map {
      case IndexedProperty(PropertyKeyToken(name, _), getValueBehavior, _) =>
        s"${wrapInQuotations(name)} -> ${objectName(getValueBehavior)}"
    }.mkString("Map(", ", ", ")")
  }

  private def createCreateCommandToString(create: CreateCommand) = create match {
    case c: CreateNode         => createNodeToString(c)
    case c: CreateRelationship => createRelationshipToString(c)
  }

  private def createNodeToString(createNode: CreateNode) = createNode match {
    case CreateNode(idName, labels, None) =>
      s"createNode(${wrapInQuotationsAndMkString(idName.name +: labels.map(_.name).toSeq)})"
    case CreateNode(idName, labels, Some(props)) =>
      s"createNodeWithProperties(${wrapInQuotations(idName.name)}, Seq(${wrapInQuotationsAndMkString(labels.map(_.name))}), ${wrapInQuotations(expressionStringifier(props))})"
  }

  private def createRelationshipToString(rel: CreateRelationship) = {
    val propString = rel.properties.map(p => s", Some(${wrapInQuotations(expressionStringifier(p))})").getOrElse("")
    s"createRelationship(${wrapInQuotationsAndMkString(
        Seq(rel.variable.name, rel.leftNode.name, rel.relType.name, rel.rightNode.name)
      )}, ${rel.direction}$propString)"
  }

  private def mutationToString(op: SimpleMutatingPattern): String = op match {
    case c: CreatePattern =>
      s"createPattern(Seq(${c.nodes.map(createNodeToString).mkString(", ")}), Seq(${c.relationships.map(createRelationshipToString).mkString(", ")}))"
    case org.neo4j.cypher.internal.ir.DeleteExpression(expression, forced) =>
      s"delete(${wrapInQuotations(expressionStringifier(expression))}, $forced)"
    case SetLabelPattern(node, labelNames) =>
      s"setLabel(${wrapInQuotationsAndMkString(node.name +: labelNames.map(_.name))})"
    case RemoveLabelPattern(node, labelNames) =>
      s"removeLabel(${wrapInQuotationsAndMkString(node.name +: labelNames.map(_.name))})"
    case SetNodePropertyPattern(node, propertyKey, value) =>
      s"setNodeProperty(${wrapInQuotationsAndMkString(Seq(node.name, propertyKey.name, expressionStringifier(value)))})"
    case SetRelationshipPropertyPattern(relationship, propertyKey, value) =>
      s"setRelationshipProperty(${wrapInQuotationsAndMkString(Seq(relationship.name, propertyKey.name, expressionStringifier(value)))})"
    case SetNodePropertiesFromMapPattern(idName, expression, removeOtherProps) =>
      s"setNodePropertiesFromMap(${wrapInQuotationsAndMkString(Seq(idName.name, expressionStringifier(expression)))}, $removeOtherProps)"
    case SetRelationshipPropertiesFromMapPattern(idName, expression, removeOtherProps) =>
      s"setRelationshipPropertiesFromMap(${wrapInQuotationsAndMkString(Seq(idName.name, expressionStringifier(expression)))}, $removeOtherProps)"
    case SetPropertyPattern(entityExpression, propertyKey, value) =>
      s"setProperty(${wrapInQuotationsAndMkString(Seq(expressionStringifier(entityExpression), propertyKey.name, expressionStringifier(value)))})"
    case SetPropertiesFromMapPattern(entityExpression, map, removeOtherProps) =>
      s"setPropertyFromMap(${wrapInQuotationsAndMkString(Seq(expressionStringifier(entityExpression), expressionStringifier(map)))}, $removeOtherProps)"
    case SetPropertiesPattern(entity, items) =>
      s"setProperties(${setPropertiesParam(expressionStringifier(entity), items)})"
    case SetNodePropertiesPattern(entity, items) =>
      s"setNodeProperties(${setPropertiesParam(entity, items)})"
    case SetRelationshipPropertiesPattern(entity, items) =>
      s"setRelationshipProperties(${setPropertiesParam(entity, items)})"
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
  ): String = {
    val propName = properties.head.propertyKeyToken.name
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"
    val inclusiveStr = s", inclusive = $inclusive"
    val getValueBehavior = properties.map(_.getValueFromIndex).reduce {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException(
            "Index operators with different getValueFromIndex behaviors not supported."
          )
        }
    }
    val getValueStr = s", getValue = ${objectName(getValueBehavior)}"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)
    s""" "${idName.name}", "${labelToken.name}", "$propName", "${expressionStringifier(
        point
      )}", ${expressionStringifier(
        distance
      )}$indexOrderStr$argStr$getValueStr$inclusiveStr$indexTypeStr """.trim
  }

  private def pointBoundingBoxNodeIndexSeek(
    idName: LogicalVariable,
    labelToken: LabelToken,
    properties: Seq[IndexedProperty],
    lowerLeft: Expression,
    upperRight: Expression,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    indexType: IndexType
  ): String = {
    val propName = properties.head.propertyKeyToken.name
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"
    val getValueBehavior = properties.map(_.getValueFromIndex).reduce {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException(
            "Index operators with different getValueFromIndex behaviors not supported."
          )
        }
    }
    val getValueStr = s", getValue = ${objectName(getValueBehavior)}"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)
    s""" "${idName.name}", "${labelToken.name}", "$propName", "${expressionStringifier(
        lowerLeft
      )}", "${expressionStringifier(
        upperRight
      )}"$indexOrderStr$argStr$getValueStr$indexTypeStr """.trim
  }

  private def pointBoundingBoxRelationshipIndexSeek(
    idName: LogicalVariable,
    start: LogicalVariable,
    end: LogicalVariable,
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    lowerLeft: Expression,
    upperRight: Expression,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    indexType: IndexType,
    directed: Boolean
  ): String = {
    val propName = properties.head.propertyKeyToken.name
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"
    val getValueBehavior = properties.map(_.getValueFromIndex).reduce {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException(
            "Index operators with different getValueFromIndex behaviors not supported."
          )
        }
    }
    val directedString = s", directed = $directed"
    val getValueStr = s", getValue = ${objectName(getValueBehavior)}"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)
    s""" "${idName.name}", "${start.name}", "${end.name}", "${typeToken.name}", "$propName", "${expressionStringifier(
        lowerLeft
      )}", "${expressionStringifier(upperRight)}"$directedString$indexOrderStr$argStr$getValueStr$indexTypeStr """.trim
  }

  private def pointDistanceRelationshipIndexSeek(
    idName: LogicalVariable,
    start: LogicalVariable,
    end: LogicalVariable,
    typeToken: RelationshipTypeToken,
    properties: Seq[IndexedProperty],
    point: Expression,
    distance: Expression,
    argumentIds: Set[LogicalVariable],
    indexOrder: IndexOrder,
    indexType: IndexType,
    directed: Boolean,
    inclusive: Boolean
  ): String = {
    val propName = properties.head.propertyKeyToken.name
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapVarsInQuotationsAndMkString(argumentIds)})"
    val getValueBehavior = properties.map(_.getValueFromIndex).reduce {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException(
            "Index operators with different getValueFromIndex behaviors not supported."
          )
        }
    }
    val directedStr = s", directed = $directed"
    val inclusiveStr = s", inclusive = $inclusive"
    val getValueStr = s", getValue = ${objectName(getValueBehavior)}"
    val indexTypeStr = indexTypeToNamedArgumentString(indexType)
    s""" "${idName.name}", "${start.name}", "${end.name}", "${typeToken.name}", "$propName", "${expressionStringifier(
        point
      )}", ${expressionStringifier(
        distance
      )}$directedStr$inclusiveStr$getValueStr$indexOrderStr$argStr$indexTypeStr """.trim
  }

  private def idsStr(ids: SeekableArgs) = {
    def stringify(expr: Expression): String = expr match {
      case literal: NumberLiteral => expressionStringifier(literal)
      case expr                   => wrapInQuotations(expressionStringifier(expr))
    }

    val idsStr = ids match {
      case SingleSeekableArg(expr)                    => stringify(expr)
      case ManySeekableArgs(ListLiteral(expressions)) => expressions.map(stringify).mkString(", ")
      case ManySeekableArgs(expr)                     => stringify(expr)
    }
    idsStr
  }

  private def integerString(count: Expression) = {
    count match {
      case SignedDecimalIntegerLiteral(i) => i
      case _                              => "/* " + count + "*/"
    }
  }

  private def sortItemsStr(sortItems: Seq[ColumnOrder]) = {
    sortItems.map(sortItemStr).mkString(", ")
  }

  private def sortItemsStrSeq(sortItems: Seq[ColumnOrder]) = {
    sortItems.map(sortItemStr).mkString("Seq(", ", ", ")")
  }

  private def sortItemStr(si: ColumnOrder): String = {
    s"\"${escapeIdentifier(si.id.name)} ${if (si.isAscending) "ASC" else "DESC"}\""
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
    case EagernessReason.ReadDeleteConflict(identifier) =>
      s"${objectName(EagernessReason.ReadDeleteConflict)}(${wrapInQuotations(identifier)})"
    case EagernessReason.ReadCreateConflict =>
      s"${objectName(EagernessReason.ReadCreateConflict)}"
    case EagernessReason.PropertyReadSetConflict(property) =>
      s"${objectName(EagernessReason.PropertyReadSetConflict)}(PropertyKeyName(${wrapInQuotations(property.name)})(InputPosition.NONE))"
    case EagernessReason.UnknownPropertyReadSetConflict =>
      s"${objectName(EagernessReason.UnknownPropertyReadSetConflict)}"
  }

  private def variablePredicates(predicates: Seq[VariablePredicate], name: String): String = {
    val predStrs = predicates.map(vp =>
      s"""Predicate("${vp.variable.name}", "${expressionStringifier(vp.predicate)}") """.trim
    ).mkString(", ")
    s", $name = Seq(" + predStrs + ")"
  }

  private[plans] def relTypeStr(types: Seq[RelTypeName]) = {
    types match {
      case head +: tail => s":${head.name}${tail.map(t => s"|${t.name}").mkString("")}"
      case _            => ""
    }
  }

  private def projectStrs(map: Iterable[(String, Expression)]): String = wrapInQuotationsAndMkString(map.map {
    case (alias, expr) => s"${expressionStringifier(expr)} AS ${escapeIdentifier(alias)}"
  })

  private def projectVars(map: Map[LogicalVariable, Expression]): String = {
    projectStrs(map.view.map { case (key, e) => key.name -> e })
  }

  private def escapeIdentifier(alias: String) = {
    if (alias.matches("\\w+")) alias else s"`$alias`"
  }

  private def wrapInQuotations(c: String): String = "\"" + c + "\""
  private def wrapInQuotations(v: LogicalVariable): String = wrapInQuotations(v.name)

  private def wrapInQuotationsAndMkString(strings: Iterable[String]): String =
    strings.map(wrapInQuotations).mkString(", ")

  private def wrapVarsInQuotationsAndMkString(vars: Iterable[LogicalVariable]): String =
    vars.map(v => wrapInQuotations(v)).mkString(", ")

  private def objectName(obj: AnyRef): String = {
    val str = obj.getClass.getSimpleName
    str.substring(0, str.length - 1)
  }

  private[plans] def arrows(dir: SemanticDirection): (String, String) = dir match {
    case SemanticDirection.OUTGOING => ("-", "->")
    case SemanticDirection.INCOMING => ("<-", "-")
    case SemanticDirection.BOTH     => ("-", "-")
  }

  private def indexTypeToNamedArgumentString(indexType: IndexType): String = {
    s", indexType = ${indexType.getDeclaringClass.getSimpleName}.${indexType.name}"
  }

  private def callInTxParams(
    batchSize: Expression,
    concurrency: TransactionConcurrency,
    onErrorBehaviour: InTransactionsOnErrorBehaviour,
    maybeReportAs: Option[LogicalVariable]
  ): String = {
    val params =
      Seq(
        expressionStringifier(batchSize),
        concurrency match {
          case TransactionConcurrency.Concurrent(Some(concurrency)) => s"Concurrent(Some($concurrency))"
          case c                                                    => c.toString
        },
        onErrorBehaviour.toString
      ) ++ maybeReportAs.map(_.name)
    params.mkString(", ")
  }
}

object PointFunction {

  def unapply(point: Expression): Option[Expression] = point match {
    case FunctionInvocation(FunctionName(_, "point"), _, args, _, _) => Some(args.head)
    case _                                                           => None
  }
}
