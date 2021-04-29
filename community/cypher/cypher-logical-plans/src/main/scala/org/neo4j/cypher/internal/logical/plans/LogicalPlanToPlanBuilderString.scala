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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.util.NonEmptyList

object LogicalPlanToPlanBuilderString {
  private val expressionStringifier = ExpressionStringifier(expressionStringifierExtension, preferSingleQuotes = true)

  private case class LevelPlanItem(level: Int, plan: LogicalPlan)

  /**
   * Generates a string that plays nicely together with `AbstractLogicalPlanBuilder`.
   */
  def apply(logicalPlan: LogicalPlan): String = render(logicalPlan, None, None)

  def apply(logicalPlan: LogicalPlan,
            extra: LogicalPlan => String,
            planPrefixDot: LogicalPlan => String): String = render(logicalPlan, Some(extra), Some(planPrefixDot))

  def expressionStringifierExtension(expression: Expression): String = {
    expression match {
      case p@CachedProperty(_, _, _, NODE_TYPE, false) => s"cacheN[${p.propertyAccessString}]"
      case p@CachedProperty(_, _, _, RELATIONSHIP_TYPE, false) => s"cacheR[${p.propertyAccessString}]"
      case p@CachedProperty(_, _, _, NODE_TYPE, true) => s"cacheNFromStore[${p.propertyAccessString}]"
      case p@CachedProperty(_, _, _, RELATIONSHIP_TYPE, true) => s"cacheRFromStore[${p.propertyAccessString}]"
      case e => e.asCanonicalStringVal
    }
  }

  private def render(logicalPlan: LogicalPlan, extra: Option[LogicalPlan => String], planPrefixDot: Option[LogicalPlan => String]) = {
    var childrenStack = LevelPlanItem(0, logicalPlan) :: Nil
    val sb = new StringBuilder()

    while (childrenStack.nonEmpty) {
      val LevelPlanItem(level, plan) = childrenStack.head
      childrenStack = childrenStack.tail

      sb ++= ".|" * level

      sb ++= planPrefixDot.fold(".")(_.apply(plan))
      sb ++= pre(plan)
      sb += '('
      sb ++= par(plan)
      sb += ')'
      extra.foreach(e => sb ++= e.apply(plan))

      plan.lhs.foreach(lhs => childrenStack ::= LevelPlanItem(level, lhs))
      plan.rhs.foreach(rhs => childrenStack ::= LevelPlanItem(level + 1, rhs))

      if (childrenStack.nonEmpty) sb ++= System.lineSeparator()
    }

    if (extra.isEmpty) {
      sb ++= System.lineSeparator()
      sb ++= ".build()"
    }


    sb.toString()
  }

  private def pre(logicalPlan: LogicalPlan): String = {
    val specialCases: PartialFunction[LogicalPlan, String] = {
      case _:ProduceResult => "produceResults"
      case _:AllNodesScan => "allNodeScan"
      case e:Expand => if(e.mode == ExpandAll) "expandAll" else "expandInto"
      case _:VarExpand => "expand"
      case e:OptionalExpand => if(e.mode == ExpandAll) "optionalExpandAll" else "optionalExpandInto"
      case _:Selection => "filter"
      case _:UnwindCollection => "unwind"
      case _:FindShortestPaths => "shortestPath"
      case _:NodeIndexScan => "nodeIndexOperator"
      case _:DirectedRelationshipIndexScan => "relationshipIndexOperator"
      case NodeIndexSeek(_, _, _, RangeQueryExpression(PointDistanceSeekRangeWrapper(_)), _, _) => "pointDistanceNodeIndexSeek"
      case _:NodeIndexSeek => "nodeIndexOperator"
      case _:NodeUniqueIndexSeek => "nodeIndexOperator"
      case _:NodeIndexContainsScan => "nodeIndexOperator"
      case _:NodeIndexEndsWithScan => "nodeIndexOperator"
      case _:MultiNodeIndexSeek => "multiNodeIndexSeekOperator"
      case _:DirectedRelationshipIndexSeek => "relationshipIndexOperator"
      case _:UndirectedRelationshipIndexSeek => "relationshipIndexOperator"
      case _:DirectedRelationshipIndexContainsScan => "relationshipIndexOperator"
      case _:UndirectedRelationshipIndexContainsScan => "relationshipIndexOperator"
      case _:DirectedRelationshipIndexEndsWithScan => "relationshipIndexOperator"
      case _:UndirectedRelationshipIndexEndsWithScan => "relationshipIndexOperator"
      case _:DirectedRelationshipIndexScan => "relationshipIndexOperator"
      case _:UndirectedRelationshipIndexScan => "relationshipIndexOperator"
      case _: DirectedRelationshipTypeScan => "relationshipTypeScan"
      case _: UndirectedRelationshipTypeScan => "relationshipTypeScan"
      case _: EitherPlan => "either"
    }
    specialCases.applyOrElse(logicalPlan, classNameFormat)
  }

  private def classNameFormat(logicalPlan: LogicalPlan): String = {
    val className = logicalPlan.getClass.getSimpleName
    val head = Character.toLowerCase(className.head)
    head +: className.tail
  }

  private def par(logicalPlan: LogicalPlan): String = {
    val plansWithContent: PartialFunction[LogicalPlan, String] = {
      case Aggregation(_, groupingExpressions, aggregationExpression) =>
        s"Seq(${projectStrs(groupingExpressions)}), Seq(${projectStrs(aggregationExpression)})"
      case OrderedAggregation(_, groupingExpressions, aggregationExpression, orderToLeverage) =>
        s"Seq(${projectStrs(groupingExpressions)}), Seq(${projectStrs(aggregationExpression)}), Seq(${wrapInQuotationsAndMkString(orderToLeverage.map(expressionStringifier(_)))})"
      case Distinct(_, groupingExpressions) =>
        projectStrs(groupingExpressions)
      case OrderedDistinct(_, groupingExpressions, orderToLeverage) =>
        s""" Seq(${wrapInQuotationsAndMkString(orderToLeverage.map(expressionStringifier(_)))}), ${projectStrs(groupingExpressions)} """.trim
      case Projection(_, projectExpressions) =>
        projectStrs(projectExpressions)
      case UnwindCollection(_, variable, expression) =>
        projectStrs(Map(variable -> expression))
      case AllNodesScan(idName, argumentIds) =>
        wrapInQuotationsAndMkString(idName +: argumentIds.toSeq)
      case Argument(argumentIds) =>
        wrapInQuotationsAndMkString(argumentIds.toSeq)
      case CacheProperties(_, properties) =>
        wrapInQuotationsAndMkString(properties.toSeq.map(expressionStringifier(_)))
      case Create(_, nodes, Seq()) =>
        nodes.map(createNodeToString).mkString(", ")
      case Create(_, nodes, relationships) =>
        s"Seq(${nodes.map(createNodeToString).mkString(", ")}), Seq(${relationships.map(createRelationshipToString).mkString(", ")})"
      case Merge(_, createNodes, createRelationships, onMatch, onCreate) =>


        val nodesToCreate = createNodes.map(createNodeToString)
        val relsToCreate = createRelationships.map(createRelationshipToString)

        val onMatchString = onMatch.map(mutationToString)
        val onCreateString = onCreate.map(mutationToString)

        s"Seq(${nodesToCreate.mkString(", ")}), Seq(${relsToCreate.mkString(", ")}), Seq(${onMatchString.mkString(", ")}), Seq(${onCreateString.mkString(", ")})"

      case Foreach(_, variable, list, mutations) =>
        s"${wrapInQuotations(variable)}, ${wrapInQuotations(expressionStringifier(list))}, Seq(${mutations.map(mutationToString).mkString(", ")})"

      case Expand(_, from, dir, types, to, relName, _) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        s""" "($from)$dirStrA[$relName$typeStr]$dirStrB($to)" """.trim
      case VarExpand(_, from, dir, pDir, types, to, relName, length, mode, nodePredicate, relationshipPredicate) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"${length.min}..${length.max.getOrElse("")}"
        val modeStr = s", expandMode = ${objectName(mode)}"
        val pDirStr = s", projectedDir = ${objectName(pDir)}"
        val nPredStr = variablePredicate(nodePredicate, "nodePredicate")
        val rPredStr = variablePredicate(relationshipPredicate, "relationshipPredicate")
        s""" "($from)$dirStrA[$relName$typeStr*$lenStr]$dirStrB($to)"$modeStr$pDirStr$nPredStr$rPredStr """.trim
      case FindShortestPaths(_, shortestPath, predicates, withFallBack, disallowSameNode) =>
        val fbStr = if(withFallBack) ", withFallback = true" else ""
        val dsnStr = if(!disallowSameNode) ", disallowSameNode = false" else ""
        shortestPath match {
          case ShortestPathPattern(maybePathName, PatternRelationship(relName, (from, to), dir, types, length), single) =>
            val lenStr = length match {
              case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
              case SimplePatternLength => ""
            }
            val (dirStrA, dirStrB) = arrows(dir)
            val typeStr = relTypeStr(types)
            val pNameStr = maybePathName.map(p => s", pathName = Some(${wrapInQuotations(p)})").getOrElse("")
            val allStr = if (single) "" else ", all = true"
            val predStr = if(predicates.isEmpty) "" else ", predicates = Seq(" + wrapInQuotationsAndMkString(predicates.map(expressionStringifier(_))) +")"
            s""" "($from)$dirStrA[$relName$typeStr$lenStr]$dirStrB($to)"$pNameStr$allStr$predStr$fbStr$dsnStr """.trim
        }
      case PruningVarExpand(_, from, dir, types, to, minLength, maxLength, nodePredicate, relationshipPredicate) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"$minLength..$maxLength"
        val nPredStr = variablePredicate(nodePredicate, "nodePredicate")
        val rPredStr = variablePredicate(relationshipPredicate, "relationshipPredicate")
        s""" "($from)$dirStrA[$typeStr*$lenStr]$dirStrB($to)"$nPredStr$rPredStr """.trim
      case Limit(_, count) =>
        integerString(count)
      case ExhaustiveLimit(_, count) =>
        integerString(count)
      case Skip(_, count) =>
        integerString(count)
      case NodeByLabelScan(idName, label, argumentIds, indexOrder) =>
        val args = Seq(idName, label.name).map(wrapInQuotations) ++ Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        args.mkString(", ")
      case Optional(_, protectedSymbols) =>
        wrapInQuotationsAndMkString(protectedSymbols)
      case OptionalExpand(_, from, dir, types, to, relName, _, predicate) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val predStr = predicate.fold("")(p => s""", Some("${expressionStringifier(p)}")""")
        s""" "($from)$dirStrA[$relName$typeStr]$dirStrB($to)"$predStr""".trim
      case ProcedureCall(_, ResolvedCall(ProcedureSignature(QualifiedName(namespace, name), _, _, _, _, _, _, _, _, _, _), callArguments, callResults, _, _, yieldAll)) =>
        val yielding = if (yieldAll) {
          " YIELD *"
        } else if (callResults.isEmpty) {
          ""
        } else {
          callResults.map(i => expressionStringifier(i.variable)).mkString(" YIELD ", ",", "")
        }
        s""" "${namespace.mkString(".")}.$name(${callArguments.map(expressionStringifier(_)).mkString(", ")})$yielding" """.trim
      case ProduceResult(_, columns) =>
        wrapInQuotationsAndMkString(columns.map(escapeIdentifier))
      case ProjectEndpoints(_, relName, start, startInScope, end, endInScope, types, directed, length) =>
        val (dirStrA, dirStrB) = arrows(if (directed) OUTGOING else BOTH)
        val typeStr = relTypeStr(types.getOrElse(Seq.empty))
        val lenStr = length match {
          case SimplePatternLength => ""
          case VarPatternLength(min, max) => s"*$min..${max.getOrElse("")}"
        }
        s""" "($start)$dirStrA[$relName$typeStr$lenStr]$dirStrB($end)", startInScope = $startInScope, endInScope = $endInScope """.trim
      case ValueHashJoin(_, _, join) =>
        wrapInQuotations(expressionStringifier(join))
      case NodeHashJoin(nodes, _, _) =>
        wrapInQuotationsAndMkString(nodes)
      case RightOuterHashJoin(nodes, _, _) =>
        wrapInQuotationsAndMkString(nodes)
      case LeftOuterHashJoin(nodes, _, _) =>
        wrapInQuotationsAndMkString(nodes)
      case Sort(_, sortItems) =>
        sortItemsStr(sortItems)
      case Top(_, sortItems, limit) =>
        val siStr = sortItemsStr(sortItems)
        val lStr = integerString(limit)
        s""" $siStr, $lStr """.trim
      case Top1WithTies(_, sortItems) =>
        sortItemsStr(sortItems)
      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix, skipSortingPrefixLength) =>
        val asStr = sortItemsStr(alreadySortedPrefix)
        val stsStr = sortItemsStr(stillToSortSuffix)
        val ssplStr = skipSortingPrefixLength.map(integerString) match {
          case Some(value) => s", $value"
          case None => ""
        }
        s""" $asStr, $stsStr$ssplStr """.trim
      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit, skipSortingPrefixLength) =>
        val asStr = sortItemsStr(alreadySortedPrefix)
        val stsStr = sortItemsStr(stillToSortSuffix)
        val lStr = integerString(limit)
        val ssplStr = skipSortingPrefixLength.map(integerString) match {
          case Some(value) => s", $value"
          case None => ""
        }
        s""" $asStr, $stsStr, $lStr$ssplStr """.trim
      case OrderedUnion(_, _, sortedColumns) =>
        sortItemsStr(sortedColumns)
      case ErrorPlan(_, exception) =>
        // This is by no means complete, but the best we can do.
        s"new ${exception.getClass.getSimpleName}()"
      case Input(nodes, rels, vars, nullable) =>
        s""" Seq(${wrapInQuotationsAndMkString(nodes)}), Seq(${wrapInQuotationsAndMkString(rels)}), Seq(${wrapInQuotationsAndMkString(vars)}), $nullable  """.trim
      case RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, argumentIds) =>
        val args = if(argumentIds.isEmpty) "" else ", " + wrapInQuotationsAndMkString(argumentIds.toSeq)
        s""" "$idName", ${startLabel.map(l => wrapInQuotations(l.name))}, Seq(${wrapInQuotationsAndMkString(typeNames.map(_.name))}), ${endLabel.map(l => wrapInQuotations(l.name))}$args """.trim
      case NodeCountFromCountStore(idName, labelNames, argumentIds) =>
        val args = if(argumentIds.isEmpty) "" else ", " + wrapInQuotationsAndMkString(argumentIds.toSeq)
        val labelStr = labelNames.map(_.map(l => wrapInQuotations(l.name)).toString).mkString(", ")
        s""" "$idName", Seq($labelStr)$args """.trim
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
        wrapInQuotationsAndMkString(Seq(idName, propertyKey.name, expressionStringifier(value)))
      case SetRelationshipProperty(_, idName, propertyKey, value) =>
        wrapInQuotationsAndMkString(Seq(idName, propertyKey.name, expressionStringifier(value)))
      case SetPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        s""" ${wrapInQuotationsAndMkString(Seq(expressionStringifier(idName), expressionStringifier(expression)))}, $removeOtherProps """.trim
      case SetNodePropertiesFromMap(_, idName, expression, removeOtherProps) =>
        s""" ${wrapInQuotationsAndMkString(Seq(idName, expressionStringifier(expression)))}, $removeOtherProps """.trim
      case SetRelationshipPropertiesFromMap(_, idName, expression, removeOtherProps) =>
        s""" ${wrapInQuotationsAndMkString(Seq(idName, expressionStringifier(expression)))}, $removeOtherProps """.trim
      case Selection(ands, _) =>
        wrapInQuotationsAndMkString(ands.exprs.map(expressionStringifier(_)))
      case SelectOrSemiApply(_, _, predicate) => wrapInQuotations(expressionStringifier(predicate))
      case LetSelectOrSemiApply(_, _, idName, predicate) => wrapInQuotationsAndMkString(Seq(idName, expressionStringifier(predicate)))
      case SelectOrAntiSemiApply(_, _, predicate) => wrapInQuotations(expressionStringifier(predicate))
      case LetSelectOrAntiSemiApply(_, _, idName, predicate) => wrapInQuotationsAndMkString(Seq(idName, expressionStringifier(predicate)))
      case NodeByIdSeek(idName, ids, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapInQuotations(idName)}, Set(${wrapInQuotationsAndMkString(argumentIds)}), $idsString """.trim
      case UndirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapInQuotationsAndMkString(Seq(idName, leftNode, rightNode))}, Set(${wrapInQuotationsAndMkString(argumentIds)}), $idsString """.trim
      case DirectedRelationshipByIdSeek(idName, ids, leftNode, rightNode, argumentIds) =>
        val idsString: String = idsStr(ids)
        s""" ${wrapInQuotationsAndMkString(Seq(idName, leftNode, rightNode))}, Set(${wrapInQuotationsAndMkString(argumentIds)}), $idsString """.trim
      case DirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        val args = Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        s""" "($start)-[$idName:${typ.name}]->($end)", ${args.mkString(", ")} """.trim
      case UndirectedRelationshipTypeScan(idName, start, typ, end, argumentIds, indexOrder) =>
        val args = Seq(objectName(indexOrder)) ++ argumentIds.map(wrapInQuotations)
        s""" "($start)-[$idName:${typ.name}]-($end)", ${args.mkString(", ")} """.trim
      case NodeIndexScan(idName, labelToken, properties, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        nodeIndexOperator(idName, labelToken, properties, argumentIds, indexOrder, unique = false, propNames.mkString(", "))
      case NodeIndexContainsScan(idName, labelToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        nodeIndexOperator(idName, labelToken, Seq(property), argumentIds, indexOrder, unique = false, s"$propName CONTAINS ${expressionStringifier(valueExpr)}")
      case NodeIndexEndsWithScan(idName, labelToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        nodeIndexOperator(idName, labelToken, Seq(property), argumentIds, indexOrder, unique = false, s"$propName ENDS WITH ${expressionStringifier(valueExpr)}")
      case NodeIndexSeek(idName, labelToken, properties, RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(FunctionInvocation(_, FunctionName("point"), _, args), distance, inclusive))), argumentIds, indexOrder) =>
        pointDistanceNodeIndexSeek(idName, labelToken, properties, args.head, distance, argumentIds, indexOrder, inclusive = inclusive)
      case NodeIndexSeek(idName, labelToken, properties, valueExpr, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        nodeIndexOperator(idName, labelToken, properties, argumentIds, indexOrder, unique = false, queryStr)
      case NodeUniqueIndexSeek(idName, labelToken, properties, valueExpr, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        nodeIndexOperator(idName, labelToken, properties, argumentIds, indexOrder, unique = true, queryStr)
      case DirectedRelationshipIndexSeek(idName, start, end, typeToken, properties, valueExpr, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        relationshipIndexOperator(idName, start, end, typeToken, properties, argumentIds, indexOrder, directed = true, queryStr)
      case UndirectedRelationshipIndexSeek(idName, start, end, typeToken, properties, valueExpr, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        relationshipIndexOperator(idName, start, end, typeToken, properties, argumentIds, indexOrder, directed = false, queryStr)
      case DirectedRelationshipIndexScan(idName, start, end, typeToken, properties, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        relationshipIndexOperator(idName, start, end, typeToken, properties, argumentIds, indexOrder, directed = true, propNames.mkString(", "))
      case UndirectedRelationshipIndexScan(idName, start, end, typeToken, properties, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        relationshipIndexOperator(idName, start, end, typeToken, properties, argumentIds, indexOrder, directed = false, propNames.mkString(", "))
      case DirectedRelationshipIndexContainsScan(idName, start, end, typeToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        relationshipIndexOperator(idName, start, end, typeToken, Seq(property), argumentIds, indexOrder, directed = true,s"$propName CONTAINS ${expressionStringifier(valueExpr)}")
      case UndirectedRelationshipIndexContainsScan(idName, start, end, typeToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        relationshipIndexOperator(idName, start, end, typeToken, Seq(property), argumentIds, indexOrder, directed = false,s"$propName CONTAINS ${expressionStringifier(valueExpr)}")
      case DirectedRelationshipIndexEndsWithScan(idName, start, end, typeToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        relationshipIndexOperator(idName, start, end, typeToken, Seq(property), argumentIds, indexOrder, directed = true,s"$propName ENDS WITH ${expressionStringifier(valueExpr)}")
      case UndirectedRelationshipIndexEndsWithScan(idName, start, end, typeToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        relationshipIndexOperator(idName, start, end, typeToken, Seq(property), argumentIds, indexOrder, directed = false,s"$propName ENDS WITH ${expressionStringifier(valueExpr)}")
      case RollUpApply(_, _, collectionName, variableToCollect) =>
        s"""${wrapInQuotations(collectionName)}, ${wrapInQuotations(variableToCollect)}"""
      case ForeachApply(_, _, variable, expression) =>
        Seq(variable, expressionStringifier(expression)).map(wrapInQuotations).mkString(", ")
      case ConditionalApply(_, _, items) => wrapInQuotationsAndMkString(items)
      case AntiConditionalApply(_, _, items) => wrapInQuotationsAndMkString(items)
      case LetSemiApply(_, _, idName) => wrapInQuotations(idName)
      case LetAntiSemiApply(_, _, idName) => wrapInQuotations(idName)
      case TriadicSelection(_, _, positivePredicate, sourceId, seenId, targetId) =>
        s"$positivePredicate, ${wrapInQuotationsAndMkString(Seq(sourceId, seenId, targetId))}"
      case TriadicBuild(_, sourceId, seenId, triadicSelectionId) =>
        s"${triadicSelectionId.value.x}, ${wrapInQuotationsAndMkString(Seq(sourceId, seenId))}"
      case TriadicFilter(_, positivePredicate, sourceId, targetId, triadicSelectionId) =>
        s"${triadicSelectionId.value.x}, $positivePredicate, ${wrapInQuotationsAndMkString(Seq(sourceId, targetId))}"
      case AssertSameNode(idName, _, _) =>
        wrapInQuotations(idName)
      case LockNodes(_, nodesToLock) => wrapInQuotationsAndMkString(nodesToLock)
      case Prober(_, _) => "Prober.NoopProbe" // We do not preserve the object reference through the string transformation
      case RemoveLabels(_, idName, labelNames) => wrapInQuotationsAndMkString(idName +: labelNames.map(_.name))
      case LoadCSV(_, url, variableName, format, fieldTerminator, _, _) =>
        val fieldTerminatorStr = fieldTerminator.fold("None")(ft => s"Some(${wrapInQuotations(ft)})")
        Seq(wrapInQuotations(expressionStringifier(url)), wrapInQuotations(variableName), format, fieldTerminatorStr).mkString(", ")
      case Apply(_, _, fromSubquery) => fromSubquery.toString
    }
    val plansWithContent2: PartialFunction[LogicalPlan, String] = {
      case MultiNodeIndexSeek(indexSeekLeafPlans: Seq[NodeIndexSeekLeafPlan]) =>
        indexSeekLeafPlans.map(p => s"_.nodeIndexSeek(${plansWithContent(p)})").mkString(", ")
    }
    plansWithContent.orElse(plansWithContent2).applyOrElse(logicalPlan, (_: LogicalPlan) => "")
  }

  private def queryExpressionStr(valueExpr: QueryExpression[Expression],
                                 propNames: Seq[String]): String = {
    valueExpr match {
      case SingleQueryExpression(expression)                                     => s"${propNames.head} = ${expressionStringifier(expression)}"
      case ManyQueryExpression(ListLiteral(expressions))                         => s"${propNames.head} = ${expressions.map(expressionStringifier(_)).mkString(" OR ")}"
      case ManyQueryExpression(expr)                                             => s"${propNames.head} IN ${expressionStringifier(expr)}"
      case ExistenceQueryExpression()                                            => propNames.head
      case RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(expression))) => s"${propNames.head} STARTS WITH ${expressionStringifier(expression)}"
      case RangeQueryExpression(InequalitySeekRangeWrapper(range))               => rangeStr(range, propNames.head).toString
      case CompositeQueryExpression(inner)                                       => inner.zip(propNames).map { case (qe, propName) => queryExpressionStr(qe, Seq(propName)) }.mkString(", ")
      case _                                                                     => ""
    }
  }

  case class RangeStr(pre: Option[(String, String)], expr: String, post: (String, String)) {
    override def toString: String = {
      val preStr = pre match {
        case Some((vl, sign)) => s"$vl $sign "
        case None => ""
      }
      val postStr = s" ${post._1} ${post._2}"
      s"$preStr$expr$postStr"
    }
  }

  private def rangeStr(range: InequalitySeekRange[Expression], propName: String): RangeStr = {
    range match {
      case RangeGreaterThan(NonEmptyList(ExclusiveBound(expression))) => RangeStr(None, propName, (">", expressionStringifier(expression)))
      case RangeGreaterThan(NonEmptyList(InclusiveBound(expression))) => RangeStr(None, propName, (">=", expressionStringifier(expression)))
      case RangeLessThan(NonEmptyList(ExclusiveBound(expression))) => RangeStr(None, propName, ("<", expressionStringifier(expression)))
      case RangeLessThan(NonEmptyList(InclusiveBound(expression))) => RangeStr(None, propName, ("<=", expressionStringifier(expression)))
      case RangeBetween(greaterThan, lessThan) =>
        val gt = rangeStr(greaterThan, propName)
        val lt = rangeStr(lessThan, propName)
        val pre = (gt.post._2, switchInequalitySign(gt.post._1))
        RangeStr(Some(pre), propName, lt.post)
    }
  }

  private def switchInequalitySign(s: String): String = switchInequalitySign(s.head) +: s.tail

  private def switchInequalitySign(c: Char): Char = c match {
    case '>' => '<'
    case '<' => '>'
  }

  private def nodeIndexOperator(idName: String,
                            labelToken: LabelToken,
                            properties: Seq[IndexedProperty],
                            argumentIds: Set[String],
                            indexOrder: IndexOrder,
                            unique: Boolean,
                            parenthesesContent: String): String = {
    val indexStr = s"$idName:${labelToken.name}($parenthesesContent)"
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapInQuotationsAndMkString(argumentIds)})"
    val uniqueStr = s", unique = $unique"
    val getValueBehavior = properties.map(_.getValueFromIndex).reduce {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException("Index operators with different getValueFromIndex behaviors not supported.")
        }
    }
    val getValueStr = s", getValue = ${objectName(getValueBehavior)}"
    s""" "$indexStr"$indexOrderStr$argStr$getValueStr$uniqueStr """.trim
  }

  private def relationshipIndexOperator(idName: String,
                                        start: String,
                                        end: String,
                                        typeToken: RelationshipTypeToken,
                                        properties: Seq[IndexedProperty],
                                        argumentIds: Set[String],
                                        indexOrder: IndexOrder,
                                        directed: Boolean,
                                        parenthesesContent: String): String = {
    val rarrow = if (directed) "->" else "-"
    val indexStr = s"($start)-[$idName:${typeToken.name}($parenthesesContent)]$rarrow($end)"
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapInQuotationsAndMkString(argumentIds)})"
    val getValueBehavior = properties.map(_.getValueFromIndex).reduce {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException("Index operators with different getValueFromIndex behaviors not supported.")
        }
    }
    val getValueStr = s", getValue = ${objectName(getValueBehavior)}"
    s""" "$indexStr"$indexOrderStr$argStr$getValueStr """.trim
  }

  private def createNodeToString(createNode: CreateNode) = createNode match {
    case CreateNode(idName, labels, None) =>
      s"createNode(${wrapInQuotationsAndMkString(idName +: labels.map(_.name))})"
    case CreateNode(idName, labels, Some(props)) =>
      s"createNodeWithProperties(${wrapInQuotations(idName)}, Seq(${wrapInQuotationsAndMkString(labels.map(_.name))}), ${wrapInQuotations(expressionStringifier(props))})"
  }

  private def createRelationshipToString(rel: CreateRelationship) =  {
    val propString = rel.properties.map(p => s", Some(${wrapInQuotations(expressionStringifier(p))})").getOrElse("")
    s"createRelationship(${wrapInQuotationsAndMkString(Seq(rel.idName, rel.leftNode, rel.relType.name, rel.rightNode))}, ${rel.direction}$propString)"
  }

  private def mutationToString(op: SimpleMutatingPattern): String = op match {
    case CreatePattern(nodes, relationships) =>
      s"createPattern(Seq(${nodes.map(createNodeToString).mkString(", ")}), Seq(${relationships.map(createRelationshipToString).mkString(", ")}))"
    case org.neo4j.cypher.internal.ir.DeleteExpression(expression, forced) =>
      s"delete(${wrapInQuotations(expressionStringifier(expression))}, $forced)"
    case SetLabelPattern(node, labelNames) =>
      s"setLabel(${wrapInQuotationsAndMkString(node +: labelNames.map(_.name))})"
    case RemoveLabelPattern(node, labelNames) =>
      s"removeLabel(${wrapInQuotationsAndMkString(node +: labelNames.map(_.name))})"
    case SetNodePropertyPattern(node, propertyKey, value) =>
      s"setNodeProperty(${wrapInQuotationsAndMkString(Seq(node, propertyKey.name, expressionStringifier(value)))})"
    case SetRelationshipPropertyPattern(relationship, propertyKey, value) =>
      s"setRelationshipProperty(${wrapInQuotationsAndMkString(Seq(relationship, propertyKey.name, expressionStringifier(value)))})"
    case SetNodePropertiesFromMapPattern(idName, expression, removeOtherProps) =>
      s"setNodePropertiesFromMap(${wrapInQuotationsAndMkString(Seq(idName, expressionStringifier(expression)))}, $removeOtherProps)"
    case SetRelationshipPropertiesFromMapPattern(idName, expression, removeOtherProps) =>
      s"setRelationshipPropertiesFromMap(${wrapInQuotationsAndMkString(Seq(idName, expressionStringifier(expression)))}, $removeOtherProps)"
    case SetPropertyPattern(entityExpression, propertyKey, value) =>
      s"setProperty(${wrapInQuotationsAndMkString(Seq(expressionStringifier(entityExpression), propertyKey.name, expressionStringifier(value)))})"
    case SetPropertiesFromMapPattern(entityExpression, map, removeOtherProps) =>
      s"setPropertyFromMap(${wrapInQuotationsAndMkString(Seq(expressionStringifier(entityExpression), expressionStringifier(map)))}, $removeOtherProps)"
  }

  private def pointDistanceNodeIndexSeek(idName: String,
                                         labelToken: LabelToken,
                                         properties: Seq[IndexedProperty],
                                         point: Expression,
                                         distance: Expression,
                                         argumentIds: Set[String],
                                         indexOrder: IndexOrder,
                                         inclusive: Boolean): String = {
    val propName = properties.head.propertyKeyToken.name
    val indexOrderStr = ", indexOrder = " + objectName(indexOrder)
    val argStr = s", argumentIds = Set(${wrapInQuotationsAndMkString(argumentIds)})"
    val inclusiveStr = s", inclusive = $inclusive"
    val getValueBehavior = properties.map(_.getValueFromIndex).reduce {
      (v1, v2) =>
        if (v1 == v2) {
          v1
        } else {
          throw new UnsupportedOperationException("Index operators with different getValueFromIndex behaviors not supported.")
        }
    }
    val getValueStr = s", getValue = ${objectName(getValueBehavior)}"
    s""" "$idName", "${labelToken.name}", "$propName", "${expressionStringifier(point)}", ${expressionStringifier(distance)}$indexOrderStr$argStr$getValueStr$inclusiveStr """.trim
  }

  private def idsStr(ids: SeekableArgs) = {
    val idsStr = ids match {
      case SingleSeekableArg(expr) => expressionStringifier(expr)
      case ManySeekableArgs(ListLiteral(expressions)) => expressions.map(expressionStringifier(_)).mkString(", ")
      // This will not be working code, but can still be useful during debugging.
      case ManySeekableArgs(expr) => expressionStringifier(expr)
    }
    idsStr
  }

  private def integerString(count: Expression) = {
    count match {
      case SignedDecimalIntegerLiteral(i) => i
      case _ => "/* " + count + "*/"
    }
  }

  private def sortItemsStr(sortItems: Seq[ColumnOrder]) = {
    sortItems.map(sortItemStr).mkString("Seq(", ", ", ")")
  }

  private def sortItemStr(si: ColumnOrder): String =  s""" ${si.getClass.getSimpleName}("${si.id}") """.trim

  private def variablePredicate(nodePredicate: Option[VariablePredicate], name: String) = {
    nodePredicate.map(vp => s""", $name = Predicate("${vp.variable.name}", "${expressionStringifier(vp.predicate)}") """.trim).getOrElse("")
  }

  private def relTypeStr(types: Seq[RelTypeName]) = {
    types match {
      case Seq() => ""
      case head +: tail => s":${head.name}${tail.map(t => s"|${t.name}").mkString("")}"
    }
  }

  private def projectStrs(map: Map[String, Expression]): String = wrapInQuotationsAndMkString(map.map {
    case (alias, expr) => s"${expressionStringifier(expr)} AS ${escapeIdentifier(alias)}"
  })

  private def escapeIdentifier(alias: String) = {
    if (alias.matches("\\w+")) alias else s"`$alias`"
  }

  private def wrapInQuotations(c: String): String = "\"" + c + "\""

  private def wrapInQuotationsAndMkString(strings: Iterable[String]): String = strings.map(wrapInQuotations).mkString(", ")

  private def objectName(obj: AnyRef): String = {
    val str = obj.getClass.getSimpleName
    str.substring(0, str.length - 1)
  }

  private def arrows(dir: SemanticDirection): (String, String) = dir match {
    case SemanticDirection.OUTGOING => ("-", "->")
    case SemanticDirection.INCOMING => ("<-", "-")
    case SemanticDirection.BOTH => ("-", "-")
  }
}
