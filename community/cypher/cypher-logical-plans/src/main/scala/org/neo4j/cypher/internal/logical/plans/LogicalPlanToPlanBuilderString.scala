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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.util.NonEmptyList

object LogicalPlanToPlanBuilderString {
  private val expressionStringifier = ExpressionStringifier(_.asCanonicalStringVal, preferSingleQuotes = true)

  private case class LevelPlanItem(level: Int, plan: LogicalPlan)

  /**
   * Generates a string that plays nicely together with `AbstractLogicalPlanBuilder`.
   */
  def apply(logicalPlan: LogicalPlan): String = {
    var childrenStack = LevelPlanItem(0, logicalPlan) :: Nil
    val sb = new StringBuilder()

    while (childrenStack.nonEmpty) {
      val LevelPlanItem(level, plan) = childrenStack.head
      childrenStack = childrenStack.tail

      sb ++= ".|" * level

      sb += '.'
      sb ++= pre(plan)
      sb += '('
      sb ++= par(plan)
      sb += ')'
      sb ++= System.lineSeparator()

      plan.lhs.foreach(lhs => childrenStack ::= LevelPlanItem(level, lhs))
      plan.rhs.foreach(rhs => childrenStack ::= LevelPlanItem(level + 1, rhs))
    }

    sb ++= ".build()"

    sb.toString()
  }

  private def pre(logicalPlan: LogicalPlan): String = {
    val specialCases: PartialFunction[LogicalPlan, String] = {
      case _:ProduceResult => "produceResults"
      case _:AllNodesScan => "allNodeScan"
      case e:Expand if e.mode == ExpandAll && e.expandProperties.nonEmpty => "expand"
      case e:Expand => if(e.mode == ExpandAll) "expandAll" else "expandInto"
      case _:VarExpand => "expand"
      case e:OptionalExpand => if(e.mode == ExpandAll) "optionalExpandAll" else "optionalExpandInto"
      case _:Selection => "filter"
      case _:UnwindCollection => "unwind"
      case _:FindShortestPaths => "shortestPath"
      case _:NodeIndexScan => "nodeIndexOperator"
      case NodeIndexSeek(_, _, _, RangeQueryExpression(PointDistanceSeekRangeWrapper(_)), _, _) => "pointDistanceIndexSeek"
      case _:NodeIndexSeek => "nodeIndexOperator"
      case _:NodeUniqueIndexSeek => "nodeIndexOperator"
      case _:NodeIndexContainsScan => "nodeIndexOperator"
      case _:NodeIndexEndsWithScan => "nodeIndexOperator"
      case _:MultiNodeIndexSeek => "multiNodeIndexSeekOperator"
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
      case Create(_, nodes, _) =>
        nodes.map(createNode => "createNode(" + wrapInQuotationsAndMkString(createNode.idName +: createNode.labels.map(_.name)) + ")").mkString(", ")
      case Expand(_, from, dir, _, to, relName, _, expandProperties) =>
        val (dirStrA, dirStrB) = arrows(dir)
        s""" "($from)$dirStrA[$relName]$dirStrB($to)"${propertiesToCache(expandProperties)}""".trim
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
              case VarPatternLength(min, max) => s"${min}..${max.getOrElse("")}"
              case _ => throw new IllegalStateException("Shortest path should have a variable length pattern")
            }
            val (dirStrA, dirStrB) = arrows(dir)
            val typeStr = relTypeStr(types)
            val pNameStr = maybePathName.map(p => s", pathName = Some(${wrapInQuotations(p)})").getOrElse("")
            val allStr = if (single) "" else ", all = true"
            val predStr = if(predicates.isEmpty) "" else ", predicates = Seq(" + wrapInQuotationsAndMkString(predicates.map(expressionStringifier(_))) +")"
            s""" "($from)$dirStrA[$relName$typeStr*$lenStr]$dirStrB($to)"$pNameStr$allStr$predStr$fbStr$dsnStr """.trim
        }
      case PruningVarExpand(_, from, dir, types, to, minLength, maxLength, nodePredicate, relationshipPredicate) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val typeStr = relTypeStr(types)
        val lenStr = s"$minLength..$maxLength"
        val nPredStr = variablePredicate(nodePredicate, "nodePredicate")
        val rPredStr = variablePredicate(relationshipPredicate, "relationshipPredicate")
        s""" "($from)$dirStrA[$typeStr*$lenStr]$dirStrB($to)"$nPredStr$rPredStr """.trim
      case Limit(_, count, _) =>
        integerString(count)
      case Skip(_, count) =>
        integerString(count)
      case NodeByLabelScan(idName, label, argumentIds, indexOrder) =>
        s""" "$idName", "${label.name}", ${objectName(indexOrder)}, ${wrapInQuotationsAndMkString(argumentIds)} """.trim
      case Optional(_, protectedSymbols) =>
        wrapInQuotationsAndMkString(protectedSymbols)
      case OptionalExpand(_, from, dir, _, to, relName, _, predicate, expandProperties) =>
        val (dirStrA, dirStrB) = arrows(dir)
        val predStr = predicate.fold("")(p => s""", Some("${expressionStringifier(p)}")""")
        s""" "($from)$dirStrA[$relName]$dirStrB($to)"$predStr${propertiesToCache(expandProperties)}""".trim
      case ProcedureCall(_, ResolvedCall(ProcedureSignature(QualifiedName(namespace, name), _, _, _, _, _, _, _, _, _), callArguments, callResults, _, _)) =>
        val yielding = if(callResults.isEmpty) "" else callResults.map(i => expressionStringifier(i.variable)).mkString(" YIELD ", ",", "")
        s""" "${namespace.mkString(".")}.$name(${callArguments.map(expressionStringifier(_)).mkString(", ")})$yielding" """.trim
      case ProduceResult(_, columns) =>
        wrapInQuotationsAndMkString(columns)
      case ProjectEndpoints(_, relName, start, startInScope, end, endInScope, types, directed, length) =>
        val (dirStrA, dirStrB) = arrows(if (directed) OUTGOING else BOTH)
        val typeStr = relTypeStr(types.getOrElse(Seq.empty))
        val lenStr = length match {
          case SimplePatternLength => ""
          case VarPatternLength(min, max) => s"*${min}..${max.getOrElse("")}"
        }
        s""" "($start)$dirStrA[$relName$typeStr$lenStr]$dirStrB($end)", $startInScope, $endInScope """.trim
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
      case PartialSort(_, alreadySortedPrefix, stillToSortSuffix) =>
        val asStr = sortItemsStr(alreadySortedPrefix)
        val stsStr = sortItemsStr(stillToSortSuffix)
        s""" $asStr, $stsStr """.trim
      case PartialTop(_, alreadySortedPrefix, stillToSortSuffix, limit) =>
        val asStr = sortItemsStr(alreadySortedPrefix)
        val stsStr = sortItemsStr(stillToSortSuffix)
        val lStr = integerString(limit)
        s""" $asStr, $stsStr, $lStr """.trim
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
      case SetProperty(_, entity, propertyKey, value) =>
        wrapInQuotationsAndMkString(Seq(expressionStringifier(entity), propertyKey.name, expressionStringifier(value)))
      case SetNodeProperty(_, idName, propertyKey, value) =>
        wrapInQuotationsAndMkString(Seq(idName, propertyKey.name, expressionStringifier(value)))
      case SetRelationshipProperty(_, idName, propertyKey, value) =>
        wrapInQuotationsAndMkString(Seq(idName, propertyKey.name, expressionStringifier(value)))
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
      case NodeIndexScan(idName, labelToken, properties, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        indexOperator(idName, labelToken, properties, argumentIds, indexOrder, unique = false, propNames.mkString(", "))
      case NodeIndexContainsScan(idName, labelToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        indexOperator(idName, labelToken, Seq(property), argumentIds, indexOrder, unique = false, s"$propName CONTAINS ${expressionStringifier(valueExpr)}")
      case NodeIndexEndsWithScan(idName, labelToken, property, valueExpr, argumentIds, indexOrder) =>
        val propName = property.propertyKeyToken.name
        indexOperator(idName, labelToken, Seq(property), argumentIds, indexOrder, unique = false, s"$propName ENDS WITH ${expressionStringifier(valueExpr)}")
      case NodeIndexSeek(idName, labelToken, properties, RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(FunctionInvocation(_, FunctionName("point"), _, args), distance, inclusive))), argumentIds, indexOrder) =>
        pointDistanceIndexSeek(idName, labelToken, properties, args.head, distance, argumentIds, indexOrder, inclusive = inclusive)
      case NodeIndexSeek(idName, labelToken, properties, valueExpr, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        indexOperator(idName, labelToken, properties, argumentIds, indexOrder, unique = false, queryStr)
      case NodeUniqueIndexSeek(idName, labelToken, properties, valueExpr, argumentIds, indexOrder) =>
        val propNames = properties.map(_.propertyKeyToken.name)
        val queryStr = queryExpressionStr(valueExpr, propNames)
        indexOperator(idName, labelToken, properties, argumentIds, indexOrder, unique = true, queryStr)
      case RollUpApply(_, _, collectionName, variableToCollect) =>
        s"""${wrapInQuotations(collectionName)}, ${wrapInQuotations(variableToCollect)}"""
      case ConditionalApply(_, _, items) => wrapInQuotationsAndMkString(items)
      case AntiConditionalApply(_, _, items) => wrapInQuotationsAndMkString(items)
      case LetSemiApply(_, _, idName) => wrapInQuotations(idName)
      case LetAntiSemiApply(_, _, idName) => wrapInQuotations(idName)
    }
    val plansWithContent2: PartialFunction[LogicalPlan, String] = {
      case MultiNodeIndexSeek(indexSeekLeafPlans: Seq[IndexSeekLeafPlan]) =>
        indexSeekLeafPlans.map(p => s"_.indexSeek(${plansWithContent(p)})").mkString(", ")
    }
    plansWithContent.orElse(plansWithContent2).applyOrElse(logicalPlan, (_: LogicalPlan) => "")
  }

  private def propertiesToCache(expandProperties: Option[ExpandCursorProperties]) = {
    expandProperties.map(p => {
      val sb = new StringBuilder
      if (p.nodeProperties.nonEmpty) {
        sb.append(", ")
        sb.append(s"""cacheNodeProperties = Seq(${p.nodeProperties.map(p => s""""${p.propertyKeyName.name}"""").mkString(", ")})""")
      }
      if (p.relProperties.nonEmpty) {
        sb.append(", ")
        sb.append(s"""cacheRelProperties = Seq(${p.relProperties.map(p => s""""${p.propertyKeyName.name}"""").mkString(", ")})"""
        )
      }
      sb.toString()
    }).getOrElse(" ")
  }

  private def queryExpressionStr(valueExpr: QueryExpression[Expression],
                                 propNames: Seq[String]): String = {
    valueExpr match {
      case SingleQueryExpression(expression) => s"${propNames.head} = ${expressionStringifier(expression)}"
      case ManyQueryExpression(ListLiteral(expressions)) => s"${propNames.head} = ${expressions.map(expressionStringifier(_)).mkString(" OR ")}"
      case RangeQueryExpression(InequalitySeekRangeWrapper(range)) =>
        rangeStr(range, propNames.head).toString
      case RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(expression))) => s"${propNames.head} STARTS WITH ${expressionStringifier(expression)}"
      case CompositeQueryExpression(inner) => inner.zip(propNames).map {
        case (qe, propName) => queryExpressionStr(qe, Seq(propName))
      }.mkString(", ")
      case x => ""
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

  private def indexOperator(idName: String,
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

  private def pointDistanceIndexSeek(idName: String,
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
      case _ => throw new UnsupportedOperationException(s"Id string cannot be constructed from $ids.")
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
    case (alias, expr) => s"${expressionStringifier(expr)} AS $alias"
  })

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
