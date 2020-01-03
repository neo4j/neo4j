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

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen
import org.neo4j.cypher.internal.v4_0.util.{InputPosition, LabelId, NonEmptyList, PropertyKeyId}

import scala.collection.mutable.ArrayBuffer

/**
  * Helper object for constructing node index operators from strings.
  */
object IndexSeek {

  // primitives
  private val ID = "([a-zA-Z][a-zA-Z0-9]*)"
  private val VALUE = "([0-9]+|'.*'|\\?\\?\\?)"
  private val INT = "([0-9]+)".r
  private val STRING = s"'(.*)'".r
  private val PARAM = "???"

  // entry point
  private val INDEX_SEEK_PATTERN = s"$ID: ?$ID ?\\(([^\\)]+)\\)".r

  // predicates
  private val EXACT = s"$ID ?= ?$VALUE".r
  private val EXACT_TWO = s"$ID = ?$VALUE OR ?$VALUE".r
  private val IN = s"$ID IN \\?\\?\\?".r
  private val EXISTS = s"$ID".r
  private val LESS_THAN = s"$ID ?< ?$VALUE".r
  private val LESS_THAN_OR_EQ = s"$ID ?<= ?$VALUE".r
  private val GREATER_THAN = s"$ID ?> ?$VALUE".r
  private val GREATER_THAN_OR_EQ = s"$ID ?>= ?$VALUE".r
  private val STARTS_WITH = s"$ID STARTS WITH $VALUE".r
  private val ENDS_WITH = s"$ID ENDS WITH $VALUE".r
  private val CONTAINS = s"$ID CONTAINS $VALUE".r

  private val pos = InputPosition.NONE

  /**
    * Extracts just the label from an index seek string
    */
  def labelFromIndexSeekString(indexSeekString: String): String = {
    val INDEX_SEEK_PATTERN(_, labelStr, _) = indexSeekString.trim
    labelStr
  }

  /**
    * Construct a node index seek/scan operator by parsing a string.
    */
  def apply(indexSeekString: String,
            getValue: GetValueFromIndexBehavior = DoNotGetValue,
            indexOrder: IndexOrder = IndexOrderNone,
            paramExpr: Option[Expression] = None,
            argumentIds: Set[String] = Set.empty,
            propIds: Option[PartialFunction[String, Int]] = None,
            labelId: Int = 0,
            unique: Boolean = false,
            customQueryExpression: Option[QueryExpression[Expression]] = None)(implicit idGen: IdGen): IndexLeafPlan = {

    val INDEX_SEEK_PATTERN(node, labelStr, predicateStr) = indexSeekString.trim
    val label = LabelToken(labelStr, LabelId(labelId))
    val predicates = predicateStr.split(',').map(_.trim)

    var propId = -1
    def nextPropId(): Int = {
      propId += 1
      propId
    }

    def prop(prop: String): IndexedProperty = {
      val id =
        if (propIds.isDefined) {
          val func = propIds.get
          if (func.isDefinedAt(prop)) {
            func(prop)
          } else {
            throw new IllegalArgumentException(s"Property `$prop` has no provided id. Either provide ids for all properties, or provide none.")
          }
        } else {
          nextPropId()
        }

      IndexedProperty(PropertyKeyToken(PropertyKeyName(prop)(pos), PropertyKeyId(id)), getValue)
    }

    def value(value: String): Expression =
      value match {
        case INT(int) => SignedDecimalIntegerLiteral(int)(pos)
        case STRING(str) => StringLiteral(str)(pos)
        case PARAM  => paramExpr.getOrElse(throw new IllegalArgumentException("Cannot use parameter syntax '???' without providing parameter expression 'paramExpr' to IndexSeek()"))
        case _ => throw new IllegalArgumentException(s"Value `$value` is not supported")
      }

    def createSeek(properties: Seq[IndexedProperty],
                   valueExpr: QueryExpression[Expression]): IndexSeekLeafPlan =
      if (unique) {
        NodeUniqueIndexSeek(node, label, properties, valueExpr, argumentIds, indexOrder)
      } else {
        NodeIndexSeek(node, label, properties, valueExpr, argumentIds, indexOrder)
      }

    if (predicates.length == 1) {
      predicates.head match {
        case EXACT_TWO(propStr, valueAStr, valueBStr) =>
          val valueExpr = ManyQueryExpression(ListLiteral(Seq(value(valueAStr), value(valueBStr)))(pos))
          createSeek(List(prop(propStr)), valueExpr)

        case IN(propStr) =>
          val expression = paramExpr.getOrElse(throw new IllegalArgumentException(
            "Cannot use parameter syntax '???' without providing parameter expression 'paramExpr' to IndexSeek()"))
          val valueExpr = ManyQueryExpression(expression)
          createSeek(List(prop(propStr)), valueExpr)

        case EXACT(propStr, valueStr) =>
          val valueExpr = SingleQueryExpression(value(valueStr))
          createSeek(List(prop(propStr)), valueExpr)

        case LESS_THAN(propStr, valueStr) =>
          val valueExpr = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(value(valueStr)))))(pos))
          createSeek(List(prop(propStr)), valueExpr)

        case LESS_THAN_OR_EQ(propStr, valueStr) =>
          val valueExpr = RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(InclusiveBound(value(valueStr)))))(pos))
          createSeek(List(prop(propStr)), valueExpr)

        case GREATER_THAN(propStr, valueStr) =>
          val valueExpr = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(value(valueStr)))))(pos))
          createSeek(List(prop(propStr)), valueExpr)

        case GREATER_THAN_OR_EQ(propStr, valueStr) =>
          val valueExpr = RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(InclusiveBound(value(valueStr)))))(pos))
          createSeek(List(prop(propStr)), valueExpr)

        case STARTS_WITH(propStr, string) =>
          val valueExpr = RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(value(string)))(pos))
          createSeek(List(prop(propStr)), valueExpr)

        case ENDS_WITH(propStr, string) =>
          NodeIndexEndsWithScan(node, label, prop(propStr), value(string), argumentIds, indexOrder)

        case CONTAINS(propStr, string) =>
          NodeIndexContainsScan(node, label, prop(propStr), value(string), argumentIds, indexOrder)

        case EXISTS(propStr) if customQueryExpression.isDefined =>
          createSeek(List(prop(propStr)), customQueryExpression.get)

        case EXISTS(propStr) =>
          NodeIndexScan(node, label, Seq(prop(propStr)), argumentIds, indexOrder)
      }
    } else if (predicates.length > 1) {
      val properties = new ArrayBuffer[IndexedProperty]()
      val valueExprs = new ArrayBuffer[QueryExpression[Expression]]()

      val equalityPred = predicates.takeWhile {
        case EXACT_TWO(_, _, _) => true
        case EXACT(_, _) => true
        case _ => false
      }
      val equalityAndNextPred =
        if(equalityPred sameElements predicates) equalityPred
        else equalityPred :+ predicates(equalityPred.length)

      val restOfPred = predicates.slice(equalityAndNextPred.length, predicates.length)

      for (predicate <- equalityAndNextPred)
        predicate match {
          case EXACT_TWO(propStr, valueAStr, valueBStr) =>
            valueExprs += ManyQueryExpression(ListLiteral(Seq(value(valueAStr), value(valueBStr)))(pos))
            properties += prop(propStr)
          case EXACT(propStr, valueStr) =>
            valueExprs += SingleQueryExpression(value(valueStr))
            properties += prop(propStr)
          case LESS_THAN(propStr, valueStr) =>
            valueExprs += RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(value(valueStr)))))(pos))
            properties += prop(propStr)
          case LESS_THAN_OR_EQ(propStr, valueStr) =>
            valueExprs += RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(InclusiveBound(value(valueStr)))))(pos))
            properties += prop(propStr)
          case GREATER_THAN(propStr, valueStr) =>
            valueExprs += RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(ExclusiveBound(value(valueStr)))))(pos))
            properties += prop(propStr)
          case GREATER_THAN_OR_EQ(propStr, valueStr) =>
            valueExprs += RangeQueryExpression(InequalitySeekRangeWrapper(RangeGreaterThan(NonEmptyList(InclusiveBound(value(valueStr)))))(pos))
            properties += prop(propStr)
          case STARTS_WITH(propStr, string) =>
            valueExprs += RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(value(string)))(pos))
            properties += prop(propStr)
          case EXISTS(propStr) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case ENDS_WITH(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case CONTAINS(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case _ => throw new IllegalArgumentException(s"$predicate is not allowed in composite seeks.")
        }

      for(predicate <- restOfPred)
        predicate match {
          case EXACT_TWO(propStr, _, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case EXACT(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case LESS_THAN(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case LESS_THAN_OR_EQ(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case GREATER_THAN(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case GREATER_THAN_OR_EQ(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case STARTS_WITH(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case EXISTS(propStr) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case ENDS_WITH(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case CONTAINS(propStr, _) =>
            valueExprs += ExistenceQueryExpression()
            properties += prop(propStr)
          case _ => throw new IllegalArgumentException(s"$predicate is not allowed in composite seeks.")
        }

      if (equalityAndNextPred.length == 1 && (equalityAndNextPred.head match {
        case EXISTS(_) => true
        case ENDS_WITH(_, _) => true
        case CONTAINS(_, _) => true
        case _ => false
      }))
        NodeIndexScan(node, label, properties, argumentIds, indexOrder)
      else
        createSeek(properties, CompositeQueryExpression(valueExprs))
    } else
      throw new IllegalArgumentException(s"Cannot parse `$indexSeekString` as a index seek.")
  }
}
