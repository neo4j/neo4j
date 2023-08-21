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
package org.neo4j.cypher.internal.ir.helpers.overlaps

import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.label_expressions.SolvableLabelExpression
import org.neo4j.cypher.internal.util.tailrec.TailRecOption

import scala.annotation.tailrec

object Expressions {

  /**
   * Recursively split an expression into a list of conjoint expressions.
   * {{{
   *    splitExpression(Ands(Or(a, b), c, And(d, !e))) = List(Or(a, b), c, d, !e)
   * }}}
   */
  def splitExpression(expression: Expression): List[Expression] = {
    @tailrec
    def splitExpressionRec(expressions: List[Expression], splitExpressions: List[Expression]): List[Expression] =
      expressions match {
        case Nil                       => splitExpressions
        case And(lhs, rhs) :: tail     => splitExpressionRec(lhs :: rhs :: tail, splitExpressions)
        case Ands(conjunction) :: tail => splitExpressionRec(conjunction.toList ++ tail, splitExpressions)
        case atom :: tail              => splitExpressionRec(tail, atom :: splitExpressions)
      }

    splitExpressionRec(List(expression), Nil).reverse
  }

  /**
   * Extract the property key name if the expression is either specifying the value of a property or merely requiring the existence of a property.
   * It will extract the 'quantity' property in the following examples:
   * {{{
   *   item.quantity = 42 => Some(quantity)
   *   item.quantity IN [1, 6, 21, 107] => Some(quantity)
   *   item.quantity IS NOT NULL => Some(quantity)
   * }}}
   * Any other type of expression will be rejected, including logic combinators. For example, the following expression will get rejected:
   * {{{
   *   item.quantity = 42 AND (item.quantity = 10 OR item.offer IS NOT NULL) => None
   * }}}
   * Note how in this last example, item.quantity = 42 could have been evaluated independently, consider using [[splitExpression]] before calling this function.
   */
  def extractPropertyExpression(expression: Expression): Option[PropertyKeyName] =
    expression match {
      // When running this from LP eager analysis, In can have been rewritten back to Equals
      case Equals(property: Property, _) => Some(property.propertyKey)
      case Equals(_, property: Property) => Some(property.propertyKey)
      case In(property: Property, _)     => Some(property.propertyKey)
      case IsNotNull(property: Property) => Some(property.propertyKey)
      case _                             => None
    }

  /**
   * Extract a label expression if the expression matches either of:
   *   - The label wildcard (:%)
   *   - A specific label (:Item)
   *   - A logic combinator: NOT, AND(s), OR(s), XOR where sub-expressions are all label expressions
   * This will reject the combination of label expressions with other types of expressions:
   * {{{
   *   (:Item) AND (NOT (:Deleted) OR item.quantity IS NOT NULL) => None
   * }}}
   * Note how in this last example, (:Item) could have been evaluated independently, consider using [[splitExpression]] before calling this function.
   */
  def extractLabelExpression(expression: Expression): Option[SolvableLabelExpression] =
    extractLabelExpressionRec(expression).result

  private def extractLabelExpressionRec(expression: Expression): TailRecOption[SolvableLabelExpression] =
    expression match {
      case HasALabel(_) =>
        TailRecOption.some(SolvableLabelExpression.wildcard)
      case HasLabels(_, Seq(label)) =>
        TailRecOption.some(SolvableLabelExpression.label(label.name))
      case HasTypes(_, Seq(relType)) =>
        // Note: The logic for RelType and Labels is currently the same when using overlap logic so we can consider the RelType to be a Label in this instance
        // but this should be updated when specific overlap logic for RelTypes is added.
        TailRecOption.some(SolvableLabelExpression.label(relType.name))
      case Not(not) =>
        TailRecOption.tailcall(extractLabelExpressionRec(not)).map(_.not)
      case And(lhs, rhs) =>
        TailRecOption.map2(extractLabelExpressionRec(lhs), extractLabelExpressionRec(rhs))(_.and(_))
      case Ands(conjointExpressions) =>
        TailRecOption.traverse(conjointExpressions.toList)(extractLabelExpressionRec).map(_.reduceLeft(_.and(_)))
      case Or(lhs, rhs) =>
        TailRecOption.map2(extractLabelExpressionRec(lhs), extractLabelExpressionRec(rhs))(_.or(_))
      case Ors(disjointExpressions) =>
        TailRecOption.traverse(disjointExpressions.toList)(extractLabelExpressionRec).map(_.reduceLeft(_.or(_)))
      case Xor(lhs, rhs) =>
        TailRecOption.map2(extractLabelExpressionRec(lhs), extractLabelExpressionRec(rhs))(_.xor(_))
      case _ =>
        TailRecOption.none
    }
}
