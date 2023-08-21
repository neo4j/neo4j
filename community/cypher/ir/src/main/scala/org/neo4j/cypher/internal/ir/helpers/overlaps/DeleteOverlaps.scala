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

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.label_expressions.NodeLabels
import org.neo4j.cypher.internal.label_expressions.SolvableLabelExpression

import scala.annotation.tailrec

object DeleteOverlaps {

  /**
   * Maximum number of unique labels that Delete.overlap will process.
   * Number of solutions for an expression grows 2 to thew power of n, be careful.
   */
  val maximumNumberOfUniqueLabels: Int = 8

  /**
   * Checks whether the predicates on both nodes allow for a possible overlap based on labels only.
   * If there is an overlap, it only calculates the first example for performance reasons, this could easily be changed in the future if needed.
   * Note that there is no checks on the variable name in the predicates, it assumes that they have been filtered previously.
   *
   * @param predicatesOnRead predicates of the (unstable) node in a read clause.
   * @param predicatesOnDelete predicates of the node being deleted.
   * @return Whether their might be an overlap between the two nodes based on their respective labels.
   */
  def overlap(predicatesOnRead: Seq[Expression], predicatesOnDelete: Seq[Expression]): Result = {
    // Ignoring some expressions may only lead to additional Eager plans as this function is conservative by default – it considers that two nodes overlap unless it can prove the contrary.
    // We can't possibly handles all possible expressions here, though the exact specification may evolve over time, we need to draw a line somewhere.
    val (unsupportedExpressions, labelExpressions) = extractLabelExpressions(predicatesOnRead ++ predicatesOnDelete)

    SolvableLabelExpression.allSolutions(labelExpressions).headOption match {
      case None           => NoLabelOverlap
      case Some(solution) => Overlap(unsupportedExpressions, solution)
    }
  }

  sealed trait Result

  /**
   * Proves that there is no overlap because of labels.
   */
  case object NoLabelOverlap extends Result

  /**
   * Details of the potential overlap on delete.
   * @param unprocessedExpressions The expressions that were not processed by the evaluator.
   * @param labelsOverlap example of a combination of labels that would lead to an overlap.
   */
  case class Overlap(unprocessedExpressions: Seq[Expression], labelsOverlap: NodeLabels) extends Result

  /**
   * Tries to extract a label expression from each expression.
   * It will stop if the total number of unique labels goes beyond [[maximumNumberOfUniqueLabels]].
   * Alongside all the valid label expressions, it returns all the expressions that are not valid label expressions as well as all the remaining expressions if the maximum number of unique labels was reached.
   *
   * @return A tuple containing expressions that couldn't be processed and the extracted label expressions
   */
  private def extractLabelExpressions(expressions: Seq[Expression]): (Seq[Expression], Seq[SolvableLabelExpression]) =
    extractLabelExpressionsRec(expressions.flatMap(Expressions.splitExpression).toList, Nil, Nil, Set.empty)

  @tailrec
  private def extractLabelExpressionsRec(
    expressions: List[Expression],
    invalid: List[Expression],
    labelExpressions: List[SolvableLabelExpression],
    allLabels: Set[String]
  ): (List[Expression], List[SolvableLabelExpression]) =
    expressions match {
      case Nil          => (invalid.reverse, labelExpressions.reverse)
      case head :: tail =>
        // Note that it goes through the whole "head" expression first before checking the total number of labels
        // We would have to change the definition of extractLabelExpression to make it stop as soon as the limit is reached
        Expressions.extractLabelExpression(head) match {
          case Some(labelExpression) =>
            val newLabels = allLabels.union(labelExpression.allLabels)
            // The number of label combinations grows exponentially, and so we need to cap it to keep eagerness analysis reasonably quick.
            // Ignoring label expressions may only lead to extra eagerness as we assume overlap by default.
            if (newLabels.size > maximumNumberOfUniqueLabels)
              ((head :: tail) ++ invalid, labelExpressions)
            else
              extractLabelExpressionsRec(tail, invalid, labelExpression :: labelExpressions, newLabels)
          case None =>
            extractLabelExpressionsRec(tail, head :: invalid, labelExpressions, allLabels)
        }
    }
}
