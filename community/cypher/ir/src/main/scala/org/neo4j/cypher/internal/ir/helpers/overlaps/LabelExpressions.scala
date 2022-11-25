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
package org.neo4j.cypher.internal.ir.helpers.overlaps

import org.neo4j.cypher.internal.ir.helpers.overlaps.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.ir.helpers.overlaps.NodeLabels.LabelName
import org.neo4j.cypher.internal.ir.helpers.overlaps.NodeLabels.SomeUnknownLabels

/**
 * Conjunction of zero or more label expressions, builds a lazy list of all the solutions.
 * Used as an accumulator when folding a list of conjoint label expressions.
 * {{{[:%, :!(A&B), :C].foldLeft(LabelExpressions.any)(_ and _)}}} is equivalent to {{{(:% & !(A&B) & C).solution}}} only more efficient.
 * Instead of generating all candidates upfront, and filtering them one by one, this generates and prunes candidates on the fly and will terminate as early as possible.
 * @param allKnownLabels Union of all the label names explicitly mentioned in the accumulated label expressions.
 * @param solutions Set of solutions of the conjunction of labels expressions, in no particular order. No computation happens before the first value gets evaluated.
 */
case class LabelExpressions(
  allKnownLabels: Set[LabelName],
  solutions: Stream[NodeLabels],
  rejectedCandidates: Stream[NodeLabels]
) {

  /**
   * Add a new expression to the accumulated conjunction.
   * If the expression refers to labels that haven't been encountered so far, we first grow the list of candidates to account for it.
   * We then shrink that list down by matching each candidate against the label expression.
   *
   * For example, if we want to find all the solution for the following node pattern (:% & !(A&B) & C), we evaluate:
   * {{{[:%, :!(A&B), :C].foldLeft(LabelExpressions.any)(_ and _)}}}
   * Here is how it unrolls using ∅ to represent the empty set of labels, and ? to represent a non-empty set of yet unknown labels:
   * {{{
   *    Start: {∅, ?}
   *    .and(:%):
   *      (:%).allLabels is empty, no need to grow our list of solutions: {∅, ?}
   *      solutions.filter(:%) rules out ∅: {?}
   *    .and(:!(A&B)):
   *      (:!(A&B)).allLabels introduces labels A and B, solutions grows to: {A, B, AB, ?}
   *      solutions.filter(:!(A&B)) rules out AB: {A, B, ?}
   *    .and(:C):
   *      (:C).allLabels introduces label C, solutions grows to: {AC, BC, C, ?}
   *      solutions.filter(:C) rules out ?: {AC, BC, C}
   * }}}
   *
   * Note that {AC, BC, C} is the set of all solutions in the context of {A, B, C} being all the known labels so far.
   * AC really represents {A, C} and any combination of labels not in {A, B, C} as it is impossible to match on AC exclusively.
   * There is one exception: the empty set of labels ∅. We can specifically match on it using (:!%), and so ∅ exclusively refers to empty set of labels regardless of context.
   *
   * This only builds the computation, but does not actually execute it, no evaluation happens before the first value gets pulled.
   */
  def and(expression: LabelExpression): LabelExpressions = {
    val newLabels = expression.allLabels.diff(allKnownLabels)
    lazy val newLabelCombinations = LabelExpressions.nonEmptySubsets(newLabels)
    val candidates = solutions.flatMap {
      case KnownLabels(labelNames) =>
        // Note that the empty set of labels is a special case, it behaves differently.
        // The pattern (:A) matches all nodes that at least have the label A: (:A) but also (:A:B), (:A:B:C:D) etc. There is no way to express exclusively A.
        // However, the pattern (:!%) matches exclusively on nodes with no patterns.
        // So when we encounter a new label, say B for example, pattern (:A) now matches (:A) and (:A:B) whereas pattern (:!%) still only matches ().
        if (labelNames.isEmpty)
          Stream(KnownLabels(Set.empty))
        else
          KnownLabels(labelNames) +: newLabelCombinations.map(newLabelCombination =>
            KnownLabels(labelNames.union(newLabelCombination))
          )
      case SomeUnknownLabels =>
        newLabelCombinations.map(KnownLabels) :+ SomeUnknownLabels
    }
    val (newSolutions, newRejectedCandidates) = candidates.partition(expression.matches)
    LabelExpressions(
      allKnownLabels = allKnownLabels.union(newLabels),
      solutions = newSolutions,
      rejectedCandidates = rejectedCandidates ++ newRejectedCandidates
    )
  }
}

object LabelExpressions {

  /**
   * Identity of the conjunction of label expressions.
   * It represents (:%|!%), also known as ().
   */
  def any: LabelExpressions =
    LabelExpressions(Set.empty, Stream(KnownLabels(Set.empty), SomeUnknownLabels), Stream.empty)

  def fold(conjointExpressions: Seq[LabelExpression]): LabelExpressions =
    conjointExpressions.foldLeft(LabelExpressions.any)(_.and(_))

  /**
   * Generates all the possible subsets of [[labels]] lazily, minus the empty set.
   * {{{
   *   nonEmptySubsets {A, B, C} = [{A}, {B}, {C}, {A,B}, {A,C}, {B,C}, {A,B,C}]
   *   nonEmptySubsets {} = []
   *   nonEmptySubsets {A} = [{A}]
   * }}}
   */
  def nonEmptySubsets(labels: Set[LabelName]): Stream[Set[LabelName]] =
    Stream.range(1, labels.size + 1).flatMap(labels.subsets)
}
