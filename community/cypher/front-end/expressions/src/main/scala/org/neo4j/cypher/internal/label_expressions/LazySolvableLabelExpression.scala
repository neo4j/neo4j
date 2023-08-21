/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.label_expressions

import org.neo4j.cypher.internal.label_expressions.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.label_expressions.NodeLabels.LabelName
import org.neo4j.cypher.internal.label_expressions.NodeLabels.SomeUnknownLabels

/**
 * Conjunction of zero or more label expressions, builds a lazy list of all the solutions.
 * Used as an accumulator when folding a list of conjoint label expressions.
 * {{{[:%, :!(A&B), :C].foldLeft(LazySolvableLabelExpression.any)(_ and _)}}} is equivalent to {{{(:% & !(A&B) & C).solution}}} only more efficient.
 * Instead of generating all candidates upfront, and filtering them one by one, this generates and prunes candidates on the fly and will terminate as early as possible.
 * @param allKnownLabels Union of all the label names explicitly mentioned in the accumulated label expressions.
 * @param solutions Set of solutions of the conjunction of labels expressions, in no particular order. No computation happens before the first value gets evaluated.
 */
case class LazySolvableLabelExpression(
  allKnownLabels: Set[LabelName],
  solutions: LazyList[NodeLabels],
  rejectedCandidates: LazyList[NodeLabels]
) {

  /**
   * Add a new expression to the accumulated conjunction.
   * If the expression refers to labels that haven't been encountered so far, we first grow the list of candidates to account for it.
   * We then shrink that list down by matching each candidate against the label expression.
   *
   * For example, if we want to find all the solution for the following node pattern (:% & !(A&B) & C), we evaluate:
   * {{{[:%, :!(A&B), :C].foldLeft(LazySolvableLabelExpression.any)(_ and _)}}}
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
  def and(expression: SolvableLabelExpression): LazySolvableLabelExpression = {
    val newLabels = expression.allLabels.diff(allKnownLabels)
    lazy val newLabelCombinations = LazySolvableLabelExpression.nonEmptySubsets(newLabels)
    val candidates = solutions.flatMap {
      case KnownLabels(labelNames) =>
        // Note that the empty set of labels is a special case, it behaves differently.
        // The pattern (:A) matches all nodes that at least have the label A: (:A) but also (:A:B), (:A:B:C:D) etc. There is no way to express exclusively A.
        // However, the pattern (:!%) matches exclusively on nodes with no patterns.
        // So when we encounter a new label, say B for example, pattern (:A) now matches (:A) and (:A:B) whereas pattern (:!%) still only matches ().
        if (labelNames.isEmpty)
          LazyList(KnownLabels(Set.empty))
        else
          KnownLabels(labelNames) +: newLabelCombinations.map(newLabelCombination =>
            KnownLabels(labelNames.union(newLabelCombination))
          )
      case SomeUnknownLabels =>
        newLabelCombinations.map(KnownLabels) :+ SomeUnknownLabels
    }
    val (newSolutions, newRejectedCandidates) = candidates.partition(expression.matches)
    LazySolvableLabelExpression(
      allKnownLabels = allKnownLabels.union(newLabels),
      solutions = newSolutions,
      rejectedCandidates = rejectedCandidates ++ newRejectedCandidates
    )
  }
}

object LazySolvableLabelExpression {

  /**
   * Identity of the conjunction of label expressions.
   * It represents (:%|!%), also known as ().
   */
  def any: LazySolvableLabelExpression =
    LazySolvableLabelExpression(Set.empty, LazyList(KnownLabels(Set.empty), SomeUnknownLabels), LazyList.empty)

  def fold(conjointExpressions: Seq[SolvableLabelExpression]): LazySolvableLabelExpression =
    conjointExpressions.foldLeft(LazySolvableLabelExpression.any)(_.and(_))

  /**
   * Generates all the possible subsets of [[labels]] lazily, minus the empty set.
   * {{{
   *   nonEmptySubsets {A, B, C} = [{A}, {B}, {C}, {A,B}, {A,C}, {B,C}, {A,B,C}]
   *   nonEmptySubsets {} = []
   *   nonEmptySubsets {A} = [{A}]
   * }}}
   */
  def nonEmptySubsets(labels: Set[LabelName]): LazyList[Set[LabelName]] =
    LazyList.range(1, labels.size + 1).flatMap(labels.subsets)
}
