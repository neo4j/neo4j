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

import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.label_expressions.NodeLabels.KnownLabels
import org.neo4j.cypher.internal.label_expressions.NodeLabels.LabelName
import org.neo4j.cypher.internal.label_expressions.NodeLabels.SomeUnknownLabels
import org.neo4j.cypher.internal.util.tailrec.TailCallsUtil

import scala.util.control.TailCalls
import scala.util.control.TailCalls.TailRec

/**
 * A label expression, found on a node pattern, recombined from its predicates.
 *
 * @param allLabels All the label names explicitly mentioned in the expression. For example (:%&!(A&B)) contains {A,B}.
 * @param matches Returns whether the synthetic labels of a given node match the label expression or not.
 *                For example, (:A|B).matches({C}) is false, and (:!A).matches(SomeUnknownLabels) is true.
 */
case class SolvableLabelExpression(allLabels: Set[LabelName], matches: NodeLabels => Boolean) {

  /**
   * Tests whether the actual set of labels on a specific node match against the label expression.
   * Mainly intended to be used on nodes found in a CREATE clause.
   * @param labelNames the set of labels on a given node.
   * @return whether the labels match against the label expression.
   *         For example, (:%&!(A&B)).matchesLabels((:A:C)) is true, (:A|B).matchesLabels(()) is false.
   */
  def matchesLabels(labelNames: Set[LabelName]): Boolean =
    matches(KnownLabels(labelNames))

  /**
   * Calculate the set of solutions for this label expression taken in isolation.
   * (:%&!(A&B)).solutions gives us {{A}, {B}, SomeUnknownLabels}
   * Note that we cannot express matching on a label exclusively, (:A) means that the node must contain at least the label A.
   * This whole package treats the set of all possible labels as infinite, meaning that solutions are always incomplete.
   * Our set of solution is {{A}, {B}, SomeUnknownLabels} in the context of having only encountered labels A and B, hence the taken in isolation.
   * If we were to encounter a label C, then it would represent {{A}, {A,C}, {B}, {B,C}, {C}, SomeUnknownLabels}.
   *
   * See [[SolvableLabelExpression.allSolutions]] to evaluate a sequence of conjoint expressions, like a list of predicates on the same node.
   */
  def solutions: Set[NodeLabels] =
    allLabels
      .subsets()
      .map(KnownLabels)
      .toSet[NodeLabels]
      .incl(SomeUnknownLabels)
      .filter(matches)

  /**
   * Determine whether the solutions of this label expression could be fulfilled by just a single label. This is useful for testing whether this
   * label expression could evaluate to `true` on a relationship.
   */
  def containsSolutionsForRelationship: Boolean =
    allLabels
      .map(label => KnownLabels(Set(label)))
      .toSet[NodeLabels]
      .incl(SomeUnknownLabels)
      .exists(matches)

  /**
   * !this
   */
  def not: SolvableLabelExpression =
    SolvableLabelExpression.build(allLabels) { nodeLabels =>
      !matches(nodeLabels)
    }

  private def binary(rhs: SolvableLabelExpression)(f: (Boolean, Boolean) => Boolean): SolvableLabelExpression =
    SolvableLabelExpression.build(allLabels.union(rhs.allLabels)) { nodeLabels =>
      f(matches(nodeLabels), rhs.matches(nodeLabels))
    }

  /**
   * this & rhs
   */
  def and(rhs: SolvableLabelExpression): SolvableLabelExpression =
    binary(rhs)(_ && _)

  /**
   * this | rhs
   */
  def or(rhs: SolvableLabelExpression): SolvableLabelExpression =
    binary(rhs)(_ || _)

  /**
   * this XOR rhs
   */
  def xor(rhs: SolvableLabelExpression): SolvableLabelExpression =
    binary(rhs) { (left, right) =>
      left && !right || !left && right
    }
}

object SolvableLabelExpression {

  def from(labelExpression: LabelExpression): SolvableLabelExpression =
    extractLabelExpressionRec(labelExpression).result

  private def extractLabelExpressionRec(labelExpression: LabelExpression): TailRec[SolvableLabelExpression] =
    labelExpression match {
      case Wildcard(_) =>
        TailCalls.done(SolvableLabelExpression.wildcard)
      case Leaf(label, _) =>
        TailCalls.done(SolvableLabelExpression.label(label.name))
      case Negation(not: LabelExpression, _) =>
        TailCalls.tailcall(extractLabelExpressionRec(not)).map(_.not)
      case ColonConjunction(lhs: LabelExpression, rhs: LabelExpression, _) =>
        TailCallsUtil.map2(extractLabelExpressionRec(lhs), extractLabelExpressionRec(rhs))(_.and(_))
      case Conjunctions(conjointExpressions: Seq[LabelExpression], _) =>
        TailCallsUtil.traverse(conjointExpressions.toList)(le => extractLabelExpressionRec(le)).map(
          _.reduceLeft(_.and(_))
        )
      case ColonDisjunction(lhs: LabelExpression, rhs: LabelExpression, _) =>
        TailCallsUtil.map2(extractLabelExpressionRec(lhs), extractLabelExpressionRec(rhs))(_.or(_))
      case Disjunctions(disjointExpressions: Seq[LabelExpression], _) =>
        TailCallsUtil.traverse(disjointExpressions.toList)(le => extractLabelExpressionRec(le)).map(
          _.reduceLeft(_.or(_))
        )
    }

  /**
   * Curried version of SolvableLabelExpression.apply for ease of use
   */
  def build(labels: Set[LabelName])(matches: NodeLabels => Boolean): SolvableLabelExpression =
    SolvableLabelExpression(labels, matches)

  /**
   * :%
   */
  def wildcard: SolvableLabelExpression =
    build(Set.empty) {
      case KnownLabels(labels) => labels.nonEmpty
      case SomeUnknownLabels   => true
    }

  /**
   * :Label
   */
  def label(labelName: LabelName): SolvableLabelExpression =
    build(Set(labelName)) {
      case KnownLabels(labels) => labels.contains(labelName)
      case SomeUnknownLabels   => false
    }

  /**
   * Lazily evaluates all the solutions for a given list of conjoint label expressions.
   * For example, allSolutions(:%, :!(A&B), :C) is {{A,C}, {B,C}, {C}}.
   * It is strictly equivalent to (:% & !(A&B) & C).solution, but it is lazy and so will prune candidates aggressively and terminate early if it runs out.
   * Whereas (:A & !A & (B|C|D)).solutions will generate 17 candidates and reject them one by one, allSolutions(:A, :!A, :B|C|D) will stop after !A as it contradicts A.
   * Getting a single solution requires processing all the expressions, and so calling .toList is marginally more expensive than calling .headOption.
   */
  def allSolutions(conjointExpressions: Seq[SolvableLabelExpression]): LazyList[NodeLabels] =
    LazySolvableLabelExpression.fold(conjointExpressions).solutions
}
