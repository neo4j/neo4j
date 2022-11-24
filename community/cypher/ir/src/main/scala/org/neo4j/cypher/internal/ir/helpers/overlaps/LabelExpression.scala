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
 * A label expression, found on a node pattern, recombined from its predicates.
 *
 * @param allLabels All the label names explicitly mentioned in the expression. For example (:%&!(A&B)) contains {A,B}.
 * @param matches Returns whether the synthetic labels of a given node match the label expression or not.
 *                For example, (:A|B).matches({C}) is false, and (:!A).matches(SomeUnknownLabels) is true.
 */
case class LabelExpression(allLabels: Set[LabelName], matches: NodeLabels => Boolean) {

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
   * See [[LabelExpression.allSolutions]] to evaluate a sequence of conjoint expressions, like a list of predicates on the same node.
   */
  def solutions: Set[NodeLabels] =
    allLabels
      .subsets()
      .map(KnownLabels)
      .toSet[NodeLabels]
      .+(SomeUnknownLabels)
      .filter(matches)

  /**
   * !this
   */
  def not: LabelExpression =
    LabelExpression.build(allLabels) { nodeLabels =>
      !matches(nodeLabels)
    }

  private def binary(rhs: LabelExpression)(f: (Boolean, Boolean) => Boolean): LabelExpression =
    LabelExpression.build(allLabels.union(rhs.allLabels)) { nodeLabels =>
      f(matches(nodeLabels), rhs.matches(nodeLabels))
    }

  /**
   * this & rhs
   */
  def and(rhs: LabelExpression): LabelExpression =
    binary(rhs)(_ && _)

  /**
   * this | rhs
   */
  def or(rhs: LabelExpression): LabelExpression =
    binary(rhs)(_ || _)

  /**
   * this XOR rhs
   */
  def xor(rhs: LabelExpression): LabelExpression =
    binary(rhs) { (left, right) =>
      left && !right || !left && right
    }
}

object LabelExpression {

  /**
   * Curried version of LabelExpression.apply for ease of use
   */
  def build(labels: Set[LabelName])(matches: NodeLabels => Boolean): LabelExpression =
    LabelExpression(labels, matches)

  /**
   * :%
   */
  def wildcard: LabelExpression =
    build(Set.empty) {
      case KnownLabels(labels) => labels.nonEmpty
      case SomeUnknownLabels   => true
    }

  /**
   * :Label
   */
  def label(labelName: LabelName): LabelExpression =
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
  def allSolutions(conjointExpressions: Seq[LabelExpression]): Stream[NodeLabels] =
    LabelExpressions.fold(conjointExpressions).solutions
}
