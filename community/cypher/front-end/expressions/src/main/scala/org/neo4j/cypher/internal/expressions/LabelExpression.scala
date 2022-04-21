/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Label
import org.neo4j.cypher.internal.util.InputPosition

/**
 * @param entity expression to evaluate to the entity we want to check
 */
case class LabelExpressionPredicate(entity: Expression, labelExpression: LabelExpression)(val position: InputPosition)
    extends BooleanExpression

trait LabelExpression extends Expression {

  /**
   * Whether this label expression was permitted in Cypher before the introduction of GPM label expressions.
   */
  def isNonGpm: Boolean = this match {
    case conj: ColonConjunction => conj.lhs.isNonGpm && conj.rhs.isNonGpm
    case _: Label               => true
    case _                      => false
  }

  def flatten: Seq[LabelName]
}

trait BinaryLabelExpression extends LabelExpression {
  def lhs: LabelExpression
  def rhs: LabelExpression

  override def flatten: Seq[LabelName] = lhs.flatten ++ rhs.flatten
}

object LabelExpression {

  case class Conjunction(lhs: LabelExpression, rhs: LabelExpression)(val position: InputPosition)
      extends BinaryLabelExpression

  /**
   * This represents a conjunction that does not use the ampersand '&' as specified by GPM but rather the colon ':'
   * as specified by Cypher previously:
   * `n:A:B` instead of `n:A&B`
   */
  case class ColonConjunction(lhs: LabelExpression, rhs: LabelExpression)(val position: InputPosition)
      extends BinaryLabelExpression

  case class Disjunction(lhs: LabelExpression, rhs: LabelExpression)(val position: InputPosition)
      extends BinaryLabelExpression

  case class Negation(e: LabelExpression)(val position: InputPosition) extends LabelExpression {
    override def flatten: Seq[LabelName] = e.flatten
  }

  case class Wildcard()(val position: InputPosition) extends LabelExpression {
    override def flatten: Seq[LabelName] = Seq.empty
  }

  // the type `LabelName` is necessary for resolveTokens
  case class Label(label: LabelName)(val position: InputPosition) extends LabelExpression {
    override def flatten: Seq[LabelName] = Seq(label)
  }
}
