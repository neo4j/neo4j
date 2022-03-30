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
import org.neo4j.cypher.internal.expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Conjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Disjunction
import org.neo4j.cypher.internal.expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.topDown

/**
 * @param entity expression to evaluate to the entity we want to check
 */
case class LabelExpressionPredicate(entity: Expression, labelExpression: LabelExpression)(val position: InputPosition)
    extends BooleanExpression

sealed trait LabelExpression extends ASTNode {

  /**
   * Whether this label expression was permitted in Cypher before the introduction of GPM label expressions.
   */
  def containsGpmSpecificLabelExpression: Boolean = this match {
    case conj: ColonConjunction =>
      conj.lhs.containsGpmSpecificLabelExpression || conj.rhs.containsGpmSpecificLabelExpression
    case _: Leaf => false
    case _       => true
  }

  def containsGpmSpecificRelTypeExpression: Boolean = this match {
    case Disjunction(lhs, rhs) =>
      lhs.containsGpmSpecificRelTypeExpression || rhs.containsGpmSpecificRelTypeExpression
    case ColonDisjunction(lhs, rhs) =>
      lhs.containsGpmSpecificRelTypeExpression || rhs.containsGpmSpecificRelTypeExpression
    case _: Leaf => false
    case _       => true
  }

  def replaceColonSyntax: LabelExpression = this.endoRewrite(topDown({
    case disj @ ColonDisjunction(lhs, rhs) => Disjunction(lhs, rhs)(disj.position)
    case conj @ ColonConjunction(lhs, rhs) => Conjunction(lhs, rhs)(conj.position)
    case expr                              => expr
  }))

  def flatten: Seq[LabelExpressionLeafName]
}

trait LabelExpressionLeafName extends SymbolicName

sealed trait BinaryLabelExpression extends LabelExpression {
  def lhs: LabelExpression
  def rhs: LabelExpression

  override def flatten: Seq[LabelExpressionLeafName] = lhs.flatten ++ rhs.flatten
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
  /* This is the old now deprecated relationship type disjunction [r:A|:B]
   */

  case class ColonDisjunction(lhs: LabelExpression, rhs: LabelExpression)(val position: InputPosition)
      extends BinaryLabelExpression

  case class Negation(e: LabelExpression)(val position: InputPosition) extends LabelExpression {
    override def flatten: Seq[LabelExpressionLeafName] = e.flatten
  }

  case class Wildcard()(val position: InputPosition) extends LabelExpression {
    override def flatten: Seq[LabelExpressionLeafName] = Seq.empty
  }

  case class Leaf(name: LabelExpressionLeafName) extends LabelExpression {
    val position: InputPosition = name.position

    override def flatten: Seq[LabelExpressionLeafName] = Seq(name)

    // We are breaking the implicit assumption that every ASTNode has a position as second parameter list.
    // That is why, we need to adjust the dup method's behaviour
    override def dup(children: Seq[AnyRef]): Leaf.this.type = children match {
      case Seq(name, _: InputPosition) => super.dup(Seq(name))
      case _                           => super.dup(children)
    }
  }

  def getRelTypes(relTypes: Option[LabelExpression]): Seq[RelTypeName] = {
    relTypes.map(_.flatten.map(_.asInstanceOf[RelTypeName])).getOrElse(Seq.empty)
  }

  def containsGpmSpecificRelType(labelExpression: Option[LabelExpression]): Boolean =
    labelExpression.exists(_.containsGpmSpecificRelTypeExpression)

  def disjoinRelTypesToLabelExpression(relTypes: Seq[RelTypeName]): Option[LabelExpression] = {
    val labelExpressions = relTypes.map(Leaf(_))
    labelExpressions.foldLeft[Option[LabelExpression]](None) {
      case (None, rhs)      => Some(rhs)
      case (Some(lhs), rhs) => Some(LabelExpression.Disjunction(lhs, rhs)(InputPosition.NONE))
    }
  }
}
