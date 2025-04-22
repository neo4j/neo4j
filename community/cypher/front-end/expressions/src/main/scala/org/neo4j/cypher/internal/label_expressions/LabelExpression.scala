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

import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.DynamicLeaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.DeprecatedFeature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.bottomUp

import scala.annotation.tailrec

/**
 * @param entity          expression to evaluate to the entity we want to check
 * @param isParenthesized indicator if the label expression predicate was parenthesized, e.g. (n:L)
 *                        Note that isParenthesized may be false, even if the predicate was parenthesized.
 *                        Currently, isParenthesized is only set to true for parenthesized
 *                        label expression predicate in the Cypher5 parser.
 */
case class LabelExpressionPredicate(
  entity: Expression,
  labelExpression: LabelExpression
)(val position: InputPosition, val isParenthesized: Boolean)
    extends BooleanExpression {
  override def isConstantForQuery: Boolean = false

  override def dup(children: Seq[AnyRef]): this.type =
    children.size match {
      case 2 =>
        LabelExpressionPredicate(
          children.head.asInstanceOf[Expression],
          children(1).asInstanceOf[LabelExpression]
        )(position, isParenthesized).asInstanceOf[this.type]
      case 3 =>
        LabelExpressionPredicate(
          children.head.asInstanceOf[Expression],
          children(1).asInstanceOf[LabelExpression]
        )(
          children(2).asInstanceOf[InputPosition],
          isParenthesized
        ).asInstanceOf[this.type]
      case 4 =>
        LabelExpressionPredicate(
          children.head.asInstanceOf[Expression],
          children(1).asInstanceOf[LabelExpression]
        )(
          children(2).asInstanceOf[InputPosition],
          children(3).asInstanceOf[Boolean]
        ).asInstanceOf[this.type]
      case _ => throw new IllegalStateException("LabelExpressionPredicate has at least 2 and at most 4 children.")
    }
}

object LabelExpressionPredicate {

  val isParenthesizedDefault: Boolean = false

  // ... + n:P
  //       ^
  case object UnparenthesizedLabelPredicateOnRhsOfAdd extends DeprecatedFeature.DeprecatedIn5ErrorIn25
}

sealed trait LabelExpression extends ASTNode with HasMappableExpressions[LabelExpression] {

  /**
   * Whether this label expression was defined using the IS syntax
   */
  def containsIs: Boolean

  /**
   * Whether this label expression was permitted in Cypher before the introduction of GPM label expressions.
   */
  final def containsGpmSpecificLabelExpression: Boolean = this match {
    case conj: ColonConjunction =>
      conj.lhs.containsGpmSpecificLabelExpression || conj.rhs.containsGpmSpecificLabelExpression
    case _: Leaf        => false
    case _: DynamicLeaf => false
    case _              => true
  }

  final def containsGpmSpecificRelTypeExpression: Boolean = this match {
    case Disjunctions(children, _) =>
      children.exists(_.containsGpmSpecificRelTypeExpression)
    case ColonDisjunction(lhs, rhs, _) =>
      lhs.containsGpmSpecificRelTypeExpression || rhs.containsGpmSpecificRelTypeExpression
    case _: Leaf        => false
    case _: DynamicLeaf => false
    case _              => true
  }

  /**
   * Whether this label expression is disallowed in CREATE and MERGE.
   * Only label expressions that are allowed are:
   * leaf (:A)
   * colon conjunction (:A:B)
   * conjunction(:A&B)
   * dynamicLabels (:$(A))
   */
  final def containsMatchSpecificLabelExpression: Boolean = this match {
    case conj: ColonConjunction =>
      conj.lhs.containsMatchSpecificLabelExpression || conj.rhs.containsMatchSpecificLabelExpression
    case conj: Conjunctions =>
      conj.children.exists(expr => expr.containsMatchSpecificLabelExpression)
    case _: Leaf        => false
    case _: DynamicLeaf => false
    case _              => true
  }

  /**
   * Dynamic Label Expressions are only allowed in:
   * CREATE, MATCH, MERGE, SET and REMOVE clauses at this time.
   */
  final def containsDynamicLabelOrTypeExpression: Boolean = this match {
    case conj: BinaryLabelExpression =>
      conj.lhs.containsDynamicLabelOrTypeExpression || conj.rhs.containsDynamicLabelOrTypeExpression
    case conj: MultiOperatorLabelExpression => conj.children.exists(expr => expr.containsDynamicLabelOrTypeExpression)
    case Negation(e, _)                     => e.containsDynamicLabelOrTypeExpression
    case _: Leaf                            => false
    case _: Wildcard                        => false
    case _: DynamicLeaf                     => true
  }

  final def replaceColonSyntax: LabelExpression = this.endoRewrite(bottomUp({
    case disj @ ColonDisjunction(lhs, rhs, _) => Disjunctions.flat(lhs, rhs, disj.position, disj.containsIs)
    case conj @ ColonConjunction(lhs, rhs, _) => Conjunctions.flat(lhs, rhs, conj.position, conj.containsIs)
    case disj: Disjunctions                   => disj.unnestDisjunctions
    case conj: Conjunctions                   => conj.unnestConjunctions
    case expr                                 => expr
  }))

  def flatten: Seq[LabelExpressionLeafName]
}

trait LabelExpressionLeafName extends SymbolicName

trait LabelExpressionDynamicLeafExpression extends ASTNode
    with HasMappableExpressions[LabelExpressionDynamicLeafExpression] {
  def expression: Expression
  def all: Boolean
}

sealed trait BinaryLabelExpression extends LabelExpression {
  def lhs: LabelExpression
  def rhs: LabelExpression

  override def flatten: Seq[LabelExpressionLeafName] = lhs.flatten ++ rhs.flatten
}

sealed trait MultiOperatorLabelExpression extends LabelExpression {
  def children: Seq[LabelExpression]

  override def flatten: Seq[LabelExpressionLeafName] = children.flatMap(_.flatten)
}

object LabelExpression {

  final case class Disjunctions(children: Seq[LabelExpression], override val containsIs: Boolean = false)(
    val position: InputPosition
  ) extends MultiOperatorLabelExpression {

    def unnestDisjunctions: Disjunctions = {
      if (children.exists(_.isInstanceOf[Disjunctions])) {
        val unnested = children.flatMap {
          case Disjunctions(children, _) => children
          case x                         => Seq(x)
        }
        copy(children = unnested, containsIs)(position)
      } else {
        this
      }
    }

    override def mapExpressions(f: Expression => Expression): LabelExpression = copy(
      children = children.map(_.mapExpressions(f))
    )(this.position)
  }

  object Disjunctions {

    def flat(lhs: LabelExpression, rhs: LabelExpression, position: InputPosition, containsIs: Boolean): Disjunctions = {
      Disjunctions(Vector(lhs, rhs), containsIs)(position).unnestDisjunctions
    }
  }

  case class Conjunctions(children: Seq[LabelExpression], override val containsIs: Boolean = false)(
    val position: InputPosition
  ) extends MultiOperatorLabelExpression {

    def unnestConjunctions: Conjunctions = {
      if (children.exists(_.isInstanceOf[Conjunctions])) {
        val unnested = children.flatMap {
          case Conjunctions(children, _) => children
          case x                         => Seq(x)
        }
        copy(children = unnested, containsIs)(position)
      } else {
        this
      }
    }

    override def mapExpressions(f: Expression => Expression): LabelExpression = copy(
      children = children.map(_.mapExpressions(f))
    )(this.position)
  }

  object Conjunctions {

    def flat(lhs: LabelExpression, rhs: LabelExpression, position: InputPosition, containsIs: Boolean): Conjunctions = {
      Conjunctions(Vector(lhs, rhs), containsIs)(position).unnestConjunctions
    }
  }

  /**
   * This represents a conjunction that does not use the ampersand '&' as specified by GPM but rather the colon ':'
   * as specified by Cypher previously:
   * `n:A:B` instead of `n:A&B`
   */
  case class ColonConjunction(lhs: LabelExpression, rhs: LabelExpression, override val containsIs: Boolean = false)(
    val position: InputPosition
  ) extends BinaryLabelExpression {

    override def mapExpressions(f: Expression => Expression): LabelExpression = copy(
      lhs = lhs.mapExpressions(f),
      rhs = rhs.mapExpressions(f)
    )(this.position)
  }

  /* This is the old now deprecated relationship type disjunction [r:A|:B]
   */
  case class ColonDisjunction(lhs: LabelExpression, rhs: LabelExpression, override val containsIs: Boolean = false)(
    val position: InputPosition
  ) extends BinaryLabelExpression {

    override def mapExpressions(f: Expression => Expression): LabelExpression = copy(
      lhs = lhs.mapExpressions(f),
      rhs = rhs.mapExpressions(f)
    )(this.position)
  }

  case class Negation(e: LabelExpression, override val containsIs: Boolean = false)(val position: InputPosition)
      extends LabelExpression {

    @tailrec
    final override def flatten: Seq[LabelExpressionLeafName] = {
      e match {
        case e: Negation => e.flatten
        case e           => e.flatten
      }
    }

    override def mapExpressions(f: Expression => Expression): LabelExpression = copy(
      e = e.mapExpressions(f)
    )(this.position)
  }

  case class Wildcard(override val containsIs: Boolean = false)(val position: InputPosition) extends LabelExpression {
    override def flatten: Seq[LabelExpressionLeafName] = Seq.empty

    override def mapExpressions(f: Expression => Expression): LabelExpression = this
  }

  case class Leaf(name: LabelExpressionLeafName, override val containsIs: Boolean = false) extends LabelExpression {
    val position: InputPosition = name.position

    override def flatten: Seq[LabelExpressionLeafName] = Seq(name)

    // We are breaking the implicit assumption that every ASTNode has a position as second parameter list.
    // That is why, we need to adjust the dup method's behaviour
    override def dup(children: Seq[AnyRef]): Leaf.this.type = children match {
      case Seq(dupName, dupContainsIs, _: InputPosition) => super.dup(Seq(dupName, dupContainsIs))
      case _                                             => super.dup(children)
    }

    override def mapExpressions(f: Expression => Expression): LabelExpression = this
  }

  case class DynamicLeaf(expr: LabelExpressionDynamicLeafExpression, override val containsIs: Boolean = false)
      extends LabelExpression {
    val position: InputPosition = expr.position

    override def flatten: Seq[LabelExpressionLeafName] = Seq.empty // TODO

    // We are breaking the implicit assumption that every ASTNode has a position as second parameter list.
    // That is why, we need to adjust the dup method's behaviour
    override def dup(children: Seq[AnyRef]): DynamicLeaf.this.type = children match {
      case Seq(name, containsIs, _: InputPosition) => super.dup(Seq(name, containsIs))
      case _                                       => super.dup(children)
    }

    override def mapExpressions(f: Expression => Expression): DynamicLeaf = copy(
      expr = expr.mapExpressions(f)
    )
  }

  def getRelTypes(relTypes: Option[LabelExpression]): Seq[RelTypeName] = {
    relTypes.map(_.flatten.map(_.asInstanceOf[RelTypeName])).getOrElse(Seq.empty)
  }

  def containsGpmSpecificRelType(labelExpression: Option[LabelExpression]): Boolean =
    labelExpression.exists(_.containsGpmSpecificRelTypeExpression)

  def disjoinRelTypesToLabelExpression(relTypes: Seq[RelTypeName]): Option[LabelExpression] = {
    val labelExpressions = relTypes.map(Leaf(_))
    if (labelExpressions.length > 1)
      Some(LabelExpression.Disjunctions(labelExpressions)(InputPosition.NONE))
    else
      labelExpressions.headOption
  }
}
