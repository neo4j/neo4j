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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter

import scala.collection.mutable.ArrayBuffer
import org.neo4j.cypher.internal.v4_0.expressions.functions.Exists
import org.neo4j.cypher.internal.v4_0.util.Foldable.FoldableAny

case class Selections(predicates: Set[Predicate] = Set.empty) {
  def isEmpty = predicates.isEmpty

  def predicatesGiven(ids: Set[String]): Seq[Expression] = {
    val buffer = new ArrayBuffer[Expression]()
    predicates.foreach {
      p =>
        if (p.hasDependenciesMet(ids)) {
          buffer += p.expr
        }
    }
    buffer
  }

  def scalarPredicatesGiven(ids: Set[String]): Seq[Expression] = predicatesGiven(ids).filterNot(containsPatternPredicates)

  def patternPredicatesGiven(ids: Set[String]): Seq[Expression] = predicatesGiven(ids).filter(containsPatternPredicates)

  private def containsPatternPredicates(e: Expression): Boolean = e match {
    case _: ExistsSubClause                => true
    case Not(_: ExistsSubClause)           => true
    case Exists(_: PatternExpression)      => true
    case Not(Exists(_: PatternExpression)) => true
    case Ors(exprs)                        => exprs.exists(containsPatternPredicates)
    case _                                 => false
  }

  def flatPredicates: Seq[Expression] =
    predicates.map(_.expr).toIndexedSeq

  /**
   * The top level label predicates for each variable.
   * That means if "a" -> hasLabels("a", "A") is returned, we can safely assume that a has the label A.
   */
  lazy val labelPredicates: Map[String, Set[HasLabels]] =
    predicates.foldLeft(Map.empty[String, Set[HasLabels]]) {
      case (acc, Predicate(_, hasLabels@HasLabels(Variable(name), _))) =>
        acc.updated(name, acc.getOrElse(name, Set.empty) + hasLabels)
      case (acc, _) => acc
    }

  /**
   * All label predicates for each variable.
   * This includes deeply nested predicates (e.g. in OR).
   */
  lazy val allHasLabelsInvolving: Map[String, Set[HasLabels]] = {
    predicates.treeFold(Map.empty[String, Set[HasLabels]]) {
      case hasLabels@HasLabels(Variable(name), _) => acc =>
        (acc.updated(name, acc.getOrElse(name, Set.empty) + hasLabels), None)
      //val newMap = acc.updated(name, acc.getOrElse(name, Set.empty) + hasLabels)
        //SkipChildren(newMap)
    }
  }

  /**
   * All property predicates for each variable.
   * This includes deeply nested predicates (e.g. in OR).
   */
  lazy val allPropertyPredicatesInvolving: Map[String, Set[Property]] = {
    predicates.treeFold(Map.empty[String, Set[Property]]) {
      case prop@Property(Variable(name), _) => acc =>
        (acc.updated(name, acc.getOrElse(name, Set.empty) + prop), None)
      //val newMap = acc.updated(name, acc.getOrElse(name, Set.empty) + prop)
        //SkipChildren(newMap)
    }
  }

  def variableDependencies: Set[String] = predicates.flatMap(_.dependencies)

  def labelsOnNode(id: String): Set[LabelName] = labelInfo.getOrElse(id, Set.empty)

  lazy val labelInfo: Map[String, Set[LabelName]] =
    labelPredicates.mapValues(_.map(_.labels.head))

  def coveredBy(solvedPredicates: Seq[Expression]): Boolean =
    flatPredicates.forall( solvedPredicates.contains )

  def contains(e: Expression): Boolean = predicates.exists { _.expr == e }

  def ++(other: Selections): Selections = {
    val otherPredicates = other.predicates
    val keptPredicates  = predicates.filter {
      case Predicate(_, expr: PartialPredicate[_]) =>
        !expr.coveringPredicate.asPredicates.forall(expr => otherPredicates.contains(expr) || predicates.contains(expr))

      case _ =>
        true
    }

    Selections(keptPredicates ++ other.predicates)
  }

  def ++(expressions: Traversable[Expression]): Selections = Selections(predicates ++ expressions.flatMap(_.asPredicates))

  def nonEmpty: Boolean = !isEmpty
}

object Selections {
  def from(expressions: Traversable[Expression]): Selections = new Selections(expressions.flatMap(_.asPredicates).toSet)
  def from(expressions: Expression): Selections = new Selections(expressions.asPredicates)
}
