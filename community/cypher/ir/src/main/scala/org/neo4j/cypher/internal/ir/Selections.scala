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

import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.ir.Selections.containsPatternPredicates
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren

import scala.collection.mutable.ArrayBuffer

case class Selections private (predicates: Set[Predicate]) {
  def isEmpty: Boolean = predicates.isEmpty

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

  def expressionsContainingVariable: Map[String, Set[Predicate]] = {
    predicates.flatMap { predicate =>
      predicate.findAllByClass[Variable].map(v => v.name -> predicate)
    }.groupBy(_._1).mapValues(p => p.map(_._2))
  }

  def scalarPredicatesGiven(ids: Set[String]): Seq[Expression] = predicatesGiven(ids).filterNot(containsPatternPredicates)

  def patternPredicatesGiven(ids: Set[String]): Seq[Expression] = predicatesGiven(ids).filter(containsPatternPredicates)

  def flatPredicatesSet: Set[Expression] =
    predicates.map(_.expr)

  def flatPredicates: Seq[Expression] =
    flatPredicatesSet.toIndexedSeq

  def filter(filterExpression: Predicate => Boolean): Selections =
    new Selections(predicates.filter(filterExpression))

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
        val newMap = acc.updated(name, acc.getOrElse(name, Set.empty) + hasLabels)
        SkipChildren(newMap)
    }
  }

  /**
   * All label/type predicates for each variable.
   * This includes deeply nested predicates (e.g. in OR).
   */
  lazy val allHasLabelsOrTypesInvolving: Map[String, Set[HasLabelsOrTypes]] = {
    predicates.treeFold(Map.empty[String, Set[HasLabelsOrTypes]]) {
      case hasLabels@HasLabelsOrTypes(Variable(name), _) => acc =>
        val newMap = acc.updated(name, acc.getOrElse(name, Set.empty) + hasLabels)
        SkipChildren(newMap)
    }
  }

  /**
   * All type predicates for each variable.
   * This includes deeply nested predicates (e.g. in OR).
   */
  lazy val allHasTypesInvolving: Map[String, Set[HasTypes]] = {
    predicates.treeFold(Map.empty[String, Set[HasTypes]]) {
      case hasTypes@HasTypes(Variable(name), _) => acc =>
        val newMap = acc.updated(name, acc.getOrElse(name, Set.empty) + hasTypes)
        SkipChildren(newMap)
    }
  }

  /**
   * All property predicates for each variable.
   * This includes deeply nested predicates (e.g. in OR).
   */
  lazy val allPropertyPredicatesInvolving: Map[String, Set[Property]] = {
    predicates.treeFold(Map.empty[String, Set[Property]]) {
      case prop@Property(Variable(name), _) => acc =>
        val newMap = acc.updated(name, acc.getOrElse(name, Set.empty) + prop)
        SkipChildren(newMap)
    }
  }

  def variableDependencies: Set[String] = predicates.flatMap(_.dependencies)

  def labelsOnNode(id: String): Set[LabelName] = labelInfo.getOrElse(id, Set.empty)

  lazy val labelInfo: Map[String, Set[LabelName]] =
    labelPredicates.mapValues(_.map(_.labels.head))

  def coveredBy(solvedPredicates: Seq[Expression]): Boolean =
    flatPredicates.forall( solvedPredicates.contains )

  def contains(e: Expression): Boolean = predicates.exists { _.expr == e }

  def ++(other: Selections): Selections = Selections(predicates ++ other.predicates)

  def ++(expressions: Traversable[Expression]): Selections = Selections(predicates ++ expressions.flatMap(_.asPredicates))

  def nonEmpty: Boolean = !isEmpty
}

object Selections {

  def apply(): Selections = new Selections(Set.empty)

  /**
   * Create a new Selections from a set of predicates
   *
   * Any PartialPredicate where the covering predicate is itself a predicate in the set is removed
   */
  def apply(predicates: Set[Predicate]): Selections = {

    def isCoveredByOtherPredicate(partial: PartialPredicate[_]): Boolean =
      partial.coveringPredicate.asPredicates.forall(subExpr => predicates.contains(subExpr))

    val keptPredicates = predicates.filter {
      case Predicate(_, partial: PartialPredicate[_]) => !isCoveredByOtherPredicate(partial)
      case _                                          => true
    }

    new Selections(keptPredicates)
  }

  def from(expressions: Traversable[Expression]): Selections = Selections(expressions.flatMap(_.asPredicates).toSet)
  def from(expressions: Expression): Selections = Selections(expressions.asPredicates)

  def containsPatternPredicates(e: Expression): Boolean = e match {
    case _: ExistsSubClause                => true
    case Not(_: ExistsSubClause)           => true
    case Exists(_: PatternExpression)      => true
    case Not(Exists(_: PatternExpression)) => true
    case Ors(exprs)                        => exprs.exists(containsPatternPredicates)
    case _                                 => false
  }
}
