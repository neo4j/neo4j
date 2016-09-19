/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.ir.v3_1

import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.ir.v3_1.helpers.UnNamedNameGenerator._
import org.neo4j.cypher.internal.ir.v3_1.Selections.asPredicates

object Selections {
  def from(expressions: Expression*): Selections = new Selections(expressions.flatMap(asPredicates).toSet)

  def asPredicates(predicate: Expression): Set[Predicate] = {
    predicate.treeFold(Set.empty[Predicate]) {
      // n:Label
      case p@HasLabels(Variable(name), labels) =>
        acc => val newAcc = acc ++ labels.map { label =>
          Predicate(Set(IdName(name)), p.copy(labels = Seq(label))(p.position))
        }
          (newAcc, None)
      // and
      case _: Ands =>
        acc => (acc, Some(identity))
      case p: Expression =>
        acc => (acc + Predicate(p.idNames, p), None)
    }.map(filterUnnamed)
  }

  private def filterUnnamed(predicate: Predicate): Predicate = predicate match {
    case Predicate(deps, e: PatternExpression) =>
      Predicate(deps.filter(x => isNamed(x.name)), e)
    case Predicate(deps, e@Not(_: PatternExpression)) =>
      Predicate(deps.filter(x => isNamed(x.name)), e)
    case Predicate(deps, ors@Ors(exprs)) =>
      val newDeps = exprs.foldLeft(Set.empty[IdName]) { (acc, exp) =>
        exp match {
          case e: PatternExpression =>
            acc ++ e.idNames.filter(x => isNamed(x.name))
          case e@Not(_: PatternExpression) =>
            acc ++ e.idNames.filter(x => isNamed(x.name))
          case e if e.exists { case _: PatternExpression => true } =>
            acc ++ (e.idNames -- unnamedIdNamesInNestedPatternExpressions(e))
          case e =>
            acc ++ e.idNames
        }
      }
      Predicate(newDeps, ors)
    case Predicate(deps, expr) if expr.exists { case _: PatternExpression => true } =>
      Predicate(deps -- unnamedIdNamesInNestedPatternExpressions(expr), expr)
    case p => p
  }

  private def unnamedIdNamesInNestedPatternExpressions(expression: Expression) = {
    val patternExpressions = expression.treeFold(Seq.empty[PatternExpression]) {
      case p: PatternExpression => acc => (acc :+ p, None)
    }

    val unnamedIdsInPatternExprs = patternExpressions.flatMap(_.idNames)
      .filterNot(x => isNamed(x.name))
      .toSet

    unnamedIdsInPatternExprs
  }
}

case class Selections(predicates: Set[Predicate] = Set.empty) {
  def isEmpty = predicates.isEmpty

  def predicatesGiven(ids: Set[IdName]): Seq[Expression] = predicates.collect {
    case p@Predicate(_, predicate) if p.hasDependenciesMet(ids) => predicate
  }.toIndexedSeq

  def predicatesGivenForRequiredSymbol(allowed: Set[IdName], required: IdName): Seq[Expression] = predicates.collect {
    case p@Predicate(_, predicate) if p.hasDependenciesMetForRequiredSymbol(allowed, required) => predicate
  }.toIndexedSeq

  def scalarPredicatesGiven(ids: Set[IdName]): Seq[Expression] = predicatesGiven(ids).filterNot(containsPatternPredicates)

  def patternPredicatesGiven(ids: Set[IdName]): Seq[Expression] = predicatesGiven(ids).filter(containsPatternPredicates)

  private def containsPatternPredicates(e: Expression): Boolean = e match {
    case _: PatternExpression      => true
    case Not(_: PatternExpression) => true
    case Ors(exprs)                => exprs.exists(containsPatternPredicates)
    case _                         => false
  }

  def flatPredicates: Seq[Expression] =
    predicates.map(_.expr).toIndexedSeq

  def labelPredicates: Map[IdName, Set[HasLabels]] =
    predicates.foldLeft(Map.empty[IdName, Set[HasLabels]]) {
      case (acc, Predicate(_, hasLabels@HasLabels(Variable(name), labels))) =>
        // FIXME: remove when we have test for checking that we construct the expected plan
        if (labels.size > 1) {
          throw new IllegalStateException("Rewriting should introduce single label HasLabels predicates in the WHERE clause")
        }
        val idName = IdName(name)
        acc.updated(idName, acc.getOrElse(idName, Set.empty) + hasLabels)
      case (acc, _) => acc
    }

  def propertyPredicatesForSet: Map[IdName, Set[Property]] = {
    def updateMap(map: Map[IdName, Set[Property]], key: IdName, prop: Property) =
      map.updated(key, map.getOrElse(key, Set.empty) + prop)

    predicates.foldLeft(Map.empty[IdName, Set[Property]]) {

      // We rewrite set property expressions to use In (and not Equals)
      case (acc, Predicate(_, In(prop@Property(key: Variable, _), _))) =>
        updateMap(acc, IdName.fromVariable(key), prop)
      case (acc, Predicate(_, In(_, prop@Property(key: Variable, _)))) =>
        updateMap(acc, IdName.fromVariable(key), prop)
      case (acc, _) => acc
    }
  }

  def labelsOnNode(id: IdName): Set[LabelName] = labelInfo.getOrElse(id, Set.empty)

  lazy val labelInfo: Map[IdName, Set[LabelName]] =
    labelPredicates.mapValues(_.map(_.labels.head))

  def coveredBy(solvedPredicates: Seq[Expression]): Boolean =
    flatPredicates.forall( solvedPredicates.contains )

  def contains(e: Expression): Boolean = predicates.exists { _.expr == e }

  def ++(other: Selections): Selections = {
    val otherPredicates = other.predicates
    val keptPredicates  = predicates.filter {
      case pred@Predicate(_, expr: PartialPredicate[_]) =>
        val predicates = asPredicates(expr.coveringPredicate)
        !predicates.forall(expr => otherPredicates.contains(expr) || predicates.contains(expr))

      case pred =>
        true
    }

    Selections(keptPredicates ++ other.predicates)
  }

  // Value joins are equality comparisons between two expressions. As long as they depend on different, non-overlapping
  // sets of variables, they can be solved with a traditional hash join, similar to what a SQL database would
  lazy val valueJoins: Set[Equals] = flatPredicates.collect {
    case e@Equals(l, r)
      if l.dependencies.nonEmpty &&
         r.dependencies.nonEmpty &&
         r.dependencies != l.dependencies => e
  }.toSet

  def ++(expressions: Expression*): Selections = Selections(predicates ++ expressions.flatMap(asPredicates))

  def nonEmpty: Boolean = !isEmpty
}
