/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.predicates

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, Property}
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList

// This transforms
//
//   Seq(Pred[n](n.prop > 34), Pred[n](n.foo = 'bar'), Pred[n](n.prop >= n.goo))
//
// into
//
//   Seq(Pred[n](n.foo = 'bar'), Pred[n](AndedInequalities(n, n.prop, Seq(n.prop > 34, n.prop >= n.goo))
//
// i.e it groups inequalities by property lookup and collects each group of inequalities into
// an instance of AndedPropertyInequalities
//
object groupInequalityPredicatesForLegacy extends (NonEmptyList[Predicate] => NonEmptyList[Predicate]) {

  def apply(inputPredicates: NonEmptyList[Predicate]): NonEmptyList[Predicate] = {

    // categorize predicates according to whether they contain an inequality or not
    val partitions: Either[(NonEmptyList[ComparablePredicate], Option[NonEmptyList[Predicate]]), (Option[NonEmptyList[ComparablePredicate]], NonEmptyList[Predicate])] = inputPredicates.partition {
        case comparable: ComparablePredicate => Left(comparable)
        case predicate => Right(predicate)
    }

    // detect if we need to transform the input
    val optInequalities = partitions match {
      case Left((comparables, remainder)) => Some(comparables -> remainder)
      case Right((Some(comparables), remainder)) => Some(comparables -> Some(remainder))
      case Right((None, _)) => None
    }

    optInequalities match {
      case Some((comparables, optRemainder)) =>
        // group by property lookup
        val groupedPredicates = groupedComparablePredicates(comparables)

        // collect together all inequalities over some property lookup
        val combinedComparablePredicates = groupedPredicates.map {
          case (Some((ident, prop)), groupComparables) =>
            NonEmptyList(AndedPropertyComparablePredicates(ident, prop, groupComparables))

          case (None, predicates) =>
            predicates
        }.flatMap[Predicate](identity)

        // concatenate with remaining predicates
        optRemainder.map(_ ++ combinedComparablePredicates).getOrElse(combinedComparablePredicates)

      case None =>
        inputPredicates
    }
  }

  private def groupedComparablePredicates(comparables: NonEmptyList[ComparablePredicate]) = {
    comparables.groupBy { (input: ComparablePredicate) =>
      input.left match {
        case prop@Property(ident: Identifier, _) => Some(ident -> prop)
        case _ => None
      }
    }.toNonEmptyListOption.get
  }
}
