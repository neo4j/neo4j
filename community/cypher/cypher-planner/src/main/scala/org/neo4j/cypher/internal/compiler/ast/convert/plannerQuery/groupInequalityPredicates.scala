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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.util.NonEmptyList.IterableConverter

/**
 * This transforms
 *
 * Seq(Pred[n](n.prop > 34), Pred[n](n.foo = 'bar'), Pred[n](n.prop >= n.goo))
 *
 * into
 *
 * Seq(Pred[n](n.foo = 'bar'), Pred[n](AndedInequalities(n, n.prop, Seq(n.prop > 34, n.prop >= n.goo))
 *
 * i.e it groups inequalities by property lookup and collects each group of inequalities into
 * an instance of AndedPropertyInequalities
 */
object groupInequalityPredicates extends (Seq[Predicate] => Seq[Predicate]) {

  override def apply(inputPredicates: Seq[Predicate]): Seq[Predicate] = {

    // categorize predicates according to whether they contain an inequality on a property or not
    val (propertyInequalities, otherPredicates) = inputPredicates.partition {
      case Predicate(_, InequalityExpression(Property(_: Variable, _), _)) => true
      case _ => false
    }

    // group by property lookup
    val predicatesGroupedByProperty = propertyInequalities.groupBy {
      case Predicate(_, InequalityExpression(prop: Property, _)) => prop
    }

    // collect together all inequalities over some property lookup
    val rewrittenPropertyInequalities = predicatesGroupedByProperty.map {
      case (prop@Property(variable: Variable, _), groupInequalities) =>
        val dependencies = groupInequalities.flatMap(_.dependencies).toSet
        val inequalityExpressions = groupInequalities.collect {
          case Predicate(_, ie: InequalityExpression) => ie
        }
        val newExpr = AndedPropertyInequalities(variable, prop, inequalityExpressions.toNonEmptyList)
        Predicate(dependencies, newExpr)
    }

    // concatenate with remaining predicates
    otherPredicates ++ rewrittenPropertyInequalities
  }
}
