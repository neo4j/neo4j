/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.helpers.FreshIdNameGenerator

object PropertyPredicateNormalizer extends MatchPredicateNormalizer {
  override val extract: PartialFunction[AnyRef, IndexedSeq[Expression]] = {
    case NodePattern(Some(id), _, Some(props)) if !isParameter(props) =>
      propertyPredicates(id, props)

    case RelationshipPattern(Some(id), _, None, Some(props), _) if !isParameter(props) =>
      propertyPredicates(id, props)

    case rp@RelationshipPattern(Some(id), _, Some(_), Some(props), _) if !isParameter(props) =>
      Vector(varLengthPropertyPredicates(id, props, rp.position))
  }

  override val replace: PartialFunction[AnyRef, AnyRef] = {
    case p@NodePattern(Some(_) ,_, Some(props)) if !isParameter(props)                  => p.copy(properties = None)(p.position)
    case p@RelationshipPattern(Some(_), _, _, Some(props), _) if !isParameter(props) => p.copy(properties = None)(p.position)
  }

  private def isParameter(expr: Expression) = expr match {
    case _: Parameter => true
    case _            => false
  }

  private def propertyPredicates(id: Variable, props: Expression): IndexedSeq[Expression] = props match {
    case mapProps: MapExpression =>
      mapProps.items.map {
        // MATCH (a {a: 1, b: 2}) => MATCH (a) WHERE a.a = 1 AND a.b = 2
        case (propId, expression) => Equals(Property(id.copyId, propId)(mapProps.position), expression)(mapProps.position)
      }.toIndexedSeq
    case expr: Expression =>
      Vector(Equals(id.copyId, expr)(expr.position))
    case _ =>
      Vector.empty
  }

  private def varLengthPropertyPredicates(id: Variable, props: Expression, patternPosition: InputPosition): Expression = {
    val idName = FreshIdNameGenerator.name(patternPosition)
    val newId = Variable(idName)(id.position)
    val expressions = propertyPredicates(newId, props)
    val conjunction = conjunct(expressions)
    AllIterablePredicate(newId, id.copyId, Some(conjunction))(props.position)
  }

  private def conjunct(exprs: Seq[Expression]): Expression = exprs match {
    case Nil           => throw new IllegalArgumentException("There should be at least one predicate to be rewritten")
    case expr +: Nil   => expr
    case expr +: tail  => And(expr, conjunct(tail))(expr.position)
  }
}
