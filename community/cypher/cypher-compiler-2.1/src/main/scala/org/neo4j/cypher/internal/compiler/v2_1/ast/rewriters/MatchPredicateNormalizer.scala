/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1._
import ast._

object PropertyPredicateNormalization extends MatchPredicateNormalization(PropertyPredicateNormalizer)

object LabelPredicateNormalization extends MatchPredicateNormalization(LabelPredicateNormalizer)

trait MatchPredicateNormalizer {
  val extract: PartialFunction[AnyRef, Vector[Expression]]
  val replace: PartialFunction[AnyRef, AnyRef]
}

sealed class MatchPredicateNormalization(normalizer: MatchPredicateNormalizer) extends Rewriter {
  
  def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val instance: Rewriter = Rewriter.lift {
   case m@Match(_, pattern, _, where) =>
      val predicates = pattern.fold(Vector.empty[Expression]) {
        case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
        case _                                                          => identity
      }

      if (predicates.isEmpty)
        m
      else {
        val rewrittenPredicates = predicates ++ where.map(_.expression)
        val rewrittenPredicate = rewrittenPredicates reduceLeftOption { (lhs, rhs) => And(lhs, rhs)(m.position) }
        m.copy(
          pattern = pattern.rewrite(topDown(Rewriter.lift(normalizer.replace))).asInstanceOf[Pattern],
          where = rewrittenPredicate.map(Where(_)(where.map(_.position).getOrElse(m.position)))
        )(m.position)
      }
  }
}

object PropertyPredicateNormalizer extends MatchPredicateNormalizer {
  override val extract: PartialFunction[AnyRef, Vector[Expression]] = {
    case NodePattern(Some(id), _, Some(props), _)               => propertyPredicates(id, props)
    case RelationshipPattern(Some(id), _, _, _, Some(props), _) => propertyPredicates(id, props)
  }
  
  override val replace: PartialFunction[AnyRef, AnyRef] = {
    case p@NodePattern(Some(_) ,_, Some(_), _)               => p.copy(properties = None)(p.position)
    case p@RelationshipPattern(Some(_), _, _, _, Some(_), _) => p.copy(properties = None)(p.position)
  }

  private def propertyPredicates(id: Identifier, props: Expression): Vector[Expression] = props match {
    case mapProps: MapExpression =>
      mapProps.items.map {
        case (propId, expression) => Equals(Property(id, propId)(mapProps.position), expression)(mapProps.position)
      }.toVector
    case expr: Expression =>
      Vector(Equals(id, expr)(expr.position))
    case _ =>
      Vector.empty
  }
}

object LabelPredicateNormalizer extends MatchPredicateNormalizer {
  override val extract: PartialFunction[AnyRef, Vector[Expression]] = {
    case p@NodePattern(Some(id), labels, _, _) if !labels.isEmpty => Vector(HasLabels(id, labels)(p.position))
  }

  override val replace: PartialFunction[AnyRef, AnyRef] = {
    case p@NodePattern(Some(id), labels, _, _) if !labels.isEmpty => p.copy(labels = Seq.empty)(p.position)
  }
}

