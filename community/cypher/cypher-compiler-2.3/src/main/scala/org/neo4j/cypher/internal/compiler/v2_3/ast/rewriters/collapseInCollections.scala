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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, bottomUp}

import scala.collection.immutable.Iterable

/*
This class merges multiple IN predicates into larger ones.
These can later be turned into index lookups or node-by-id ops
 */
case object collapseInCollections extends Rewriter {

  override def apply(that: AnyRef) = bottomUp(instance)(that)

  case class InValue(lhs: Expression, expr: Expression)

  private val instance: Rewriter = Rewriter.lift {
    case predicate@Ors(exprs) =>
      // Find all the expressions we want to rewrite
      val (const: List[Expression], nonRewritable: List[Expression]) = exprs.toList.partition {
        case in@In(_, rhs: Collection) => true
        case _ => false
      }

      // For each expression on the RHS of any IN, produce a InValue place holder
      val ins: List[InValue] = const.flatMap {
        case In(lhs, rhs: Collection) =>
          rhs.expressions.map(expr => InValue(lhs, expr))
      }

      // Find all IN against the same predicate and rebuild the collection with all available values
      val groupedINPredicates = ins.groupBy(_.lhs)
      val flattenConst: Iterable[In] = groupedINPredicates.map {
        case (lhs, values) =>
          val pos = lhs.position
          In(lhs, Collection(values.map(_.expr).toSeq)(pos))(pos)
      }

      // Return the original non-rewritten predicates with our new ones
      nonRewritable ++ flattenConst match {
        case head :: Nil => head
        case l => Ors(l.toSet)(predicate.position)
      }
  }
}
