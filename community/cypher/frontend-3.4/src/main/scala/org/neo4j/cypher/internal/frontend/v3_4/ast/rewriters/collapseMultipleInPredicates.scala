/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast.{Expression, In, ListLiteral, Ors}
import org.neo4j.cypher.internal.frontend.v3_4.{Rewriter, bottomUp}

import scala.collection.immutable.Iterable

/*
This class merges multiple IN predicates into larger ones.
These can later be turned into index lookups or node-by-id ops
 */
case object collapseMultipleInPredicates extends Rewriter {

  override def apply(that: AnyRef) = instance(that)

  case class InValue(lhs: Expression, expr: Expression)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case predicate@Ors(exprs) =>
      // Find all the expressions we want to rewrite
      val (const: Seq[Expression], nonRewritable: Seq[Expression]) = exprs.toList.partition {
        case in@In(_, rhs: ListLiteral) => true
        case _ => false
      }

      // For each expression on the RHS of any IN, produce a InValue place holder
      val ins: Seq[InValue] = const.flatMap {
        case In(lhs, rhs: ListLiteral) =>
          rhs.expressions.map(expr => InValue(lhs, expr))
      }

      // Find all IN against the same predicate and rebuild the collection with all available values
      val groupedINPredicates = ins.groupBy(_.lhs)
      val flattenConst: Iterable[In] = groupedINPredicates.map {
        case (lhs, values) =>
          val pos = lhs.position
          In(lhs, ListLiteral(values.map(_.expr).toIndexedSeq)(pos))(pos)
      }

      // Return the original non-rewritten predicates with our new ones
      nonRewritable ++ flattenConst match {
        case head :: Nil => head
        case l => Ors(l.toSet)(predicate.position)
      }
  })
}
