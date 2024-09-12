/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue

case class CaseExpression(alternatives: IndexedSeq[(Predicate, Expression)], default: Option[Expression])
    extends Expression {

  require(alternatives.nonEmpty)

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val thisMatch: Option[Expression] = alternatives collectFirst {
      case (p, res) if p.isTrue(row, state) => res
    }

    thisMatch match {
      case Some(result) => result(row, state)
      case None         => default.getOrElse(Null()).apply(row, state)
    }
  }

  private def alternativePredicates: IndexedSeq[Predicate] = alternatives.map(_._1)
  private def alternativeExpressions: IndexedSeq[Expression] = alternatives.map(_._2)

  override def arguments: Seq[Expression] = alternativePredicates ++ alternativeExpressions ++ default

  override def children: Seq[AstNode[_]] = arguments

  override def rewrite(f: Expression => Expression): Expression = {
    val newAlternatives: IndexedSeq[(Predicate, Expression)] = alternatives map {
      case (p, e) => (p.rewriteAsPredicate(f), e.rewrite(f))
    }

    val newDefault = default.map(_.rewrite(f))

    f(CaseExpression(newAlternatives, newDefault))
  }

}
