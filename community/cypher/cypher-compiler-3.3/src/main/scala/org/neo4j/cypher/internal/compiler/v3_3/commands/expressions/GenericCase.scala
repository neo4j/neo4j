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
package org.neo4j.cypher.internal.compiler.v3_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_3.pipes.QueryState

case class GenericCase(alternatives: IndexedSeq[(Predicate, Expression)], default: Option[Expression]) extends Expression {

  require(alternatives.nonEmpty)

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val thisMatch: Option[Expression] = alternatives collectFirst {
      case (p, res) if p.isTrue(ctx) => res
    }

    thisMatch match {
      case Some(result) => result(ctx)
      case None         => default.getOrElse(Null()).apply(ctx)
    }
  }

  private def alternativePredicates: IndexedSeq[Predicate] = alternatives.map(_._1)
  private def alternativeExpressions: IndexedSeq[Expression] = alternatives.map(_._2)

  def arguments = alternatives.map(_._1) ++ alternatives.map(_._2) ++ default.toIndexedSeq

  def rewrite(f: (Expression) => Expression): Expression = {
    val newAlternatives: IndexedSeq[(Predicate, Expression)] = alternatives map {
      case (p, e) => (p.rewriteAsPredicate(f), e.rewrite(f))
    }

    val newDefault = default.map(_.rewrite(f))

    f(GenericCase(newAlternatives, newDefault))
  }

  def symbolTableDependencies: Set[String] = {
    val expressions = alternativePredicates ++ default.toIndexedSeq ++ alternativeExpressions
    expressions.flatMap(_.symbolTableDependencies).toSet
  }
}
