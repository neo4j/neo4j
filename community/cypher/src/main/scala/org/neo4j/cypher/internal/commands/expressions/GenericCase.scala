/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.internal.symbols.{AnyType, CypherType, SymbolTable}
import org.neo4j.cypher.internal.commands.{Predicate, AstNode}
import org.neo4j.cypher.internal.ExecutionContext


case class GenericCase(alternatives: Seq[(Predicate, Expression)], default: Option[Expression]) extends Expression {

  require(alternatives.nonEmpty)

  def apply(ctx: ExecutionContext): Any = {
    val thisMatch: Option[Expression] = alternatives find {
      case (p, e) => p.isMatch(ctx)
    } map (_._2)

    thisMatch match {
      case Some(result) => result(ctx)
      case None         => default.getOrElse(Null()).apply(ctx)
    }
  }

  private def alternativePredicates: Seq[Predicate] = alternatives.map(_._1)
  private def alternativeExpressions: Seq[Expression] = alternatives.map(_._2)

  def children: Seq[AstNode[_]] = alternatives.map(_._1) ++ alternatives.map(_._2) ++ default.toSeq

  protected def calculateType(symbols: SymbolTable): CypherType =
    (alternativeExpressions ++ default.toSeq).
      map(_.evaluateType(AnyType(), symbols)).
      reduce(_ mergeWith _)

  def rewrite(f: (Expression) => Expression): Expression = {
    val newAlternatives: Seq[(Predicate, Expression)] = alternatives map {
      case (p, e) => (p.rewrite(f), e.rewrite(f))
    }

    val newDefault = default.map(_.rewrite(f))

    f(GenericCase(newAlternatives, newDefault))
  }

  def symbolTableDependencies: Set[String] = (alternativePredicates ++ default.toSeq ++ alternativeExpressions).
    flatMap(_.symbolTableDependencies).toSet

}