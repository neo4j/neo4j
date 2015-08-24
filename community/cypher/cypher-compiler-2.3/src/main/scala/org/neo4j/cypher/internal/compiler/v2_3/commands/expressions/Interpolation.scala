/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.InterpolationValue
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.symbols.{CypherType, _}

import scala.annotation.tailrec
import scala.collection.mutable

// See ast.Interpolation for the role of this class
case class Interpolation(parts: NonEmptyList[Either[Expression, String]]) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val partsValues = compile(parts, ctx, state, NonEmptyList.newBuilder[InterpolationStringPart])
    val result = partsValues.map(InterpolationValue).orNull
    result
  }

  @tailrec
  private def compile(
    remaining: NonEmptyList[Either[Expression, String]],
    ctx: ExecutionContext,
    state: QueryState,
    acc: mutable.Builder[InterpolationStringPart, Option[NonEmptyList[InterpolationStringPart]]])
  : Option[NonEmptyList[InterpolationStringPart]] = {

    val stop = remaining.head match {
      case Left(expr) =>
        val value = expr.apply(ctx)(state)
        if (value == null)
          true
        else {
          acc += InterpolatedStringPart(toStringValue(value))
          false
        }
      case Right(string) =>
        acc += LiteralStringPart(string)
        false
    }

    if (stop)
      None
    else
      remaining.tailOption match {
        case None => acc.result()
        case Some(tail) => compile(tail, ctx, state, acc)
      }
  }

  def rewrite(f: (Expression) => Expression) = f(copy(parts = parts.map(_.left.map(_.rewrite(f)))))

  def arguments = Nil

  def calculateType(symbols: SymbolTable): CypherType = CTString

  def symbolTableDependencies = parts.map(_.left.toOption.map(_.symbolTableDependencies).getOrElse(Set.empty)).reduceLeft(_ ++ _)

  override def toString = s"Interpolation(${parts.map(_.toString).reduceLeft(_ + ", " + _)}})"
}

// See ast.Interpolation for the role of this class
sealed trait InterpolationStringPart {
  def value: String
}

final case class InterpolatedStringPart(value: String) extends InterpolationStringPart
final case class LiteralStringPart(value: String) extends InterpolationStringPart

