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
package org.neo4j.cypher.internal.compiler.v2_3.parser

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SyntaxException}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.parser.Expressions
import org.parboiled.scala._

// Tested by CompileInterpolationsTest
case object Interpolations extends Parser with Expressions {

  type TrailingChars = Option[String]

  def ExpressionPart: Rule1[Left[Expression, Nothing]] =
    group("${" ~ WS ~ Expression ~ WS ~ "}") ~~> (Left(_))

  def TextPart: Rule1[Right[Nothing, String]] =
    InStringCharacters ~~> (Right(_))

  def AnyPart: Rule1[Either[Expression, String]] =
    ExpressionPart | TextPart

  def ErrorPart: Rule1[TrailingChars] =
    zeroOrMore(ANY) ~> (str => if (str.isEmpty) None else Some(str))

  def Interpolation: Rule1[(ast.Interpolation, TrailingChars)] =
    (oneOrMore(AnyPart) ~ ErrorPart) ~~>> { (parts: List[Either[Expression, String]], trailing: TrailingChars) =>
      pos => (ast.Interpolation(NonEmptyList.from(parts))(pos), trailing)
    }

  @throws(classOf[SyntaxException])
  def parse(queryText: String, offset: InputPosition): ast.Interpolation = {
    if (queryText.isEmpty)
      ast.Interpolation(NonEmptyList(Right("")))(offset)
    else {
      parseOrThrow(queryText, Some(offset), Interpolation) match {
        case (_, Some(trailing)) =>
          throw new SyntaxException(s"Invalid interpolation literal. Could not parse trailing string: $trailing")

        case (result, _) =>
          result
      }
    }
  }
}
