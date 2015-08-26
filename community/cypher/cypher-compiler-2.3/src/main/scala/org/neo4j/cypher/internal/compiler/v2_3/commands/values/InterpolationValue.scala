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
package org.neo4j.cypher.internal.compiler.v2_3.commands.values

import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.parser.{LikePatternParser, MatchText, ParsedLikePattern}

// See ast.Interpolation for the role of this class
case class InterpolationValue(parts: NonEmptyList[InterpolationStringPart]) {
  def interpolate[T](mode: InterpolationMode[T]): T = mode(parts)
}

sealed trait InterpolationStringPart {
  def value: String
}

final case class InterpolatedStringPart(value: String) extends InterpolationStringPart
final case class LiteralStringPart(value: String) extends InterpolationStringPart

sealed trait InterpolationMode[+T] extends (NonEmptyList[InterpolationStringPart] => T)

case object TextInterpolationMode extends InterpolationMode[String] {

  def apply(parts: NonEmptyList[InterpolationStringPart]): String =
    parts.map(_.value).foldLeft("")(_ + _)
}

case object PatternInterpolationMode extends InterpolationMode[java.util.regex.Pattern] {

  def apply(parts: NonEmptyList[InterpolationStringPart]): java.util.regex.Pattern = {
    val builder = new StringBuilder()
    parts.foreach {
      case InterpolatedStringPart(partText) =>
        builder ++= java.util.regex.Pattern.quote(partText)

      case LiteralStringPart(partText) =>
        builder ++= partText
    }
    builder.result().r.pattern
  }
}

case object ParsedLikePatternInterpolationMode extends InterpolationMode[ParsedLikePattern] {
  def apply(parts: NonEmptyList[InterpolationStringPart]): ParsedLikePattern = {
    val newParts = parts.map {
      case InterpolatedStringPart(part) => List(MatchText(part))
      case LiteralStringPart(part) => LikePatternParser(part).ops
    }.toList.flatten
    ParsedLikePattern(newParts)
  }
}
