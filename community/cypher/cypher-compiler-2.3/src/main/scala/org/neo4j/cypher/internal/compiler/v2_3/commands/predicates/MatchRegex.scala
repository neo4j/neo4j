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
package org.neo4j.cypher.internal.compiler.v2_3.commands.predicates

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.InterpolationValue
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException

sealed abstract class MatchRegex[R <: Expression] extends Predicate with StringHelper {

  def lhs: Expression
  def regex: R

  def regexConverter: RegexConverter

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(regex.rewrite(f) match {
    case lit: Literal => MatchLiteralRegex(lhs.rewrite(f), lit)(regexConverter)
    case other        => MatchDynamicRegex(lhs.rewrite(f), other)(regexConverter)
  })

  def arguments = Seq(lhs, regex)
  def symbolTableDependencies = lhs.symbolTableDependencies ++ regex.symbolTableDependencies
}

final case class MatchLiteralRegex(lhs: Expression, regex: Literal)
                                  (implicit val regexConverter: RegexConverter = defaultRegexConverter)
  extends MatchRegex[Literal] {

  lazy val pattern = regexConverter(regex.v).getOrElse(
    throw new CypherTypeException(s"Expected regex pattern but got: ${regex.v}")
  )

  def isMatch(m: ExecutionContext)(implicit state: QueryState) =
    asOptString(lhs(m)).map(pattern.matcher(_).matches())

  override def toString = s"$lhs =~ $regex"
}

final case class MatchDynamicRegex(lhs: Expression, regex: Expression)
                                  (implicit val regexConverter: RegexConverter = defaultRegexConverter)
  extends MatchRegex[Expression] {

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] =
    asOptString(lhs(m)).flatMap { lhs =>
      regexConverter(regex(m)).map(_.matcher(lhs).matches())
    }

  override def toString: String = s"$lhs =~ /$regex/"
}

object defaultRegexConverter extends (Any => Option[java.util.regex.Pattern]) with StringHelper {
  override def apply(v: Any) = {
    val patternText = v match {
      case patternText: String => patternText
      case InterpolationValue(parts) =>
        val builder = new StringBuilder()
        parts.foreach {
          case InterpolatedStringPart(partText) =>
            builder ++= java.util.regex.Pattern.quote(partText)

          case LiteralStringPart(partText) =>
            builder ++= partText
        }
        builder.result()
    }
    Some(patternText.r.pattern)
  }
}

