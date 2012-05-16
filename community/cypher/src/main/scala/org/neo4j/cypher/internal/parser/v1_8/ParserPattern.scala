/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v1_8

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.{Entity, Expression}


trait ParserPattern extends Base with Expressions {

  abstract sealed class AbstractPattern

  case class ParsedEntity(expression: Expression, props: Map[String, Expression]) extends AbstractPattern

  case class ParsedRelation(expression: Expression,
                            props: Map[String, Expression],
                            start: ParsedEntity,
                            end: ParsedEntity,
                            typ: String,
                            dir: Direction) extends AbstractPattern

  def usePattern[T](translator: AbstractPattern => Option[T]): Parser[Seq[T]] = Parser {
    case in => {
      pattern(in) match {
        case Success(patterns, rest) if patterns.exists(p => translator.apply(p).isEmpty) => Failure("", rest)
        case Success(patterns, rest) => Success(patterns.map(p => translator(p).get), rest)
        case Failure(msg, rest) => Failure(msg, rest)
        case Error(msg, rest) => Error(msg, rest)
      }
    }
  }

  private def pattern: Parser[Seq[AbstractPattern]] = commaList(patternBit) ^^ (patterns => patterns.flatten)

  private def patternBit: Parser[Seq[AbstractPattern]] = relationship |
    node ^^ (n => Seq(n))


  private def node: Parser[ParsedEntity] =
                           expression ^^ ( name => ParsedEntity(name, Map[String, Expression]())) |
      parens(opt(identity) ~ props) ^^ { case id ~ props => ParsedEntity(Entity(namer.name(id)), props)      }

  private def relationship: Parser[Seq[AbstractPattern]] = node ~ rep1(tail) ^^ {
    case head ~ tails => {
      var start = head
      val links = tails.map {
        case l ~ "-[" ~ rel ~ ":" ~ typ ~ properties ~ "]-" ~ r ~ end => {
          val t = ParsedRelation(Entity(namer.name(rel)), properties, start, end, typ, direction(l, r))
          start = end
          t
        }
      }

      Seq(links: _*)
    }
  }

  private def tail = opt("<") ~ "-[" ~ opt(identity) ~ ":" ~ identity ~ props ~ "]-" ~ opt(">") ~ node

  private def props = opt(properties) ^^ {
    case None => Map[String, Expression]()
    case Some(x) => x
  }

  private def properties =
    expression ^^ (x => Map[String, Expression]("*" -> x)) |
      "{" ~> repsep(propertyAssignment, ",") <~ "}" ^^ (_.toMap)


  private def direction(l: Option[String], r: Option[String]): Direction = (l, r) match {
    case (None, Some(_)) => Direction.OUTGOING
    case (Some(_), None) => Direction.INCOMING
    case _ => Direction.BOTH

  }

  private def propertyAssignment: Parser[(String, Expression)] = identity ~ ":" ~ expression ^^ {
    case id ~ ":" ~ exp => (id, exp)
  }

}