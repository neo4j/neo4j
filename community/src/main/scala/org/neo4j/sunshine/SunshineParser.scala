/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.sunshine

import commands._
import scala.util.parsing.combinator._
import org.neo4j.graphdb.Direction
import scala.Some

class SunshineParser extends JavaTokenParsers {
  def ignoreCase(str:String): Parser[String] = ("""(?i)\Q""" + str + """\E""").r

  def query: Parser[Query] = start ~ opt(matching) ~ opt(where) ~ returns ^^ {
    case start ~ matching ~ where ~ returns => Query(returns._1, start, matching, where, returns._2)
  }

  def start: Parser[Start] = ignoreCase("start") ~> repsep(nodeByIds | nodeByIndex, ",") ^^ (Start(_: _*))

  def matching: Parser[Match] = ignoreCase("match") ~> rep1sep(relatedTo, ",") ^^ { case matching:List[List[Pattern]] => Match(matching.flatten: _*) }

  def relatedTo: Parser[List[Pattern]] = relatedHead ~ rep1(relatedTail) ^^ {
    case head ~ tails => {
      val namer = new NodeNamer
      var last = namer.name(head)
      val list = tails.map((item) => {
        val back = item._1
        val relName = item._2
        val relType = item._3
        val forward = item._4
        val end = namer.name(item._5)

        val result: Pattern = RelatedTo(last, end, relName, relType, getDirection(back, forward))

        last = end

        result
      })

      list
    }
  }

  def relatedHead:Parser[Option[String]] = "(" ~> opt(ident) <~ ")" ^^ { case name => name }

  def relatedTail = opt("<") ~ "-" ~ opt("[" ~> (relationshipInfo1 | relationshipInfo2) <~ "]") ~ "-" ~ opt(">") ~ "(" ~ opt(ident) ~ ")" ^^ {
    case back ~ "-" ~ relInfo ~ "-" ~ forward ~ "(" ~ end ~ ")" => relInfo match {
      case Some(x) => (back, x._1, x._2, forward, end)
      case None => (back, None, None, forward, end)
    }
  }

  def relationshipInfo1 = opt(ident <~ ",") ~ ":" ~ ident ^^ {
    case relName ~ ":" ~ relType => (relName, Some(relType))
  }

  def relationshipInfo2 = ident ^^ {
    case relName => (Some(relName), None)
  }

  def nodeByIds = ident ~ "=" ~ "(" ~ rep1sep(wholeNumber, ",") ~ ")" ^^ {
    case varName ~ "=" ~ "(" ~ id ~ ")" => NodeById(varName, id.map(_.toLong).toSeq: _*)
  }

  def nodeByIndex = ident ~ "=" ~ "(" ~ ident ~ "," ~ ident ~ "," ~ stringLiteral ~ ")" ^^ {
    case varName ~ "=" ~ "(" ~ index ~ "," ~ key ~ "," ~ value ~ ")" => NodeByIndex(varName, index, key, stripQuotes(value))
  }

  def returns: Parser[(Return, Option[Aggregation])] = ignoreCase("return") ~> rep1sep((count | nullablePropertyOutput | propertyOutput | nodeOutput), ",") ^^
    { case items => {
      val list = items.filter(_.isInstanceOf[AggregationItem]).map(_.asInstanceOf[AggregationItem])

      (
        Return(items.filter(!_.isInstanceOf[AggregationItem]): _*),
        list match {
          case List() => None
          case _ => Some(Aggregation(list : _*))
        }
      )
    }}

  def nodeOutput:Parser[ReturnItem] = ident ^^ { EntityOutput(_) }

  def propertyOutput:Parser[ReturnItem] = ident ~ "." ~ ident ^^
    { case c ~ "." ~ p => PropertyOutput(c,p) }

  def nullablePropertyOutput:Parser[ReturnItem] = ident ~ "." ~ ident ~ "?" ^^
    { case c ~ "." ~ p ~ "?" => NullablePropertyOutput(c,p) }

  def count:Parser[ReturnItem] = ignoreCase("count") ~ "(" ~ "*" ~ ")" ^^
    { case count ~ "(" ~ "*" ~ ")" => Count("*") }

  def where: Parser[Clause] = ignoreCase("where") ~> clause ^^ { case klas => klas }

  def clause: Parser[Clause] = (orderedComparison | not | notEquals | equals | regexp | parens) * (
    "and" ^^^ { (a: Clause, b: Clause) => And(a, b) } |
    "or" ^^^ {  (a: Clause, b: Clause) => Or(a, b) })

  def regexp: Parser[Clause] = value ~ "=~" ~ regularLiteral ^^ {
    case a ~ "=~" ~ b => RegularExpression(a, stripQuotes(b))
  }


  def regularLiteral = ("/"+"""([^"\p{Cntrl}\\]|\\[\\/bfnrt]|\\u[a-fA-F0-9]{4})*"""+"/").r


  def parens: Parser[Clause] = "(" ~> clause <~ ")"

  def equals: Parser[Clause] = value ~ "=" ~ value ^^ {
    case l ~ "=" ~ r => Equals(l,r)
  }

  def orderedComparison:Parser[Clause] = (lessThanOrEqual | greaterThanOrEqual | lessThan | greaterThan)

  def lessThan: Parser[Clause] = value ~ "<" ~ value ^^ {
    case l ~ "<" ~ r => LessThan(l,r)
  }

  def greaterThan: Parser[Clause] = value ~ ">" ~ value ^^ {
    case l ~ ">" ~ r => GreaterThan(l,r)
  }

  def lessThanOrEqual: Parser[Clause] = value ~ "<=" ~ value ^^ {
    case l ~ "<=" ~ r => LessThanOrEqual(l,r)
  }

  def greaterThanOrEqual: Parser[Clause] = value ~ ">=" ~ value ^^ {
    case l ~ ">=" ~ r => GreaterThanOrEqual(l,r)
  }

  def notEquals: Parser[Clause] = value ~ "<>" ~ value ^^ {
    case l ~ "<>" ~ r => Not(Equals(l,r))
  }

  def not:Parser[Clause] = ignoreCase("not") ~ "(" ~ clause ~ ")" ^^ {
    case not ~ "(" ~ inner ~ ")" => Not(inner)
  }

  def value: Parser[Value] = (boolean | property | string | decimal)

  def property: Parser[Value] = ident ~ "." ~ ident ^^ {  case v ~ "." ~ p => PropertyValue(v,p) }

  def string: Parser[Value] = stringLiteral ^^ { case str => Literal(stripQuotes(str)) }

  def decimal: Parser[Value] = decimalNumber ^^ { case num => Literal(num.toLong) }

  def boolean: Parser[Value] = (trueX | falseX)
  def trueX: Parser[Value] = ignoreCase("true") ^^ { case str => Literal(true) }
  def falseX: Parser[Value] = ignoreCase("false") ^^ { case str => Literal(false) }

  private def stripQuotes(s: String) = s.substring(1, s.length - 1)

  private def getDirection(back:Option[String], forward:Option[String]):Direction =
    (back.nonEmpty, forward.nonEmpty) match {
      case (true,false) => Direction.INCOMING
      case (false,true) => Direction.OUTGOING
      case (false,false) => Direction.BOTH
      case (true,true) => Direction.BOTH
    }

  @throws(classOf[SyntaxError])
  def parse(sunshineQuery: String): Query =
    parseAll(query, sunshineQuery) match {
      case Success(r, q) => r
      case NoSuccess(message, input) => throw new SyntaxError(message)
    }

  class NodeNamer {
    var lastNodeNumber = 0

    def name(s:Option[String]):String = s match {
      case None => {
        lastNodeNumber += 1
        "___NODE" + lastNodeNumber
      }
      case Some(x) => x
    }
  }
}