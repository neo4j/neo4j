package org.neo4j.cypher.parser

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

import org.neo4j.cypher.commands._
import scala.util.parsing.combinator._

trait Clauses extends JavaTokenParsers with Tokens with Values {
  def clause: Parser[Clause] = (isNull | isNotNull | orderedComparison | not | notEquals | equals | regexp | hasProperty | parens(clause) | sequenceClause) * (
    ignoreCase("and") ^^^ { (a: Clause, b: Clause) => And(a, b)  } |
    ignoreCase("or") ^^^  { (a: Clause, b: Clause) => Or(a, b) }
    )

  def regexp: Parser[Clause] = value ~ "=~" ~ (regularLiteral|parameter) ^^ {
    case a ~ "=~" ~ b => RegularExpression(a, b)
  }

  def hasProperty: Parser[Clause] = property ^^ {
    case prop => Has(prop.asInstanceOf[PropertyValue])
  }

  def sequenceClause: Parser[Clause] = (allInSeq | anyInSeq | noneInSeq | singleInSeq)

  def symbolIterableClause : Parser[(Value, String, Clause)] = identity ~ ignoreCase("in") ~ value ~ ":" ~ clause ^^{
    case symbol ~ in ~ iterable ~ ":" ~ klas => (iterable, symbol, klas)
  }

  def allInSeq: Parser[Clause] = ignoreCase("all") ~> parens(symbolIterableClause) ^^ ( x=> AllInSeq(x._1, x._2, x._3))
  def anyInSeq: Parser[Clause] = ignoreCase("any") ~> parens(symbolIterableClause) ^^ ( x=> AnyInSeq(x._1, x._2, x._3))
  def noneInSeq: Parser[Clause] = ignoreCase("none") ~> parens(symbolIterableClause) ^^ ( x=> NoneInSeq(x._1, x._2, x._3))
  def singleInSeq: Parser[Clause] = ignoreCase("single") ~> parens(symbolIterableClause) ^^ ( x=> SingleInSeq(x._1, x._2, x._3))

  def equals: Parser[Clause] = value ~ "=" ~ value ^^ {
    case l ~ "=" ~ r => Equals(l, r)
  }

  def notEquals: Parser[Clause] = value ~ ("!=" | "<>") ~ value ^^ {
    case l ~ wut ~ r => Not(Equals(l, r))
  }

  def orderedComparison: Parser[Clause] = (lessThanOrEqual | greaterThanOrEqual | lessThan | greaterThan)

  def lessThan: Parser[Clause] = value ~ "<" ~ value ^^ {
    case l ~ "<" ~ r => LessThan(l, r)
  }

  def greaterThan: Parser[Clause] = value ~ ">" ~ value ^^ {
    case l ~ ">" ~ r => GreaterThan(l, r)
  }

  def lessThanOrEqual: Parser[Clause] = value ~ "<=" ~ value ^^ {
    case l ~ "<=" ~ r => LessThanOrEqual(l, r)
  }

  def greaterThanOrEqual: Parser[Clause] = value ~ ">=" ~ value ^^ {
    case l ~ ">=" ~ r => GreaterThanOrEqual(l, r)
  }

  def not: Parser[Clause] = ignoreCase("not") ~ "(" ~ clause ~ ")" ^^ {
    case not ~ "(" ~ inner ~ ")" => Not(inner)
  }

  def isNull: Parser[Clause] = (value|entityValue) <~ ignoreCase("is null") ^^ ( x => IsNull(x) )
  def isNotNull: Parser[Clause] = (value|entityValue) <~ ignoreCase("is not null") ^^ ( x => Not(IsNull(x)) )

}













