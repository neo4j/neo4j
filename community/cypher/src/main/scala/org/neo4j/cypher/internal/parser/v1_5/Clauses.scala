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
package org.neo4j.cypher.internal.parser.v1_5

import org.neo4j.cypher.commands._
import scala.util.parsing.combinator._

trait Clauses extends JavaTokenParsers with Tokens with Values {
  def clause: Parser[Predicate] = (isNull | isNotNull | orderedComparison | not | notEquals | equals | regexp | hasProperty | parens(clause) | sequenceClause) * (
    ignoreCase("and") ^^^ { (a: Predicate, b: Predicate) => And(a, b)  } |
    ignoreCase("or") ^^^  { (a: Predicate, b: Predicate) => Or(a, b) }
    )

  def regexp: Parser[Predicate] = value ~ "=~" ~ (regularLiteral|parameter) ^^ {
    case a ~ "=~" ~ b => RegularExpression(a, b)
  }

  def hasProperty: Parser[Predicate] = property ^^ {
    case prop => Has(prop.asInstanceOf[Property])
  }

  def sequenceClause: Parser[Predicate] = (allInSeq | anyInSeq | noneInSeq | singleInSeq)

  def symbolIterableClause : Parser[(Expression, String, Predicate)] = identity ~ ignoreCase("in") ~ value ~ ":" ~ clause ^^{
    case symbol ~ in ~ iterable ~ ":" ~ klas => (iterable, symbol, klas)
  }

  def allInSeq: Parser[Predicate] = ignoreCase("all") ~> parens(symbolIterableClause) ^^ ( x=> AllInIterable(x._1, x._2, x._3))
  def anyInSeq: Parser[Predicate] = ignoreCase("any") ~> parens(symbolIterableClause) ^^ ( x=> AnyInIterable(x._1, x._2, x._3))
  def noneInSeq: Parser[Predicate] = ignoreCase("none") ~> parens(symbolIterableClause) ^^ ( x=> NoneInIterable(x._1, x._2, x._3))
  def singleInSeq: Parser[Predicate] = ignoreCase("single") ~> parens(symbolIterableClause) ^^ ( x=> SingleInIterable(x._1, x._2, x._3))

  def equals: Parser[Predicate] = value ~ "=" ~ value ^^ {
    case l ~ "=" ~ r => Equals(l, r)
  }

  def notEquals: Parser[Predicate] = value ~ ("!=" | "<>") ~ value ^^ {
    case l ~ wut ~ r => Not(Equals(l, r))
  }

  def orderedComparison: Parser[Predicate] = (lessThanOrEqual | greaterThanOrEqual | lessThan | greaterThan)

  def lessThan: Parser[Predicate] = value ~ "<" ~ value ^^ {
    case l ~ "<" ~ r => LessThan(l, r)
  }

  def greaterThan: Parser[Predicate] = value ~ ">" ~ value ^^ {
    case l ~ ">" ~ r => GreaterThan(l, r)
  }

  def lessThanOrEqual: Parser[Predicate] = value ~ "<=" ~ value ^^ {
    case l ~ "<=" ~ r => LessThanOrEqual(l, r)
  }

  def greaterThanOrEqual: Parser[Predicate] = value ~ ">=" ~ value ^^ {
    case l ~ ">=" ~ r => GreaterThanOrEqual(l, r)
  }

  def not: Parser[Predicate] = ignoreCase("not") ~ "(" ~ clause ~ ")" ^^ {
    case not ~ "(" ~ inner ~ ")" => Not(inner)
  }

  def isNull: Parser[Predicate] = (value|entityValue) <~ ignoreCase("is null") ^^ ( x => IsNull(x) )
  def isNotNull: Parser[Predicate] = (value|entityValue) <~ ignoreCase("is not null") ^^ ( x => Not(IsNull(x)) )

}













