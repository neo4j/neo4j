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
package org.neo4j.cypher.internal.parser.v1_7

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commands._

trait Predicates extends Base with Expressions {
  def predicate: Parser[Predicate] = (
    expressionOrEntity <~ ignoreCase("is null") ^^ (x => IsNull(x))
      | expressionOrEntity <~ ignoreCase("is not null") ^^ (x => Not(IsNull(x)))
      | operators
      | ignoreCase("not") ~> predicate ^^ ( inner => Not(inner) )
      | ignoreCase("has") ~> parens(property) ^^ ( prop => Has(prop.asInstanceOf[Property]))
      | parens(predicate)
      | sequencePredicate
      | hasRelationshipTo
      | hasRelationship
      | aggregateFunctionNames ~> parens(expression) ~> failure("aggregate functions can not be used in the WHERE clause")

    ) * (
    ignoreCase("and") ^^^ {
      (a: Predicate, b: Predicate) => And(a, b)
    } |
      ignoreCase("or") ^^^ {
        (a: Predicate, b: Predicate) => Or(a, b)
      }
    )

  def sequencePredicate: Parser[Predicate] = allInSeq | anyInSeq | noneInSeq | singleInSeq

  def symbolIterablePredicate: Parser[(Expression, String, Predicate)] =
    (identity ~ ignoreCase("in") ~ expression ~ ignoreCase("where")  ~ predicate ^^ {    case symbol ~ in ~ iterable ~ where ~ klas => (iterable, symbol, klas)  }
      |identity ~> ignoreCase("in") ~ expression ~> failure("expected where"))


  def allInSeq: Parser[Predicate] = ignoreCase("all") ~> parens(symbolIterablePredicate) ^^ (x => AllInIterable(x._1, x._2, x._3))

  def anyInSeq: Parser[Predicate] = ignoreCase("any") ~> parens(symbolIterablePredicate) ^^ (x => AnyInIterable(x._1, x._2, x._3))

  def noneInSeq: Parser[Predicate] = ignoreCase("none") ~> parens(symbolIterablePredicate) ^^ (x => NoneInIterable(x._1, x._2, x._3))

  def singleInSeq: Parser[Predicate] = ignoreCase("single") ~> parens(symbolIterablePredicate) ^^ (x => SingleInIterable(x._1, x._2, x._3))

  def operators:Parser[Predicate] =
    (expression ~ "=" ~ expression ^^ { case l ~ "=" ~ r => nullable(Equals(l, r),l,r)  } |
      expression ~ ("<"~">") ~ expression ^^ { case l ~ wut ~ r => nullable(Not(Equals(l, r)),l,r) } |
      expression ~ "<" ~ expression ^^ { case l ~ "<" ~ r => nullable(LessThan(l, r),l,r) } |
      expression ~ ">" ~ expression ^^ { case l ~ ">" ~ r => nullable(GreaterThan(l, r),l,r) } |
      expression ~ "<=" ~ expression ^^ { case l ~ "<=" ~ r => nullable(LessThanOrEqual(l, r),l,r) } |
      expression ~ ">=" ~ expression ^^ { case l ~ ">=" ~ r => nullable(GreaterThanOrEqual(l, r),l,r) } |
      expression ~ "=~" ~ regularLiteral ^^ { case a ~ "=~" ~ b => nullable(LiteralRegularExpression(a, b),a,b) } |
      expression ~ "=~" ~ expression ^^ { case a ~ "=~" ~ b => nullable(RegularExpression(a, b),a,b) } |
      expression ~> "!" ~> failure("The exclamation symbol is used as a nullable property operator in Cypher. The 'not equal to' operator is <>"))

  private def nullable(pred:Predicate, e:Expression*):Predicate = if(!e.exists(_.isInstanceOf[Nullable]))
    pred
  else
  {
    val map = e.filter(x => x.isInstanceOf[Nullable]).
      map( x => (x, x.isInstanceOf[DefaultTrue]))

    NullablePredicate(pred, map  )
  }


  def expressionOrEntity = expression | entity

  def hasRelationshipTo: Parser[Predicate] = expressionOrEntity ~ relInfo ~ expressionOrEntity ^^ { case a ~ rel ~ b => HasRelationshipTo(a, b, rel._1, rel._2) }

  def hasRelationship: Parser[Predicate] = expressionOrEntity ~ relInfo <~ "()" ^^ { case a ~ rel  => HasRelationship(a, rel._1, rel._2) }

  def relInfo: Parser[(Direction, Option[String])] = opt("<") ~ "-" ~ opt("[:" ~> identity <~ "]") ~ "-" ~ opt(">") ^^ {
    case Some("<") ~ "-" ~ relType ~ "-" ~ Some(">") => throw new SyntaxException("Can't be connected both ways.", "query", 666)
    case Some("<") ~ "-" ~ relType ~ "-" ~ None => (Direction.INCOMING, relType)
    case None ~ "-" ~ relType ~ "-" ~ Some(">") => (Direction.OUTGOING, relType)
    case None ~ "-" ~ relType ~ "-" ~ None => (Direction.BOTH, relType)
  }
}













