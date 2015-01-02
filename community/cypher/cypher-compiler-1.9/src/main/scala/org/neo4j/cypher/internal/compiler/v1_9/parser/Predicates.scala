/**
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
package org.neo4j.cypher.internal.compiler.v1_9.parser

import org.neo4j.cypher.internal.compiler.v1_9.commands._
import expressions.{Property, Identifier, Nullable, Expression}


trait Predicates extends Base with ParserPattern with StringLiteral {
  def predicate: Parser[Predicate] = predicateLvl1 ~ rep( ignoreCase("or") ~> predicateLvl1 ) ^^ {
    case head ~ rest => rest.foldLeft(head)((a,b) => Or(a,b))
  }

  def predicateLvl1: Parser[Predicate] = predicateLvl2 ~ rep( ignoreCase("and") ~> predicateLvl2 ) ^^{
    case head ~ rest => rest.foldLeft(head)((a,b) => And(a,b))
  }

  def predicateLvl2: Parser[Predicate] = (
    expressionOrEntity <~ ignoreCase("is null") ^^ (x => IsNull(x))
      | expressionOrEntity <~ ignoreCase("is not null") ^^ (x => Not(IsNull(x)))
      | operators
      | ignoreCase("not") ~> parens(predicate) ^^ ( inner => Not(inner) )
      | ignoreCase("not") ~> predicate ^^ ( inner => Not(inner) )
      | hasProperty
      | parens(predicate)
      | sequencePredicate
      | patternPredicate
      | aggregateFunctionNames ~> parens(expression) ~> failure("aggregate functions can not be used in the WHERE clause")
    )

  def hasProperty = ignoreCase("has") ~> parens(property) ^^ {
    case x =>
      val prop = x.asInstanceOf[Property]
      Has(prop.mapExpr, prop.property)
  }


  def sequencePredicate: Parser[Predicate] = allInSeq | anyInSeq | noneInSeq | singleInSeq | in

  def symbolIterablePredicate: Parser[(Expression, String, Predicate)] =
    (identity ~ ignoreCase("in") ~ expression ~ ignoreCase("where")  ~ predicate ^^ { case symbol ~ in ~ collection ~ where ~ klas => (collection, symbol, klas) }
      |identity ~> ignoreCase("in") ~ expression ~> failure("expected where"))

  def in: Parser[Predicate] = expression ~ ignoreCase("in") ~ expression ^^ {
    case checkee ~ in ~ collection => nullable(AnyInCollection(collection, "-_-INNER-_-", Equals(checkee, Identifier("-_-INNER-_-"))), collection)
  }

  def allInSeq: Parser[Predicate] = ignoreCase("all") ~> parens(symbolIterablePredicate) ^^ (x => nullable(AllInCollection(x._1, x._2, x._3), x._1))
  def anyInSeq: Parser[Predicate] = ignoreCase("any") ~> parens(symbolIterablePredicate) ^^ (x => nullable(AnyInCollection(x._1, x._2, x._3), x._1))
  def noneInSeq: Parser[Predicate] = ignoreCase("none") ~> parens(symbolIterablePredicate) ^^ (x => nullable(NoneInCollection(x._1, x._2, x._3), x._1))
  def singleInSeq: Parser[Predicate] = ignoreCase("single") ~> parens(symbolIterablePredicate) ^^ (x => nullable(SingleInCollection(x._1, x._2, x._3), x._1))

  def operators:Parser[Predicate] =
    (expression ~ "=" ~ expression ^^ { case l ~ "=" ~ r => nullable(Equals(l, r),l,r)  } |
      expression ~ ("<"~">") ~ expression ^^ { case l ~ wut ~ r => nullable(Not(Equals(l, r)),l,r) } |
      expression ~ "<" ~ expression ^^ { case l ~ "<" ~ r => nullable(LessThan(l, r),l,r) } |
      expression ~ ">" ~ expression ^^ { case l ~ ">" ~ r => nullable(GreaterThan(l, r),l,r) } |
      expression ~ "<=" ~ expression ^^ { case l ~ "<=" ~ r => nullable(LessThanOrEqual(l, r),l,r) } |
      expression ~ ">=" ~ expression ^^ { case l ~ ">=" ~ r => nullable(GreaterThanOrEqual(l, r),l,r) } |
      expression ~ "=~" ~ stringLit ^^ { case a ~ "=~" ~ b => nullable(LiteralRegularExpression(a, b),a,b) } |
      expression ~ "=~" ~ expression ^^ { case a ~ "=~" ~ b => nullable(RegularExpression(a, b),a,b) } |
      expression ~> "!" ~> failure("The exclamation symbol is used as a nullable property operator in Cypher. The 'not equal to' operator is <>"))

  private def nullable(pred: Predicate, e: Expression*): Predicate = if (!e.exists(_.isInstanceOf[Nullable]))
    pred
  else {
    val map = e.filter(x => x.isInstanceOf[Nullable]).
      map(x => (x, x.isInstanceOf[DefaultTrue]))

    NullablePredicate(pred, map)
  }

  def patternPredicate = pathExpression ^^ (NonEmpty(_))

  def expressionOrEntity = expression | entity

  def expression: Parser[Expression]
  def aggregateFunctionNames:Parser[String]
  def property: Parser[Expression]
  def entity: Parser[Identifier]
  def pathExpression: Parser[Expression]
}
