/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.internal.commands._
import expressions.{Property, Identifier, Nullable, Expression}


trait Predicates extends Base with ParserPattern with StringLiteral with Labels {
  def predicate: Parser[Predicate] = predicateLvl1 ~ rep( OR ~> predicateLvl1 ) ^^ {
    case head ~ rest => rest.foldLeft(head)((a,b) => Or(a,b))
  }

  def predicateLvl1: Parser[Predicate] = predicateLvl2 ~ rep( AND ~> predicateLvl2 ) ^^{
    case head ~ rest => rest.foldLeft(head)((a,b) => And(a,b))
  }

  def predicateLvl2: Parser[Predicate] = (
        TRUE ^^^ True()
      | FALSE ^^^ Not(True())
      | hasLabel
      | expressionOrEntity <~ IS <~ NULL ^^ (x => IsNull(x))
      | expressionOrEntity <~ IS <~ NOT <~ NULL ^^ (x => Not(IsNull(x)))
      | operators
      | NOT ~> parens(predicate) ^^ ( inner => Not(inner) )
      | NOT ~> predicate ^^ ( inner => Not(inner) )
      | hasProperty
      | parens(predicate)
      | sequencePredicate
      | patternPredicate
      | aggregateFunctionNames ~> parens(expression) ~> failure("aggregate functions can not be used in the WHERE clause")
    )

  def hasLabel: Parser[Predicate] = entity ~ labelShortForm ^^ {
    case identifier ~ (labels: LabelSet) => HasLabel(identifier, labels.asLabelSet.labelVals)
  }

  def hasProperty = HAS ~> parens(property) ^^ {
    case prop:Property => Has(prop.mapExpr, prop.property)
  }

  def sequencePredicate: Parser[Predicate] = allInSeq | anyInSeq | noneInSeq | singleInSeq | in

  def symbolIterablePredicate: Parser[(Expression, String, Predicate)] =
    (identity ~ IN ~ expression ~ WHERE  ~ predicate ^^ { case symbol ~ in ~ collection ~ where ~ klas => (collection, symbol, klas) }
      |identity ~> IN ~ expression ~> failure("expected where"))

  def in: Parser[Predicate] = expression ~ IN ~ expression ^^ {
    case checkee ~ in ~ collection => nullable(AnyInCollection(collection, "-_-INNER-_-", Equals(checkee, Identifier("-_-INNER-_-"))), collection)
  }

  def allInSeq: Parser[Predicate] = ALL ~> parens(symbolIterablePredicate) ^^ (x => nullable(AllInCollection(x._1, x._2, x._3), x._1))
  def anyInSeq: Parser[Predicate] = ANY ~> parens(symbolIterablePredicate) ^^ (x => nullable(AnyInCollection(x._1, x._2, x._3), x._1))
  def noneInSeq: Parser[Predicate] = NONE ~> parens(symbolIterablePredicate) ^^ (x => nullable(NoneInCollection(x._1, x._2, x._3), x._1))
  def singleInSeq: Parser[Predicate] = SINGLE ~> parens(symbolIterablePredicate) ^^ (x => nullable(SingleInCollection(x._1, x._2, x._3), x._1))

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