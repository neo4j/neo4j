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
package org.neo4j.cypher.internal.parser.legacy

import org.neo4j.cypher.internal.commands._
import expressions._
import expressions.Property
import org.neo4j.cypher.internal.commands.RegularExpression
import org.neo4j.cypher.internal.commands.GreaterThan
import org.neo4j.cypher.internal.commands.GreaterThanOrEqual
import org.neo4j.cypher.internal.commands.SingleInCollection
import org.neo4j.cypher.internal.commands.PatternPredicate
import org.neo4j.cypher.internal.commands.LessThanOrEqual
import org.neo4j.cypher.internal.commands.LiteralRegularExpression
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.AllInCollection
import org.neo4j.cypher.internal.commands.IsNull
import org.neo4j.cypher.internal.commands.ShortestPath
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.commands.Or
import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.commands.NoneInCollection
import org.neo4j.cypher.internal.commands.Not
import org.neo4j.cypher.internal.commands.AnyInCollection
import org.neo4j.cypher.internal.commands.HasLabel
import org.neo4j.cypher.internal.commands.Has
import org.neo4j.cypher.internal.commands.LessThan
import org.neo4j.cypher.internal.parser.{No, Yes, Maybe, AbstractPattern}

trait Predicates extends Base with ParserPattern with StringLiteral with Labels {
  def predicate: Parser[Predicate] = predicateLvl1 ~ rep(OR ~> predicateLvl1) ^^ {
    case head ~ rest => rest.foldLeft(head)((a, b) => Or(a, b))
  }

  def predicateLvl1: Parser[Predicate] = predicateLvl2 ~ rep(XOR ~> predicateLvl2) ^^ {
    case head ~ rest => rest.foldLeft(head)((a, b) => Xor(a, b))
  }

  def predicateLvl2: Parser[Predicate] = predicateLvl3 ~ rep(AND ~> predicateLvl3) ^^ {
    case head ~ rest => rest.foldLeft(head)((a, b) => And(a, b))
  }

  def predicateLvl3: Parser[Predicate] = (
        operators
      | TRUE ^^^ True()
      | FALSE ^^^ Not(True())
      | hasLabel
      | expressionOrEntity <~ IS <~ NULL ^^ (x => IsNull(x))
      | expressionOrEntity <~ IS <~ NOT <~ NULL ^^ (x => Not(IsNull(x)))
      | NOT ~> parens(predicate) ^^ ( inner => Not(inner) )
      | NOT ~> predicate ^^ ( inner => Not(inner) )
      | hasProperty
      | parens(predicate)
      | sequencePredicate
      | patternPredicate
      | aggregateFunctionNames ~> parens(expression) ~> failure("aggregate functions can not be used in the WHERE clause")
    )

  def hasLabel: Parser[Predicate] = entity ~ labelShortForm ^^ {
    case identifier ~ labels => True().andWith(labels.map(HasLabel(identifier,_)): _*)
  }

  def hasProperty = HAS ~> parens(property) ^^ {
    case prop:Property => Has(prop.mapExpr, prop.propertyKey)
  }

  def sequencePredicate: Parser[Predicate] = allInSeq | anyInSeq | noneInSeq | singleInSeq | in

  def symbolIterablePredicate: Parser[(Expression, String, Predicate)] =
    (identity ~ IN ~ expression ~ WHERE  ~ predicate ^^ { case symbol ~ in ~ collection ~ where ~ klas => (collection, symbol, klas) }
      |identity ~> IN ~ expression ~> failure("expected where"))

  def in: Parser[Predicate] = expression ~ IN ~ expression ^^ {
    case checkee ~ in ~ collection => AnyInCollection(collection, "-_-INNER-_-", Equals(checkee, Identifier("-_-INNER-_-")))
  }

  def allInSeq: Parser[Predicate] = ALL ~> parens(symbolIterablePredicate) ^^ (x => AllInCollection(x._1, x._2, x._3))
  def anyInSeq: Parser[Predicate] = ANY ~> parens(symbolIterablePredicate) ^^ (x => AnyInCollection(x._1, x._2, x._3))
  def noneInSeq: Parser[Predicate] = NONE ~> parens(symbolIterablePredicate) ^^ (x => NoneInCollection(x._1, x._2, x._3))
  def singleInSeq: Parser[Predicate] = SINGLE ~> parens(symbolIterablePredicate) ^^ (x => SingleInCollection(x._1, x._2, x._3))

  def operators:Parser[Predicate] =
    (expression ~ "=" ~ expression ^^ { case l ~ "=" ~ r => Equals(l, r)  } |
      expression ~ ("<"~">") ~ expression ^^ { case l ~ wut ~ r => Not(Equals(l, r)) } |
      expression ~ "<" ~ expression ^^ { case l ~ "<" ~ r => LessThan(l, r) } |
      expression ~ ">" ~ expression ^^ { case l ~ ">" ~ r => GreaterThan(l, r) } |
      expression ~ "<=" ~ expression ^^ { case l ~ "<=" ~ r => LessThanOrEqual(l, r) } |
      expression ~ ">=" ~ expression ^^ { case l ~ ">=" ~ r => GreaterThanOrEqual(l, r) } |
      expression ~ "=~" ~ stringLit ^^ { case a ~ "=~" ~ b => LiteralRegularExpression(a, b) } |
      expression ~ "=~" ~ expression ^^ { case a ~ "=~" ~ b => RegularExpression(a, b) } |
      expression ~> "!" ~> failure("Cypher does not support != for inequality comparisons. Use <> instead."))

  def patternPredicate: Parser[Predicate] = {
    def translate(abstractPattern: AbstractPattern): Maybe[(Pattern, Predicate)] = matchTranslator(abstractPattern) match {
      case Yes(p) if p.size == 1 && p.head.isInstanceOf[SingleNode] => No(Seq(""))
      case Yes(Seq(np)) if np.isInstanceOf[ShortestPath] => No(Seq("Shortest path is not a predicate"))
      case Yes(Seq(np)) if np.isInstanceOf[NamedPath]    => No(Seq("Can't assign to an identifier in a pattern predicate"))
      case n: No                                         => n
      case Yes(p@Seq(pattern: Pattern))                  =>
        val patterns = p.asInstanceOf[Seq[Pattern]]

        if (patterns.exists(_.optional))
          No(Seq("Optional patterns cannot be used as predicates"))
        else {
          val predicates = abstractPattern.parsedLabelPredicates
          val pred = True().andWith(predicates: _*)

          Yes(patterns.map( (_, pred) ))
        }
    }

    usePath(translate) ^^ {
      case combo:Seq[(Pattern,Predicate)] =>

        val patterns: Seq[Pattern] = combo.map(_._1)
        val predicates: Seq[Predicate] = combo.flatMap(_._2.atoms).distinct

        PatternPredicate(patterns, True().andWith(predicates: _*))
    }
  }

  def expressionOrEntity = expression | entity

  def expression: Parser[Expression]
  def aggregateFunctionNames:Parser[String]
  def property: Parser[Expression]
  def entity: Parser[Identifier]

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any]
}
