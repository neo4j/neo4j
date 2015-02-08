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
import expressions._
import org.neo4j.cypher.SyntaxException

trait Expressions extends Base with ParserPattern with Predicates with StringLiteral {
  def expression: Parser[Expression] = term ~ rep("+" ~ term | "-" ~ term) ^^ {
    case head ~ rest =>
      var result = head
      rest.foreach {
        case "+" ~ f => result = Add(result, f)
        case "-" ~ f => result = Subtract(result, f)
      }

    result
  }

  def term: Parser[Expression] = factor ~ rep("*" ~ factor | "/" ~ factor | "%" ~ factor | "^" ~ factor) ^^ {
    case head ~ rest =>
      var result = head
      rest.foreach {
        case "*" ~ f => result = Multiply(result, f)
        case "/" ~ f => result = Divide(result, f)
        case "%" ~ f => result = Modulo(result, f)
        case "^" ~ f => result = Pow(result, f)
      }

      result
  }

  def factor: Parser[Expression] =
  (ignoreCase("true") ^^^ Literal(true)
      | ignoreCase("false") ^^^ Literal(false)
      | ignoreCase("null") ^^^ Literal(null)
      | pathExpression
      | extract
      | reduce
      | function
      | aggregateExpression
      | percentileFunction
      | coalesceFunc
      | filterFunc
      | nullableProperty
      | property
      | stringLit
      | numberLiteral
      | collectionLiteral
      | parameter
      | entity
      | parens(expression)
      | failure("illegal value"))

  def expressionOrPredicate: Parser[Expression] = pathExpression | predicate | expression

  def numberLiteral: Parser[Expression] = number ^^ (x => {
    val value: Any = if (x.contains("."))
      x.toDouble
    else
      x.toLong

    Literal(value)
  })

  def entity: Parser[Identifier] = identity ^^ (x => Identifier(x))

  def collectionLiteral: Parser[Expression] = "[" ~> repsep(expression, ",") <~ "]" ^^ (seq => Collection(seq: _*))

  def property: Parser[Expression] = identity ~ "." ~ identity ^^ {
    case v ~ "." ~ p => createProperty(v, p)
  }

  def createProperty(entity: String, propName: String): Expression

  def nullableProperty: Parser[Expression] = (
    property ~> "!=" ^^^ (throw new SyntaxException("Cypher does not support != for inequality comparisons. " +
                                                    "It's used for nullable properties instead.\n" +
                                                    "You probably meant <> instead. Read more about this in the operators chapter in the manual.")) |
    property <~ "?" ^^ (p => new Nullable(p) with DefaultTrue) |
    property <~ "!" ^^ (p => new Nullable(p) with DefaultFalse))

  def extract: Parser[Expression] = ignoreCase("extract") ~> parens(identity ~ ignoreCase("in") ~ expression ~ (":" | "|") ~ expression) ^^ {
    case (id ~ _ ~ iter ~ _ ~ expression) => ExtractFunction(iter, id, expression)
  }

  def reduce: Parser[Expression] = ignoreCase("reduce") ~> parens(identity ~ "=" ~ expression ~ "," ~ identity ~ ignoreCase("in") ~ expression ~ (":" | "|") ~ expression) ^^ {
    case (acc ~ _ ~ init ~ _ ~ id ~ _ ~ iter ~ _ ~ expression) => ReduceFunction(iter, id, expression, acc, init)
  }

  def coalesceFunc: Parser[Expression] = ignoreCase("coalesce") ~> parens(commaList(expression)) ^^ {
    case expressions => CoalesceFunction(expressions: _*)
  }

  def filterFunc: Parser[Expression] = ignoreCase("filter") ~> parens(identity ~ ignoreCase("in") ~ expression ~ (ignoreCase("where") | ":") ~ predicate) ^^ {
    case symbol ~ in ~ collection ~ where ~ pred => FilterFunction(collection, symbol, pred)
  }

  def function: Parser[Expression] = Parser {
    case in => {
      val inner = identity ~ parens(opt(commaList(expression | entity)))

      val innerResult = inner(in)

      if (!innerResult.successful) {
        innerResult.asInstanceOf[ParseResult[Nothing]]
      } else {
        val (name ~ args) = innerResult.get
        val arguments: List[Expression] = args.toList.flatten
        val funcOption = functions.get(name.toLowerCase)

        if (funcOption.isEmpty) {
          failure("unknown function", innerResult.next)
        } else {
          val func = funcOption.get
          if (!func.acceptsTheseManyArguments(arguments.size)) {
            failure("Wrong number of parameters for function " + name, innerResult.next)
          }

          Success(func.create(arguments), innerResult.next)
        }
      }

    }
  }


  private def func(numberOfArguments: Int, create: List[Expression] => Expression) = new Function(x => x == numberOfArguments, create)

  case class Function(acceptsTheseManyArguments: Int => Boolean, create: List[Expression] => Expression)

  val functions = Map(
    "type" -> func(1, args => RelationshipTypeFunction(args.head)),
    "id" -> func(1, args => IdFunction(args.head)),
    "length" -> func(1, args => LengthFunction(args.head)),
    "nodes" -> func(1, args => NodesFunction(args.head)),
    "rels" -> func(1, args => RelationshipFunction(args.head)),
    "relationships" -> func(1, args => RelationshipFunction(args.head)),
    "abs" -> func(1, args => AbsFunction(args.head)),
    "round" -> func(1, args => RoundFunction(args.head)),
    "sqrt" -> func(1, args => SqrtFunction(args.head)),
    "sign" -> func(1, args => SignFunction(args.head)),
    "head" -> func(1, args => HeadFunction(args.head)),
    "last" -> func(1, args => LastFunction(args.head)),
    "tail" -> func(1, args => TailFunction(args.head)),
    "replace" -> func(3, args => ReplaceFunction(args(0), args(1), args(2))),
    "left" -> func(2, args => LeftFunction(args(0), args(1))),
    "right" -> func(2, args => RightFunction(args(0), args(1))),
    "substring" -> Function(x => x == 2 || x == 3, args =>
      if(args.size == 2) SubstringFunction(args(0), args(1), None)
      else SubstringFunction(args(0), args(1), Some(args(2)))
    ),
    "lower" -> func(1, args => LowerFunction(args.head)),
    "upper" -> func(1, args => UpperFunction(args.head)),
    "ltrim" -> func(1, args => LTrimFunction(args.head)),
    "rtrim" -> func(1, args => RTrimFunction(args.head)),
    "trim" -> func(1, args => TrimFunction(args.head)),
    "str" -> func(1, args => StrFunction(args.head)),
    "timestamp" -> func(0, args => TimestampFunction()),
    "shortestpath" -> Function(x => false, args => null),
    "range" -> Function(x => x == 2 || x == 3, args => {
      val step = if (args.size == 2) Literal(1) else args(2)
      RangeFunction(args(0), args(1), step)
    })
  )

  def aggregateExpression: Parser[Expression] = countStar | aggregationFunction

  def aggregateFunctionNames: Parser[String] = ignoreCases("count", "sum", "min", "max", "avg", "collect")

  def aggregationFunction: Parser[Expression] = aggregateFunctionNames ~ parens(opt(ignoreCase("distinct")) ~ expression) ^^ {
    case function ~ (distinct ~ inner) => {

      val aggregateExpression = function match {
        case "count" => Count(inner)
        case "sum" => Sum(inner)
        case "min" => Min(inner)
        case "max" => Max(inner)
        case "avg" => Avg(inner)
        case "collect" => Collect(inner)
      }

      if (distinct.isEmpty) {
        aggregateExpression
      }
      else {
        Distinct(aggregateExpression, inner)
      }
    }
  }

  def percentileFunctionNames: Parser[String] = ignoreCases("percentile_cont", "percentile_disc")

  def percentileFunction: Parser[Expression] = percentileFunctionNames ~ parens(expression ~ "," ~ expression) ^^ {
    case function ~ (property ~ "," ~ percentile) => function match {
      case "percentile_cont" => PercentileCont(property, percentile)
      case "percentile_disc" => PercentileDisc(property, percentile)
    } 
  }

  def countStar: Parser[Expression] = ignoreCase("count") ~> parens("*") ^^^ CountStar()

  def pathExpression: Parser[Expression] = usePath(translate) ^^ {
    case Seq(x:ShortestPath) => ShortestPathExpression(x)
    case patterns => PathExpression(patterns)
  }

  private def translate(abstractPattern: AbstractPattern): Maybe[Pattern] = matchTranslator(abstractPattern) match {
    case Yes(p) if p.size == 1 && p.head.isInstanceOf[SingleNode] => No(Seq(""))
    case Yes(Seq(np)) if np.isInstanceOf[NamedPath]               => No(Seq("Can't assign to an identifier in a pattern expression"))
    case Yes(p@Seq(pattern: Pattern))                             => Yes(p.asInstanceOf[Seq[Pattern]])
    case n: No                                                    => n
  }

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any]
}

trait DefaultTrue

trait DefaultFalse












