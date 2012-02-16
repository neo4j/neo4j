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

import org.neo4j.cypher.internal.commands._


trait Expressions extends Base {

  def term: Parser[Expression] = factor ~ rep("*" ~ factor | "/" ~ factor | "%" ~ factor | "^" ~ factor) ^^ {
    case head ~ rest => {
      var result = head
      rest.foreach {
        case "*" ~ f => result = Multiply(result,f)
        case "/" ~ f => result = Divide(result,f)
        case "%" ~ f => result = Modulo(result,f)
        case "^" ~ f => result = Pow(result,f)
      }

      result
    }
  }
  
  def expression: Parser[Expression] = term ~ rep( "+" ~ term | "-" ~ term) ^^ {
    case head ~ rest => {
      var result = head
      rest.foreach {
        case "+" ~ f => result = Add(result, f)
        case "-" ~ f => result = Subtract(result, f)
      }

      result
    }
  }

  def factor: Parser[Expression] =
    ( ignoreCase("true") ^^ (x => Literal(true))
      | ignoreCase("false") ^^ (x => Literal(false))
      | extract
      | function
      | aggregateExpression
      | identity~>parens(expression | entity)~>failure("unknown function")
      | coalesceFunc
      | nullableProperty
      | property
      | string ^^ (x => Literal(x))
      | number ^^ (x => Literal(x.toDouble))
      | parameter
      | entity
      | parens(expression)
      | failure("illegal start of value") ) 


  def entity: Parser[Entity] = identity ^^ (x => Entity(x))

  def property: Parser[Expression] = identity ~ "." ~ identity ^^ {
    case v ~ "." ~ p => createProperty(v, p)
  }
  
  def createProperty(entity:String, propName:String):Expression

  trait DefaultTrue
  trait DefaultFalse

  def nullableProperty: Parser[Expression] = (
    property <~ "?" ^^ (p => new Nullable(p) with DefaultTrue) |
    property <~ "!" ^^ (p => new Nullable(p) with DefaultFalse))

  def extract: Parser[Expression] = ignoreCase("extract") ~> parens(identity ~ ignoreCase("in") ~ expression ~ ":" ~ expression) ^^ {
    case (id ~ in ~ iter ~ ":" ~ expression) => ExtractFunction(iter, id, expression)
  }

  def coalesceFunc: Parser[Expression] = ignoreCase("coalesce") ~> parens(comaList(expression)) ^^ {
    case expressions => CoalesceFunction(expressions: _*)
  }

  def functionNames = ignoreCases("type", "id", "length", "nodes", "rels", "relationships", "abs", "round", "sqrt", "sign")
  def function: Parser[Expression] = functionNames ~ parens(expression | entity) ^^ {
    case functionName ~ inner => functionName.toLowerCase match {
      case "type" => RelationshipTypeFunction(inner)
      case "id" => IdFunction(inner)
      case "length" => LengthFunction(inner)
      case "nodes" => NodesFunction(inner)
      case "rels" => RelationshipFunction(inner)
      case "relationships" => RelationshipFunction(inner)
      case "abs" => AbsFunction(inner)
      case "round" => RoundFunction(inner)
      case "sqrt" => SqrtFunction(inner)
      case "sign" => SignFunction(inner)
    }
  }

  def aggregateExpression: Parser[Expression] = countStar | aggregationFunction

  def aggregateFunctionNames = ignoreCases("count", "sum", "min", "max", "avg", "collect")
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

  def countStar: Parser[Expression] = ignoreCase("count") ~> parens("*") ^^^ CountStar()
}












