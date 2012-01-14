package org.neo4j.cypher.internal.parser.v16

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
import org.neo4j.cypher.SyntaxException

trait Expressions extends JavaTokenParsers with Tokens {

  def entity: Parser[Entity] = identity ^^ (x => Entity(x))

  def expression: Parser[Expression] = (boolean | extract | function | coalesceFunc | nullableProperty | property | stringExpression | decimal | parameter | entity)

  def property: Parser[Expression] = identity ~ "." ~ identity ^^ {
    case v ~ "." ~ p => Property(v, p)
  }

  def nullableProperty: Parser[Expression] = property <~ "?" ^^ {
    case p => Nullable(p)
  }

  def stringExpression: Parser[Expression] = string ^^ (x => Literal(x))

  def decimal: Parser[Expression] = number ^^ (x => Literal(x.toDouble))

  def boolean: Parser[Expression] = (trueX | falseX)

  def trueX: Parser[Expression] = ignoreCase("true") ^^ (x => Literal(true))

  def falseX: Parser[Expression] = ignoreCase("false") ^^ (x => Literal(false))

  def extract: Parser[Expression] = ignoreCase("extract") ~> parens(identity ~ ignoreCase("in") ~ expression ~ ":" ~ expression) ^^ {
    case (id ~ in ~ iter ~ ":" ~ expression) => ExtractFunction(iter, id, expression)
  }

  def coalesceFunc: Parser[Expression] = ignoreCase("coalesce") ~> parens(rep1sep(expression, ",")) ^^ {
    case expressions => CoalesceFunction(expressions:_*)
  }

  def function: Parser[Expression] = ident ~ parens(expression | entity) ^^ {
    case functionName ~ inner => functionName.toLowerCase match {
      case "type" => RelationshipTypeFunction(inner)
      case "id" => IdFunction(inner)
      case "length" => LengthFunction(inner)
      case "nodes" => NodesFunction(inner)
      case "rels" => RelationshipFunction(inner)
      case "relationships" => RelationshipFunction(inner)
      case x => throw new SyntaxException("No function '" + x + "' exists.")
    }
  }
}












