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

import scala.util.parsing.combinator._
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commands._

trait Values extends JavaTokenParsers with Tokens {

  def entityValue: Parser[Entity] = identity ^^ ( x => Entity(x) )

  def value: Parser[Expression] = (boolean | function | nullableProperty | property | stringValue | decimal | parameter)

  def property: Parser[Expression] = identity ~ "." ~ identity ^^ {
    case v ~ "." ~ p => Property(v, p)
  }

  def nullableProperty: Parser[Expression] = property <~ "?" ^^ {
    case Property(e, p) => Nullable(Property(e, p))
  }

  def stringValue: Parser[Expression] = string ^^ (x => Literal(x))

  def decimal: Parser[Expression] = decimalNumber ^^ (x => Literal(x.toDouble))

  def boolean: Parser[Expression] = (trueX | falseX)

  def trueX: Parser[Expression] = ignoreCase("true") ^^ (x => Literal(true))

  def falseX: Parser[Expression] = ignoreCase("false") ^^ (x => Literal(false))

  def function: Parser[Expression] = ident ~ parens(value | entityValue) ^^{
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












