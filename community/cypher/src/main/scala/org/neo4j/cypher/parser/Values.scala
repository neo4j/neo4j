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
import org.neo4j.cypher.SyntaxException

trait Values extends JavaTokenParsers with Tokens {

  def entityValue: Parser[EntityValue] = identity ^^ ( x => EntityValue(x) )

  def value: Parser[Value] = (boolean | function | nullableProperty | property | stringValue | decimal | parameter)

  def property: Parser[Value] = identity ~ "." ~ identity ^^ {
    case v ~ "." ~ p => PropertyValue(v, p)
  }

  def nullableProperty: Parser[Value] = property <~ "?" ^^ {
    case PropertyValue(e, p) => NullablePropertyValue(e, p)
  }

  def stringValue: Parser[Value] = string ^^ (x => Literal(x))

  def decimal: Parser[Value] = decimalNumber ^^ (x => Literal(x.toDouble))

  def boolean: Parser[Value] = (trueX | falseX)

  def trueX: Parser[Value] = ignoreCase("true") ^^ (x => Literal(true))

  def falseX: Parser[Value] = ignoreCase("false") ^^ (x => Literal(false))

  def function: Parser[Value] = ident ~ parens(value | entityValue) ^^{
    case functionName ~ inner => functionName.toLowerCase match {
      case "type" => RelationshipTypeValue(inner)
      case "id" => IdValue(inner)
      case "length" => ArrayLengthValue(inner)
      case "nodes" => PathNodesValue(inner)
      case "rels" => PathRelationshipsValue(inner)
      case "relationships" => PathRelationshipsValue(inner)
      case x => throw new SyntaxException("No function '" + x + "' exists.")
    }
  }
}












