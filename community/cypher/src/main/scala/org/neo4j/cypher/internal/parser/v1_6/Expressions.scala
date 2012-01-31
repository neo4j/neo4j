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
package org.neo4j.cypher.internal.parser.v1_6

import org.neo4j.cypher.internal.commands._


trait Expressions extends Base {

  def entity: Parser[Entity] = identity ^^ (x => Entity(x))

  def expression: Parser[Expression] =
    (ignoreCase("true") ^^ (x => Literal(true))
      | ignoreCase("false") ^^ (x => Literal(false))
      | extract
      | function
      | identity~>parens(expression | entity)~>failure("unknown function")
      | coalesceFunc
      | nullableProperty
      | property
      | string ^^ (x => Literal(x))
      | number ^^ (x => Literal(x.toDouble))
      | parameter
      | entity
      | failure("illegal start of value") )

  def property: Parser[Expression] = identity ~ "." ~ identity ^^ {
    case v ~ "." ~ p => createProperty(v, p)
  }
  
  def createProperty(entity:String, propName:String):Expression

  def nullableProperty: Parser[Expression] = property <~ "?" ^^ (p => Nullable(p))

  def extract: Parser[Expression] = ignoreCase("extract") ~> parens(identity ~ ignoreCase("in") ~ expression ~ ":" ~ expression) ^^ {
    case (id ~ in ~ iter ~ ":" ~ expression) => ExtractFunction(iter, id, expression)
  }

  def coalesceFunc: Parser[Expression] = ignoreCase("coalesce") ~> parens(comaList(expression)) ^^ {
    case expressions => CoalesceFunction(expressions: _*)
  }

  def function: Parser[Expression] = ignoreCases("type", "id", "length", "nodes", "rels", "relationships") ~ parens(expression | entity) ^^ {
    case functionName ~ inner => functionName.toLowerCase match {
      case "type" => RelationshipTypeFunction(inner)
      case "id" => IdFunction(inner)
      case "length" => LengthFunction(inner)
      case "nodes" => NodesFunction(inner)
      case "rels" => RelationshipFunction(inner)
      case "relationships" => RelationshipFunction(inner)
    }
  }
}












