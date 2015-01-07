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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0._
import ast.convert.ExpressionConverters._
import commands.{expressions => legacy}
import commands.values.TokenType.PropertyKey
import org.junit.Test

class ExpressionsTest extends ParserTest[ast.Expression, legacy.Expression] with Expressions {
  implicit val parserToTest = Expression

  @Test def simple_cases() {
    parsing("CASE 1 WHEN 1 THEN 'ONE' END") shouldGive
      legacy.SimpleCase(legacy.Literal(1), Seq((legacy.Literal(1), legacy.Literal("ONE"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""") shouldGive
      legacy.SimpleCase(legacy.Literal(1), Seq((legacy.Literal(1), legacy.Literal("ONE")), (legacy.Literal(2), legacy.Literal("TWO"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END""") shouldGive
      legacy.SimpleCase(legacy.Literal(1), Seq((legacy.Literal(1), legacy.Literal("ONE")), (legacy.Literal(2), legacy.Literal("TWO"))), Some(legacy.Literal("DEFAULT")))
  }

  @Test def generic_cases() {
    parsing("CASE WHEN true THEN 'ONE' END") shouldGive
      legacy.GenericCase(Seq((commands.True(), legacy.Literal("ONE"))), None)

    val alt1 = (commands.Equals(legacy.Literal(1), legacy.Literal(2)), legacy.Literal("ONE"))
    val alt2 = (commands.Equals(legacy.Literal(2), legacy.Literal("apa")), legacy.Literal("TWO"))

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""") shouldGive
      legacy.GenericCase(Seq(alt1, alt2), None)

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""") shouldGive
      legacy.GenericCase(Seq(alt1, alt2), Some(legacy.Literal("OTHER")))
  }

  @Test def list_comprehension() {
    val predicate = commands.Equals(legacy.Property(legacy.Identifier("x"), PropertyKey("prop")), legacy.Literal(42))
    val mapExpression = legacy.Property(legacy.Identifier("x"), PropertyKey("name"))

    parsing("[x in collection WHERE x.prop = 42 | x.name]") shouldGive
      legacy.ExtractFunction(legacy.FilterFunction(legacy.Identifier("collection"), "x", predicate), "x", mapExpression)

    parsing("[x in collection WHERE x.prop = 42]") shouldGive
      legacy.FilterFunction(legacy.Identifier("collection"), "x", predicate)

    parsing("[x in collection | x.name]") shouldGive
      legacy.ExtractFunction(legacy.Identifier("collection"), "x", mapExpression)
  }

  @Test def array_indexing() {
    val collection = legacy.Collection(legacy.Literal(1), legacy.Literal(2), legacy.Literal(3), legacy.Literal(4))

    parsing("[1,2,3,4][1..2]") shouldGive
      legacy.CollectionSliceExpression(collection, Some(legacy.Literal(1)), Some(legacy.Literal(2)))

    parsing("[1,2,3,4][1..2][2..3]") shouldGive
      legacy.CollectionSliceExpression(legacy.CollectionSliceExpression(collection, Some(legacy.Literal(1)), Some(legacy.Literal(2))), Some(legacy.Literal(2)), Some(legacy.Literal(3)))

    parsing("collection[1..2]") shouldGive
      legacy.CollectionSliceExpression(legacy.Identifier("collection"), Some(legacy.Literal(1)), Some(legacy.Literal(2)))

    parsing("[1,2,3,4][2]") shouldGive
      legacy.CollectionIndex(collection, legacy.Literal(2))

    parsing("[[1,2]][0][6]") shouldGive
      legacy.CollectionIndex(legacy.CollectionIndex(legacy.Collection(legacy.Collection(legacy.Literal(1), legacy.Literal(2))), legacy.Literal(0)), legacy.Literal(6))

    parsing("collection[1..2][0]") shouldGive
      legacy.CollectionIndex(legacy.CollectionSliceExpression(legacy.Identifier("collection"), Some(legacy.Literal(1)), Some(legacy.Literal(2))), legacy.Literal(0))

    parsing("collection[..-2]") shouldGive
      legacy.CollectionSliceExpression(legacy.Identifier("collection"), None, Some(legacy.Literal(-2)))

    parsing("collection[1..]") shouldGive
      legacy.CollectionSliceExpression(legacy.Identifier("collection"), Some(legacy.Literal(1)), None)
  }

  @Test def literal_maps() {
    parsing("{ name: 'Andres' }") shouldGive
      legacy.LiteralMap(Map("name" -> legacy.Literal("Andres")))

    parsing("{ meta : { name: 'Andres' } }") shouldGive
      legacy.LiteralMap(Map("meta" -> legacy.LiteralMap(Map("name" -> legacy.Literal("Andres")))))

    parsing("{ }") shouldGive
      legacy.LiteralMap(Map())
  }

  @Test def better_map_support() {
    parsing("map.key1.key2.key3") shouldGive
      legacy.Property(legacy.Property(legacy.Property(legacy.Identifier("map"), PropertyKey("key1")), PropertyKey("key2")), PropertyKey("key3"))

    parsing("({ key: 'value' }).key") shouldGive
      legacy.Property(legacy.LiteralMap(Map("key" -> legacy.Literal("value"))), PropertyKey("key"))

    parsing("({ inner1: { inner2: 'Value' } }).key") shouldGive
      legacy.Property(legacy.LiteralMap(Map("inner1" -> legacy.LiteralMap(Map("inner2" -> legacy.Literal("Value"))))), PropertyKey("key"))

  }

  def convert(astNode: ast.Expression): legacy.Expression = astNode.asCommandExpression
}
