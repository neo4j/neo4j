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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0.{commands => oldCommands}
import org.neo4j.cypher.internal.compiler.v2_0._
import ast.Expression
import commands.{expressions => old}
import commands.expressions.GenericCase
import commands.values.TokenType.PropertyKey
import org.junit.Test

class ExpressionsTest extends ParserTest[ast.Expression, old.Expression] with Expressions {
  implicit val parserToTest = Expression

  @Test def simple_cases() {
    parsing("CASE 1 WHEN 1 THEN 'ONE' END") shouldGive
      old.SimpleCase(old.Literal(1), Seq((old.Literal(1), old.Literal("ONE"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""") shouldGive
      old.SimpleCase(old.Literal(1), Seq((old.Literal(1), old.Literal("ONE")), (old.Literal(2), old.Literal("TWO"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END""") shouldGive
      old.SimpleCase(old.Literal(1), Seq((old.Literal(1), old.Literal("ONE")), (old.Literal(2), old.Literal("TWO"))), Some(old.Literal("DEFAULT")))
  }

  @Test def generic_cases() {
    parsing("CASE WHEN true THEN 'ONE' END") shouldGive
      GenericCase(Seq((oldCommands.True(), old.Literal("ONE"))), None)

    val alt1 = (oldCommands.Equals(old.Literal(1), old.Literal(2)), old.Literal("ONE"))
    val alt2 = (oldCommands.Equals(old.Literal(2), old.Literal("apa")), old.Literal("TWO"))

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""") shouldGive
      GenericCase(Seq(alt1, alt2), None)

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""") shouldGive
      GenericCase(Seq(alt1, alt2), Some(old.Literal("OTHER")))
  }

  @Test def list_comprehension() {
    val predicate = oldCommands.Equals(old.Property(old.Identifier("x"), PropertyKey("prop")), old.Literal(42))
    val mapExpression = old.Property(old.Identifier("x"), PropertyKey("name"))

    parsing("[x in collection WHERE x.prop = 42 | x.name]") shouldGive
      old.ExtractFunction(old.FilterFunction(old.Identifier("collection"), "x", predicate), "x", mapExpression)

    parsing("[x in collection WHERE x.prop = 42]") shouldGive
      old.FilterFunction(old.Identifier("collection"), "x", predicate)

    parsing("[x in collection | x.name]") shouldGive
      old.ExtractFunction(old.Identifier("collection"), "x", mapExpression)
  }

  @Test def array_indexing() {
    val collection = old.Collection(old.Literal(1), old.Literal(2), old.Literal(3), old.Literal(4))

    parsing("[1,2,3,4][1..2]") shouldGive
      old.CollectionSliceExpression(collection, Some(old.Literal(1)), Some(old.Literal(2)))

    parsing("[1,2,3,4][1..2][2..3]") shouldGive
      old.CollectionSliceExpression(old.CollectionSliceExpression(collection, Some(old.Literal(1)), Some(old.Literal(2))), Some(old.Literal(2)), Some(old.Literal(3)))

    parsing("collection[1..2]") shouldGive
      old.CollectionSliceExpression(old.Identifier("collection"), Some(old.Literal(1)), Some(old.Literal(2)))

    parsing("[1,2,3,4][2]") shouldGive
      old.CollectionIndex(collection, old.Literal(2))

    parsing("[[1,2]][0][6]") shouldGive
      old.CollectionIndex(old.CollectionIndex(old.Collection(old.Collection(old.Literal(1), old.Literal(2))), old.Literal(0)), old.Literal(6))

    parsing("collection[1..2][0]") shouldGive
      old.CollectionIndex(old.CollectionSliceExpression(old.Identifier("collection"), Some(old.Literal(1)), Some(old.Literal(2))), old.Literal(0))

    parsing("collection[..-2]") shouldGive
      old.CollectionSliceExpression(old.Identifier("collection"), None, Some(old.Literal(-2)))

    parsing("collection[1..]") shouldGive
      old.CollectionSliceExpression(old.Identifier("collection"), Some(old.Literal(1)), None)
  }

  @Test def literal_maps() {
    parsing("{ name: 'Andres' }") shouldGive
      old.LiteralMap(Map("name" -> old.Literal("Andres")))

    parsing("{ meta : { name: 'Andres' } }") shouldGive
      old.LiteralMap(Map("meta" -> old.LiteralMap(Map("name" -> old.Literal("Andres")))))

    parsing("{ }") shouldGive
      old.LiteralMap(Map())
  }

  @Test def better_map_support() {
    parsing("map.key1.key2.key3") shouldGive
      old.Property(old.Property(old.Property(old.Identifier("map"), PropertyKey("key1")), PropertyKey("key2")), PropertyKey("key3"))

    parsing("({ key: 'value' }).key") shouldGive
      old.Property(old.LiteralMap(Map("key" -> old.Literal("value"))), PropertyKey("key"))

    parsing("({ inner1: { inner2: 'Value' } }).key") shouldGive
      old.Property(old.LiteralMap(Map("inner1" -> old.LiteralMap(Map("inner2" -> old.Literal("Value"))))), PropertyKey("key"))

  }

  def convert(astNode: Expression): old.Expression = astNode.toCommand
}
