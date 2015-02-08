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
package org.neo4j.cypher.internal.compiler.v1_9.symbols

import org.junit.Test
import org.neo4j.cypher.{CypherTypeException, SyntaxException}
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.{Expression, Add}
import org.neo4j.cypher.internal.compiler.v1_9.ExecutionContext
import org.neo4j.cypher.internal.compiler.v1_9.pipes.QueryState

class SymbolTableTest extends Assertions {
  @Test def anytype_is_ok() {
    //given
    val s = createSymbols("p" -> PathType())

    //then
    assert(s.evaluateType("p", AnyType()) === PathType())
  }

  @Test def missing_identifier() {
    //given
    val s = createSymbols()

    //then
    intercept[SyntaxException](s.evaluateType("p", AnyType()))
  }

  @Test def identifier_with_wrong_type() {
    //given
    val symbolTable = createSymbols("x" -> StringType())

    //then
    intercept[CypherTypeException](symbolTable.evaluateType("x", NumberType()))
  }

  @Test def identifier_with_type_not_specific_enough() {
    //given
    val symbolTable = createSymbols("x" -> MapType())

    //then
    symbolTable.evaluateType("x", RelationshipType())
  }

  @Test def adding_string_with_string_gives_string_type() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(StringType()), new FakeExpression(StringType()))

    //when
    val returnType = exp.evaluateType(AnyType(), symbolTable)

    //then
    assert(returnType === StringType())
  }

  @Test def adding_number_with_number_gives_number_type() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(NumberType()), new FakeExpression(NumberType()))

    //when
    val returnType = exp.evaluateType(AnyType(), symbolTable)

    //then
    assert(returnType === NumberType())
  }

  @Test def adding_to_string_collection() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(new CollectionType(StringType())), new FakeExpression(StringType()))

    //when
    val returnType = exp.evaluateType(AnyType(), symbolTable)

    //then
    assert(returnType === new CollectionType(StringType()))
  }

  @Test def covariance() {
    //given
    val actual = new CollectionType(NodeType())
    val expected = new CollectionType(MapType())

    //then
    assert(expected.isAssignableFrom(actual))
  }


  def createSymbols(elems: (String, CypherType)*): SymbolTable = {
    SymbolTable(elems.toMap)
  }
}

class FakeExpression(typ: CypherType) extends Expression {
  def apply(v1: ExecutionContext)(implicit state: QueryState): Any = null

  def rewrite(f: (Expression) => Expression): Expression = null

  def children = Nil

  def calculateType(symbols: SymbolTable) = typ

  def symbolTableDependencies = Set()
}
