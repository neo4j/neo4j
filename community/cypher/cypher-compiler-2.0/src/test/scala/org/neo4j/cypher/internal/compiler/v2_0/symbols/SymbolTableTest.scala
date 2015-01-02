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
package org.neo4j.cypher.internal.compiler.v2_0.symbols

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.{Expression, Add}
import pipes.QueryState
import org.neo4j.cypher.{CypherTypeException, SyntaxException}
import org.junit.Test
import org.scalatest.Assertions

class SymbolTableTest extends Assertions {
  @Test def anytype_is_ok() {
    //given
    val s = createSymbols("p" -> CTPath)

    //then
    assert(s.evaluateType("p", CTAny) === CTPath)
  }

  @Test def missing_identifier() {
    //given
    val s = createSymbols()

    //then
    intercept[SyntaxException](s.evaluateType("p", CTAny))
  }

  @Test def identifier_with_wrong_type() {
    //given
    val symbolTable = createSymbols("x" -> CTString)

    //then
    intercept[CypherTypeException](symbolTable.evaluateType("x", CTNumber))
  }

  @Test def identifier_with_type_not_specific_enough() {
    //given
    val symbolTable = createSymbols("x" -> CTMap)

    //then
    symbolTable.evaluateType("x", CTRelationship)
  }

  @Test def adding_string_with_string_gives_string_type() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(CTString), new FakeExpression(CTString))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    assert(returnType === CTString)
  }

  @Test def adding_number_with_number_gives_number_type() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(CTNumber), new FakeExpression(CTNumber))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    assert(returnType === CTNumber)
  }

  @Test def adding_to_string_collection() {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(CTCollection(CTString)), new FakeExpression(CTString))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    assert(returnType === CTCollection(CTString))
  }

  @Test def covariance() {
    //given
    val actual = CTCollection(CTNode)
    val expected = CTCollection(CTMap)

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

  def arguments = Nil

  def calculateType(symbols: SymbolTable) = typ

  def symbolTableDependencies = Set()
}
