/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.symbols

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Add, Expression}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.QueryState

class SymbolTableTest extends CypherFunSuite {

  test("anytype_is_ok") {
    //given
    val s = createSymbols("p" -> CTPath)

    //then
    s.evaluateType("p", CTAny) should equal(CTPath)
  }

  test("missing_identifier") {
    //given
    val s = createSymbols()

    //then
    intercept[SyntaxException](s.evaluateType("p", CTAny))
  }

  test("identifier_with_wrong_type") {
    //given
    val symbolTable = createSymbols("x" -> CTString)

    //then
    intercept[CypherTypeException](symbolTable.evaluateType("x", CTNumber))
  }

  test("identifier_with_type_not_specific_enough") {
    //given
    val symbolTable = createSymbols("x" -> CTMap)

    //then
    symbolTable.evaluateType("x", CTRelationship)
  }

  test("adding_string_with_string_gives_string_type") {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(CTString), new FakeExpression(CTString))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    returnType should equal(CTString)
  }

  test("adding_number_with_number_gives_number_type") {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(CTNumber), new FakeExpression(CTNumber))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    returnType should equal(CTNumber)
  }

  test("adding_to_string_collection") {
    //given
    val symbolTable = createSymbols()
    val exp = new Add(new FakeExpression(CTCollection(CTString)), new FakeExpression(CTString))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    returnType should equal(CTCollection(CTString))
  }

  test("covariance") {
    //given
    val actual = CTCollection(CTNode)
    val expected = CTCollection(CTMap)

    //then
    expected.isAssignableFrom(actual) should equal(true)
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
