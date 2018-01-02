/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.symbols

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Add, Expression}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{CypherTypeException, SyntaxException}

class SymbolTableTest extends CypherFunSuite {

  test("anytype_is_ok") {
    //given
    val s = symbols("p" -> CTPath)

    //then
    s.evaluateType("p", CTAny) should equal(CTPath)
  }

  test("missing_identifier") {
    //given
    val s = symbols()

    //then
    intercept[SyntaxException](s.evaluateType("p", CTAny))
  }

  test("identifier_with_wrong_type") {
    //given
    val symbolTable = symbols("x" -> CTString)

    //then
    intercept[CypherTypeException](symbolTable.evaluateType("x", CTNumber))
  }

  test("identifier_with_type_not_specific_enough") {
    //given
    val symbolTable = symbols("x" -> CTMap)

    //then
    symbolTable.evaluateType("x", CTRelationship)
  }

  test("adding_string_with_string_gives_string_type") {
    //given
    val symbolTable = symbols()
    val exp = new Add(new FakeExpression(CTString), new FakeExpression(CTString))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    returnType should equal(CTString)
  }

  test("adding_number_with_number_gives_number_type") {
    //given
    val symbolTable = symbols()
    val exp = new Add(new FakeExpression(CTNumber), new FakeExpression(CTNumber))

    //when
    val returnType = exp.evaluateType(CTAny, symbolTable)

    //then
    returnType should equal(CTNumber)
  }

  test("adding_to_string_collection") {
    //given
    val symbolTable = symbols()
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

  test("intersection of two symbol tables") {
    SymbolTable() intersect SymbolTable() should equal(SymbolTable())

    symbols("a" -> CTString).intersect(
    symbols("a" -> CTString)) should equal(
    symbols("a" -> CTString))

    symbols("a" -> CTString).intersect(
    symbols("a" -> CTAny)) should equal(
    symbols("a" -> CTAny))

    symbols("a" -> CTString).intersect(
    symbols("a" -> CTNumber)) should equal(
    symbols("a" -> CTString.leastUpperBound(CTNumber)))

    symbols("a" -> CTString, "b" -> CTString).intersect(
    symbols("a" -> CTString, "c" -> CTString)) should equal(
    symbols("a" -> CTString))

    symbols("a" -> CTString, "b" -> CTString).intersect(
    symbols()) should equal(
    symbols())

    symbols("a" -> CTString).
      intersect(symbols("a" -> CTString)) should equal(symbols("a" -> CTString))
  }


  def symbols(elems: (String, CypherType)*): SymbolTable = {
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
