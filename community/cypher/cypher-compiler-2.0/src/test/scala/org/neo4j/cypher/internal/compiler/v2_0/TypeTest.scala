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
package org.neo4j.cypher.internal.compiler.v2_0

import commands.expressions._
import commands.expressions.Add
import commands.expressions.Literal
import commands.expressions.Multiply
import commands.expressions.Subtract
import pipes.QueryStateHelper
import org.junit.Test
import org.hamcrest.CoreMatchers._
import org.hamcrest.Matcher

class TypeTest {
  @Test
  def plus_int_int() {
    val op = Add(Literal(1), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Integer]))
  }

  @Test
  def plus_double_int() {
    val op = Add(Literal(1.2), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Double]))
  }

  @Test
  def minus_int_int() {
    val op = Subtract(Literal(1), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Integer]))
  }

  @Test
  def minus_double_int() {
    val op = Subtract(Literal(1.2), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Double]))
  }

  @Test
  def multiply_int_int() {
    val op = Multiply(Literal(1), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Integer]))
  }

  @Test
  def multiply_double_int() {
    val op = Multiply(Literal(1.2), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Double]))
  }

  @Test
  def divide_int_int() {
    val op = Divide(Literal(1), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Integer]))
  }

  @Test
  def divide_double_int() {
    val op = Divide(Literal(1.2), Literal(2))

    val result = calc(op)

    assertThat(result, instanceOf(classOf[java.lang.Double]))
  }

  private def assertThat(x:Any, matcher:Matcher[AnyRef]) {
    org.junit.Assert.assertThat("\nGot a: " + x.getClass, x.asInstanceOf[Object], matcher)
  }

  private def calc(e:Expression) = e.apply(ExecutionContext.empty)(QueryStateHelper.empty)
}
