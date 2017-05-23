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
package org.neo4j.cypher.internal.compiler.v3_3.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.ParameterWrongTypeException
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class ToBooleanFunctionTest extends CypherFunSuite {

  test("null in null out") {
    assert(toBoolean(null) === null)
  }

  test("converts strings to booleans") {
    Seq("true  ", "TRUE", " tRuE").foreach { s =>
      toBoolean(s) shouldBe true
    }
    Seq("false", " FALSE", "FaLsE  ").foreach { s =>
      toBoolean(s) shouldBe false
    }
  }

  test("identity for booleans") {
    toBoolean(true) shouldBe true
    toBoolean(false) shouldBe false
  }

  test("null for bad strings") {
    toBoolean("tru") shouldBe null.asInstanceOf[Any]
    toBoolean("") shouldBe null.asInstanceOf[Any]
  }

  test("throws for wrong types") {
    a [ParameterWrongTypeException] shouldBe thrownBy(toBoolean(1))
    a [ParameterWrongTypeException] shouldBe thrownBy(toBoolean(1.0))
    a [ParameterWrongTypeException] shouldBe thrownBy(ToBooleanFunction(ListLiteral.empty)(ExecutionContext.empty)(QueryStateHelper.empty))
  }

  private def toBoolean(orig: Any) = {
    ToBooleanFunction(Literal(orig))(ExecutionContext.empty)(QueryStateHelper.empty)
  }

}
