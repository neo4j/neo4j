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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.aux.v3_4.ParameterWrongTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values._

class ToBooleanFunctionTest extends CypherFunSuite {

  test("null in null out") {
    assert(toBoolean(null) === NO_VALUE)
  }

  test("converts strings to booleans") {
    Seq("true  ", "TRUE", " tRuE").foreach { s =>
      toBoolean(s) shouldBe TRUE
    }
    Seq("false", " FALSE", "FaLsE  ").foreach { s =>
      toBoolean(s) shouldBe FALSE
    }
  }

  test("identity for booleans") {
    toBoolean(true) shouldBe TRUE
    toBoolean(false) shouldBe FALSE
  }

  test("null for bad strings") {
    toBoolean("tru") shouldBe NO_VALUE
    toBoolean("") shouldBe NO_VALUE
  }

  test("throws for wrong types") {
    a [ParameterWrongTypeException] shouldBe thrownBy(toBoolean(1))
    a [ParameterWrongTypeException] shouldBe thrownBy(toBoolean(1.0))
    val toBooleanOfList = ToBooleanFunction(ListLiteral.empty)
    a [ParameterWrongTypeException] shouldBe thrownBy(toBooleanOfList(ExecutionContext.empty, QueryStateHelper.empty))
  }

  private def toBoolean(orig: Any) = {
    ToBooleanFunction(Literal(orig))(ExecutionContext.empty, QueryStateHelper.empty)
  }

}
