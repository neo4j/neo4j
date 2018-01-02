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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.scalatest.FunSuite
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper

class SplitFunctionTest extends CypherFunSuite {

  val nullSeq = null.asInstanceOf[Seq[String]]
  val nullString = null.asInstanceOf[String]

  test("passing null to split() returns null") {
    split("something", nullString) should be(nullSeq)
    split(nullString, "something") should be(nullSeq)
  }

  test("splitting non-empty strings with one character") {
    split("first,second", ",") should be(Seq("first", "second"))
  }

  test("splitting non-empty strings with more than one character") {
    split("first11second11third", "11") should be(Seq("first", "second", "third"))
  }

  test("splitting an empty string should return an empty string") {
    split("", ",") should be(Seq(""))
  }

  test("splitting a string containing only the split pattern should return two empty strings") {
    split(",", ",") should be(Seq("",""))
  }

  test("using an empty separator should split on every character") {
    split("banana", "") should be(Seq("b", "a", "n", "a", "n", "a"))
    split("a", "") should be(Seq("a"))
    split("", "") should be(Seq(""))
  }

  private def split(orig: String, splitPattern: String) = {
    val expr = SplitFunction(Literal(orig), Literal(splitPattern))
    expr(ExecutionContext.empty)(QueryStateHelper.empty)
  }
}
