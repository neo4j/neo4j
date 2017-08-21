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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values._
import org.neo4j.values.virtual.VirtualValues.list

class SplitFunctionTest extends CypherFunSuite {

  val nullSeq = null.asInstanceOf[Seq[String]]
  val nullString = null.asInstanceOf[String]

  test("passing null to split() returns null") {
    split("something", nullString) should be(NO_VALUE)
    split(nullString, "something") should be(NO_VALUE)
  }

  test("splitting non-empty strings with one character") {
    split("first,second", ",") should be(seq("first", "second"))
  }

  test("splitting non-empty strings with more than one character") {
    split("first11second11third", "11") should be(seq("first", "second", "third"))
  }

  test("splitting an empty string should return an empty string") {
    split("", ",") should be(seq(""))
  }

  test("splitting a string containing only the split pattern should return two empty strings") {
    split(",", ",") should be(seq("",""))
  }

  test("using an empty separator should split on every character") {
    split("banana", "") should be(seq("b", "a", "n", "a", "n", "a"))
    split("a", "") should be(seq("a"))
    split("", "") should be(seq(""))
  }

  private def seq(vals: String*) = list(vals.map(stringValue):_*)

  private def split(orig: String, splitPattern: String) = {
    val expr = SplitFunction(Literal(orig), Literal(splitPattern))
    expr(ExecutionContext.empty)(QueryStateHelper.empty)
  }
}
