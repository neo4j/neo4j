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
package org.neo4j.cypher.internal.compiler.v3_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.replaceAliasedFunctionInvocations
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AstConstructionTestSupport, FunctionInvocation, FunctionName}
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite

class ReplaceAliasedFunctionInvocationsTest extends CypherFunSuite with AstConstructionTestSupport {

  val rewriter = replaceAliasedFunctionInvocations

  test("should rewrite toInt()") {
    val before = FunctionInvocation(FunctionName("toInt")(pos), literalInt(1))(pos)

    rewriter(before) should equal(before.copy(functionName = FunctionName("toInteger")(pos))(pos))
  }

  test("doesn't touch toInteger()") {
    val before = FunctionInvocation(FunctionName("toInteger")(pos), literalInt(1))(pos)

    rewriter(before) should equal(before)
  }

}
