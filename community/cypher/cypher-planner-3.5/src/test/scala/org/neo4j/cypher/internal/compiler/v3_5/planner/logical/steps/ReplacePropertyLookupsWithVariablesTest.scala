/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.replacePropertyLookupsWithVariables.firstAs
import org.opencypher.v9_0.ast.{ASTAnnotationMap, AstConstructionTestSupport}
import org.opencypher.v9_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.util.symbols._

class ReplacePropertyLookupsWithVariablesTest extends CypherFunSuite with AstConstructionTestSupport {
  // Have specific input positions to test semantic table
  private val variable = Variable("n")(InputPosition(1,2,3))
  private val property = Property(variable, PropertyKeyName("prop")(InputPosition(1,2,4)))(InputPosition(1,2,5))

  private val newVariable = Variable("foo")(property.position)

  test("should rewrite n.prop to foo") {
    val rewriter = replacePropertyLookupsWithVariables(Map(property -> "foo"))
    val initialTable = SemanticTable(types = ASTAnnotationMap[Expression, ExpressionTypeInfo]((property, ExpressionTypeInfo(CTInteger))))

    val (newExpression, newTable) = firstAs[Expression](rewriter(property, initialTable))
    newExpression should equal(varFor("foo"))
    newTable.types(property) should equal(newTable.types(newVariable))
  }

  test("should rewrite [n.prop] to [foo]") {
    val rewriter = replacePropertyLookupsWithVariables(Map(property -> "foo"))
    val initialTable = SemanticTable(types = ASTAnnotationMap[Expression, ExpressionTypeInfo]((property, ExpressionTypeInfo(CTInteger))))

    val (newExpression, newTable) = firstAs[Expression](rewriter(ListLiteral(Seq(property))(pos), initialTable))
    newExpression should equal(ListLiteral(Seq(varFor("foo")))(pos))
    newTable.types(property) should equal(newTable.types(newVariable))
  }

}
