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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.frontend.v2_3.ast.{ASTAnnotationMap, AstConstructionTestSupport, Expression, Identifier}
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{ExpressionTypeInfo, InputPosition, InternalException, SemanticTable}

class SemanticTableTest extends CypherFunSuite with AstConstructionTestSupport {

  override implicit def ident(name: String): Identifier = Identifier(name)(pos)

  test("can add nodes to a SemanticTable") {
    val table = SemanticTable().addNode("x")

    table.isNode("x") should be(true)
    table.isRelationship("x") should be(false)
  }

  test("can add rels to a SemanticTable") {
    val table = SemanticTable().addRelationship("r")

    table.isRelationship("r") should be(true)
    table.isNode("r") should be(false)
  }

  test("doesn't share mutable references after being copied") {
    val table1 = SemanticTable()
    val table2 = table1.copy()

    (table1.resolvedLabelIds eq table2.resolvedLabelIds) should be(false)
    (table1.resolvedPropertyKeyNames eq table2.resolvedPropertyKeyNames) should be(false)
    (table1.resolvedRelTypeNames eq table2.resolvedRelTypeNames) should be(false)
  }

  test("should be able to tell the type of an identifier") {
    val table = SemanticTable().
      addNode(Identifier("a")(InputPosition(1,2,3))).
      addRelationship(Identifier("b")(InputPosition(1,2,3)))

    table.getTypeFor("a") should be (CTNode.invariant)
    table.getTypeFor("b") should be (CTRelationship.invariant)
  }

  test("should be able to tell the type of an identifier if there is an unknown type involved") {
    val table = SemanticTable(ASTAnnotationMap.empty.
      updated(Identifier("a")(InputPosition(0,0,0)), ExpressionTypeInfo(TypeSpec.all, None))).
      addNode(Identifier("a")(InputPosition(1, 2, 3)))

    table.getTypeFor("a") should be (CTNode.invariant)
  }

  test("should be able to tell the type of an identifier if there is an unknown type involved other order") {
    val table = SemanticTable(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo].
      updated(Identifier("a")(InputPosition(1, 2, 3)), ExpressionTypeInfo(CTNode.invariant, None)).
      updated(Identifier("a")(InputPosition(0, 0, 0)), ExpressionTypeInfo(TypeSpec.all, None)))

    table.getTypeFor("a") should be (CTNode.invariant)
  }

  test("should fail when asking for an unknown identifier") {
    val table = SemanticTable()

    intercept[InternalException](table.getTypeFor("a"))
  }

  test("should fail if the semantic table is confusing") {
    val table = SemanticTable(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo].
      updated(Identifier("a")(InputPosition(1, 2, 3)), ExpressionTypeInfo(CTNode.invariant, None)).
      updated(Identifier("a")(InputPosition(0, 0, 0)), ExpressionTypeInfo(CTRelationship.invariant, None)))

    intercept[InternalException](table.getTypeFor("a"))
  }
}
