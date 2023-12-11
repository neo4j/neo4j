/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SemanticTableTest extends CypherFunSuite with AstConstructionTestSupport {

  private val position123 = InputPosition(1, 2, 3)
  private val position000 = InputPosition(0, 0, 0)

  test("can add nodes to a SemanticTable") {
    val table = SemanticTable().addNode(varFor("x"))

    table.typeFor("x").is(CTNode) should be(true)
    table.typeFor("x").is(CTRelationship) should be(false)
  }

  test("can add rels to a SemanticTable") {
    val table = SemanticTable().addRelationship(varFor("r"))

    table.typeFor("r").is(CTRelationship) should be(true)
    table.typeFor("r").is(CTNode) should be(false)
  }

  test("should be able to tell the type of an variable") {
    val table = SemanticTable().addNode(varFor("a", position123)).addRelationship(varFor("b", position123))

    table.typeFor("a").typeInfo should be(Some(CTNode.invariant))
    table.typeFor("b").typeInfo should be(Some(CTRelationship.invariant))
  }

  test("should be able to tell the type of an variable if there is an unknown type involved") {
    val table = SemanticTable(ASTAnnotationMap.empty.updated(
      varFor("a", position000),
      ExpressionTypeInfo(TypeSpec.all, None)
    )).addNode(varFor("a", position123))

    table.typeFor("a").typeInfo should be(Some(CTNode.invariant))
  }

  test("should be able to tell the type of an variable if there is an unknown type involved other order") {
    val table = SemanticTable(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo].updated(
      varFor("a", position123),
      ExpressionTypeInfo(CTNode.invariant, None)
    ).updated(varFor("a", position000), ExpressionTypeInfo(TypeSpec.all, None)))

    table.typeFor("a").typeInfo should be(Some(CTNode.invariant))
  }

  test("should return None when asking for an unknown variable") {
    val table = SemanticTable()

    table.typeFor("a").typeInfo should be(None)
  }

  test("should return None if the semantic table has conflicting type information") {
    val table = SemanticTable(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo].updated(
      varFor("a", position123),
      ExpressionTypeInfo(CTNode.invariant, None)
    ).updated(varFor("a", position000), ExpressionTypeInfo(CTRelationship.invariant, None)))

    table.typeFor("a").typeInfo should be(None)
  }

  test("is(CTInteger) should only be true if we are certain it is an integer") {
    val table = SemanticTable(ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
      .updated(varFor("a", position000), ExpressionTypeInfo(CTInteger.invariant, None))
      .updated(varFor("b", position000), ExpressionTypeInfo(TypeSpec.all, None))
      .updated(varFor("c", position000), ExpressionTypeInfo(CTInteger.invariant | CTString.invariant, None)))

    table.typeFor(varFor("a", position000)).is(CTInteger) should be(true)
    table.typeFor(varFor("b", position000)).is(CTInteger) should be(false)
    table.typeFor(varFor("c", position000)).is(CTInteger) should be(false)
  }
}
