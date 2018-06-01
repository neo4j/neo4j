/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.mockito.Mockito.when
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast.{NodeProperty, NodePropertyLate, RelationshipProperty, RelationshipPropertyLate}
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CodeGeneration, IntermediateCodeGeneration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.expressions.Expression
import org.opencypher.v9_0.util.EntityNotFoundException

class CompiledExpressionTest extends ExecutionEngineFunSuite with AstConstructionTestSupport {

  test("node property access") {
    // Given
    val node = createNode("prop" -> "hello").getId
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node)
    val expression = NodeProperty(offset, propertyToken("prop"), "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    graph.inTx(
      compiled.evaluate(ctx, transaction, EMPTY_MAP) should equal(stringValue("hello"))
    )
  }

  test("late node property access") {
    // Given
    val node = createNode("prop" -> "hello").getId
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node)
    // Then
    graph.inTx {
      compile(NodePropertyLate(offset, "prop", "prop")(null)).evaluate(ctx, transaction, EMPTY_MAP) should
        equal(stringValue("hello"))
      compile(NodePropertyLate(offset, "notThere", "prop")(null)).evaluate(ctx, transaction, EMPTY_MAP) should
        equal(NO_VALUE)
    }
  }

  test("should fail if node has been deleted in transaction") {
    // Given
    val node = createNode("prop" -> "hello")
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node.getId)
    val expression = NodeProperty(offset, propertyToken("prop"), "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    graph.inTx {
      node.delete()
      an[EntityNotFoundException] should be thrownBy compiled.evaluate(ctx, transaction, EMPTY_MAP)
    }
  }

  test("relationship property access") {
    // Given
    val relationship = relate(createNode(), createNode(), "prop" -> "hello").getId
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(relationship)
    val expression = RelationshipProperty(offset, graph.inTx(propertyToken("prop")), "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    graph.inTx(
      compiled.evaluate(ctx, transaction, EMPTY_MAP) should equal(stringValue("hello"))
    )
  }

  test("late relationship property access") {
    // Given
    val relationship = relate(createNode(), createNode(), "prop" -> "hello").getId
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(relationship)
    // Then
    graph.inTx {
      compile(RelationshipPropertyLate(offset, "prop", "prop")(null)).evaluate(ctx, transaction, EMPTY_MAP) should
        equal(stringValue("hello"))
      compile(RelationshipPropertyLate(offset, "notThere", "prop")(null)).evaluate(ctx, transaction, EMPTY_MAP) should
        equal(NO_VALUE)
    }
  }

  test("should fail if relationship has been deleted in transaction") {
    // Given
    val relationship = relate(createNode(), createNode(), "prop" -> "hello")
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(relationship.getId)
    val expression = RelationshipProperty(offset, graph.inTx(propertyToken("prop")), "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    graph.inTx {
      relationship.delete()
      an[EntityNotFoundException] should be thrownBy compiled.evaluate(ctx, transaction, EMPTY_MAP)
    }
  }

  private def compile(e: Expression) =
    CodeGeneration.compile(IntermediateCodeGeneration.compile(e).getOrElse(fail()))

}
