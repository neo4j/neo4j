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
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast._
import org.neo4j.cypher.internal.runtime.EntityProducer
import org.neo4j.cypher.internal.runtime.compiled.expressions.{CodeGeneration, IntermediateCodeGeneration}
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.kernel.impl.util.ValueUtils.{fromNodeProxy, fromRelationshipProxy}
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.expressions.{Expression, SemanticDirection}
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
      compiled.evaluate(ctx, transaction, producer, EMPTY_MAP) should equal(stringValue("hello"))
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
      compile(NodePropertyLate(offset, "prop", "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(stringValue("hello"))
      compile(NodePropertyLate(offset, "notThere", "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
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
      an[EntityNotFoundException] should be thrownBy compiled.evaluate(ctx, transaction, producer, EMPTY_MAP)
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
      compiled.evaluate(ctx, transaction, producer, EMPTY_MAP) should equal(stringValue("hello"))
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
      compile(RelationshipPropertyLate(offset, "prop", "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(stringValue("hello"))
      compile(RelationshipPropertyLate(offset, "notThere", "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
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
      an[EntityNotFoundException] should be thrownBy compiled.evaluate(ctx, transaction, producer, EMPTY_MAP)
    }
  }

  test("getDegree without type") {
    // Given
    val node = createNode()
    relate(node, createNode())
    relate(node, createNode())
    relate(node, createNode())
    relate(createNode(), node)
    relate(createNode(), node)

    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node.getId)
    // Then
    graph.inTx {
      compile(GetDegreePrimitive(offset, None, SemanticDirection.OUTGOING)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.longValue(3))
      compile(GetDegreePrimitive(offset, None, SemanticDirection.INCOMING)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.longValue(2))
      compile(GetDegreePrimitive(offset, None, SemanticDirection.BOTH)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.longValue(5))
    }
  }

  test("getDegree with type") {
    // Given
    val node = createNode()
    relate(node, createNode(), "R")
    relate(node, createNode(), "OTHER")
    relate(node, createNode(), "R")
    relate(createNode(), node, "OTHER")
    relate(createNode(), node, "R")

    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node.getId)
    // Then
    graph.inTx {
      compile(GetDegreePrimitive(offset, Some("R"), SemanticDirection.OUTGOING)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.longValue(2))
      compile(GetDegreePrimitive(offset, Some("R"), SemanticDirection.INCOMING)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.longValue(1))
      compile(GetDegreePrimitive(offset, Some("R"), SemanticDirection.BOTH)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.longValue(3))
    }
  }

  test("NodePropertyExists") {
    // Given
    val node = createNode("prop" -> 42)
    createNode("otherProp" -> 42)

    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node.getId)
    graph.inTx {
      compile(NodePropertyExists(offset, propertyToken("prop"), "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.TRUE)
      compile(NodePropertyExists(offset, propertyToken("otherProp"), "otherProp")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
      //this property probably doesn't exists in the db
      compile(NodePropertyExists(offset, 1234567, "otherProp")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
    }
  }

  test("NodePropertyExistsLate") {
    // Given
    val node = createNode("prop" -> 42)
    createNode("otherProp" -> 42)

    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node.getId)
    graph.inTx {
      compile(NodePropertyExistsLate(offset, "prop", "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.TRUE)
      compile(NodePropertyExistsLate(offset, "otherProp", "otherProp")(null))
        .evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
      compile(NodePropertyExistsLate(offset, "NOT_THERE_AT_ALL", "otherProp")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
    }
  }

  test("RelationshipPropertyExists") {
    // Given
    val relationship = relate(createNode("otherProp" -> 42), createNode(), "prop" -> 43)

    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(relationship.getId)
    graph.inTx {
      compile(RelationshipPropertyExists(offset, propertyToken("prop"), "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.TRUE)
      compile(RelationshipPropertyExists(offset, propertyToken("otherProp"), "otherProp")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
      //this property probably doesn't exists in the db
      compile(RelationshipPropertyExists(offset, 1234567, "otherProp")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
    }
  }

  test("RelationshipPropertyExistsLate") {
    // Given
    val relationship = relate(createNode("otherProp" -> 42), createNode(), "prop" -> 43)

    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(relationship.getId)
    graph.inTx {
      compile(RelationshipPropertyExistsLate(offset, "prop", "prop")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.TRUE)
      compile(RelationshipPropertyExistsLate(offset, "otherProp", "otherProp")(null))
        .evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
      compile(RelationshipPropertyExistsLate(offset, "NOT_THERE_AT_ALL", "otherProp")(null)).evaluate(ctx, transaction, producer, EMPTY_MAP) should
        equal(Values.FALSE)
    }
  }

  test("NodeFromSlot") {
    // Given
    val node = createNode("prop" -> "hello")
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(node.getId)
    val expression = NodeFromSlot(offset, "foo")

    // When
    val compiled = compile(expression)

    // Then
    graph.inTx(
      compiled.evaluate(ctx, transaction, producer, EMPTY_MAP) should equal(fromNodeProxy(node))
    )
  }

  test("RelationshipFromSlot") {
    // Given
    val relationship = relate(createNode(), createNode())
    val offset = 42
    val ctx = mock[ExecutionContext]
    when(ctx.getLongAt(offset)).thenReturn(relationship.getId)
    val expression = RelationshipFromSlot(offset, "foo")

    // When
    val compiled = compile(expression)

    // Then
    graph.inTx(
      compiled.evaluate(ctx, transaction, producer, EMPTY_MAP) should equal(fromRelationshipProxy(relationship))
    )
  }

  private def compile(e: Expression) =
    CodeGeneration.compile(IntermediateCodeGeneration.compile(e).getOrElse(fail()))

  private val producer: EntityProducer = new EntityProducer {
    override def nodeById(id: Long): NodeValue = fromNodeProxy(graphOps.getNodeById(id))
    override def relationshipById(id: Long): RelationshipValue = fromRelationshipProxy(graphOps.getRelationshipById(id))
  }

}
