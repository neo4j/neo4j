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
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast._
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.{NO_VALUE, stringValue}
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.expressions.{Expression, SemanticDirection}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class CodeGenerationDbAccessTest extends CypherFunSuite with AstConstructionTestSupport {

  test("node property access") {
    // Given
    val expression = NodeProperty(nodeOffset, property, "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP) should equal(stringValue("hello"))
  }

  test("late node property access") {
    compile(NodePropertyLate(nodeOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(stringValue("hello"))
    compile(NodePropertyLate(nodeOffset, "notThere", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(NO_VALUE)
  }

  test("relationship property access") {
    // Given
    val expression = RelationshipProperty(relOffset, property, "prop")(null)

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP) should equal(stringValue("hello"))
  }

  test("late relationship property access") {
    compile(RelationshipPropertyLate(relOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(stringValue("hello"))
    compile(RelationshipPropertyLate(relOffset, "notThere", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(NO_VALUE)
  }

  test("getDegree without type") {
    compile(GetDegreePrimitive(nodeOffset, None, SemanticDirection.OUTGOING)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.longValue(3))
    compile(GetDegreePrimitive(nodeOffset, None, SemanticDirection.INCOMING)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.longValue(2))
    compile(GetDegreePrimitive(nodeOffset, None, SemanticDirection.BOTH)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.longValue(5))
  }

  test("getDegree with type") {
    compile(GetDegreePrimitive(nodeOffset, Some(relType), SemanticDirection.OUTGOING))
      .evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.longValue(2))
    compile(GetDegreePrimitive(nodeOffset, Some(relType), SemanticDirection.INCOMING))
      .evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.longValue(1))
    compile(GetDegreePrimitive(nodeOffset, Some(relType), SemanticDirection.BOTH))
      .evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.longValue(3))
  }

  test("NodePropertyExists") {
    compile(NodePropertyExists(nodeOffset, property, "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.TRUE)
    compile(NodePropertyExists(nodeOffset, nonExistingProperty, "otherProp")(null))
      .evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.FALSE)
  }

  test("NodePropertyExistsLate") {
    compile(NodePropertyExistsLate(nodeOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.TRUE)
    compile(NodePropertyExistsLate(nodeOffset, "otherProp", "otherProp")(null))
      .evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.FALSE)
  }

  test("RelationshipPropertyExists") {
    compile(RelationshipPropertyExists(relOffset, property, "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.TRUE)
    compile(RelationshipPropertyExists(relOffset, nonExistingProperty, "otherProp")(null))
      .evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.FALSE)
  }

  test("RelationshipPropertyExistsLate") {
    compile(RelationshipPropertyExistsLate(relOffset, "prop", "prop")(null)).evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.TRUE)
    compile(RelationshipPropertyExistsLate(relOffset, "otherProp", "otherProp")(null))
      .evaluate(ctx, dbAccess, EMPTY_MAP) should
      equal(Values.FALSE)
  }

  test("NodeFromSlot") {
    // Given
    val expression = NodeFromSlot(nodeOffset, "foo")

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP) should equal(nodeValue)
  }

  test("RelationshipFromSlot") {
    // Given
    val expression = RelationshipFromSlot(relOffset, "foo")

    // When
    val compiled = compile(expression)

    // Then
    compiled.evaluate(ctx, dbAccess, EMPTY_MAP) should equal(relationshipValue)
  }

  private def compile(e: Expression) =
    CodeGeneration.compile(new IntermediateCodeGeneration(SlotConfiguration.empty).compile(e).getOrElse(fail()))

  private val node = 11
  private val nodeValue = mock[NodeValue]
  private val relationshipValue = mock[RelationshipValue]
  private val relationship = 12
  private val relType = "R"
  private val relTypeId = 56
  private val property = 1337
  private val nonExistingProperty = 1338
  private val nodeOffset = 42
  private val relOffset = 43
  private val ctx = mock[ExecutionContext]
  private val dbAccess = mock[DbAccess]
  when(ctx.getLongAt(nodeOffset)).thenReturn(node)
  when(ctx.getLongAt(relOffset)).thenReturn(relationship)
  when(dbAccess.nodeProperty(node, property)).thenReturn(stringValue("hello"))
  when(dbAccess.propertyKey("prop")).thenReturn(property)
  when(dbAccess.propertyKey("notThere")).thenReturn(nonExistingProperty)
  when(dbAccess.relationshipProperty(relationship, property)).thenReturn(stringValue("hello"))
  when(dbAccess.relationshipProperty(relationship, nonExistingProperty)).thenReturn(NO_VALUE)
  when(dbAccess.nodeProperty(node, property)).thenReturn(stringValue("hello"))
  when(dbAccess.nodeProperty(node, nonExistingProperty)).thenReturn(NO_VALUE)
  when(dbAccess.nodeHasProperty(node, property)).thenReturn(true)
  when(dbAccess.nodeHasProperty(node, nonExistingProperty)).thenReturn(false)
  when(dbAccess.relationshipHasProperty(relationship, property)).thenReturn(true)
  when(dbAccess.relationshipHasProperty(relationship, nonExistingProperty)).thenReturn(false)
  when(dbAccess.nodeGetOutgoingDegree(node)).thenReturn(3)
  when(dbAccess.nodeGetIncomingDegree(node)).thenReturn(2)
  when(dbAccess.nodeGetTotalDegree(node)).thenReturn(5)
  when(dbAccess.nodeGetOutgoingDegree(node, relTypeId)).thenReturn(2)
  when(dbAccess.nodeGetIncomingDegree(node, relTypeId)).thenReturn(1)
  when(dbAccess.nodeGetTotalDegree(node, relTypeId)).thenReturn(3)
  when(dbAccess.nodeById(node)).thenReturn(nodeValue)
  when(dbAccess.relationshipById(relationship)).thenReturn(relationshipValue)
  when(dbAccess.getRelTypeId(relType)).thenReturn(relTypeId)

}
