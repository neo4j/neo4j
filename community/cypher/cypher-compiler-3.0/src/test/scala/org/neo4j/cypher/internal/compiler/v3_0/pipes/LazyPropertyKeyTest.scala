package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v3_0.{PropertyKeyId, SemanticTable, DummyPosition}
import org.neo4j.cypher.internal.frontend.v3_0.ast.PropertyKeyName
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.mockito.Mockito._

import scala.collection.mutable

class LazyPropertyKeyTest extends CypherFunSuite {
  private val pos = DummyPosition(0)
  private val PROPERTY_KEY_NAME = PropertyKeyName("foo")(pos)
  private val PROPERTY_KEY_ID = PropertyKeyId(42)

  test("if key is resolved, don't do any lookups") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[TokenContext]
    when(table.resolvedPropertyKeyNames).thenReturn(mutable.Map(PROPERTY_KEY_NAME.name -> PROPERTY_KEY_ID))

    //WHEN
    val id = LazyPropertyKey(PROPERTY_KEY_NAME).id(context)

    // THEN
    id should equal(Some(PROPERTY_KEY_ID))
    verifyZeroInteractions(context)
  }

  test("if key is not resolved, do a lookup") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[TokenContext]
    when(context.getOptPropertyKeyId(PROPERTY_KEY_NAME.name)).thenReturn(Some(PROPERTY_KEY_ID.id))
    when(table.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])

    // WHEN
    val id = LazyPropertyKey(PROPERTY_KEY_NAME).id(context)

    // THEN
    id should equal(Some(PROPERTY_KEY_ID))
    verify(context, times(1)).getOptPropertyKeyId("foo")
    verifyNoMoreInteractions(context)
  }

  test("multiple calls to id should result in only one lookup") {
    // GIVEN
    implicit val table = mock[SemanticTable]
    val context = mock[TokenContext]
    when(context.getOptPropertyKeyId(PROPERTY_KEY_NAME.name)).thenReturn(Some(PROPERTY_KEY_ID.id))
    when(table.resolvedPropertyKeyNames).thenReturn(mutable.Map.empty[String, PropertyKeyId])

    // WHEN
    val lazyPropertyKey = LazyPropertyKey(PROPERTY_KEY_NAME)
    for (i <- 1 to 100) lazyPropertyKey.id(context)

    // THEN
    verify(context, times(1)).getOptPropertyKeyId("foo")
    verifyNoMoreInteractions(context)
  }
}
