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
package org.neo4j.cypher.internal.spi

import org.mockito.Mockito.{times, verify, verifyNoMoreInteractions, when}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.core.NodeManager

class BeansAPIRelationshipIteratorTest extends CypherFunSuite {

  test("should close the inner iterator when exhausted and do nothing on close") {
    val relationships = mock[RelationshipIterator]
    when( relationships.hasNext ).thenReturn(false)
    val iterator = new BeansAPIRelationshipIterator(relationships, mock[NodeManager], new ResourceManager)

    iterator.hasNext
    verify(relationships).hasNext
    verify(relationships).close()

    iterator.close()
    verifyNoMoreInteractions(relationships)
  }

  test("should close the inner iterator on close when not exhausted") {
    val relationships = mock[RelationshipIterator]
    when( relationships.hasNext ).thenReturn(true, false)
    val iterator = new BeansAPIRelationshipIterator(relationships, mock[NodeManager], new ResourceManager)

    iterator.hasNext
    verify(relationships).hasNext
    verify(relationships, times(0)).close()

    iterator.close()
    verify(relationships).close()
    verifyNoMoreInteractions(relationships)
  }

  test("close should be idempotent and never close the inner iterator twice") {
    val relationships = mock[RelationshipIterator]
    when( relationships.hasNext ).thenReturn(true, false)
    val iterator = new BeansAPIRelationshipIterator(relationships, mock[NodeManager], new ResourceManager)

    iterator.hasNext
    verify(relationships).hasNext
    verify(relationships, times(0)).close()

    iterator.close()
    verify(relationships).close()
    iterator.close()
    verifyNoMoreInteractions(relationships)
  }
}
