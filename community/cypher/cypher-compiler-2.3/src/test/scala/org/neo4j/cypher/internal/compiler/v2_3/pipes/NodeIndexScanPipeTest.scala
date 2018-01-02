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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.ast.{LabelToken, PropertyKeyToken, _}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{LabelId, PropertyKeyId}
import org.neo4j.graphdb.Node

class NodeIndexScanPipeTest extends CypherFunSuite with AstConstructionTestSupport {

  private implicit val monitor = mock[PipeMonitor]

  private val label = LabelToken(LabelName("LabelName")_, LabelId(11))
  private val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName")_, PropertyKeyId(10))
  private val descriptor = IndexDescriptor(label.nameId.id, propertyKey.nameId.id)
  private val node = mock[Node]

  test("should return nodes found by index scan when both labelId and property key id are solved at compile time") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = scanFor(Iterator(node))
    )

    // when
    val pipe = NodeIndexScanPipe("n", label, propertyKey)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  private def scanFor(nodes: Iterator[Node]): QueryContext = {
    val query = mock[QueryContext]
    when(query.indexScan(descriptor)).thenReturn(nodes)
    query
  }
}
