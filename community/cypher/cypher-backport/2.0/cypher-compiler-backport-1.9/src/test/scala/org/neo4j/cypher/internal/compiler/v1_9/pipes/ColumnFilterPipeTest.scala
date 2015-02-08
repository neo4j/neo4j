/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import org.junit.Assert
import org.junit.Test
import org.scalatest.junit.JUnitSuite
import org.neo4j.cypher.internal.compiler.v1_9.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v1_9.symbols.NodeType
import collection.mutable.Map
import org.neo4j.cypher.internal.compiler.v1_9.commands.expressions.Identifier

class ColumnFilterPipeTest extends JUnitSuite {
  @Test def shouldReturnColumnsFromReturnItems() {
    val col = "extractReturnItems"
    val returnItems = List(ReturnItem(Identifier(col), col))
    val source = new FakePipe(List(Map("x" -> "x", col -> "bar")), col -> NodeType())

    val columnPipe = new ColumnFilterPipe(source, returnItems)

    Assert.assertEquals(Map(col -> NodeType()), columnPipe.symbols.identifiers)
    Assert.assertEquals(List(Map(col -> "bar")), columnPipe.createResults(QueryState()).toList)
  }
}
