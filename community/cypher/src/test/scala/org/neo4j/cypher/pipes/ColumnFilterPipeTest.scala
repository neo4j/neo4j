/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.junit.Assert
import org.junit.Test
import org.neo4j.cypher.SymbolTable
import org.scalatest.junit.JUnitSuite
import org.neo4j.cypher.commands.{EntityValue, ValueReturnItem, NodeIdentifier}

class ColumnFilterPipeTest extends JUnitSuite {
  @Test def shouldReturnColumnsFromReturnItems() {
    val returnItems = List(ValueReturnItem(EntityValue("extractReturnItems")))
    val source = new FakePipe(List(Map("x" -> "x", "extractReturnItems" -> "bar")), new SymbolTable(NodeIdentifier("extractReturnItems")))
    val columnPipe = new ColumnFilterPipe(source, returnItems)

    Assert.assertEquals(Set(NodeIdentifier("extractReturnItems")), columnPipe.symbols.identifiers)
    Assert.assertEquals(List(Map("extractReturnItems" -> "bar")), columnPipe.toList)
  }
}