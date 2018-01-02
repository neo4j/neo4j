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

import org.neo4j.cypher.internal.compiler.v2_3.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Identifier
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

import scala.collection.mutable.Map

class ColumnFilterPipeTest extends CypherFunSuite {

  test("should return columns from return items") {
    val col = "extractReturnItems"
    val returnItems = List(ReturnItem(Identifier(col), col))
    val source = new FakePipe(List(Map("x" -> "x", col -> "bar")), col -> CTNode)

    val columnPipe = new ColumnFilterPipe(source, returnItems)(mock[PipeMonitor])

    columnPipe.symbols.identifiers should equal(Map(col -> CTNode))
    columnPipe.createResults(QueryStateHelper.empty).toList should equal(List(Map(col -> "bar")))
  }
}
