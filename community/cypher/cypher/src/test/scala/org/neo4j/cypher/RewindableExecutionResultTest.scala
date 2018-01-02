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
package org.neo4j.cypher

import org.neo4j.graphdb.Node

class RewindableExecutionResultTest extends ExecutionEngineFunSuite {
  test("can do toList twice and get the same result") {
    val a = createNode()
    val b = createNode()

    val result = execute("match (n) return n")

    result.toList should equal(List(Map("n" -> a), Map("n" -> b)))
    result.toList should equal(List(Map("n" -> a), Map("n" -> b)))
  }

  test("can dumpToString and then use toList") {
    val a = createNode("name" -> "Aslan")
    val b = createNode("name" -> "White Queen")

    val result = execute(" match (n) return n")

    assert(List(Map("n" -> a), Map("n" -> b)) === result.toList)

    val textDump = result.dumpToString()

    textDump should include("Aslan")
    textDump should include("White Queen")
  }

  test("can dumpToString and then use columnAs") {
    val a = createNode("name" -> "Aslan")
    val b = createNode("name" -> "White Queen")

    val result = execute("match (n) return n")

    assert(List(Map("n" -> a), Map("n" -> b)) === result.toList)

    val nodes = result.columnAs[Node]("n").toList

    nodes should equal(List(a,b))
  }
}
