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
//package org.neo4j.cypher.docgen
//
//import org.junit.Test
//import org.junit.Assert._
//import org.neo4j.graphdb.Node
//
//class SortTest extends DocumentingTestBase
//{
//  def indexProps = List()
//
//  def graphDescription = List("A KNOWS B", "B KNOWS C")
//
//  def section = "Sort"
//
//  @Test def sortByName()
//  {
//    testQuery(
//      title = "Sort nodes by property",
//      text = "SORT BY is used to sort the output",
//      queryText = """start n=(%C%,%A%,%B%) return n sort by n.name""",
//      returns = """The nodes, sorted by their name.""",
//      (p) => assertEquals(List(node("A"), node("B"), node("C")), p.columnAs[Node]("n") .toList)
//    )
//  }
//}