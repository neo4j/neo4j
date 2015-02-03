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
package org.neo4j.cypher

import internal.helpers.CollectionSupport
import org.scalatest.Assertions
import org.neo4j.graphdb.{Relationship, Node}
import org.scalautils.LegacyTripleEquals

class KeysAcceptanceTest extends ExecutionEngineFunSuite  with QueryStatisticsTestSupport {

  test("Using_keys_function_with_NODE_Not_Empty_result") {

    val n = createNode(Map("name" -> "Andres", "surname" -> "Lopez"))

    val result = execute("match (n) where id(n) = " + n.getId + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("name","surname"))
  }

  test("Using_keys_function_with_MULTIPLE_NODES_Not_Empty_result") {

    val n1 = createNode(Map("name" -> "Andres", "surname" -> "Lopez"))
    val n2 = createNode(Map("otherName" -> "Andres", "otherSurname" -> "Lopez"))

    val result = execute("match (n) where id(n) = " + n1.getId + " or id(n) = " + n2.getId + " unwind (keys(n)) AS theProps return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("name","surname","otherName","otherSurname"))
  }

  test("Using_keys_function_with_NODE_Empty_result") {

    val n = createNode()

    val result = execute("match (n) where id(n) = " + n.getId() + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List())
  }

  test("Using_keys_function_with_RELATIONSHIP_Not_Empty_result") {

    val r = relate(createNode(), createNode(), "KNOWS", Map("level" -> "bad", "year" -> "2015"))

    val result = execute("match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("level","year"))
  }

  test("Using_keys_function_with_RELATIONSHIP_Empty_result") {

    val r = relate(createNode(), createNode(), "KNOWS")

    val result = execute("match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList  should equal(List())
  }


}
