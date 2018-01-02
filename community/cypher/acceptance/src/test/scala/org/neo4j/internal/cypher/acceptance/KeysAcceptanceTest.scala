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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class KeysAcceptanceTest extends ExecutionEngineFunSuite  with QueryStatisticsTestSupport {

  test("Using keys() function with NODE Not Empty result") {

    val n = createNode(Map("name" -> "Andres", "surname" -> "Lopez"))

    val result = execute("match (n) where id(n) = " + n.getId + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("name","surname"))
  }

  test("Using keys() function with MULTIPLE_NODES Not-Empty result") {

    val n1 = createNode(Map("name" -> "Andres", "surname" -> "Lopez"))
    val n2 = createNode(Map("otherName" -> "Andres", "otherSurname" -> "Lopez"))

    val result = execute("match (n) where id(n) = " + n1.getId + " or id(n) = " + n2.getId + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("name","surname","otherName","otherSurname"))
  }

  test("Using keys() function with NODE Empty result") {

    val n = createNode()

    val result = execute("match (n) where id(n) = " + n.getId() + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List())
  }


  test("Using keys() function with NODE NULL result") {

    val n = createNode()

    val result = execute("optional match (n) where id(n) = " + n.getId() + " unwind (keys(n)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List())
  }

  test("Using keys() function with RELATIONSHIP Not-Empty result") {

    val r = relate(createNode(), createNode(), "KNOWS", Map("level" -> "bad", "year" -> "2015"))

    val result = execute("match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List("level","year"))
  }

  test("Using keys() function with RELATIONSHIP Empty result") {

    val r = relate(createNode(), createNode(), "KNOWS")

    val result = execute("match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList  should equal(List())
  }

  test("Using keys() function with RELATIONSHIP NULL result") {

    val r = relate(createNode(), createNode(), "KNOWS")

    val result = execute("optional match ()-[r:KNOWS]-() where id(r) = " + r.getId + " unwind (keys(r)) AS x return distinct(x) as theProps")

    result.columnAs[String]("theProps").toList should equal(List())
  }

  test("Using keys() on literal maps") {

    val result = execute("""return keys({name:'Alice', age:38, address:{city:'London', residential:true}}) as k""")

    result.toList should equal(List(Map("k" -> Seq("name", "age", "address"))))
  }

  test("Using keys() with map from parameter") {

    val result = execute("""return keys({param}) as k""",
      "param"->Map("name" -> "Alice", "age" -> 38, "address" -> Map("city" -> "London", "residential" -> true)))

    result.toList should equal(List(Map("k" -> Seq("name", "age", "address"))))
  }
}
