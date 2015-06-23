/*
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.{ConstraintValidationException, ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class MandatoryPropertyConstraintValidationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with  CollectionSupport {

  test("should enforce constraints on creation") {
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    val e = intercept[ConstraintValidationException](execute("create (node:Label1)"))
    e.getMessage should endWith("with label \"Label1\" does not have a \"key1\" property")
  }

  test("should enforce on removing property") {
    execute("create constraint on (node:Label1) assert node.key1 is not null")
    execute("create (node1:Label1 {key1:'value1'})")

    intercept[ConstraintValidationException](execute("match (node:Label1) remove node.key1"))
  }

  test("should enforce on setting property to null") {
    execute("create constraint on (node:Label1) assert node.key1 is not null")
    execute("create ( node1:Label1 {key1:'value1' } )")
    intercept[ConstraintValidationException](execute("match (node:Label1) set node.key1 = null"))
  }

  test("should allow to break constraint within statement") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    // WHEN
    val res = execute("create (node:Label1) set node.key1 = 'foo' return node")

    // THEN
    res.toList should have size 1
  }

  test("should allow creation of non-conflicting data") {
    // GIVEN
    execute("create constraint on (node:Label1) assert node.key1 is not null")

    // WHEN
    execute("create (node {key1:'value1'} )")
    execute("create (node:Label2)")
    execute("create (node:Label1 { key1:'value1'})")
    execute("create (node:Label1 { key1:'value1'})")

    // THEN
    val result = execute("match (n) return count(*) as nodeCount")
    result.columnAs[Int]("nodeCount").toList should equal(List(4))
  }
}
