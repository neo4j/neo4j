/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

// Only put tests that assert on memory performance behaviour in this class
class MemoryPerformanceAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("check for contents of collection that contains only a single null") {
    createNode()

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode(
      "MATCH (a) WHERE 42 IN [a.prop] RETURN *", "param" -> null
    )

    result shouldBe empty
  }

  test("should be able to handle a large DNF predicate without running out of memory") {
    // given
    val query = """MATCH (a)-[r]->(b) WHERE
                  |(ID(a)= 12466 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12462 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12458 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12447 AND type(r)= 'class2') OR
                  |(ID(a)= 12466 AND ID(b)= 12459 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12464 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12460 AND type(r)= 'class1') OR
                  |(ID(a)= 12466 AND ID(b)= 12446 AND type(r)= 'class3') OR
                  |(ID(a)= 12466 AND ID(b)= 12472 AND type(r)= 'class4') OR
                  |(ID(a)= 12466 AND ID(b)= 12457 AND type(r)= 'class1') OR
                  |(ID(a)= 12467 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12467 AND ID(b)= 12459 AND type(r)= 'class1') OR
                  |(ID(a)= 12467 AND ID(b)= 12451 AND type(r)= 'class2') OR
                  |(ID(a)= 12467 AND ID(b)= 12470 AND type(r)= 'class4') OR
                  |(ID(a)= 12467 AND ID(b)= 12445 AND type(r)= 'class3') OR
                  |(ID(a)= 12471 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12471 AND ID(b)= 12467 AND type(r)= 'class4') OR
                  |(ID(a)= 12471 AND ID(b)= 12455 AND type(r)= 'class1') OR
                  |(ID(a)= 12471 AND ID(b)= 12459 AND type(r)= 'class1') OR
                  |(ID(a)= 12471 AND ID(b)= 12452 AND type(r)= 'class2') OR
                  |(ID(a)= 12471 AND ID(b)= 12451 AND type(r)= 'class3') OR
                  |(ID(a)= 12469 AND ID(b)= 12449 AND type(r)= 'class1') OR
                  |(ID(a)= 12469 AND ID(b)= 12450 AND type(r)= 'class2') OR
                  |(ID(a)= 12469 AND ID(b)= 12447 AND type(r)= 'class3')
                  |RETURN ID(a), ID(b), type(r)""".stripMargin

    // when
    executeWithAllPlannersAndCompatibilityMode(query)
    // then it should not fail or run out of memory
  }

  test("should unwind a long range without going OOM") {
    val expectedResult = 20000000

    val result = executeScalarWithAllPlanners[Long](s"UNWIND range(1, $expectedResult) AS i RETURN count(*) AS c")

    result should equal(expectedResult)
  }

}
