/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class ColumnNameAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  override def initTest() {
    super.initTest()
    createNode()
  }

  // TCK'd
  test("should keep used expression 1") {
    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return cOuNt( * )")
    result.columns should equal(List("cOuNt( * )"))
  }

  // TCK'd
  test("should keep used expression 2") {
    val result = executeWithAllPlannersAndCompatibilityMode("match p=(n)-->(b) return nOdEs( p )")
    result.columns should equal(List("nOdEs( p )"))
  }

  // TCK'd
  test("should keep used expression 3") {
    val result = executeWithAllPlannersAndCompatibilityMode("match p=(n)-->(b) return coUnt( dIstInct p )")
    result.columns should equal(List("coUnt( dIstInct p )"))
  }

  // TCK'd
  test("should keep used expression 4") {
    val result = executeWithAllPlannersAndCompatibilityMode("match p=(n)-->(b) return aVg(    n.aGe     )")
    result.columns should equal(List("aVg(    n.aGe     )"))
  }
}
