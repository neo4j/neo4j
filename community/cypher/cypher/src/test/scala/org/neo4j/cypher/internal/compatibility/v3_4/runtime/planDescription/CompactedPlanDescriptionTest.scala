/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows, Time}
import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite
import org.scalatest.mock.MockitoSugar

class CompactedPlanDescriptionTest extends CypherFunSuite with MockitoSugar {
  test("empty in empty out") {
    // Given two plans with empty argument
    val planA = mock[InternalPlanDescription]
    val planB = mock[InternalPlanDescription]
    when(planA.arguments).thenReturn(Seq.empty)
    when(planB.arguments).thenReturn(Seq.empty)

    // When
    val compactPlan = CompactedPlanDescription(Seq(planA, planB))

    // Then
    compactPlan shouldBe a [CompactedPlanDescription]
    compactPlan.arguments shouldBe empty
  }

  test("dbHits accumulate") {
    // Given two plans with empty argument
    val planA = mock[InternalPlanDescription]
    val planB = mock[InternalPlanDescription]
    val planC = mock[InternalPlanDescription]
    when(planA.arguments).thenReturn(Seq(DbHits(1)))
    when(planB.arguments).thenReturn(Seq(DbHits(1)))
    when(planC.arguments).thenReturn(Seq(DbHits(2)))

    // When
    val compactPlan = CompactedPlanDescription(Seq(planA, planB, planC))

    // Then
    compactPlan shouldBe a [CompactedPlanDescription]
    compactPlan.arguments should equal(Seq(DbHits(4)))
  }

  test("time (sadly) accumulates") {
    // Given two plans with empty argument
    val planA = mock[InternalPlanDescription]
    val planB = mock[InternalPlanDescription]
    val planC = mock[InternalPlanDescription]
    when(planA.arguments).thenReturn(Seq(Time(1)))
    when(planB.arguments).thenReturn(Seq(Time(1)))
    when(planC.arguments).thenReturn(Seq(Time(0)))

    // When
    val compactPlan = CompactedPlanDescription(Seq(planA, planB))

    // Then
    compactPlan shouldBe a [CompactedPlanDescription]
    compactPlan.arguments should equal(Seq(Time(2)))
  }

  test("rows should just show the max numbers of rows. in most situations, these should be the same number") {
    // Given two plans with empty argument
    val planA = mock[InternalPlanDescription]
    val planB = mock[InternalPlanDescription]
    val planC = mock[InternalPlanDescription]
    when(planA.arguments).thenReturn(Seq(Rows(10)))
    when(planB.arguments).thenReturn(Seq(Rows(30)))
    when(planC.arguments).thenReturn(Seq(Rows(20)))

    // When
    val compactPlan = CompactedPlanDescription(Seq(planA, planB, planC))

    // Then
    compactPlan shouldBe a [CompactedPlanDescription]
    compactPlan.arguments should equal(Seq(Rows(30)))
  }

  test("do it all together") {
    // Given two plans with empty argument
    val planA = mock[InternalPlanDescription]
    val planB = mock[InternalPlanDescription]
    val planC = mock[InternalPlanDescription]
    when(planA.arguments).thenReturn(Seq(Rows(10), DbHits(1), Time(1)))
    when(planB.arguments).thenReturn(Seq(Rows(30), DbHits(1), Time(2)))
    when(planC.arguments).thenReturn(Seq(Rows(20), DbHits(2), Time(0)))

    // When
    val compactPlan = CompactedPlanDescription(Seq(planA, planB, planC))

    // Then
    compactPlan shouldBe a [CompactedPlanDescription]
    compactPlan.arguments.toSet should equal(Set(Rows(30), DbHits(4), Time(3)))
  }
}
