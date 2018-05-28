/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class ValueHashJoinImplementationAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("find friends of others") {
    // given
    createLabeledNode(Map("id" -> 1), "A")
    val a = createLabeledNode(Map("id" -> 2), "A")
    val b = createLabeledNode(Map("id" -> 2), "B")
    createLabeledNode(Map("id" -> 3), "B")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a, b")

    // then
    result should use("ValueHashJoin")
  }

  test("should reverse direction if lhs is much larger than rhs") {
    // given
    (0 to 1000) foreach { x =>
      createLabeledNode(Map("id" -> x), "A")
    }

    (0 to 10) foreach { x =>
      createLabeledNode(Map("id" -> x), "B")
    }

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a, b")

    // then
    result should use("ValueHashJoin")
  }
}
