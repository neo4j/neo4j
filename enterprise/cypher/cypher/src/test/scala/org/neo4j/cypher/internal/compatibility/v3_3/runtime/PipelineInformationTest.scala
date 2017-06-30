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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class PipelineInformationTest extends CypherFunSuite {
  test("can't overwrite variable name by mistake1") {
    // given
    val pipeline = PipelineInformation.empty
    pipeline.newLong("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](pipeline.newLong("x", nullable = false, CTNode))
  }

  test("can't overwrite variable name by mistake2") {
    // given
    val pipeline = PipelineInformation.empty
    pipeline.newLong("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](pipeline.newReference("x", nullable = false, CTNode))
  }

  test("can't overwrite variable name by mistake3") {
    // given
    val pipeline = PipelineInformation.empty
    pipeline.newReference("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](pipeline.newLong("x", nullable = false, CTNode))
  }

  test("can't overwrite variable name by mistake4") {
    // given
    val pipeline = PipelineInformation.empty
    pipeline.newReference("x", nullable = false, CTNode)

    // when && then
    intercept[InternalException](pipeline.newReference("x", nullable = false, CTNode))
  }

  test("deepClone creates an immutable copy") {
    // given
    val pipeline = PipelineInformation(Map(
      "x" -> LongSlot(0, nullable = false, CTNode),
      "y" -> LongSlot(1, nullable = false, CTNode)), numberOfLongs = 2, numberOfReferences = 0)
    val clone: PipelineInformation = pipeline.deepClone()
    pipeline should equal(clone)

    // when
    pipeline.newReference("a", nullable = false, CTNode)

    // then
    pipeline("a") should equal(RefSlot(0, nullable = false, CTNode))
    clone.get("a") shouldBe empty
    clone.numberOfReferences should equal(0)
  }
}
