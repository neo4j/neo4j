/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.planner.spi

import org.neo4j.cypher.internal.planner.spi.LeafStability.MvccEmptyTx
import org.neo4j.cypher.internal.planner.spi.LeafStability.MvccNonEmptyTx
import org.neo4j.cypher.internal.planner.spi.LeafStability.NonMvcc
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.StableLeafPlans
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ImmutableStableLeafPlansTest extends CypherFunSuite {

  test("immutable stable leaf plans round-trip the stability of each marked leaf") {
    assertImmutableWorks()
    assertImmutableWorks(0 -> MvccEmptyTx)
    assertImmutableWorks(1 -> MvccNonEmptyTx)
    assertImmutableWorks(
      3 -> MvccEmptyTx,
      4 -> MvccNonEmptyTx,
      7 -> MvccEmptyTx,
      100 -> MvccNonEmptyTx
    )
  }

  test("unmarked ids default to NonMvcc across the round-trip") {
    val mutable = new StableLeafPlans
    mutable.set(Id(1), MvccEmptyTx)
    val immutable = ImmutablePlanningAttributes.StableLeafPlans(mutable)

    immutable.toMutable.get(Id(2)) shouldBe NonMvcc
  }

  private def assertImmutableWorks(values: (Int, LeafStability)*): Unit = {
    val mutable = new StableLeafPlans
    values.foreach { case (id, value) => mutable.set(Id(id), value) }
    val immutable = ImmutablePlanningAttributes.StableLeafPlans(mutable)

    immutable.toMutable.toSeq shouldBe mutable.toSeq
    values.foreach { case (id, value) => immutable.toMutable.get(Id(id)) shouldBe value }
  }
}
