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

import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PlanningAttributesTest extends CypherFunSuite {

  test("effective cardinality to mutable and back") {
    assertImmutableIsConsistent()
    assertImmutableIsConsistent(0 -> effCard(1.0, None))
    assertImmutableIsConsistent(0 -> effCard(1.0, Some(1.0)))
    assertImmutableIsConsistent(1 -> effCard(1.0, None))
    assertImmutableIsConsistent(1 -> effCard(1.0, Some(1.0)))
    assertImmutableIsConsistent(
      3 -> effCard(1.1, Some(2.1)),
      7 -> effCard(1.2, Some(2.2)),
      100 -> effCard(1.3, Some(2.3)),
      123 -> effCard(1.4, Some(2.4))
    )
    ImmutablePlanningAttributes.EffectiveCardinalities(effCards(Seq(0 -> effCard(
      Double.MinValue,
      Some(Double.MinValue)
    ))))
      .toMutable.toSeq.map(_.value) shouldBe Seq(effCard(
      math.nextUp(Double.MinValue),
      Some(math.nextUp(Double.MinValue))
    ))
  }

  private def assertImmutableIsConsistent(cs: (Int, EffectiveCardinality)*): Unit = {
    val mutable = effCards(cs)
    val immutable = ImmutablePlanningAttributes.EffectiveCardinalities(mutable)

    mutable.size shouldBe immutable.size
    mutable.iterator.foreach { case (id, c) =>
      immutable.isDefinedAt(id) shouldBe true
      immutable.get(id) shouldBe c
    }
    immutable.toMutable.toSeq shouldBe mutable.toSeq
  }

  private def effCards(cs: Seq[(Int, EffectiveCardinality)]): EffectiveCardinalities = {
    val result = new EffectiveCardinalities()
    cs.foreach { case (id, c) => result.set(Id(id), c) }
    result
  }

  private def effCard(amount: Double, original: Option[Double]): EffectiveCardinality = {
    EffectiveCardinality(amount, original.map(Cardinality.apply))
  }
}
