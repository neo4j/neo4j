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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.runtime.ExtractedRange
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedRuntimeTestSuite
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.InequalitySeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointBoundingBoxSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointDistanceSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PrefixSeekRangeExpression
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.exceptions.InternalException
import org.neo4j.values.storable.Values

class RuntimeRangeExtractorTest extends InterpretedRuntimeTestSuite {

  private val leaf: Expression = Literal(Values.intValue(1))

  test("extract PrefixSeekRangeExpression → ExtractedRange.Prefix") {
    val range = PrefixRange[Expression](Literal(Values.stringValue("abc")))
    val wrapper = PrefixSeekRangeExpression(range)

    RuntimeRangeExtractor.extract(wrapper) shouldEqual ExtractedRange.Prefix(range)
  }

  test("extract InequalitySeekRangeExpression → ExtractedRange.Inequality") {
    val range = RangeGreaterThan(NonEmptyList(InclusiveBound[Expression](leaf)))
    val wrapper = InequalitySeekRangeExpression(range)

    RuntimeRangeExtractor.extract(wrapper) shouldEqual ExtractedRange.Inequality(range)
  }

  test("extract PointDistanceSeekRangeExpression → ExtractedRange.PointDistance") {
    val range = PointDistanceRange[Expression](
      point = Literal(Values.intValue(1)),
      distance = Literal(Values.doubleValue(2.0)),
      inclusive = true
    )
    val wrapper = PointDistanceSeekRangeExpression(range)

    RuntimeRangeExtractor.extract(wrapper) shouldEqual ExtractedRange.PointDistance(range)
  }

  test("extract PointBoundingBoxSeekRangeExpression → ExtractedRange.PointBoundingBox") {
    val range = PointBoundingBoxRange[Expression](
      lowerLeft = Literal(Values.intValue(1)),
      upperRight = Literal(Values.intValue(2))
    )
    val wrapper = PointBoundingBoxSeekRangeExpression(range)

    RuntimeRangeExtractor.extract(wrapper) shouldEqual ExtractedRange.PointBoundingBox(range)
  }

  test("extract defensive — non-range Expression subtype throws InternalException") {
    val notARangeWrapper: Expression = Literal(Values.intValue(42))

    val ex = the[InternalException] thrownBy RuntimeRangeExtractor.extract(notARangeWrapper)
    ex.getMessage should include("Unexpected range expression")
  }
}
