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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.{InternalException, ValueComparisonHelper}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, RefSlot}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.values.AnyValue
import org.scalatest.matchers.{MatchResult, Matcher}

object SlottedValueComparisonHelper extends ValueComparisonHelper {

  def beEquivalentTo(result: Seq[Map[String, Any]]) = {
    new Matcher[Seq[ExecutionContext]] {
      override def apply(left: Seq[ExecutionContext]): MatchResult = MatchResult(
        matches = left.indices.forall(i =>
            left(i).asInstanceOf[PrimitiveExecutionContext].pipeline.forallSlot {
              case (k, RefSlot(offset, _, _, name)) => check(left(i).getRefAt(offset), result(i)(name))
              case _ => throw new InternalException("Slot type comparison not yet supported by test framework")
            }),
        rawFailureMessage = s"$left != $result",
        rawNegatedFailureMessage = s"$left == $result")
    }
  }

  def beEquivalentTo(value: Any) = new Matcher[AnyValue] {
    override def apply(left: AnyValue): MatchResult = MatchResult(
      matches = check(left, value),
      rawFailureMessage = s"$left != $value",
      rawNegatedFailureMessage = s"$left == $value")
  }

}
