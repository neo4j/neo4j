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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.scalatest.matchers.BeMatcher
import org.scalatest.matchers.MatchResult

import scala.reflect.ClassTag

object UsingMatcher extends UsingMatcher

trait UsingMatcher {

  /**
   * Matches logical plan tree that contains a plan of a given type.
   * Enables the following syntax:
   *
   * logicalPlan shouldBe using[NodeByLabelScan]
   */
  def using[T <: LogicalPlan](implicit tag: ClassTag[T]): BeMatcher[LogicalPlan] = new BeMatcher[LogicalPlan] {

    override def apply(actual: LogicalPlan): MatchResult = {
      val matches = actual.folder.treeFold(false) {
        case lp if tag.runtimeClass.isInstance(lp) => acc => SkipChildren(true)
      }
      MatchResult(
        matches = matches,
        rawFailureMessage = s"Plan should use ${tag.runtimeClass.getSimpleName}",
        rawNegatedFailureMessage = s"Plan should not use ${tag.runtimeClass.getSimpleName}"
      )
    }
  }
}
