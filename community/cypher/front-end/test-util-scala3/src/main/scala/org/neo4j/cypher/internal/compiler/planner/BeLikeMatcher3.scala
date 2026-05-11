/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.compiler.planner

import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

// TODO Move to better package
object BeLikeMatcher3 extends BeLikeMatcher3

trait BeLikeMatcher3 {

  class BeLike(pf: PartialFunction[Object, Unit]) extends Matcher[Object] {

    def apply(left: Object): MatchResult = {
      val matches = pf.isDefinedAt(left)

      if (matches) {
        // Run the Unit block in case it contains any assertions.
        pf(left)
      }

      MatchResult(
        matches,
        s"""$left did not match the partial function""",
        s"""$left matched the partial function"""
      )
    }
  }

  def beLike(pf: PartialFunction[Object, Unit]) = new BeLike(pf)
}
