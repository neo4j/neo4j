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
package org.neo4j.cypher.internal.util.test_helpers

import org.neo4j.cypher.internal.util.test_helpers.CompareAsPrettyStrings.Wrapper
import org.scalatest.Assertion

trait CompareAsPrettyStrings {
  self: CypherFunSuite =>

  extension (lhs: Any) {

    def asPrettyTestString: String = {
      pprint.PPrinter.BlackWhite(lhs).render
    }

    def compareAsPrettyStrings(rhs: Any): Assertion = {
      // wrap to prevent Scalatest from minimizing the String diff
      val lhsPrettyString = pprint.PPrinter.BlackWhite(lhs).render
      val rhsPrettyString = pprint.PPrinter.BlackWhite(rhs).render
      Wrapper(lhsPrettyString) shouldEqual Wrapper(rhsPrettyString)

      fail("compareAsPrettyStrings is only for debugging and should not be committed")
    }
  }
}

object CompareAsPrettyStrings {

  private case class Wrapper(str: String) {
    override def toString: String = str
  }
}
