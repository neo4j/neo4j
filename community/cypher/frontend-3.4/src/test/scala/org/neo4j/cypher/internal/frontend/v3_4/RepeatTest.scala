/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4

import org.neo4j.cypher.internal.frontend.v3_4.helpers.fixedPoint
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite

class RepeatTest extends CypherFunSuite {

  var count = 0
  val result = new Object
  val mockedRewriter = new Rewriter {
    def apply(v1: AnyRef): AnyRef = {
      count += 1
      result
    }
  }

  test("should not repeat when the output is the same of the input") {
    // given
    count = 0

    // when
    val output = fixedPoint(mockedRewriter)(result)

    // then
    output should equal(result)
    count should equal(1)
  }

  test("should repeat once when the output is different from the input") {
    // given
    count = 0

    // when
    val output = fixedPoint(mockedRewriter)(new Object)

    // then
    output should equal(result)
    count should equal(2)
  }
}
