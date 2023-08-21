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
package org.neo4j.cypher.internal.frontend.helpers

import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SeqCombinerTest extends CypherFunSuite {

  private val table = Seq(
    Seq(Seq(1, 2, 3), Seq(4, 5), Seq(6)) ->
      Seq(
        Seq(1, 4, 6),
        Seq(1, 5, 6),
        Seq(2, 4, 6),
        Seq(2, 5, 6),
        Seq(3, 4, 6),
        Seq(3, 5, 6)
      ),
    Seq(Seq(1, 2), Seq(3, 4)) ->
      Seq(Seq(1, 3), Seq(1, 4), Seq(2, 3), Seq(2, 4)),
    Seq(Seq(1, 2, 3), Seq.empty[Int], Seq(6)) -> Seq.empty,
    Seq(Seq(1), Seq(2, 3), Seq(4, 5, 6)) ->
      Seq(
        Seq(1, 2, 4),
        Seq(1, 2, 5),
        Seq(1, 2, 6),
        Seq(1, 3, 4),
        Seq(1, 3, 5),
        Seq(1, 3, 6)
      )
  )

  test("test combiners") {
    table.foreach {
      case (seq, expected) =>
        combine(seq) should equal(expected)
        combine(seq.map(_.toArray).toArray).map(_.toList).toList should equal(expected)
    }
  }
}
