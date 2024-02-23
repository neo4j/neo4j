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
package org.neo4j.cypher.internal.util.collection.immutable

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * Testing only the pieces which differ from the scala implementation.
 */
class ListSetTest extends CypherFunSuite {

  test("Can build a ListSet from a distinct Seq, and keep the insertion order") {
    val seq = Seq(1, 2, 3, 4, 5)
    ListSet.from(seq).toSeq should equal(seq)
  }

  test("Can build a ListSet from a non-distinct Seq, and keep the insertion order") {
    val seq = Seq(1, 2, 3, 1, 4, 4, 5)
    ListSet.from(seq).toSeq should equal(seq.distinct)
  }

  test("Can build a ListSet from a scala ListSet, and keep the insertion order") {
    val seq = Seq(1, 2, 3, 4, 5)
    val ls = scala.collection.immutable.ListSet.from(seq)
    ListSet.from(ls).toSeq should equal(seq)
  }
}
