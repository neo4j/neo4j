/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_5.rewriting

import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.rewriting.rewriters.normalizeNotEquals
import org.neo4j.cypher.internal.v3_5.util.DummyPosition
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions.{Equals, Not, NotEquals}


class NormalizeNotEqualsTest extends CypherFunSuite {

  val pos = DummyPosition(0)
  val lhs: Expression = StringLiteral("42")(pos)
  val rhs: Expression = StringLiteral("42")(pos)

  test("notEquals  iff  not(equals)") {
    val notEquals = NotEquals(lhs, rhs)(pos)
    val output = notEquals.rewrite(normalizeNotEquals)
    val expected: Expression = Not(Equals(lhs, rhs)(pos))(pos)
    output should equal(expected)
  }

  test("should do nothing on other expressions") {
    val output = lhs.rewrite(normalizeNotEquals)
    output should equal(lhs)
  }
}
