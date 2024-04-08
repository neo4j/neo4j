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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeNotEquals
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NormalizeNotEqualsTest extends CypherFunSuite {

  val pos = DummyPosition(0).withInputLength(0)
  val lhs: Expression = StringLiteral("42")(pos)
  val rhs: Expression = StringLiteral("42")(pos)

  test("notEquals  iff  not(equals)") {
    val notEquals = NotEquals(lhs, rhs)(pos)
    val output = notEquals.rewrite(normalizeNotEquals.instance)
    val expected: Expression = Not(Equals(lhs, rhs)(pos))(pos)
    output should equal(expected)
  }

  test("should do nothing on other expressions") {
    val output = lhs.rewrite(normalizeNotEquals.instance)
    output should equal(lhs)
  }
}
