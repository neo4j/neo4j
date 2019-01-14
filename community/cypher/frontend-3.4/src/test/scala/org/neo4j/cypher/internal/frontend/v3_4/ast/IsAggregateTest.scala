/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._

class IsAggregateTest extends CypherFunSuite with AstConstructionTestSupport {

  test("count(*) is an aggregate expression") {
    val expr: Expression = CountStar()_

    IsAggregate.unapply(expr) should equal(Some(expr))
  }

  test("max(null) is an aggregate expression") {
    val expr: Expression = FunctionInvocation(FunctionName("max")_, Null()_)_

    IsAggregate.unapply(expr) should equal(Some(expr))
  }

  test("distinct id(null) an aggregate expression") {
    val expr: Expression = FunctionInvocation(Namespace()_, FunctionName("id")_, distinct = true, Vector(Null()_))(pos)

    IsAggregate.unapply(expr) should equal(Some(expr))
  }

  test("id(null) is not an aggregate expression") {
    val expr: Expression = FunctionInvocation(Namespace()_, FunctionName("id")_, distinct = false, Vector(Null()_))(pos)

    IsAggregate.unapply(expr) should equal(None)
  }

  test("1 is not an aggregate expression") {
    val expr: Expression = SignedDecimalIntegerLiteral("1")_

    IsAggregate.unapply(expr) should equal(None)
  }
}
