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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGeometry
import org.neo4j.cypher.internal.util.symbols.CTGraphRef
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime

abstract class InequalityExpressionTestBase(ctr: (Expression, Expression) => Expression)
    extends InfixExpressionTestBase(ctr) {

  private val types = List(
    CTList(CTAny),
    CTAny,
    CTGraphRef,
    CTInteger,
    CTFloat,
    CTNumber,
    CTNode,
    CTPath,
    CTRelationship,
    CTMap,
    CTPoint,
    CTDate,
    CTDuration,
    CTBoolean,
    CTString,
    CTDateTime,
    CTGeometry,
    CTLocalDateTime,
    CTLocalTime,
    CTTime
  )

  test("should support equality checks among all types") {
    types.foreach { t1 =>
      types.foreach { t2 =>
        testValidTypes(t1, t2)(CTBoolean)
      }
    }
  }
}

class EqualsTest extends InequalityExpressionTestBase(expressions.Equals(_, _)(DummyPosition(0)))
class NotEqualsTest extends InequalityExpressionTestBase(expressions.NotEquals(_, _)(DummyPosition(0)))
class LessThanOrEqualTest extends InequalityExpressionTestBase(expressions.LessThanOrEqual(_, _)(DummyPosition(0)))
class LessThanTest extends InequalityExpressionTestBase(expressions.LessThan(_, _)(DummyPosition(0)))

class GreaterThanOrEqualTest
    extends InequalityExpressionTestBase(expressions.GreaterThanOrEqual(_, _)(DummyPosition(0)))
class GreaterThanTest extends InequalityExpressionTestBase(expressions.GreaterThan(_, _)(DummyPosition(0)))
