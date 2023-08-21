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
package org.neo4j.cypher.internal.ast.semantics.functions

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime

class ToStringTest extends FunctionTestBase("toString") {

  test("should accept correct types or types that could be correct at runtime") {
    testValidTypes(CTString)(CTString)
    testValidTypes(CTFloat)(CTString)
    testValidTypes(CTInteger)(CTString)
    testValidTypes(CTBoolean)(CTString)
    testValidTypes(CTAny.covariant)(CTString)
    testValidTypes(CTNumber.covariant)(CTString)
    testValidTypes(CTDuration)(CTString)
    testValidTypes(CTDate)(CTString)
    testValidTypes(CTTime)(CTString)
    testValidTypes(CTLocalTime)(CTString)
    testValidTypes(CTLocalDateTime)(CTString)
    testValidTypes(CTDateTime)(CTString)
    testValidTypes(CTPoint)(CTString)
  }

  test("should fail type check for incompatible arguments") {
    testInvalidApplication(CTRelationship)(
      "Type mismatch: expected Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Relationship"
    )

    testInvalidApplication(CTNode)(
      "Type mismatch: expected Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Node"
    )
  }

  test("should fail if wrong number of arguments") {
    testInvalidApplication()(
      "Insufficient parameters for function 'toString'"
    )
    testInvalidApplication(CTString, CTString)(
      "Too many parameters for function 'toString'"
    )
  }
}
