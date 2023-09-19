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
import org.neo4j.cypher.internal.util.symbols.CTGeometry
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

class ValueTypeTest extends FunctionTestBase("valueType") {

  test("shouldFailIfWrongArguments") {
    testInvalidApplication()("Insufficient parameters for function 'valueType'")
    testInvalidApplication(CTInteger, CTInteger)("Too many parameters for function 'valueType'")
    testInvalidApplication(CTFloat, CTFloat)("Too many parameters for function 'valueType'")
  }

  test("shouldHandleAllTypes") {
    testValidTypes(CTAny)(CTString)
    testValidTypes(CTBoolean)(CTString)
    testValidTypes(CTString)(CTString)
    testValidTypes(CTNumber)(CTString)
    testValidTypes(CTFloat)(CTString)
    testValidTypes(CTInteger)(CTString)
    testValidTypes(CTMap)(CTString)
    testValidTypes(CTNode)(CTString)
    testValidTypes(CTRelationship)(CTString)
    testValidTypes(CTPoint)(CTString)
    testValidTypes(CTDateTime)(CTString)
    testValidTypes(CTLocalDateTime)(CTString)
    testValidTypes(CTDate)(CTString)
    testValidTypes(CTTime)(CTString)
    testValidTypes(CTLocalTime)(CTString)
    testValidTypes(CTDuration)(CTString)
    testValidTypes(CTGeometry)(CTString)
    testValidTypes(CTPath)(CTString)
    testValidTypes(CTList(CTAny).covariant)(CTString)
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTInteger)(CTString)
    testValidTypes(CTDuration | CTNode)(CTString)
    testValidTypes(CTGeometry | CTBoolean)(CTString)
    testValidTypes(CTString | CTBoolean)(CTString)
  }
}
