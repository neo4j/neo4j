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

import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
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

class NullIfTest extends FunctionTestBase("nullIf") {

  override val context = SemanticContext.Results

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTAny, CTBoolean)(CTAny)
    testValidTypes(CTBoolean, CTString)(CTAny)
    testValidTypes(CTString, CTNumber)(CTAny)
    testValidTypes(CTNumber, CTFloat)(CTAny)
    testValidTypes(CTFloat, CTInteger)(CTAny)
    testValidTypes(CTInteger, CTMap)(CTAny)
    testValidTypes(CTMap, CTNode)(CTAny)
    testValidTypes(CTNode, CTRelationship)(CTAny)
    testValidTypes(CTRelationship, CTPoint)(CTAny)
    testValidTypes(CTPoint, CTDateTime)(CTAny)
    testValidTypes(CTDateTime, CTLocalDateTime)(CTAny)
    testValidTypes(CTLocalDateTime, CTDate)(CTAny)
    testValidTypes(CTDate, CTTime)(CTAny)
    testValidTypes(CTTime, CTLocalTime)(CTAny)
    testValidTypes(CTLocalTime, CTDuration)(CTAny)
    testValidTypes(CTDuration, CTGeometry)(CTAny)
    testValidTypes(CTGeometry, CTPath)(CTAny)
    testValidTypes(CTPath, CTList(CTAny).covariant)(CTAny)
    testValidTypes(CTList(CTAny).covariant, CTAny)(CTAny)
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTPath, CTGeometry | CTInteger)(CTAny)
  }

  test("shouldFailIfWrongArguments") {
    testInvalidApplication(CTFloat)("Insufficient parameters for function 'nullIf'")
    testInvalidApplication(CTFloat, CTFloat, CTFloat)("Too many parameters for function 'nullIf'")
  }
}
