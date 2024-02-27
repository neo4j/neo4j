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

import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTString

abstract class VectorSimilarityTest(functionName: String) extends FunctionTestBase(s"vector.similarity.$functionName") {

  test("should accept correct types") {
    testValidTypes(CTList(CTFloat), CTList(CTFloat))(CTFloat)
    testValidTypes(CTList(CTFloat), CTList(CTInteger))(CTFloat)
    testValidTypes(CTList(CTInteger), CTList(CTFloat))(CTFloat)
    testValidTypes(CTList(CTInteger), CTList(CTInteger))(CTFloat)
  }

  test("should fail if wrong types") {
    testInvalidApplication(CTList(CTNumber), CTList(CTString))(
      "Type mismatch: expected List<Float>, List<Integer> or List<Number> but was List<String>"
    )
    testInvalidApplication(CTList(CTInteger), CTList(CTBoolean))(
      "Type mismatch: expected List<Float>, List<Integer> or List<Number> but was List<Boolean>"
    )
    testInvalidApplication(CTList(CTFloat), CTPoint)(
      "Type mismatch: expected List<Float>, List<Integer> or List<Number> but was Point"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected List<Float>, List<Integer> or List<Number> but was Boolean"
    )
    testInvalidApplication(CTList(CTFloat), CTList(CTString))(
      "Type mismatch: expected List<Float>, List<Integer> or List<Number> but was List<String>"
    )
  }

  test("should fail if wrong number of arguments") {
    testInvalidApplication()(
      s"Insufficient parameters for function 'vector.similarity.$functionName'"
    )
    testInvalidApplication(CTMap)(
      s"Insufficient parameters for function 'vector.similarity.$functionName'"
    )
    testInvalidApplication(CTList(CTFloat), CTList(CTFloat), CTList(CTFloat))(
      s"Too many parameters for function 'vector.similarity.$functionName'"
    )
  }
}

class VectorSimilarityEuclideanTest extends VectorSimilarityTest("euclidean")
class VectorSimilarityCosineTest extends VectorSimilarityTest("cosine")
