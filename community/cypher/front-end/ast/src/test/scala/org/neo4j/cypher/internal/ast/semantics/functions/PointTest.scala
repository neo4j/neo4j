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
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString

class PointTest extends FunctionTestBase("point") {

  test("should accept map") {
    testValidTypes(CTMap)(CTPoint)
  }

  test("should accept node") {
    testValidTypes(CTNode)(CTPoint)
  }

  test("should accept relationship") {
    testValidTypes(CTRelationship)(CTPoint)
  }

  test("should fail type check for incompatible arguments") {
    testInvalidApplication(CTList(CTAny))(
      "Type mismatch: expected Map, Node or Relationship but was List<Any>"
    )
    testInvalidApplication(CTString)(
      "Type mismatch: expected Map, Node or Relationship but was String"
    )
  }

  test("should fail if wrong number of arguments") {
    testInvalidApplication()(
      "Insufficient parameters for function 'point'"
    )
    testInvalidApplication(CTMap, CTMap)(
      "Too many parameters for function 'point'"
    )
  }
}
