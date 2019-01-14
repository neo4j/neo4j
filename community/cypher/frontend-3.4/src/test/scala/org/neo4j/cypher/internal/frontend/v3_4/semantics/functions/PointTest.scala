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
package org.neo4j.cypher.internal.frontend.v3_4.semantics.functions

import org.neo4j.cypher.internal.util.v3_4.symbols._

class PointTest extends FunctionTestBase("point")  {

  test("should accept correct types") {
    testValidTypes(CTMap)(CTPoint)
  }

  test("should fail type check for incompatible arguments") {
    // TODO: Fix error message which seems to assume that Node and Relationships are acceptable maps
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
