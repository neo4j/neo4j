/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.semantics.functions

import org.neo4j.cypher.internal.util.symbols._
import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks

class CoalesceTest extends FunctionTestBase("coalesce") with PropertyChecks {

  test("n: CTNode, r: CTRelationship => coalesce(n, r): CTNode | CTRelationship") {
    testValidTypes(CTString, CTBoolean, CTString)(TypeSpec.union(CTBoolean, CTString))
  }

  test("output type of coalesce is the union of its input types") {
    forAll(Gen.nonEmptyListOf(genCypherType.map(_.invariant))) { inputTypes =>
      testValidTypes(inputTypes: _*)(TypeSpec.union(inputTypes: _*))
    }
  }

  def genCypherType: Gen[CypherType] =
    Gen.sized { size =>
      if (size <= 0)
        Gen.oneOf(allMonomorphicCypherTypes)
      else
        Gen.oneOf(
          Gen.resize(size - 1, genCypherType).map(CTList),
          Gen.const(allMonomorphicCypherTypes.head),
          allMonomorphicCypherTypes.tail.map(Gen.const): _*
        )
    }

  val allMonomorphicCypherTypes: List[CypherType] =
    List(
      CTAny,
      CTBoolean,
      CTString,
      CTNumber,
      CTFloat,
      CTInteger,
      CTMap,
      CTNode,
      CTRelationship,
      CTPoint,
      CTDateTime,
      CTLocalDateTime,
      CTDate,
      CTTime,
      CTLocalTime,
      CTDuration,
      CTGeometry,
      CTPath,
      CTGraphRef
    )
}
