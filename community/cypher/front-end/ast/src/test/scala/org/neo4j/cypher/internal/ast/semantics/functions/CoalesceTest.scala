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
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.scalacheck.Gen

class CoalesceTest extends FunctionTestBase("coalesce") with CypherScalaCheckDrivenPropertyChecks {

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
