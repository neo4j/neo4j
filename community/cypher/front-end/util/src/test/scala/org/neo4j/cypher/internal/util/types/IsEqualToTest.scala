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
package org.neo4j.cypher.internal.util.types

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTFloat32
import org.neo4j.cypher.internal.util.symbols.CTGeometry
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTInteger16
import org.neo4j.cypher.internal.util.symbols.CTInteger32
import org.neo4j.cypher.internal.util.symbols.CTInteger8
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNull
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CTUUID
import org.neo4j.cypher.internal.util.symbols.CTVector
import org.neo4j.cypher.internal.util.symbols.CTZonedDateTime
import org.neo4j.cypher.internal.util.symbols.CTZonedTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.IsEqualTo
import org.neo4j.cypher.internal.util.symbols.NodeReferenceValueType
import org.neo4j.cypher.internal.util.symbols.RecordType
import org.neo4j.cypher.internal.util.symbols.RelationshipReferenceValueType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IsEqualToTest extends CypherTypeTestSuite {

  private val types = Set(
    CTAny,
    CTBoolean,
    CTInteger8,
    CTInteger16,
    CTInteger32,
    CTInteger,
    CTFloat,
    CTFloat32,
    CTVector,
    CTString,
    CTList(nullableAnyType),
    CTMap,
    rt("z" :: CTBoolean),
    CTPath,
    CTPoint,
    CTGeometry,
    CTTime,
    CTLocalTime,
    CTZonedTime,
    CTDate,
    CTDateTime,
    CTLocalDateTime,
    CTZonedDateTime,
    CTDuration,
    CTUUID,
    CTNull
  )

  test("types should be equal to themselves") {
    for (t <- types) {
      assertIsEqualTo(t.notNull, t.notNull)
      assertIsEqualTo(t.nullable, t.nullable)
    }
  }

  test("types should be equal to their not null variant when ignoring nullability") {
    for (t <- types) {
      assertIsEqualToIgnoringNullability(t.notNull, t.nullable)
      assertIsEqualToIgnoringNullability(t.nullable, t.notNull)
    }
  }

  private val legacyPairs = Set(
    CTMap -> RecordType(Map.empty, isFieldOpen = true, isBaseTypeOpen = true, isNullable = true)(pos),
    CTNode -> NodeReferenceValueType(Set.empty, Map.empty, isFieldOpen = true, isNullable = true)(pos),
    CTRelationship -> RelationshipReferenceValueType(
      Option.empty,
      Map.empty,
      isFieldOpen = true,
      NodeReferenceValueType(Set.empty, Map.empty, isFieldOpen = true, isNullable = true)(pos),
      NodeReferenceValueType(Set.empty, Map.empty, isFieldOpen = true, isNullable = true)(pos),
      isNullable = true
    )(pos)
  )

  test("legacy types should be equal to their replacements") {
    for ((lt, nt) <- legacyPairs) {
      assertIsEqualTo(lt.notNull, nt.notNull)
      assertIsEqualTo(lt.nullable, nt.nullable)
      assertIsEqualTo(nt.notNull, lt.notNull)
      assertIsEqualTo(nt.nullable, lt.nullable)
      assertIsEqualToIgnoringNullability(lt.notNull, nt.nullable)
      assertIsEqualToIgnoringNullability(lt.nullable, nt.notNull)
      assertIsEqualToIgnoringNullability(nt.notNull, lt.nullable)
      assertIsEqualToIgnoringNullability(nt.nullable, lt.notNull)
    }
  }

  /*
   * checks and assertions
   */

  private def assertIsEqualTo(a: CypherType, b: CypherType): Unit = {
    if (!IsEqualTo(a, b)) {
      fail(
        s"""${a.description} should be equal to ${b.description}, but is not
           |a: ${pprint.apply(a)}
           |b: ${pprint.apply(b)}""".stripMargin
      )
    }
  }

  private def assertIsEqualToIgnoringNullability(a: CypherType, b: CypherType): Unit = {
    if (!IsEqualTo.ignoringNullability(a, b)) {
      fail(
        s"""${a.description} should be equal to ${b.description} ignoring nullability, but is not
           |a: ${pprint.apply(a)}
           |b: ${pprint.apply(b)}""".stripMargin
      )
    }
  }
}
