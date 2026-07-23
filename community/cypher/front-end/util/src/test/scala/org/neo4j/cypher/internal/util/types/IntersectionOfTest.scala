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
import org.neo4j.cypher.internal.util.symbols.CTNothing
import org.neo4j.cypher.internal.util.symbols.CTNull
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.IntersectionOf
import org.neo4j.cypher.internal.util.symbols.IsEqualTo
import org.neo4j.cypher.internal.util.symbols.IsSubtypeOf
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeReferenceValueType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.RecordType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class IntersectionOfTest extends CypherTypeTestSuite {

  test("base type representatives should not intersect") {
    shouldNotIntersect(baseTypeRepresentatives)
  }

  for (l <- Lattice.allLattices) {
    test(s"lattice: ${l.name}") {
      shouldIntersectAtMeetOfLattice(l.edges*)
    }
  }

  private def shouldNotIntersect(ts: Set[CypherType]): Unit = {
    for (a <- ts) {
      for (b <- ts) {
        if (a != b) {
          checkNotIntersect(a, b)
        }
      }
      checkInvariants(a)
    }
  }

  private def shouldIntersectAtMeetOfLattice(latticeEdges: (CypherType, CypherType)*): Unit = {
    val meets = computeMeets(latticeEdges*).map {
      case (a, b, Some(mt: MapType))  => (a, b, Some(RecordType.any(mt.isNullable)(mt.position)))
      case (a, b, Some(nt: NodeType)) => (a, b, Some(NodeReferenceValueType.any(nt.isNullable)(nt.position)))
      case x                          => x
    }
    for ((a, b, meetOpt) <- meets) {
      if (meetOpt.isDefined) {
        checkIntersectionOf(a, b)(meetOpt.get)
      } else {
        checkNotIntersect(a, b)
      }
    }
    val allTypes = latticeEdges.flatMap {
      case (a, b) => Seq(a, b)
    }
    for (t <- allTypes) {
      checkInvariants(t)
    }
  }

  private def checkIntersectionOf(a: CypherType, b: CypherType)(c: CypherType): Unit = {
    assertIntersectionOf(a, b)(c)
    assertIntersectionOf(b, a)(c)
    assertIsSubtypeOf(c, a)
    assertIsSubtypeOf(c, b)
  }

  private def checkNotIntersect(a: CypherType, b: CypherType): Unit = {
    val aNotNull = a.notNull
    val bNotNull = b.notNull
    val aNull = a.nullable
    val bNull = b.nullable
    assertIntersectionOf(aNotNull, bNotNull)(CTNothing)
    assertIntersectionOf(aNotNull, bNull)(CTNothing)
    assertIntersectionOf(aNull, bNotNull)(CTNothing)
    assertIntersectionOf(aNull, bNull)(CTNull)
  }

  private def checkInvariants(a: CypherType): Unit = {
    assertIntersectionOf(a, CTAny)(a)
    assertIntersectionOf(a, a)(a)
    assertIntersectionOf(a, CTNothing)(CTNothing)
    if (a.isNullable) {
      assertIntersectionOf(a, CTNull)(CTNull)
    } else {
      assertIntersectionOf(a, CTNull)(CTNothing)
    }
    val listOfA = l(a)
    if (noListType(a) && !IsSubtypeOf(listOfA, a)) {
      if (a.isNullable) {
        assertIntersectionOf(a, listOfA)(CTNull)
      } else {
        assertIntersectionOf(a, listOfA)(CTNothing)
      }
    }
    val recordOfA = rt("x" :: a).baseTypeClosed
    if (noRecordType(a) && !IsSubtypeOf(recordOfA, a)) {
      if (a.isNullable) {
        assertIntersectionOf(a, recordOfA)(CTNull)
      } else {
        assertIntersectionOf(a, recordOfA)(CTNothing)
      }
    }
  }

  private def assertIntersectionOf(a: CypherType, b: CypherType)(expected: CypherType): Unit = {
    val actual = IntersectionOf(a, b)
    if (!IsEqualTo(actual, expected)) {
      fail(s"""${a.description} and ${b.description} should intersect to ${expected.description}, but intersected to ${actual.description}
              |a: ${pprint.apply(a)}
              |b: ${pprint.apply(b)}
              |
              |expected: ${pprint.apply(expected)}
              |actual:   ${pprint.apply(actual)}""".stripMargin)
    }
  }

  private def assertIsSubtypeOf(sub: CypherType, sup: CypherType): Unit = {
    if (!IsSubtypeOf(sub, sup)) {
      fail(
        s"""${sub.description} should be subtype of ${sup.description}, but is not
           |sub: ${pprint.apply(sub)}
           |sup: ${pprint.apply(sup)}""".stripMargin
      )
    }
  }
}
