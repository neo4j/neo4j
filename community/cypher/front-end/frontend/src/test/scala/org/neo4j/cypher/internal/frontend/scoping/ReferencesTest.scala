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
package org.neo4j.cypher.internal.frontend.scoping

import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ReferencesTest extends CypherFunSuite {

  private def v(name: String, offset: Int): LogicalVariable =
    Variable(name)(InputPosition(offset, 0, 0), isIsolated = false)

  // Ref equality is identity-based, so these instances must be reused, never reconstructed.
  private val aRef: (Ref[LogicalVariable], Ref[LogicalVariable]) = Ref(v("a", 1)) -> Ref(v("a", 0))
  private val bRef: (Ref[LogicalVariable], Ref[LogicalVariable]) = Ref(v("b", 3)) -> Ref(v("b", 2))

  test("the hidden channel is empty by default") {
    References(Map(aRef)).hidden shouldBe empty
  }

  test("addHidden populates the hidden channel without touching the published set") {
    val refs = References(Map(aRef)).addHidden(Map(bRef))
    refs.references shouldBe Map(aRef)
    refs.hidden shouldBe Map(bRef)
    refs.getVariables.toSet shouldBe Set(aRef._1.value)
    refs.hiddenRefs.getVariables.toSet shouldBe Set(bRef._1.value)
  }

  test("publishedOnly clears the hidden channel but keeps the published set") {
    val refs = References(Map(aRef)).addHidden(Map(bRef))
    refs.publishedOnly.references shouldBe Map(aRef)
    refs.publishedOnly.hidden shouldBe empty
  }

  test("union merges both channels") {
    val left = References(Map(aRef)).addHidden(Map(bRef))
    val right = References(Map(bRef)).addHidden(Map(aRef))
    val merged = left union right
    merged.references shouldBe Map(aRef, bRef)
    merged.hidden shouldBe Map(bRef, aRef)
  }

  test("parent aggregation via publishedOnly never inherits a child's hidden entries") {
    // mirrors WorkingScope.referencedInChildren, where hidden must not propagate to the parent
    val child = References(Map(aRef)).addHidden(Map(bRef))
    val parent = References.empty union child.publishedOnly
    parent.references shouldBe Map(aRef)
    parent.hidden shouldBe empty
  }

  test("filtering the published set preserves the hidden channel") {
    val refs = References(Map(aRef, bRef)).addHidden(Map(bRef))
    refs.intersect(Set(aRef._1.value)).hidden shouldBe Map(bRef)
    refs.diff(aRef._1.value).hidden shouldBe Map(bRef)
    refs.filterTargets(_ => false).hidden shouldBe Map(bRef)
  }
}
