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
package org.neo4j.cypher.internal.ast.semantics.scoping

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class WorkingScopeHelpersTest extends CypherFunSuite with AstConstructionTestSupport {

  test("normalizeReferences collapses a chain a → b → c → d to a = d, b = d, c = d") {
    val a = refFor("a", 1); val b = refFor("b", 2); val c = refFor("c", 3); val d = refFor("d", 4)
    val input = Map(a -> b, b -> c, c -> d)

    val out = WorkingScope.normalizeReferences(input)

    out(a) should equal(d)
    out(b) should equal(d)
    out(c) should equal(d)
  }

  test("normalizeReferences keeps self-references as roots") {
    val a = refFor("a", 1); val b = refFor("b", 2)
    val input = Map(a -> a, b -> b)

    WorkingScope.normalizeReferences(input) should equal(input)
  }

  test("normalizeReferences terminates on a 2-cycle a → b → a") {
    val a = refFor("a", 1); val b = refFor("b", 2)
    val input = Map(a -> b, b -> a)

    val out = WorkingScope.normalizeReferences(input)

    out.keySet should equal(Set(a, b))
    Set(out(a), out(b)).size should be >= 1
  }

  test("normalizeReferences terminates on a cycle with a lead-in, a → b → c → b") {
    val a = refFor("a", 1); val b = refFor("b", 2); val c = refFor("c", 3)
    val input = Map(a -> b, b -> c, c -> b)

    val out = WorkingScope.normalizeReferences(input)

    out.keySet should equal(Set(a, b, c))
  }

  test("normalizeReferences leaves isolated entries alone") {
    val a = refFor("a", 1); val b = refFor("b", 2); val c = refFor("c", 3)
    val input = Map(a -> b, c -> c)

    val out = WorkingScope.normalizeReferences(input)

    out(a) should equal(b)
    out(c) should equal(c)
  }

  test("resolveByName returns empty on empty callers") {
    References.resolveByName(Seq.empty, Seq(varFor("x"))) shouldBe empty
  }

  test("resolveByName returns empty on empty candidates") {
    References.resolveByName(Seq(varFor("x")), Seq.empty) shouldBe empty
  }

  test("resolveByName maps each caller to the same-named candidate") {
    val xCaller = varFor("x"); val yCaller = varFor("y")
    val xDec = varFor("x"); val yDec = varFor("y"); val zDec = varFor("z")

    val out = References.resolveByName(Seq(xCaller, yCaller), Seq(xDec, yDec, zDec))

    out.toMap should equal(Map(Ref(xCaller) -> Ref(xDec), Ref(yCaller) -> Ref(yDec)))
  }

  test("resolveByName skips callers with no same-named candidate") {
    val xCaller = varFor("x"); val zCaller = varFor("z")
    val xDec = varFor("x"); val yDec = varFor("y")

    val out = References.resolveByName(Seq(xCaller, zCaller), Seq(xDec, yDec))

    out.toMap should equal(Map(Ref(xCaller) -> Ref(xDec)))
  }

  test("resolveByName picks the first candidate for same-named duplicates") {
    val xCaller = varFor("x")
    val xDec1 = varFor("x"); val xDec2 = varFor("x")

    val out = References.resolveByName(Seq(xCaller), Seq(xDec1, xDec2))

    out should have size 1
    out.head._2.value.name should equal("x")
  }

  private def refFor(name: String, offset: Int): Ref[LogicalVariable] =
    Ref(varFor(name).withPosition(InputPosition(offset, 1, 1)))
}
