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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.ASTAnnotationMap.PositionedNode
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ASTAnnotationMapTest extends CypherFunSuite {

  case class Exp(id: String)(val position: InputPosition) extends ASTNode

  test("ASTNodes should be treated as distinct when they differ only by InputPosition") {
    val n1 = Exp("1")(InputPosition(0, 0, 0))
    val n2 = Exp("1")(InputPosition(1, 0, 0))
    val a1 = "annotation1"
    val a2 = "annotation2"
    val astAnnotationMap = ASTAnnotationMap(n1 -> a1, n2 -> a2)
    assert(astAnnotationMap(n1) === a1)
    assert(astAnnotationMap(n2) === a2)
  }

  test("ASTNodes should be treated as equal when they do not differ by InputPosition") {
    val n1 = Exp("1")(InputPosition(0, 0, 0))
    val n2 = Exp("1")(InputPosition(0, 0, 0))
    val a1 = "annotation1"
    val a2 = "annotation2"
    val astAnnotationMap = ASTAnnotationMap(n1 -> a1, n2 -> a2)
    assert(astAnnotationMap.size === 1)
    assert(astAnnotationMap(n2) === a2)
  }

  test("replaceKeys replaces specified keys") {
    val n1 = Exp("1")(InputPosition(1, 0, 0))
    val n2 = Exp("1")(InputPosition(2, 0, 0))
    val n3 = Exp("1")(InputPosition(3, 0, 0))
    val a1 = "annotation1"
    val a2 = "annotation2"
    val astAnnotationMap1 = ASTAnnotationMap(n1 -> a1, n2 -> a2)
    val astAnnotationMap2 = astAnnotationMap1.replaceKeys((n1, n3))
    assert(astAnnotationMap2.size === 2)
    assert(!astAnnotationMap2.contains(n1))
    assert(astAnnotationMap2(n3) === a1)
    assert(astAnnotationMap2(n2) === a2)
  }

  test("PositionedNode equality is reflexive") {
    val p = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    assert(p.equals(p))
  }

  test("PositionedNode equality is symmetric") {
    val p1 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    val p2 = PositionedNode(Exp("1")(InputPosition(1, 0, 0)))
    assert(!p1.equals(p2))
    assert(!p2.equals(p1))
    val p3 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    assert(p1.equals(p3))
    assert(p3.equals(p1))
  }

  test("PositionedNode equality is transitive") {
    val p1 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    val p2 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    val p3 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    assert(p1.equals(p2))
    assert(p2.equals(p3))
    assert(p1.equals(p3))
  }

  test("PositionedNode.hashCode() is distinct for InputPosition variations") {
    val p1 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    val p2 = PositionedNode(Exp("1")(InputPosition(1, 0, 0)))
    assert(p1.hashCode != p2.hashCode)
  }

  test("PositionedNode.hashCode() is distinct for ASTNode member variations") {
    val p1 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    val p2 = PositionedNode(Exp("2")(InputPosition(0, 0, 0)))
    assert(p1.hashCode != p2.hashCode)
  }

  test("PositionedNode.hashCode() is equal when all members are equal") {
    val p1 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    val p2 = PositionedNode(Exp("1")(InputPosition(0, 0, 0)))
    assert(p1.hashCode === p2.hashCode())
  }

  test("PositionedNode.hashCode() works with null position") {
    val p = PositionedNode(Exp("1")(null))
    noException should be thrownBy p.hashCode
  }
}
