/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4

import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite

object FoldableTest {
  trait Exp extends Foldable
  case class Val(int: Int) extends Exp
  case class Add(lhs: Exp, rhs: Exp) extends Exp
  case class Sum(args: Seq[Exp]) extends Exp
}

class FoldableTest extends CypherFunSuite {
  import FoldableTest._

  test("should fold value depth first over object tree") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.fold(50) {
      case Val(x) => acc => acc + x
    }

    assert(result === 200)
  }

  test("should fold by depth then breadth left to right") {
    val ast = Add(Val(1), Add(Add(Val(2), Val(3)), Val(4)))

    val result = ast.fold(Seq.empty[Int]) {
      case Val(x) => acc => acc :+ x
    }

    assert(result === Seq(1, 2, 3, 4))
  }

  test("should tree fold over all objects") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.treeFold(50) {
      case Val(x) => acc => (acc + x, Some(identity))
    }

    assert(result === 200)
  }

  test("should be able to stop-recursion in tree fold") {
    val ast = Add(Val(55), Add(Val(43), Val(52)))

    val result = ast.treeFold(50) {
      case Val(x) => acc => (acc + x, Some(identity))
      case Add(Val(43), _) => acc => (acc + 20, None)
    }

    assert(result === 125)
  }

  test("should allow using exist to find patterns deeply nested") {
    val ast = Add(Val(1), Add(Val(2), Val(3)))

    ast.treeExists {
      case Val(x) => x == 2
    } should equal(true)

    ast.treeExists {
      case Val(x) => x == 42
    } should equal(false)
  }

}
