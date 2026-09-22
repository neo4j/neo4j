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
package org.neo4j.cypher.internal.macros

import org.neo4j.cypher.internal.macros.DupTest.CurriedNode
import org.neo4j.cypher.internal.macros.DupTest.IntId
import org.neo4j.cypher.internal.macros.DupTest.NestedHolder
import org.neo4j.cypher.internal.macros.DupTest.OperatorLikeNode
import org.neo4j.cypher.internal.macros.DupTest.SimpleNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.compiletime.testing.typeCheckErrors

class DupTest extends CypherFunSuite {

  test("reconstructs a case class with a single parameter list") {
    val dup = summon[Dup[SimpleNode]]
    val result = dup.dup(SimpleNode(1, flag = true, "x"), Seq(Integer.valueOf(2), java.lang.Boolean.FALSE, "y"))
    result shouldBe SimpleNode(2, flag = false, "y")
  }

  test("copies a later val parameter list from the original instance") {
    val original = CurriedNode(1, "a")("carried")
    val dup = summon[Dup[CurriedNode]]
    val result = dup.dup(original, Seq(Integer.valueOf(2), "b"))
    result shouldBe CurriedNode(2, "b")("ignored by equals")
    result.extra shouldBe "carried"
  }

  test("copies a later value class parameter with a default from the original instance") {
    val original = OperatorLikeNode("scan", Array("r", "s"))(IntId(42))
    val dup = summon[Dup[OperatorLikeNode]]
    val result = dup.dup(original, Seq("scan2", Array("t", "u")))
    result.workId shouldBe "scan2"
    result.id shouldBe IntId(42)
  }

  test("passes a correctly typed array child through unchanged") {
    val original = OperatorLikeNode("scan", Array("r", "s"))(IntId(1))
    val typedArray = Array("t", "u")
    val result = summon[Dup[OperatorLikeNode]].dup(original, Seq("scan", typedArray))
    result.offsets should be theSameInstanceAs typedArray
  }

  test("restores the element type of an array child rebuilt as Array[AnyRef]") {
    val original = OperatorLikeNode("scan", Array("r", "s"))(IntId(1))
    val untypedArray = Array[AnyRef]("t", "u")
    val result = summon[Dup[OperatorLikeNode]].dup(original, Seq("scan", untypedArray))
    result.offsets.getClass shouldBe classOf[Array[String]]
    result.offsets.toSeq shouldBe Seq("t", "u")
  }

  test("supports case classes nested in objects") {
    val result = summon[Dup[NestedHolder.Inner]].dup(NestedHolder.Inner(1), Seq(Integer.valueOf(2)))
    result shouldBe NestedHolder.Inner(2)
  }

  test("rejects type-parameterized case classes at compile time") {
    val errors = typeCheckErrors(
      "case class Generic[T](t: T); summon[org.neo4j.cypher.internal.macros.Dup[Generic[Int]]]"
    )
    errors should not be empty
    errors.head.message should include("type-parameterized")
  }

  test("rejects case classes nested in a class at compile time") {
    val errors = typeCheckErrors(
      """class Outer { case class Inner(a: Int) }
        |val o = new Outer
        |summon[org.neo4j.cypher.internal.macros.Dup[o.Inner]]""".stripMargin
    )
    errors should not be empty
    errors.head.message should include("outer instance")
  }

  test("rejects a later parameter list entry that is not a val at compile time") {
    val errors = typeCheckErrors(
      "summon[org.neo4j.cypher.internal.macros.Dup[org.neo4j.cypher.internal.macros.DupTest.NoValNode]]"
    )
    errors should not be empty
    errors.head.message should include("must be a val")
  }
}

object DupTest {

  case class SimpleNode(a: Int, flag: Boolean, name: String)

  case class CurriedNode(a: Int, name: String)(val extra: String)

  case class IntId(x: Int) extends AnyVal

  case class OperatorLikeNode(workId: String, offsets: Array[String])(val id: IntId = IntId(-1))

  case class NoValNode(a: Int)(b: String) {
    def describe: String = s"$a $b"
  }

  object NestedHolder {
    case class Inner(a: Int)
  }
}
