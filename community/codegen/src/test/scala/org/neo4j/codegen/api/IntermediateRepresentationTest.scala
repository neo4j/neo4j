/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.codegen.api

import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.booleanValue
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isEmpty
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.longValue
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.print
import org.neo4j.codegen.api.IntermediateRepresentation.scalaObjectInstance
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.trueValue
import org.neo4j.codegen.api.IntermediateRepresentation.tryCatchIfNecessary
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values

class IntermediateRepresentationTest extends CypherFunSuite {

  test("isEmpty") {
    isEmpty(noop()) shouldBe true
    isEmpty(block(noop(), noop(), noop())) shouldBe true
    isEmpty(block(noop(), print(constant("hello world!")), noop())) shouldBe false
    isEmpty(block()) shouldBe true
  }

  test("condition") {
    condition(constant(true))(print(constant("hello"))) shouldBe print(constant("hello"))
    condition(constant(true))(block()) shouldBe noop()
    condition(constant(false))(print(constant("hello"))) shouldBe noop()
    condition(BooleanOr(Seq(load[Boolean]("a"), constant(true))))(print(constant("hello"))) shouldBe print(
      constant("hello")
    )
  }

  test("ifElse") {
    ifElse(constant(true))(print(constant("hello")))(print(constant("there"))) shouldBe print(constant("hello"))
    ifElse(constant(false))(print(constant("hello")))(print(constant("there"))) shouldBe print(constant("there"))
    ifElse(load[Boolean]("boolean"))(print(constant("hello")))(block()) shouldBe condition(load[Boolean]("boolean"))(
      print(constant("hello"))
    )
    ifElse(load[Boolean]("boolean"))(block())(print(constant("there"))) shouldBe condition(
      notOp(load[Boolean]("boolean"))
    )(print(constant("there")))
  }

  test("ternary") {
    ternary(constant(true), print(constant("hello")), print(constant("there"))) shouldBe print(constant("hello"))
    ternary(constant(false), print(constant("hello")), print(constant("there"))) shouldBe print(constant("there"))
    BooleanOr(Seq(load[Boolean]("a"), constant(true)))
    ternary(
      BooleanOr(Seq(load[Boolean]("a"), constant(true))),
      print(constant("hello")),
      print(constant("there"))
    ) shouldBe print(constant("hello"))
    ternary(
      BooleanAnd(Seq(load[Boolean]("a"), constant(false))),
      print(constant("hello")),
      print(constant("there"))
    ) shouldBe print(constant("there"))
  }

  test("not") {
    notOp(constant(true)) shouldBe constant(false)
    notOp(constant(false)) shouldBe constant(true)
    notOp(Not(load[Boolean]("boolean"))) shouldBe load[Boolean]("boolean")
    notOp(Not(Not(load[Boolean]("boolean")))) shouldBe notOp(load[Boolean]("boolean"))
    notOp(Eq(constant(42), constant(43))) shouldBe NotEq(constant(42), constant(43))
  }

  test("or") {
    or(constant(true), constant(false)) shouldBe constant(true)
    or(constant(false), constant(true)) shouldBe constant(true)
    or(constant(false), load[Boolean]("a")) shouldBe load[Boolean]("a")
    or(load[Boolean]("a"), constant(false)) shouldBe load[Boolean]("a")
    or(constant(false), constant(false)) shouldBe constant(false)

    or(
      BooleanOr(Seq(load[Boolean]("a"), load[Boolean]("b"))),
      BooleanOr(Seq(load[Boolean]("c"), load[Boolean]("d")))
    ) shouldBe
      BooleanOr(Seq(load[Boolean]("a"), load[Boolean]("b"), load[Boolean]("c"), load[Boolean]("d")))
    or(Seq(load[Boolean]("a"), load[Boolean]("b"), constant(true), load[Boolean]("c"))) shouldBe constant(true)
    or(Seq(load[Boolean]("a"), load[Boolean]("b"), constant(false), load[Boolean]("c"))) shouldBe
      BooleanOr(Seq(load[Boolean]("a"), load[Boolean]("b"), load[Boolean]("c")))
  }

  test("and") {
    and(constant(true), constant(false)) shouldBe constant(false)
    and(constant(false), constant(true)) shouldBe constant(false)
    and(constant(true), load[Boolean]("a")) shouldBe load[Boolean]("a")
    and(load[Boolean]("a"), constant(true)) shouldBe load[Boolean]("a")
    and(constant(true), constant(true)) shouldBe constant(true)

    and(
      BooleanAnd(Seq(load[Boolean]("a"), load[Boolean]("b"))),
      BooleanAnd(Seq(load[Boolean]("c"), load[Boolean]("d")))
    ) shouldBe
      BooleanAnd(Seq(load[Boolean]("a"), load[Boolean]("b"), load[Boolean]("c"), load[Boolean]("d")))
    and(Seq(load[Boolean]("a"), load[Boolean]("b"), constant(false), load[Boolean]("c"))) shouldBe constant(false)
    and(Seq(load[Boolean]("a"), load[Boolean]("b"), constant(true), load[Boolean]("c"))) shouldBe
      BooleanAnd(Seq(load[Boolean]("a"), load[Boolean]("b"), load[Boolean]("c")))
  }

  test("booleanValue") {
    booleanValue(
      invoke(longValue(constant(42)), method[AnyValue, Boolean, AnyRef]("equals"), longValue(constant(43)))
    ) shouldBe
      invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"), Eq(constant(42), constant(43)))

  }

  test("rewrite if (booleanValue(a) == TRUE) to if (a)") {
    condition(IntermediateRepresentation.equal(
      IntermediateRepresentation.booleanValue(load[Boolean]("foo")),
      trueValue
    )) {
      print(constant("hello"))
    } shouldBe condition(load[Boolean]("foo"))(print(constant("hello")))

    condition(IntermediateRepresentation.equal(
      trueValue,
      IntermediateRepresentation.booleanValue(load[Boolean]("foo"))
    )) {
      print(constant("hello"))
    } shouldBe condition(load[Boolean]("foo"))(print(constant("hello")))

    condition(IntermediateRepresentation.equal(
      block(print(constant("hello")), IntermediateRepresentation.booleanValue(load[Boolean]("foo"))),
      trueValue
    )) {
      print(constant("hello"))
    } shouldBe condition(block(print(constant("hello")), load[Boolean]("foo")))(print(constant("hello")))
    condition(IntermediateRepresentation.equal(
      trueValue,
      block(print(constant("hello")), IntermediateRepresentation.booleanValue(load[Boolean]("foo")))
    )) {
      print(constant("hello"))
    } shouldBe condition(block(print(constant("hello")), load[Boolean]("foo")))(print(constant("hello")))

  }

  test("scala objects") {
    scalaObjectTest(TestSealedTraitObject)
    scalaObjectTest(TestSealedCaseObject)
    scalaObjectTest(TestStandAloneObject)
    scalaObjectTest(TestStandAloneCaseObject)
  }

  test("tryCatchIfNecessary") {
    tryCatchIfNecessary[RuntimeException]("e")(
      assign("result", notEqual(constant(true), getStatic[Object, Boolean]("FOO")))
    )(
      print(constant("NO!"))
    ) shouldBe assign("result", notEqual(constant(true), getStatic[Object, Boolean]("FOO")))

    tryCatchIfNecessary[RuntimeException]("e")(
      invoke(load[Object]("a"), method[Object, Unit]("superDangerous"))
    )(
      print(constant("NO!"))
    ) shouldBe TryCatch(
      invoke(load[Object]("a"), method[Object, Unit]("superDangerous")),
      print(constant("NO!")),
      typeRefOf[RuntimeException],
      "e"
    )
  }

  private def scalaObjectTest(objectInstance: AnyRef) = {
    val expectedRepresenation = GetStatic(
      owner = Some(TypeReference.typeReference(objectInstance.getClass)),
      output = TypeReference.typeReference(objectInstance.getClass),
      name = "MODULE$"
    )

    // given that this is how we have implement scala object instance retrieval
    scalaObjectInstance(objectInstance) shouldBe expectedRepresenation

    // then this should also hold true (if this test fails scala has changed in some way and intermediate representation needs to be updated)
    objectInstance.getClass.getField(expectedRepresenation.name).get(null) should be theSameInstanceAs objectInstance
  }

  // this is here just because we cannot import IntermediateRepresentation.not because of scalatest
  private def notOp(inner: IntermediateRepresentation) = IntermediateRepresentation.not(inner)
}

sealed trait TestSealedTrait
object TestSealedTraitObject extends TestSealedTrait
case object TestSealedCaseObject extends TestSealedTrait
object TestStandAloneObject
case object TestStandAloneCaseObject
