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

import org.neo4j.codegen.api.IntermediateRepresentation.add
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arraySet
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.labeledLoop
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.oneTime
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.tryCatch
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Values

class PrettyIRTest extends CypherFunSuite {

  val indent: String = " " * PrettyIR.indentSize

  test("prettify empty block") {
    PrettyIR.pretty(block()) shouldBe empty
  }

  test("prettify block with single operation") {
    val blockIr = block(Seq(
      assign("a", add(constant(0), constant(7)))
    ): _*)

    PrettyIR.pretty(blockIr) shouldBe s"a = 0 + 7"
  }

  test("prettify block") {
    val blockIr = block(Seq(
      assign("a", add(constant(0), constant(7))),
      assign("a", add(load[Int]("a"), constant(7)))
    ): _*)

    PrettyIR.pretty(blockIr) shouldBe
      s"""{
         |${indent}a = 0 + 7
         |${indent}a = a + 7
         |}""".stripMargin
  }

  test("declare and assign within a block should be merged") {
    val blockIR = block(Seq(
      declare(typeRefOf[AnyValue], "a"),
      assign("a", add(constant(0), constant(7))),
      assign("a", add(load[Int]("a"), constant(7)))
    ): _*)

    PrettyIR.pretty(blockIR) shouldBe
      s"""{
         |${indent}AnyValue a = 0 + 7
         |${indent}a = a + 7
         |}""".stripMargin
  }

  test("multiple blocks within each other should have correct indentation") {
    val blockIR = block(Seq(
      declare(typeRefOf[AnyValue], "a"),
      assign("a", add(constant(0), constant(7))),
      condition(load[Boolean]("b"))(
        block(Seq(
          assign("b", constant(false)),
          assign("a", constant(13))
        ): _*)
      )
    ): _*)

    PrettyIR.pretty(blockIR) shouldBe
      s"""{
         |${indent}AnyValue a = 0 + 7
         |${indent}if (b) {
         |${indent * 2}b = false
         |${indent * 2}a = 13
         |${indent}}
         |}""".stripMargin
  }

  test("new instance") {
    PrettyIR.pretty(newInstance(constructor[java.util.ArrayList[AnyValue]])) shouldBe "new ArrayList<AnyValue>()"
  }

  test("invoke function") {
    PrettyIR.pretty(invoke(
      load[java.util.Iterator[AnyValue]]("iter"),
      method[java.util.Iterator[AnyValue], Boolean]("hasNext")
    )) shouldBe "iter.hasNext()"
  }

  test("get static no value") {
    PrettyIR.pretty(getStatic[LongValue]("NO_VALUE")) shouldBe "NO_VALUE"
  }

  test("get static") {
    PrettyIR.pretty(getStatic[Values, IntegralValue]("ZERO_INT")) shouldBe "Values.ZERO_INT"
  }

  test("prettify constant expression") {
    PrettyIR.pretty(constant("a")) shouldBe "a"
  }

  test("prettify equal expression") {
    PrettyIR.pretty(IntermediateRepresentation.equal(constant("a"), constant("b"))) shouldBe "a == b"
  }

  test("prettify not equal expression") {
    PrettyIR.pretty(notEqual(constant("a"), constant("b"))) shouldBe "a != b"
  }

  test("prettify add expression") {
    PrettyIR.pretty(add(constant(52), constant(3))) shouldBe "52 + 3"
  }

  test("prettify sub expression") {
    PrettyIR.pretty(subtract(constant(52), constant(3))) shouldBe "52 - 3"
  }

  test("boolean or") {
    PrettyIR.pretty(or(load[Boolean]("a"), load[Boolean]("b"))) shouldBe "a || b"
  }

  test("boolean and") {
    PrettyIR.pretty(and(load[Boolean]("a"), load[Boolean]("b"))) shouldBe "a && b"
  }

  test("if-condition") {
    PrettyIR.pretty(condition(load[Boolean]("condition"))(constant(false))) shouldBe "if (condition) false"
  }

  test("if-else-condition") {
    PrettyIR.pretty(
      ifElse(load[Boolean]("condition"))(constant(false))(constant(true))
    ) shouldBe "if (condition) false else true"
  }

  test("if-else-block-condition") {
    val condition = ifElse(load[Boolean]("condition"))(
      constant(0)
    )(
      block(Seq(
        assign("a", add(constant(0), constant(7))),
        assign("a", add(load[Int]("a"), constant(7)))
      ): _*)
    )
    PrettyIR.pretty(condition) shouldBe
      s"""if (condition) 0 else {
         |${indent}a = 0 + 7
         |${indent}a = a + 7
         |}""".stripMargin
  }

  test("ternary") {
    PrettyIR.pretty(
      ternary(load[Boolean]("condition"), constant(false), constant(true))
    ) shouldBe "condition ? false : true"
  }

  test("Loop without label") {
    val loopIR = loop(notEqual(load[Int]("a"), constant(100)))(assign("a", add(load[Int]("a"), constant(1))))
    PrettyIR.pretty(loopIR) shouldBe "while (a != 100) a = a + 1"
  }

  test("Loop with label") {
    val loopIR =
      labeledLoop("loop1", notEqual(load[Int]("a"), constant(100)))(assign("a", add(load[Int]("a"), constant(1))))
    PrettyIR.pretty(loopIR) shouldBe
      """loop1:
        |while (a != 100) a = a + 1""".stripMargin
  }

  test("array load") {
    PrettyIR.pretty(arrayLoad(load[Array[Int]]("maybeList"), 0)) shouldBe "maybeList[0]"
  }

  test("array set") {
    PrettyIR.pretty(arraySet(load[Array[Int]]("maybeList"), 0, constant(1))) shouldBe "maybeList[0] = 1"
  }

  test("one time") {
    PrettyIR.pretty(oneTime(load[Array[Int]]("maybeList"))) shouldBe "oneTime(maybeList)"
  }

  test("try catch with cast") {
    val tryCatchIR = tryCatch[ClassCastException]("e")(
      cast[Double](load[Int]("a"))
    )(
      IntermediateRepresentation.fail(load[ClassCastException]("e"))
    )
    PrettyIR.pretty(tryCatchIR) shouldBe
      s"""try {
         |${indent}(double)a
         |} catch(e: ClassCastException) {
         |${indent}throw e
         |}""".stripMargin
  }

  test("Noop") {
    PrettyIR.pretty(noop()) shouldBe empty
  }

  test("Not(...)") {
    PrettyIR.pretty(
      IntermediateRepresentation.not(IntermediateRepresentation.equal(constant(true), constant(false)))
    ) shouldBe "true != false"

    PrettyIR.pretty(IntermediateRepresentation.not(IntermediateRepresentation.or(
      load[Boolean]("a"),
      load[Boolean]("b")
    ))) shouldBe "!(a || b)"
  }

}
