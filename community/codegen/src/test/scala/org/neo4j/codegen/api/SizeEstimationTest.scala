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

import org.neo4j.codegen.ByteCodeVisitor
import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.IntermediateRepresentation.add
import org.neo4j.codegen.api.IntermediateRepresentation.and
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLength
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.arraySet
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.box
import org.neo4j.codegen.api.IntermediateRepresentation.break
import org.neo4j.codegen.api.IntermediateRepresentation.cast
import org.neo4j.codegen.api.IntermediateRepresentation.condition
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.constructor
import org.neo4j.codegen.api.IntermediateRepresentation.declare
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.getStatic
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThan
import org.neo4j.codegen.api.IntermediateRepresentation.greaterThanOrEqual
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.invokeStatic
import org.neo4j.codegen.api.IntermediateRepresentation.isNull
import org.neo4j.codegen.api.IntermediateRepresentation.labeledLoop
import org.neo4j.codegen.api.IntermediateRepresentation.lessThan
import org.neo4j.codegen.api.IntermediateRepresentation.lessThanOrEqual
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loadField
import org.neo4j.codegen.api.IntermediateRepresentation.longValue
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.method
import org.neo4j.codegen.api.IntermediateRepresentation.multiply
import org.neo4j.codegen.api.IntermediateRepresentation.newArray
import org.neo4j.codegen.api.IntermediateRepresentation.newInstance
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.notEqual
import org.neo4j.codegen.api.IntermediateRepresentation.oneTime
import org.neo4j.codegen.api.IntermediateRepresentation.or
import org.neo4j.codegen.api.IntermediateRepresentation.self
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.subtract
import org.neo4j.codegen.api.IntermediateRepresentation.ternary
import org.neo4j.codegen.api.IntermediateRepresentation.tryCatch
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.unbox
import org.neo4j.codegen.api.SizeEstimationTest.arrayField
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.Measurable
import org.neo4j.values.storable.LongValue
import org.objectweb.asm.Opcodes

import java.nio.ByteBuffer

import scala.util.Random

class SizeEstimationTest extends CypherFunSuite {
  private val codeGeneration = CodeGeneration.codeGeneration()
  private val sizeComputer = new ByteSizeComputer
  val generator = codeGeneration.createGenerator()
  generator.setByteCodeVisitor(sizeComputer)

  private val callBooleanMethod: IntermediateRepresentation =
    invokeStatic(method[SizeEstimationTest, Boolean]("testBoolean"))
  private val echoMethod = method[SizeEstimationTest, AnyRef, AnyRef]("testMethod")

  private var count = 0

  test("declare and assign constant int") {
    def declareAndAssign(v: Any) =
      block(
        declare[Int]("a"),
        assign("a", constant(v))
      )

    val constants = Seq(0, -26, 130, Int.MaxValue, Int.MinValue)
    constants.foreach(v => {
      val representation = declareAndAssign(v)
      withClue(s"Wrong estimation of $v") {
        sizeOf(representation) should equal(computeSize(representation))
      }
    })
  }

  test("declare and assign constant long") {
    def declareAndAssign(v: Any) =
      block(
        declare[Long]("a"),
        assign("a", constant(v))
      )

    val constants = Seq(0L, 1L, -26L, 130L, Long.MaxValue, Long.MinValue)
    constants.foreach(v => {
      val representation = declareAndAssign(v)
      withClue(s"Wrong estimation of $v") {
        sizeOf(representation) should equal(computeSize(representation))
      }
    })
  }

  test("declare and assign boolean") {
    def declareAndAssign(v: Any) =
      block(
        declare[Boolean]("a"),
        assign("a", constant(v))
      )

    val constants = Seq(true, false)
    constants.foreach(v => {
      val representation = declareAndAssign(v)
      withClue(s"Wrong estimation of $v") {
        sizeOf(representation) should equal(computeSize(representation))
      }
    })
  }

  test("declare and assign double") {
    def declareAndAssign(v: Any) =
      block(
        declare[Double]("a"),
        assign("a", constant(v))
      )

    val constants = Seq(
      0.0,
      1.0,
      Math.E,
      Math.PI,
      Double.MinPositiveValue,
      Double.MinValue,
      Double.MaxValue,
      Double.NegativeInfinity,
      Double.PositiveInfinity
    )
    constants.foreach(v => {
      val representation = declareAndAssign(v)
      withClue(s"Wrong estimation of $v") {
        sizeOf(representation) should equal(computeSize(representation))
      }
    })
  }

  test("declare and assign references") {
    def declareAndAssign(v: Any) =
      block(
        declare[Object]("a"),
        assign("a", constant(v))
      )

    val constants = Seq("Hello", null)
    constants.foreach(v => {
      val representation = declareAndAssign(v)
      withClue(s"Wrong estimation of $v") {
        sizeOf(representation) should equal(computeSize(representation))
      }
    })
  }

  test("declare and assign many local variables") {
    val instructions =
      block((1 to 512).flatMap(i => {
        Seq(
          declare[Int](s"a$i"),
          assign(s"a$i", constant(42))
        )
      }): _*)

    val i = computeSize(instructions)
    sizeOf(instructions) should equal(i)
  }

  test("load many local variables") {
    val instructions =
      block((1 to 512).flatMap(i => {
        Seq(
          declare[Int](s"a$i"),
          assign(s"a$i", constant(42)),
          assign(s"a$i", load[Int](s"a$i"))
        )
      }): _*)

    val i = computeSize(instructions)
    sizeOf(instructions) should equal(i)
  }

  test("invoke and invoke static") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", invokeStatic(echoMethod, constant("hello"))),
        invoke(load[AnyRef]("a"), method[AnyRef, String]("toString"))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("invoke virtual") {
    val instructions =
      block(
        declare[LongValue]("a"),
        assign("a", longValue(constant(42L))),
        invoke(load[AnyRef]("a"), method[Measurable, Long]("estimatedHeapUsage"))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("load instance field") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", loadField(field[String]("field")))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("load instance field with initializer") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", loadField(arrayField))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("get static field") {
    val instructions =
      block(
        declare[Double]("a"),
        assign("a", getStatic[Math, Double]("PI"))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("set field") {
    val instructions = setField(field[String]("field"), constant("hello"))
    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("primitive array literal") {
    val instructions =
      block(
        declare[Array[Int]]("a"),
        assign("a", arrayOf[Int](constant(1), constant(2), constant(3)))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("reference array literal") {
    val instructions =
      block(
        declare[Array[String]]("a"),
        assign("a", arrayOf[String](constant("1"), constant("2"), constant("3")))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("primitive array create and set") {
    val instructions =
      block(
        declare[Array[Int]]("a"),
        assign("a", newArray(typeRefOf[Int], 3)),
        arraySet(load[Array[Int]]("a"), 1, constant(3))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("reference array create and set") {
    val instructions =
      block(
        declare[Array[String]]("a"),
        assign("a", newArray(typeRefOf[String], 3)),
        arraySet(load[Array[String]]("a"), 1, constant("hello"))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("array length") {
    val instructions =
      block(
        declare[Array[Int]]("a"),
        assign("a", arrayOf[Int](constant(1), constant(2), constant(3))),
        declare[Int]("b"),
        assign("b", arrayLength(load[Array[Int]]("a")))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("array load") {
    val instructions =
      block(
        declare[Array[Int]]("a"),
        assign("a", arrayOf[Int](constant(1), constant(2), constant(3))),
        declare[Int]("b"),
        assign("b", arrayLoad(load[Array[Int]]("a"), 1))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("ternary operator") {
    val instructions =
      block(
        declare[Int]("a"),
        assign("a", ternary(constant(true), constant(1), constant(2)))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("add") {
    val instructions =
      block(
        declare[Int]("a"),
        assign("a", add(constant(1), constant(2)))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("subtract") {
    val instructions =
      block(
        declare[Int]("a"),
        assign("a", subtract(constant(1), constant(2)))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("multiply") {
    val instructions =
      block(
        declare[Int]("a"),
        assign("a", multiply(constant(1), constant(2)))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("comparisons longs") {
    def instructions(comparison: (
      IntermediateRepresentation,
      IntermediateRepresentation
    ) => IntermediateRepresentation) =
      block(
        declare[Boolean]("a"),
        assign("a", comparison(constant(1L), constant(2L)))
      )

    val comparisons: Seq[(IntermediateRepresentation, IntermediateRepresentation) => IntermediateRepresentation] =
      Seq(greaterThan, greaterThanOrEqual, lessThan, lessThanOrEqual, IntermediateRepresentation.equal, notEqual)
    comparisons.foreach(v => {
      val representation = instructions(v)
      sizeOf(representation) should equal(computeSize(representation))
    })
  }

  test("comparisons ints") {
    def instructions(comparison: (
      IntermediateRepresentation,
      IntermediateRepresentation
    ) => IntermediateRepresentation) =
      block(
        declare[Boolean]("a"),
        assign("a", comparison(constant(1), constant(2)))
      )

    val comparisons: Seq[(IntermediateRepresentation, IntermediateRepresentation) => IntermediateRepresentation] =
      Seq(greaterThan, greaterThanOrEqual, lessThan, lessThanOrEqual, IntermediateRepresentation.equal, notEqual)
    comparisons.foreach(v => {
      val representation = instructions(v)
      sizeOf(representation) should equal(computeSize(representation))
    })
  }

  test("compare references") {
    def instructions(comparison: (
      IntermediateRepresentation,
      IntermediateRepresentation
    ) => IntermediateRepresentation) =
      block(
        declare[Boolean]("a"),
        assign("a", comparison(constant("hello"), constant("there")))
      )

    val comparisons: Seq[(IntermediateRepresentation, IntermediateRepresentation) => IntermediateRepresentation] =
      Seq(IntermediateRepresentation.equal, IntermediateRepresentation.notEqual)
    comparisons.foreach(v => {
      val representation = instructions(v)
      sizeOf(representation) should equal(computeSize(representation))
    })
  }

  test("null checks") {
    def instructions(check: IntermediateRepresentation => IntermediateRepresentation) =
      block(
        declare[Boolean]("a"),
        assign("a", check(constant(null))),
        assign("a", check(invokeStatic(echoMethod, constant(null))))
      )

    val checks = Seq(IntermediateRepresentation.isNull _, IntermediateRepresentation.isNotNull _)
    checks.foreach(v => {
      val representation = instructions(v)
      sizeOf(representation) should equal(computeSize(representation))
    })
  }

  test("instance of") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", IntermediateRepresentation.instanceOf[String](constant("hello")))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("not") {
    def notInstruction(boolean: IntermediateRepresentation) =
      block(
        declare[Boolean]("a"),
        assign("a", IntermediateRepresentation.not(boolean))
      )
    val booleanInstructions =
      Seq(
        greaterThan(constant(1), constant(2)),
        constant(true),
        IntermediateRepresentation.not(constant(true)),
        callBooleanMethod,
        or(callBooleanMethod, callBooleanMethod),
        and(callBooleanMethod, callBooleanMethod)
      )

    booleanInstructions.foreach(instruction =>
      sizeOf(notInstruction(instruction)) should equal(computeSize(notInstruction(instruction)))
    )
  }

  test("condition") {
    val instructions =
      block(
        condition(callBooleanMethod) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + not") {
    val instructions =
      block(
        condition(IntermediateRepresentation.not(callBooleanMethod)) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + IS NULL") {
    val instructions =
      block(
        condition(isNull(invokeStatic(echoMethod, constant(null)))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + equal") {
    val instructions =
      block(
        condition(IntermediateRepresentation.equal(constant(53), constant(54))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + not equal") {
    val instructions =
      block(
        condition(notEqual(constant(53), constant(54))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + greater than") {
    val instructions =
      block(
        condition(greaterThan(constant(1), constant(2))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + ors") {
    val instructions =
      block(
        condition(
          or(
            Seq(
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod
            )
          )
        ) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + not + and") {
    val instructions =
      block(
        condition(IntermediateRepresentation.not(and(callBooleanMethod, callBooleanMethod))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + not + or") {
    val instructions =
      block(
        condition(IntermediateRepresentation.not(or(callBooleanMethod, callBooleanMethod))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("if else") {
    val instructions =
      block(
        ifElse(callBooleanMethod) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        } {
          block(
            declare[Int]("b"),
            assign("b", constant(2))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("if else + not") {
    val instructions =
      block(
        ifElse(IntermediateRepresentation.not(callBooleanMethod)) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        } {
          block(
            declare[Int]("b"),
            assign("b", constant(2))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("if else + is null") {
    val instructions =
      block(
        ifElse(isNull(invokeStatic(echoMethod, constant(null)))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        } {
          block(
            declare[Int]("b"),
            assign("b", constant(2))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("if else + and") {
    val instructions =
      block(
        ifElse(and(callBooleanMethod, callBooleanMethod)) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        } {
          block(
            declare[Int]("b"),
            assign("b", constant(2))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("if else + or") {
    val instructions =
      block(
        ifElse(or(callBooleanMethod, callBooleanMethod)) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        } {
          block(
            declare[Int]("b"),
            assign("b", constant(2))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("if else + not + or") {
    val instructions =
      block(
        ifElse(IntermediateRepresentation.not(or(callBooleanMethod, callBooleanMethod))) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        } {
          block(
            declare[Int]("b"),
            assign("b", constant(2))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("and") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", and(callBooleanMethod, callBooleanMethod))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("ands") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign(
          "a",
          and(
            Seq(
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod
            )
          )
        )
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + and") {
    val instructions =
      block(
        condition(and(callBooleanMethod, callBooleanMethod)) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + ands") {
    val instructions =
      block(
        condition(
          and(
            Seq(
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod
            )
          )
        ) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("or") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", or(callBooleanMethod, callBooleanMethod))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("ors") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign(
          "a",
          or(
            Seq(
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod,
              callBooleanMethod
            )
          )
        )
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + or") {
    val instructions =
      block(
        condition(or(callBooleanMethod, callBooleanMethod)) {
          block(
            declare[Int]("a"),
            assign("a", constant(1))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + and + or") {
    val instructions =
      block(
        condition(and(Seq(callBooleanMethod, or(callBooleanMethod, callBooleanMethod)))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + and + multiple ors") {
    val instructions =
      block(
        condition(and(Seq(
          or(callBooleanMethod, or(callBooleanMethod, callBooleanMethod)),
          or(callBooleanMethod, callBooleanMethod),
          or(callBooleanMethod, callBooleanMethod)
        ))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("condition + or + and") {
    val instructions =
      block(
        condition(or(Seq(callBooleanMethod, and(callBooleanMethod, callBooleanMethod)))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("new instance") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", newInstance(constructor[Object]))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", callBooleanMethod),
        loop(load[Boolean]("a")) {
          assign("a", callBooleanMethod)
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop + not") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", callBooleanMethod),
        loop(IntermediateRepresentation.not(load[Boolean]("a"))) {
          assign("a", callBooleanMethod)
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop + is null") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", callBooleanMethod),
        loop(isNull(invokeStatic(echoMethod, constant(null)))) {
          assign("a", callBooleanMethod)
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop + ands") {
    val instructions =
      block(
        loop(and(Seq(callBooleanMethod, callBooleanMethod, callBooleanMethod))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop + and + or") {
    val instructions =
      block(
        loop(and(Seq(callBooleanMethod, or(callBooleanMethod, callBooleanMethod)))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop + not + and") {
    val instructions =
      block(
        loop(IntermediateRepresentation.not(and(Seq(callBooleanMethod, callBooleanMethod, callBooleanMethod)))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop + ors") {
    val instructions =
      block(
        loop(or(Seq(callBooleanMethod, callBooleanMethod, callBooleanMethod))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop + not + or") {
    val instructions =
      block(
        loop(IntermediateRepresentation.not(or(Seq(
          callBooleanMethod,
          callBooleanMethod,
          callBooleanMethod,
          callBooleanMethod
        )))) {
          block(
            declare[Int]("a"),
            assign("a", constant(3))
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("loop with break") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", callBooleanMethod),
        labeledLoop("FOO", load[Boolean]("a")) {
          block(
            assign("a", callBooleanMethod),
            break("FOO")
          )
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("nested loop") {
    val instructions =
      block(
        declare[Boolean]("a"),
        assign("a", callBooleanMethod),
        loop(load[Boolean]("a")) {
          loop(load[Boolean]("a")) {
            loop(load[Boolean]("a")) {
              loop(load[Boolean]("a")) {
                assign("a", callBooleanMethod)
              }
            }
          }
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("box") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", box(constant(1L)))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("unbox") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", unbox(box(constant(1L))))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("self") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", self[Object])
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("non-primitive cast") {
    val instructions =
      block(
        declare[Object]("a"),
        assign("a", constant("hello")),
        declare[String]("b"),
        assign("b", cast[String](load[Object]("a")))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("primitive cast") {
    val instructions =
      block(
        declare[Int]("a"),
        assign("a", constant(42)),
        declare[Long]("b"),
        assign("b", cast[Long](load[Int]("a")))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("invoke static side-effect") {
    val instructions = IntermediateRepresentation.print(constant("hello"))

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("try-catch") {
    val instructions =
      block(
        tryCatch[RuntimeException]("e") {
          block(
            declare[Int]("a"),
            assign("a", constant(42))
          )
        } /*catch*/ {
          IntermediateRepresentation.fail(load[RuntimeException]("e"))
        }
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  test("only count one-time once") {
    val once = oneTime(block(
      declare[Int]("a"),
      assign("a", constant(42))
    ))
    val instructions =
      block(
        declare[Int]("b"),
        assign("b", block(once, load[Int]("a"))),
        declare[Int]("c"),
        assign("c", block(once, load[Int]("a"))),
        declare[Int]("d"),
        assign("d", block(once, load[Int]("a"))),
        declare[Int]("e"),
        assign("e", block(once, load[Int]("a"))),
        declare[Int]("f"),
        assign("f", block(once, load[Int]("a")))
      )

    sizeOf(instructions) should equal(computeSize(instructions))
  }

  private def computeSize(ir: IntermediateRepresentation) = SizeEstimation.estimateByteCodeSize(ir, 1)

  private def sizeOf(ir: IntermediateRepresentation) = {
    val handle = codeGeneration.compileClass(clazz(ir), generator)
    handle.loadClass()
    sizeComputer.byteSize
  }

  private def clazz(body: IntermediateRepresentation) = {
    val declaration = new ClassDeclaration[AnyRef](
      "org.neo4j.codegen.api",
      s"Foo$count",
      None,
      Seq.empty,
      Seq.empty,
      noop(),
      () => Seq(field[String]("field", constant("hello field")), arrayField),
      Seq(MethodDeclaration(
        "test",
        TypeReference.VOID,
        Seq.empty,
        block(
          // NOTE: this is a little bit of a hack, here we insert `int tag=123` at the beginning of the method to
          //      make it easy to locate the method in the byte code later
          declare[Int]("tag"),
          assign("tag", constant(123)),
          body
        ),
        () => Seq.empty
      ))
    )
    count += 1
    declaration
  }

  class ByteSizeComputer extends ByteCodeVisitor {
    var byteSize = 0

    override def visitByteCode(name: String, bytes: ByteBuffer): Unit = {
      val byteCode = bytes.array().toIndexedSeq
      // Look for our magic marker `int tag = 123`
      val index = byteCode.lastIndexOfSlice(Seq(Opcodes.BIPUSH.toByte, 123.toByte))
      var i = index + 3 // jump over BIPUSH 123 (2 bytes) + ISTORE (1 byte)

      // the size of the method is what is between our `int tag = 123` and `RETURN`
      def done = byteCode(i) == Opcodes.RETURN.toByte && byteCode(i + 1) == 0
      while (i < byteCode.length - 1 && !done) {
        i += 1
      }
      byteSize = i - index - 3
    }
  }
}

object SizeEstimationTest {
  def testMethod(s: AnyRef): AnyRef = s

  def testBoolean: Boolean = Random.nextBoolean()

  val arrayField: InstanceField = field[Array[Int]]("arrayField", arrayOf[Int]((1 to 1023).map(i => constant(i)): _*))
}
