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
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLength
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arraySet
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.block
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.invoke
import org.neo4j.codegen.api.IntermediateRepresentation.lessThan
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.newDynamicallySizedArray
import org.neo4j.codegen.api.IntermediateRepresentation.print
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.TypedWrapper.$IR
import org.neo4j.codegen.api.TypedWrapper.TypedWrapperConversion

class TypedWrapperTest extends CodegenTestSuite {

  private val typedIr = new TypedIR(new VariableNamer {
    def nextVariableName(suffix: String*): String = suffix.mkString + "_named"
  })

  import typedIr._

  test("method invocation with 0 parameters") {
    val method = TypedMethod.on[Foo].named("test").returning[String]
    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)(())

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.empty),
      Seq.empty
    )
  }

  test("method invocation with 1 parameter") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[Int]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)($constant(1))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq(typeRefOf[Int])),
      Seq(constant(1))
    )
  }

  test("method invocation with 2 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)(($constant(1), $constant(2)))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(2)(typeRefOf[Int])),
      (1 to 2).map(constant(_))
    )
  }

  test("method invocation with 3 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)(($constant(1), $constant(2), $constant(3)))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(3)(typeRefOf[Int])),
      (1 to 3).map(constant(_))
    )
  }

  test("method invocation with 4 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)(($constant(1), $constant(2), $constant(3), $constant(4)))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(4)(typeRefOf[Int])),
      (1 to 4).map(constant(_))
    )
  }

  test("method invocation with 5 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)(($constant(1), $constant(2), $constant(3), $constant(4), $constant(5)))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(5)(typeRefOf[Int])),
      (1 to 5).map(constant(_))
    )
  }

  test("method invocation with 6 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation =
      $foo.invoke(method)(($constant(1), $constant(2), $constant(3), $constant(4), $constant(5), $constant(6)))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(6)(typeRefOf[Int])),
      (1 to 6).map(constant(_))
    )
  }

  test("method invocation with 7 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7)
    ))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(7)(typeRefOf[Int])),
      (1 to 7).map(constant(_))
    )
  }

  test("method invocation with 8 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8)
    ))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(8)(typeRefOf[Int])),
      (1 to 8).map(constant(_))
    )
  }

  test("method invocation with 9 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9)
    ))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(9)(typeRefOf[Int])),
      (1 to 9).map(constant(_))
    )
  }

  test("method invocation with 10 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9),
      $constant(10)
    ))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(10)(typeRefOf[Int])),
      (1 to 10).map(constant(_))
    )
  }

  test("method invocation with 11 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9),
      $constant(10),
      $constant(11)
    ))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(11)(typeRefOf[Int])),
      (1 to 11).map(constant(_))
    )
  }

  test("method invocation with 12 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9),
      $constant(10),
      $constant(11),
      $constant(12)
    ))

    invocation.inner shouldEqual Invoke(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(12)(typeRefOf[Int])),
      (1 to 12).map(constant(_))
    )
  }

  test("static method invocation with 0 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[Unit]
      .returning[String]

    val invocation = method.asStatic.invoke(())

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.empty),
      Seq.empty
    )
  }

  test("static method invocation with 1 parameter") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[Int]
      .returning[String]

    val invocation = method.asStatic.invoke(
      $constant(1)
    )

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq(typeRefOf[Int])),
      Seq(constant(1))
    )
  }

  test("static method invocation with 2 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(2)(typeRefOf[Int])),
      (1 to 2).map(constant(_))
    )
  }

  test("static method invocation with 3 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(3)(typeRefOf[Int])),
      (1 to 3).map(constant(_))
    )
  }

  test("static method invocation with 4 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(4)(typeRefOf[Int])),
      (1 to 4).map(constant(_))
    )
  }

  test("static method invocation with 5 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(5)(typeRefOf[Int])),
      (1 to 5).map(constant(_))
    )
  }

  test("static method invocation with 6 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(6)(typeRefOf[Int])),
      (1 to 6).map(constant(_))
    )
  }

  test("static method invocation with 7 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(7)(typeRefOf[Int])),
      (1 to 7).map(constant(_))
    )
  }

  test("static method invocation with 8 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(8)(typeRefOf[Int])),
      (1 to 8).map(constant(_))
    )
  }

  test("static method invocation with 9 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(9)(typeRefOf[Int])),
      (1 to 9).map(constant(_))
    )
  }

  test("static method invocation with 10 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9),
      $constant(10)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(10)(typeRefOf[Int])),
      (1 to 10).map(constant(_))
    )
  }

  test("static method invocation with 11 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9),
      $constant(10),
      $constant(11)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(11)(typeRefOf[Int])),
      (1 to 11).map(constant(_))
    )
  }

  test("static method invocation with 12 parameters") {
    val method = TypedMethod.on[Foo].named("test")
      .withParams[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]
      .returning[String]

    val invocation = method.asStatic.invoke((
      $constant(1),
      $constant(2),
      $constant(3),
      $constant(4),
      $constant(5),
      $constant(6),
      $constant(7),
      $constant(8),
      $constant(9),
      $constant(10),
      $constant(11),
      $constant(12)
    ))

    invocation.inner shouldEqual InvokeStatic(
      Method(typeRefOf[Foo], typeRefOf[String], "test", Seq.fill(12)(typeRefOf[Int])),
      (1 to 12).map(constant(_))
    )
  }

  test("constructor invocation") {
    val method = TypedMethod.on[Foo].withParams[(Int, String)].asConstructor

    val invocation = method.invoke(($constant(1), $constant("two")))

    invocation.inner shouldEqual NewInstance(
      Constructor(typeRefOf[Foo], Seq(typeRefOf[Int], typeRefOf[String])),
      Seq(Constant(1), Constant("two"))
    )
  }

  test("method invocation with void return type") {
    val method = TypedMethod.on[Foo].named("test").returning[Unit]
    val $foo = $constant(new Foo)

    val invocation = $foo.invoke(method)(())

    invocation.inner shouldEqual InvokeSideEffect(
      $foo.inner,
      Method(typeRefOf[Foo], typeRefOf[Unit], "test", Seq.empty),
      Seq.empty
    )
  }

  test("static method invocation with void return type") {
    val method = TypedMethod.on[Foo].named("test").returning[Unit].asStatic

    val invocation = method.invoke(())

    invocation.inner shouldEqual InvokeStaticSideEffect(
      Method(typeRefOf[Foo], typeRefOf[Unit], "test", Seq.empty),
      Seq.empty
    )
  }

  test("private method invocation with void return type") {
    val method = TypedMethod.named("test").returning[Unit].asPrivate

    val invocation = method.invoke(())

    invocation.inner shouldEqual InvokeLocalSideEffect(
      PrivateMethod(typeRefOf[Unit], "test", Seq.empty),
      Seq.empty
    )
  }

  test("chained invocations") {
    val `Foo.self` = TypedMethod.on[Foo].named("self").returning[Foo].withParams[Int]
    val `Foo.toString` = TypedMethod.on[Foo].named("toString").returning[String]
    val foo = $constant(new Foo)

    val result = foo.invoke(`Foo.self`)($constant(1))
      .invoke(`Foo.self`)($constant(2))
      .invoke(`Foo.toString`)(())

    result.inner shouldEqual
      Invoke(
        Invoke(
          Invoke(
            foo.inner,
            Method(typeRefOf[Foo], typeRefOf[Foo], "self", Seq(typeRefOf[Int])),
            Seq(Constant(1))
          ),
          Method(typeRefOf[Foo], typeRefOf[Foo], "self", Seq(typeRefOf[Int])),
          Seq(Constant(2))
        ),
        Method(typeRefOf[Foo], typeRefOf[String], "toString", Seq.empty),
        Seq.empty
      )
  }

  test("$arrayForeach") {
    val $array = $arrayOf($constant("one"), $constant("two"))

    val $loop = $arrayForeach($array) { item =>
      print(item).typed[Unit]
    }

    $loop.inner shouldEqual
      block(
        DeclareLocalVariable(typeRefOf[Int], "i_named"),
        AssignToLocalVariable("i_named", constant(0)),
        loop(lessThan(load[Int]("i_named"), arrayLength($array.inner))) {
          block(
            print(arrayLoad($array.inner, load[Int]("i_named"))),
            assign("i_named", add(load[Int]("i_named"), constant(1)))
          )
        }
      )
  }

  test("$arrayMap") {
    val $array = $arrayOf($constant("one"), $constant("two"))

    val $result = $arrayMap($array) { str =>
      str.invoke(TypedMethod[String, Int, Unit]("length"))(())
    }

    $result.inner shouldEqual
      block(
        DeclareLocalVariable(typeRefOf[Array[Int]], "mappedArray_named"),
        AssignToLocalVariable("mappedArray_named", newDynamicallySizedArray(typeRefOf[Int], arrayLength($array.inner))),
        DeclareLocalVariable(typeRefOf[Int], "i_named"),
        AssignToLocalVariable("i_named", constant(0)),
        loop(lessThan(load[Int]("i_named"), arrayLength($array.inner))) {
          block(
            arraySet(
              load[Array[Int]]("mappedArray_named"),
              load[Int]("i_named"),
              invoke(
                arrayLoad($array.inner, load[Int]("i_named")),
                Method(typeRefOf[String], typeRefOf[Int], "length", Seq.empty)
              )
            ),
            assign("i_named", add(load[Int]("i_named"), constant(1)))
          )
        },
        load[Array[Int]]("mappedArray_named")
      )
  }

  private val `Foo.doSomething` =
    TypedMethod.on[Foo].named("doSomething").withParams[Int].asStatic

  test("if") {
    val $test = $variable[Boolean]("test", $constant(true))

    val $result: $IR[Unit] = $if($test)(
      `Foo.doSomething`($constant(1)),
      $if($test) {
        `Foo.doSomething`($constant(2))
      }
    )

    $result.inner shouldEqual
      Condition(
        $load($test),
        Block(Seq(
          `Foo.doSomething`($constant(1)),
          Condition($load($test), `Foo.doSomething`($constant(2)))
        ))
      )
  }

  test("if-else") {
    val $test = $variable[Boolean]("test", $constant(true))
    val $result = $if($test)(
      `Foo.doSomething`($constant(1)),
      `Foo.doSomething`($constant(2))
    ) $else (
      `Foo.doSomething`($constant(3)),
      `Foo.doSomething`($constant(4))
    )

    $result.inner shouldEqual
      Condition(
        $load($test),
        Block(Seq(
          `Foo.doSomething`($constant(1)),
          `Foo.doSomething`($constant(2))
        )),
        Some(Block(Seq(
          `Foo.doSomething`($constant(3)),
          `Foo.doSomething`($constant(4))
        )))
      )
  }

  test("if-else if") {
    val $v1 = $variable[Boolean]("var", $constant(true))
    val $v2 = $variable[Boolean]("var", $constant(true))

    val $result: $IR[Unit] = $if($v1)(
      `Foo.doSomething`($constant(1)),
      `Foo.doSomething`($constant(2))
    ).$elseIf($v2)(
      `Foo.doSomething`($constant(3)),
      `Foo.doSomething`($constant(4))
    )

    $result.inner shouldEqual
      Condition(
        $load($v1),
        Block(Seq(
          `Foo.doSomething`($constant(1)),
          `Foo.doSomething`($constant(2))
        )),
        Some(Condition(
          $load($v2),
          Block(Seq(
            `Foo.doSomething`($constant(3)),
            `Foo.doSomething`($constant(4))
          ))
        ))
      )
  }

  test("if-else if-else") {
    val $v1 = $variable[Boolean]("var", $constant(true))
    val $v2 = $variable[Boolean]("var", $constant(true))

    val $result = $if($v1)(
      `Foo.doSomething`($constant(1)),
      `Foo.doSomething`($constant(2))
    ).$elseIf($v2)(
      `Foo.doSomething`($constant(3)),
      `Foo.doSomething`($constant(4))
    ).$else(
      `Foo.doSomething`($constant(5)),
      `Foo.doSomething`($constant(6))
    )

    $result.inner shouldEqual
      Condition(
        $load($v1),
        Block(Seq(
          `Foo.doSomething`($constant(1)),
          `Foo.doSomething`($constant(2))
        )),
        Some(Condition(
          $load($v2),
          Block(Seq(
            `Foo.doSomething`($constant(3)),
            `Foo.doSomething`($constant(4))
          )),
          Some(Block(Seq(
            `Foo.doSomething`($constant(5)),
            `Foo.doSomething`($constant(6))
          )))
        ))
      )
  }

  test("if-else if-else if-else") {
    val $v1 = $variable[Boolean]("var", $constant(true))
    val $v2 = $variable[Boolean]("var", $constant(true))
    val $v3 = $variable[Boolean]("var", $constant(true))

    val $result = $if($v1)(
      `Foo.doSomething`($constant(1)),
      `Foo.doSomething`($constant(2))
    ).$elseIf($v2)(
      `Foo.doSomething`($constant(3)),
      `Foo.doSomething`($constant(4))
    ).$elseIf($v3)(
      `Foo.doSomething`($constant(5)),
      `Foo.doSomething`($constant(6))
    ).$else(
      `Foo.doSomething`($constant(7)),
      `Foo.doSomething`($constant(8))
    )

    $result.inner shouldEqual
      Condition(
        $load($v1),
        Block(Seq(
          `Foo.doSomething`($constant(1)),
          `Foo.doSomething`($constant(2))
        )),
        Some(Condition(
          $load($v2),
          Block(Seq(
            `Foo.doSomething`($constant(3)),
            `Foo.doSomething`($constant(4))
          )),
          Some(Condition(
            $load($v3),
            Block(Seq(
              `Foo.doSomething`($constant(5)),
              `Foo.doSomething`($constant(6))
            )),
            Some(Block(Seq(
              `Foo.doSomething`($constant(7)),
              `Foo.doSomething`($constant(8))
            )))
          ))
        ))
      )
  }

  class Foo {}

}
