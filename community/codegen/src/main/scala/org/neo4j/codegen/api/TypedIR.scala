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

import org.neo4j.codegen.api.IntermediateRepresentation.arrayLength
import org.neo4j.codegen.api.IntermediateRepresentation.arrayLoad
import org.neo4j.codegen.api.IntermediateRepresentation.arrayOf
import org.neo4j.codegen.api.IntermediateRepresentation.arraySet
import org.neo4j.codegen.api.IntermediateRepresentation.assign
import org.neo4j.codegen.api.IntermediateRepresentation.constant
import org.neo4j.codegen.api.IntermediateRepresentation.field
import org.neo4j.codegen.api.IntermediateRepresentation.flattenBlock
import org.neo4j.codegen.api.IntermediateRepresentation.ifElse
import org.neo4j.codegen.api.IntermediateRepresentation.load
import org.neo4j.codegen.api.IntermediateRepresentation.loop
import org.neo4j.codegen.api.IntermediateRepresentation.newDynamicallySizedArray
import org.neo4j.codegen.api.IntermediateRepresentation.noop
import org.neo4j.codegen.api.IntermediateRepresentation.setField
import org.neo4j.codegen.api.IntermediateRepresentation.typeRef
import org.neo4j.codegen.api.IntermediateRepresentation.typeRefOf
import org.neo4j.codegen.api.IntermediateRepresentation.variable
import org.neo4j.codegen.api.TypedIR.flattenIR
import org.neo4j.codegen.api.TypedWrapper.$Field
import org.neo4j.codegen.api.TypedWrapper.$IR
import org.neo4j.codegen.api.TypedWrapper.$Ref
import org.neo4j.codegen.api.TypedWrapper.$Variable
import org.neo4j.codegen.api.TypedWrapper.TypedWrapperConversion

trait NoDependencyTypedIR {

  def $typeRef[TYPE](implicit typ: Manifest[TYPE]): $Ref[TYPE] =
    typeRef(typ).typed

  def $assign[TYPE](variable: $Variable[TYPE], value: $IR[TYPE]): $IR[Unit] =
    assign(variable, value).typed

  def $if(test: $IR[Boolean])(onTrue: $IR[_]*): IfChain =
    new IfChain(Vector(test.inner -> flattenIR(onTrue)))

  def $while(test: $IR[Boolean])(body: $IR[_]*): $IR[Unit] =
    loop(test)(flattenIR(body)).typed

  def $field[TYPE: Manifest](name: String): $Field[TYPE] =
    field(name).typed

  def $field[TYPE: Manifest](name: String, initializer: $IR[TYPE]): $Field[TYPE] =
    field(name, initializer).typed

  def $load[TYPE: Manifest](variable: String): TypedWrapper[TYPE, Load] = load(variable).typed

  def $load[TYPE: Manifest](variable: $Variable[TYPE]): TypedWrapper[TYPE, Load] = load(variable).typed

  def $setField[TYPE](field: $Field[TYPE], value: $IR[TYPE]): $IR[Unit] =
    setField(field, value).typed

  def $constant[T](value: T): $IR[T] = constant(value).typed

  val $null: $IR[Null] = $constant(null)

  def $not(bool: $IR[Boolean]): $IR[Boolean] =
    Not(bool).typed

  def flattenIR(ir: Seq[$IR[_]]): IntermediateRepresentation = flattenBlock(ir.map(_.inner): _*)

  // it would be nice if you could have a variable-length argument *first* so these overloads would not be needed
  def $block[A](a: $IR[A]): $IR[A] = flattenBlock(a).typed
  def $block[B](a: $IR[_], b: $IR[B]): $IR[B] = flattenBlock(a, b).typed
  def $block[C](a: $IR[_], b: $IR[_], c: $IR[C]): $IR[C] = flattenBlock(a, b, c).typed
  def $block[D](a: $IR[_], b: $IR[_], c: $IR[_], d: $IR[D]): $IR[D] = flattenBlock(a, b, c, d).typed
  def $block[E](a: $IR[_], b: $IR[_], c: $IR[_], d: $IR[_], e: $IR[E]): $IR[E] = flattenBlock(a, b, c, d, e).typed

  def $block[F](a: $IR[_], b: $IR[_], c: $IR[_], d: $IR[_], e: $IR[_], f: $IR[F]): $IR[F] =
    flattenBlock(a, b, c, d, e, f).typed

  def $block[G](a: $IR[_], b: $IR[_], c: $IR[_], d: $IR[_], e: $IR[_], f: $IR[_], g: $IR[G]): $IR[G] =
    flattenBlock(a, b, c, d, e, f, g).typed

  def $block[H](a: $IR[_], b: $IR[_], c: $IR[_], d: $IR[_], e: $IR[_], f: $IR[_], g: $IR[_], h: $IR[H]): $IR[H] =
    flattenBlock(a, b, c, d, e, f, g, h).typed

  def $block[I](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[I]
  ): $IR[I] = flattenBlock(a, b, c, d, e, f, g, h, i).typed

  def $block[J](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[J]
  ): $IR[J] = flattenBlock(a, b, c, d, e, f, g, h, i, j).typed

  def $block[K](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[K]
  ): $IR[K] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k).typed

  def $block[L](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[L]
  ): $IR[L] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l).typed

  def $block[M](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[M]
  ): $IR[M] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m).typed

  def $block[N](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[N]
  ): $IR[N] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n).typed

  def $block[O](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[O]
  ): $IR[O] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o).typed

  def $block[P](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[_],
    p: $IR[P]
  ): $IR[P] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p).typed

  def $block[Q](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[_],
    p: $IR[_],
    q: $IR[Q]
  ): $IR[Q] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q).typed

  def $block[R](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[_],
    p: $IR[_],
    q: $IR[_],
    r: $IR[R]
  ): $IR[R] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r).typed

  def $block[S](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[_],
    p: $IR[_],
    q: $IR[_],
    r: $IR[_],
    s: $IR[S]
  ): $IR[S] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s).typed

  def $block[T](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[_],
    p: $IR[_],
    q: $IR[_],
    r: $IR[_],
    s: $IR[_],
    t: $IR[T]
  ): $IR[T] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t).typed

  def $block[U](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[_],
    p: $IR[_],
    q: $IR[_],
    r: $IR[_],
    s: $IR[_],
    t: $IR[_],
    u: $IR[U]
  ): $IR[U] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u).typed

  def $block[V](
    a: $IR[_],
    b: $IR[_],
    c: $IR[_],
    d: $IR[_],
    e: $IR[_],
    f: $IR[_],
    g: $IR[_],
    h: $IR[_],
    i: $IR[_],
    j: $IR[_],
    k: $IR[_],
    l: $IR[_],
    m: $IR[_],
    n: $IR[_],
    o: $IR[_],
    p: $IR[_],
    q: $IR[_],
    r: $IR[_],
    s: $IR[_],
    t: $IR[_],
    u: $IR[_],
    v: $IR[V]
  ): $IR[V] = flattenBlock(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v).typed

  def $arrayOf[T: Manifest](values: $IR[T]*): $IR[Array[T]] =
    arrayOf(values.map(_.inner): _*).typed

  def $arraySet[T](array: $IR[Array[T]], offset: $IR[Int], value: $IR[T]): $IR[Unit] =
    arraySet(array, offset, value).typed

  def $arrayLoad[T](array: $IR[Array[T]], offset: $IR[Int]): $IR[T] =
    arrayLoad(array, offset).typed

  def $arrayLength(array: $IR[Array[_]]): $IR[Int] =
    arrayLength(array).typed

  def $newArray[T: Manifest](size: $IR[Int]): $IR[Array[T]] =
    newDynamicallySizedArray(typeRefOf[T], size).typed
}

object TypedIR extends NoDependencyTypedIR

class TypedIR(namer: VariableNamer) extends NoDependencyTypedIR {

  /** creates a uniquely-named variable with the given suffix */
  def $variable[TYPE: Manifest](suffix: String, initialValue: $IR[TYPE]): $Variable[TYPE] =
    variable(namer.nextVariableName(suffix), initialValue).typed

  /** generates a for loop from 0 to $limit */
  def $for($limit: $IR[Int], varName: String = "i")(f: $IR[Int] => $IR[Unit]): $IR[Unit] = {
    val $var = $variable(varName, $constant(0))
    $block(
      $var.declareAndAssign,
      $while($var < $limit)(
        f($var),
        $var := $var add $constant(1)
      )
    )
  }

  /** generates a for loop over the array with the given variable name */
  def $arrayForeach[T]($arr: $IR[Array[T]], varName: String = "i")(f: $IR[T] => $IR[Unit]): $IR[Unit] =
    $for($arrayLength($arr), varName)($i => f($arr($i)))

  /** creates a new array and loops over the original, assigning the value of f($arr[idx]) to the corresponding index in the new array */
  def $arrayMap[T, U: Manifest](
    $arr: $IR[Array[T]],
    indexName: String = "i",
    arrayName: String = "mappedArray"
  )(f: $IR[T] => $IR[U])(using arrayManifest: Manifest[Array[U]]): $IR[Array[U]] = {
    val $mappedArray = $variable(arrayName, $newArray[U]($arrayLength($arr)))
    $block(
      $mappedArray.declareAndAssign,
      $for($arr.length, indexName)($i =>
        $mappedArray($i) = f($arr($i))
      ),
      $mappedArray
    )
  }
}

/** Enables a syntax similar to unnested if-else chains in Java - see unit tests for usage */
class IfChain(cases: Vector[(IntermediateRepresentation, IntermediateRepresentation)]) {

  /** Generate the chain of conditions with an empty final else branch */
  def toIR: $IR[Unit] =
    $else(noop().typed)

  /** Generate the chain of conditions with a final else branch */
  def $else(onFalse: $IR[_]*): $IR[Unit] =
    cases.foldRight(flattenIR(onFalse)) { case ((test, ifTrue), ifFalse) =>
      ifElse(test)(ifTrue)(ifFalse)
    }.typed

  /** Append another case */
  def $elseIf(elseTest: $IR[Boolean])(elseIfTrue: $IR[_]*): IfChain =
    new IfChain(cases.appended(elseTest.inner -> flattenIR(elseIfTrue)))
}

trait VariableNamer {
  def nextVariableName(suffix: String*): String
}
