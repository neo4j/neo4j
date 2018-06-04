/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled.expressions

import org.neo4j.codegen.MethodReference
import org.neo4j.values.storable.{FloatingPointValue, IntegralValue, TextValue}

import scala.reflect.ClassTag

/**
  * IntermediateRepresentation is an intermediate step between pure byte code and the operator/expression
  *
  * The representation is intended to be quite low level and fairly close to the actual bytecode representation.
  */
sealed trait IntermediateRepresentation

/**
  * Invoke a static method
  *
  * @param method the method to invoke
  * @param params the parameter to the static method
  */
case class InvokeStatic(method: Method, params: Seq[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * Invoke a method
  *
  * @param target the target to call the method on
  * @param method the method to invoke
  * @param params the parameter to the method
  */
case class Invoke(target: IntermediateRepresentation, method: Method, params: Seq[IntermediateRepresentation])
  extends IntermediateRepresentation

/**
  * Load a local variable by name
  *
  * @param variable the name of the variable
  */
case class Load(variable: String) extends IntermediateRepresentation

/**
  * Loads constant IntegralValue
  *
  * @param value the constant value
  */
case class Integer(value: IntegralValue) extends IntermediateRepresentation

/**
  * Constant FloatingPointValue
  *
  * @param value the constant value
  */
case class Float(value: FloatingPointValue) extends IntermediateRepresentation

/**
  * Constant TextValue
  *
  * @param value the constant value
  */
case class StringLiteral(value: TextValue) extends IntermediateRepresentation

/**
  * Constant java value
  *
  * @param value the constant value
  */
case class Constant(value: Any) extends IntermediateRepresentation

/**
  * Load NO_VALUE
  */
case object NULL extends IntermediateRepresentation

/**
  * Load TRUE
  */
case object TRUE extends IntermediateRepresentation


/**
  * Load FALSE
  */
case object FALSE extends IntermediateRepresentation

/**
  * Loads an array literal of the given inputs
  * @param values the values of the array
  */
case class ArrayLiteral(values: Array[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * Defines ternary expression, i.e. `condition ? onTrue : onFalse`
  * @param condition the condition to test
  * @param onTrue will be evaluted if condition is true
  * @param onFalse will be evaluated if condition is false
  */
case class Ternary(condition: IntermediateRepresentation,
                   onTrue: IntermediateRepresentation,
                   onFalse: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines equality of identity, i.e. `lhs == rhs`
  * @param lhs the left-hand side to check
  * @param rhs the right-hand side to check
  */
case class Eq(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines a method
  *
  * @param owner  the owner of the method
  * @param output output type to the method
  * @param name   the name of the method
  * @param params the parameter types of the method
  */
case class Method(owner: Class[_], output: Class[_], name: String, params: Class[_]*) {

  def asReference: MethodReference = MethodReference.methodReference(owner, output, name, params: _*)
}

/**
  * Defines a simple dsl to facilitate constructing intermediate representation
  */
object IntermediateRepresentation {

  def method[OWNER, OUT](name: String)(implicit owner: ClassTag[OWNER], out: ClassTag[OUT]) =
    Method(owner.runtimeClass, out.runtimeClass, name)

  def method[OWNER, OUT, IN](name: String)(implicit owner: ClassTag[OWNER], out: ClassTag[OUT], in: ClassTag[IN]) =
    Method(owner.runtimeClass, out.runtimeClass, name, in.runtimeClass)

  def method[OWNER, OUT, IN1, IN2](name: String)
                                  (implicit owner: ClassTag[OWNER], out: ClassTag[OUT], in1: ClassTag[IN1],
                                   in2: ClassTag[IN2]) =
    Method(owner.runtimeClass, out.runtimeClass, name, in1.runtimeClass, in2.runtimeClass)

  def method[OWNER, OUT, IN1, IN2, IN3](name: String)
                                       (implicit owner: ClassTag[OWNER], out: ClassTag[OUT], in1: ClassTag[IN1],
                                        in2: ClassTag[IN2], in3: ClassTag[IN3]) =
    Method(owner.runtimeClass, out.runtimeClass, name, in1.runtimeClass, in2.runtimeClass, in3.runtimeClass)

  def invokeStatic(method: Method, params: IntermediateRepresentation*): IntermediateRepresentation = InvokeStatic(
    method, params)

  def invoke(owner: IntermediateRepresentation, method: Method,
             params: IntermediateRepresentation*): IntermediateRepresentation =
    Invoke(owner, method, params)

  def load(variable: String): IntermediateRepresentation = Load(variable)

  def noValue: IntermediateRepresentation = NULL

  def truthy: IntermediateRepresentation = TRUE

  def falsy: IntermediateRepresentation = FALSE

  def constant(value: Any): IntermediateRepresentation = Constant(value)

  def arrayOf(values: IntermediateRepresentation*): IntermediateRepresentation = ArrayLiteral(values.toArray)

  def ternary(condition: IntermediateRepresentation,
              onTrue: IntermediateRepresentation,
              onFalse: IntermediateRepresentation): IntermediateRepresentation = Ternary(condition, onTrue, onFalse)

  def equal(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Eq(lhs, rhs)
}