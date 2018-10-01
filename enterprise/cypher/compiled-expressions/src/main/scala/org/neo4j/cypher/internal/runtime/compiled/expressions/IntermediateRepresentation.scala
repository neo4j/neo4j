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

import org.neo4j.codegen.{MethodReference, TypeReference}
import org.neo4j.values.storable._

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
  * Invoke a void method
  *
  * @param target the target to call the method on
  * @param method the method to invoke
  * @param params the parameter to the method
  */
case class InvokeSideEffect(target: IntermediateRepresentation, method: Method, params: Seq[IntermediateRepresentation])
  extends IntermediateRepresentation

/**
  * Load a local variable by name
  *
  * @param variable the name of the variable
  */
case class Load(variable: String) extends IntermediateRepresentation

/**
  * Load a field
  *
  * @param field the field to load
  */
case class LoadField(field: Field) extends IntermediateRepresentation

/**
  * Set a field to a value
  *
  * @param field the field to set
  * @param value the value to set
  */
case class SetField(field: Field, value: IntermediateRepresentation) extends IntermediateRepresentation

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
  * Loads an array literal of the given inputs
  *
  * @param values the values of the array
  */
case class ArrayLiteral(values: Array[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * Defines ternary expression, i.e. {{{condition ? onTrue : onFalse}}}
  *
  * @param condition the condition to test
  * @param onTrue    will be evaluted if condition is true
  * @param onFalse   will be evaluated if condition is false
  */
case class Ternary(condition: IntermediateRepresentation,
                   onTrue: IntermediateRepresentation,
                   onFalse: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs + rhs}}}
  *
  * @param lhs the left-hand side to add
  * @param rhs the right-hand side to add
  */
case class Add(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs - rhs}}}
  *
  * @param lhs the left-hand side to subtract from
  * @param rhs the right-hand side to subtract
  */
case class Subtract(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs < rhs}}}
  *
  * @param lhs the left-hand side to compare
  * @param rhs the right-hand side to compare
  */
case class Lt(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs > rhs}}}
  *
  * @param lhs the left-hand side to compare
  * @param rhs the right-hand side to compare
  */
case class Gt(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines equality or identy, i.e. {{{lhs == rhs}}}
  *
  * @param lhs the left-hand side to check
  * @param rhs the right-hand side to check
  */
case class Eq(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines  {{{lhs != rhs}}}
  *
  * @param lhs the left-hand side to check
  * @param rhs the right-hand side to check
  */
case class NotEq(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines  !test
  *
  * @param test the expression to check
  */
case class Not(test: IntermediateRepresentation) extends IntermediateRepresentation

/**
  *
  */
case class IsNull(test: IntermediateRepresentation) extends IntermediateRepresentation


/**
  * A block is a sequence of operations where the block evaluates to the last expression
  * @param ops the operations to perform in the block
  */
case class Block(ops: Seq[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * A conditon executes the operation if the test evaluates to true.
  *
  *  {{{
  *  if (test)
  *  {
  *    onTrue;
  *  }
  *  }}}
  * @param test the condition to check
  * @param onTrue the opertation to perform if the `test` evaluates to true
  */
case class Condition(test: IntermediateRepresentation, onTrue: IntermediateRepresentation)
  extends IntermediateRepresentation

/**
  * A loop runs body while the provided test is true
  * @param test the body will run while this evaluates to true
  * @param body the body to run on each iteration
  */
case class Loop(test: IntermediateRepresentation, body: IntermediateRepresentation)
  extends IntermediateRepresentation

/**
  * Declare a local variable of the given type.
  *
  * {{{
  * typ name;
  * }}}
  * @param typ the type of the variable
  * @param name the name of the variable
  */
case class DeclareLocalVariable(typ: TypeReference, name: String) extends IntermediateRepresentation

/**
  * Assign a variable to a value.
  *
  * {{{
  * name = value;
  * }}}
  * @param name the name of the variable
  * @param value the value to assign to the variable
  */
case class AssignToLocalVariable(name: String, value: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * try-catch block
  * {{{
  *   try
  *   {
  *     ops;
  *   }
  *   catch (exception name)
  *   {
  *     onError;
  *   }
  * }}}
  * @param ops the operation to perform in the happy path
  * @param onError the operation to perform if an exception is caught
  * @param exception the type of the exception
  * @param name the name of the caught exception
  */
case class TryCatch(ops: IntermediateRepresentation, onError: IntermediateRepresentation, exception: TypeReference, name: String) extends IntermediateRepresentation

/**
  * Throw an error
  * @param error the error to throw
  */
case class Throw(error: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Boolean && operator
  * {{{
  *   lhs && rhs;
  * }}}
  * @param lhs the left-hand side of and
  * @param rhs the right-hand side of and
  */
case class BooleanAnd(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Boolean || operator
  * {{{
  *   lhs || rhs;
  * }}}
  * @param lhs the left-hand side of or
  * @param rhs the right-hand side of or
  */
case class BooleanOr(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Loads a static field
  * @param owner The owning class
  * @param output The type of the static field
  * @param name The name of the static field
  */
case class GetStatic(owner: TypeReference, output: TypeReference, name: String) extends IntermediateRepresentation

/**
  * Instantiate a new object
  * @param constructor the constructor to call
  * @param params the parameter to the constructor
  */
case class NewInstance(constructor: Constructor, params: Seq[IntermediateRepresentation]) extends IntermediateRepresentation

case class OneTime(inner: IntermediateRepresentation)(private var used: Boolean) extends IntermediateRepresentation {
  def isUsed: Boolean = used
  def use(): Unit = {
    used = true
  }
}

/**
  * Defines a constructor
  * @param owner the owner of the constructor, or the object to be instantiated
  * @param params the parameter to the constructor
  */
case class Constructor(owner: TypeReference, params: Seq[TypeReference]) {
  def asReference: MethodReference =
    if (params.isEmpty) MethodReference.constructorReference(owner)
    else MethodReference.constructorReference(owner, params:_*)
}

/**
  * Cast the given expression to the given type
  * @param to the type to cast to
  * @param expression the expression to cast
  */
case class Cast(to: TypeReference, expression: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Instance of check if the given expression has the given type
  * @param typ does expression have this type
  * @param expression the expression to check
  */
case class InstanceOf(typ: TypeReference, expression: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines a method
  *
  * @param owner  the owner of the method
  * @param output output type to the method
  * @param name   the name of the method
  * @param params the parameter types of the method
  */
case class Method(owner: TypeReference, output: TypeReference, name: String, params: TypeReference*) {

  def asReference: MethodReference = MethodReference.methodReference(owner, output, name, params: _*)
}

case class IntermediateExpression(ir: IntermediateRepresentation, fields: Seq[Field],
                                  variables: Seq[LocalVariable], nullCheck: Set[IntermediateRepresentation])

case class Field(typ: TypeReference, name: String, initializer: Option[IntermediateRepresentation] = None)

case class LocalVariable(typ: TypeReference, name: String, value: IntermediateRepresentation)

/**
  * Defines a simple dsl to facilitate constructing intermediate representation
  */
object IntermediateRepresentation {
  def typeRef(manifest: Manifest[_]): TypeReference = {
    val arguments = manifest.typeArguments
    val base = TypeReference.typeReference(manifest.runtimeClass)
    if (arguments.nonEmpty) {
      TypeReference.parameterizedType(base, arguments.map(typeRef): _*)
    } else {
      base
    }
  }

  def field[TYPE](name: String)(implicit typ: Manifest[TYPE]) = Field(typeRef(typ), name)

  def field[TYPE](name: String, initializer: IntermediateRepresentation)(implicit typ: Manifest[TYPE]) =
    Field(typeRef(typ), name, Some(initializer))

  def variable[TYPE](name: String, value: IntermediateRepresentation)(implicit typ: Manifest[TYPE]) =
    LocalVariable(typeRef(typ), name, value)

  def method[OWNER, OUT](name: String)(implicit owner: Manifest[OWNER], out: Manifest[OUT]) =
    Method(typeRef(owner), typeRef(out), name)

  def method[OWNER, OUT, IN](name: String)(implicit owner: Manifest[OWNER], out: Manifest[OUT], in: Manifest[IN]) =
    Method(typeRef(owner), typeRef(out), name, typeRef(in))

  def method[OWNER, OUT, IN1, IN2](name: String)
                                  (implicit owner: Manifest[OWNER], out: Manifest[OUT], in1: Manifest[IN1],
                                   in2: Manifest[IN2]) =
    Method(typeRef(owner), typeRef(out), name, typeRef(in1), typeRef(in2))

  def method[OWNER, OUT, IN1, IN2, IN3](name: String)
                                       (implicit owner: Manifest[OWNER], out: Manifest[OUT], in1: Manifest[IN1],
                                        in2: Manifest[IN2], in3: Manifest[IN3]) =
    Method(typeRef(owner), typeRef(out), name, typeRef(in1), typeRef(in2), typeRef(in3))

  def constructor[OWNER](implicit owner: Manifest[OWNER]) = Constructor(typeRef(owner), Seq.empty)

  def constructor[OWNER, IN](implicit owner: Manifest[OWNER],  in: Manifest[IN]) =
    Constructor(typeRef(owner), Seq(typeRef(in)))

  def invokeStatic(method: Method, params: IntermediateRepresentation*): IntermediateRepresentation = InvokeStatic(
    method, params)

  def invoke(owner: IntermediateRepresentation, method: Method,
             params: IntermediateRepresentation*): IntermediateRepresentation =
    Invoke(owner, method, params)

  def invokeSideEffect(owner: IntermediateRepresentation, method: Method,
                       params: IntermediateRepresentation*): IntermediateRepresentation =
    InvokeSideEffect(owner, method, params)

  def load(variable: String): IntermediateRepresentation = Load(variable)

  def cast[TO](expression: IntermediateRepresentation)(implicit to: Manifest[TO]) = Cast(typeRef(to), expression)

  def instanceOf[T](expression: IntermediateRepresentation)(implicit t: Manifest[T]) = InstanceOf(typeRef(t), expression)

  def loadField(field: Field): IntermediateRepresentation = LoadField(field)

  def setField(field: Field, value: IntermediateRepresentation): IntermediateRepresentation = SetField(field, value)

  def getStatic[OWNER, OUT](name: String)(implicit owner: Manifest[OWNER], out: Manifest[OUT]) =
    GetStatic(typeRef(owner), typeRef(out), name)

  def noValue: IntermediateRepresentation = getStatic[Values, Value]("NO_VALUE")

  def truthValue: IntermediateRepresentation = getStatic[Values, BooleanValue]("TRUE")

  def falseValue: IntermediateRepresentation =  getStatic[Values, BooleanValue]("FALSE")

  def constant(value: Any): IntermediateRepresentation = Constant(value)

  def arrayOf(values: IntermediateRepresentation*): IntermediateRepresentation = ArrayLiteral(values.toArray)

  def ternary(condition: IntermediateRepresentation,
              onTrue: IntermediateRepresentation,
              onFalse: IntermediateRepresentation): IntermediateRepresentation = Ternary(condition, onTrue, onFalse)

  def add(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Add(lhs, rhs)

  def subtract(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Subtract(lhs, rhs)

  def lessThan(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Lt(lhs, rhs)

  def greaterThan(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Gt(lhs, rhs)

  def equal(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Eq(lhs, rhs)

  def notEqual(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    NotEq(lhs, rhs)

  def block(ops: IntermediateRepresentation*): IntermediateRepresentation = Block(ops)

  def condition(test: IntermediateRepresentation)
               (onTrue: IntermediateRepresentation): IntermediateRepresentation = Condition(test, onTrue)

  def loop(test: IntermediateRepresentation)
               (body: IntermediateRepresentation): IntermediateRepresentation = Loop(test, body)

  def declare[TYPE](name: String)(implicit typ: Manifest[TYPE]) = DeclareLocalVariable(typeRef(typ), name)

  def declare(typeReference: TypeReference, name: String) = DeclareLocalVariable(typeReference, name)

  def assign(name: String, value: IntermediateRepresentation) = AssignToLocalVariable(name, value)

  def tryCatch[E](name: String)(ops: IntermediateRepresentation)(onError: IntermediateRepresentation)
                 (implicit typ: Manifest[E]) =
    TryCatch(ops, onError, typeRef(typ), name)

  def fail(error: IntermediateRepresentation) = Throw(error)

  def and(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) = BooleanAnd(lhs, rhs)

  def or(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) = BooleanOr(lhs, rhs)

  def isNull(test: IntermediateRepresentation): IntermediateRepresentation = IsNull(test)

  def newInstance(constructor: Constructor, params: IntermediateRepresentation*) = NewInstance(constructor, params)

  def not(test: IntermediateRepresentation) = Not(test)

  def oneTime(expression: IntermediateRepresentation) = OneTime(expression)(used = false)
}
