/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.codegen.api

import java.io.PrintStream

import org.neo4j.codegen
import org.neo4j.codegen.TypeReference
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
case class ArrayLiteral(typ: codegen.TypeReference, values: Array[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * Load a value from an array
  *
  * @param array array to load from
  * @param offset offset to load from
  */
case class ArrayLoad(array: IntermediateRepresentation, offset: Int) extends IntermediateRepresentation

/**
  * Set a value in an array at the given offset
  *
  * @param array array to set value in
  * @param offset offset to set at
  * @param value value to set
  */
case class ArraySet(array: IntermediateRepresentation, offset: IntermediateRepresentation, value: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Returns the lenght of an array
  * @param array the length of the array
  */
case class ArrayLength(array: IntermediateRepresentation) extends IntermediateRepresentation

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
  * Defines {{{lhs * rhs}}}
  *
  * @param lhs the left-hand side to multiply
  * @param rhs the right-hand side to multiply
  */
case class Multiply(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs < rhs}}}
  *
  * @param lhs the left-hand side to compare
  * @param rhs the right-hand side to compare
  */
case class Lt(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs <= rhs}}}
  *
  * @param lhs the left-hand side to compare
  * @param rhs the right-hand side to compare
  */
case class Lte(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs > rhs}}}
  *
  * @param lhs the left-hand side to compare
  * @param rhs the right-hand side to compare
  */
case class Gt(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines {{{lhs >= rhs}}}
  *
  * @param lhs the left-hand side to compare
  * @param rhs the right-hand side to compare
  */
case class Gte(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation

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
  * Checks if expression is null
  */
case class IsNull(test: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * A block is a sequence of operations where the block evaluates to the last expression
  * @param ops the operations to perform in the block
  */
case class Block(ops: Seq[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * Noop does absolutely nothing.
  */
case object Noop extends IntermediateRepresentation

/**
  * A conditon executes the operation if the test evaluates to true.
  *
  *  {{{
  *  if (test)
  *  {
  *    onTrue;
  *  }
  *  else
  *  {
  *    onFalse
  *  }
  *  }}}
  * @param test the condition to check
  * @param onTrue the operation to perform if the `test` evaluates to true
  * @param onFalse optional, the operation to perform on false
  */
case class Condition(test: IntermediateRepresentation, onTrue: IntermediateRepresentation,
                     onFalse: Option[IntermediateRepresentation] = None)
  extends IntermediateRepresentation

/**
  * A loop runs body while the provided test is true
  * @param test the body will run while this evaluates to true
  * @param body the body to run on each iteration
  */
case class Loop(test: IntermediateRepresentation, body: IntermediateRepresentation, labelName: String)
  extends IntermediateRepresentation

/**
  * Break out of a labeled loop.
  *
  * {{{
  * outerLoop:
  * while (outerTest) {
  *   while (innerTest) {
  *     if (done) {
  *       break outerLoop;
  *     }
  *   }
  * }
  * }}}
  * @param labelName The label name of the loop to break out of
  */
case class Break(labelName: String) extends IntermediateRepresentation

/**
  * Declare a local variable of the given type.
  *
  * {{{
  * typ name;
  * }}}
  * @param typ the type of the variable
  * @param name the name of the variable
  */
case class DeclareLocalVariable(typ: codegen.TypeReference, name: String) extends IntermediateRepresentation

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
case class TryCatch(ops: IntermediateRepresentation, onError: IntermediateRepresentation, exception: codegen.TypeReference, name: String) extends IntermediateRepresentation

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
  * @param owner Either the owning class or None if it is a local static field
  * @param output The type of the static field
  * @param name The name of the static field
  */
case class GetStatic(owner: Option[codegen.TypeReference], output: codegen.TypeReference, name: String) extends IntermediateRepresentation

/**
  * Instantiate a new object
  * @param constructor the constructor to call
  * @param params the parameter to the constructor
  */
case class NewInstance(constructor: Constructor, params: Seq[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * Instantiate a new instance of an inner class
  *
  * @param clazz     the inner-class to instantiate
  * @param arguments the arguments to the constructor
  */
case class NewInstanceInnerClass(clazz: ExtendClass, arguments: Seq[IntermediateRepresentation]) extends IntermediateRepresentation

/**
  * Instantiate a new array
  * @param baseType the type of the array elements
  * @param size the size of the array.
  */
case class NewArray(baseType: codegen.TypeReference, size: Int) extends IntermediateRepresentation

case class Returns(representation: IntermediateRepresentation) extends IntermediateRepresentation

case class OneTime(inner: IntermediateRepresentation)(private var used: Boolean) extends IntermediateRepresentation {
  def isUsed: Boolean = used
  def use(): Unit = {
    used = true
  }
}

/**
  * Box a primitive value
  * @param expression the value to box
  */
case class Box(expression: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Unbox a value to a primitive
  * @param expression the value to unbox
  */
case class Unbox(expression: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Defines a constructor
  * @param owner the owner of the constructor, or the object to be instantiated
  * @param params the parameter to the constructor
  */
case class Constructor(owner: codegen.TypeReference, params: Seq[codegen.TypeReference]) {
  def asReference: codegen.MethodReference =
    if (params.isEmpty) codegen.MethodReference.constructorReference(owner)
    else codegen.MethodReference.constructorReference(owner, params:_*)
}

/**
  * Cast the given expression to the given type
  * @param to the type to cast to
  * @param expression the expression to cast
  */
case class Cast(to: codegen.TypeReference, expression: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Instance of check if the given expression has the given type
  * @param typ does expression have this type
  * @param expression the expression to check
  */
case class InstanceOf(typ: codegen.TypeReference, expression: IntermediateRepresentation) extends IntermediateRepresentation

/**
  * Returns `this`
  */
case object Self extends IntermediateRepresentation

/**
  * A class that extends another class.
  *
  * The extending class is assumed to share the same constructor signature as the class it extends.
  *
  * @param name the name of the new class
  * @param overrides the class it extends
  * @param parameters the parameters to the contructor of the class and super class
  * @param methods the methods of the class
  * @param fields the fields of the class.
  */
case class ExtendClass(name: String,
                       overrides: TypeReference,
                       parameters: Seq[Parameter],
                       methods: Seq[MethodDeclaration],
                       fields: Seq[Field])
/**
  * Defines a method
  *
  * @param owner  the owner of the method
  * @param returnType output type to the method
  * @param name   the name of the method
  * @param params the parameter types of the method
  */
case class Method(owner: codegen.TypeReference, returnType: codegen.TypeReference, name: String, params: codegen.TypeReference*) {

  def asReference: codegen.MethodReference = codegen.MethodReference.methodReference(owner, returnType, name, params: _*)
}

case class Parameter(typ: codegen.TypeReference, name: String) {
  def asCodeGen: codegen.Parameter = codegen.Parameter.param(typ, name)
}

case class ClassDeclaration[T](packageName: String,
                            className: String,
                            extendsClass: Option[codegen.TypeReference],
                            implementsInterfaces: Seq[codegen.TypeReference],
                            constructorParameters: Seq[Parameter],
                            initializationCode: IntermediateRepresentation,
                            genFields: () => Seq[Field],
                            methods: Seq[MethodDeclaration]) {

  def fields: Seq[Field] = genFields()
}

case class MethodDeclaration(methodName: String,
                             returnType: codegen.TypeReference,
                             parameters: Seq[Parameter],
                             body: IntermediateRepresentation,
                             genLocalVariables: () => Seq[LocalVariable] = () => Seq.empty,
                             parameterizedWith: Option[(String, codegen.TypeReference.Bound)] = None,
                             throws: Option[TypeReference] = None) {
  def localVariables: Seq[LocalVariable] = genLocalVariables()
}

case class ConstructorDeclaration(constructor: Constructor,
                                  body: IntermediateRepresentation)
sealed trait Field {
  def typ: codegen.TypeReference
  def name: String
}

case class InstanceField(typ: codegen.TypeReference, name: String, initializer: Option[IntermediateRepresentation] = None) extends Field
case class StaticField(typ: codegen.TypeReference, name: String, value: Option[Any] = None) extends Field

case class LocalVariable(typ: codegen.TypeReference, name: String, value: IntermediateRepresentation)

/**
  * Defines a simple dsl to facilitate constructing intermediate representation
  */
object IntermediateRepresentation {
  def typeRef(manifest: Manifest[_]): codegen.TypeReference = {
    val arguments = manifest.typeArguments
    val base = codegen.TypeReference.typeReference(manifest.runtimeClass)
    if (arguments.nonEmpty && !manifest.runtimeClass.isArray) {
      codegen.TypeReference.parameterizedType(base, arguments.map(typeRef): _*)
    } else {
      base
    }
  }

  def typeRefOf[TYPE](implicit typ: Manifest[TYPE]): codegen.TypeReference = typeRef(typ)

  def field[TYPE](name: String)(implicit typ: Manifest[TYPE]) = InstanceField(typeRef(typ), name)

  def field[TYPE](name: String, initializer: IntermediateRepresentation)(implicit typ: Manifest[TYPE]) =
    InstanceField(typeRef(typ), name, Some(initializer))

  def staticConstant[TYPE](name: String, value: AnyRef)(implicit typ: Manifest[TYPE]) =
    StaticField(typeRef(typ), name, Some(value))

  def variable[TYPE](name: String, value: IntermediateRepresentation)(implicit typ: Manifest[TYPE]): LocalVariable =
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

  def method[OWNER, OUT, IN1, IN2, IN3, IN4](name: String)
                                       (implicit owner: Manifest[OWNER], out: Manifest[OUT], in1: Manifest[IN1],
                                        in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4]) =
    Method(typeRef(owner), typeRef(out), name, typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4))

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5](name: String)
                                            (implicit owner: Manifest[OWNER], out: Manifest[OUT], in1: Manifest[IN1],
                                             in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5]) =
    Method(typeRef(owner), typeRef(out), name, typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5))


  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6](name: String)
                                                 (implicit owner: Manifest[OWNER], out: Manifest[OUT], in1: Manifest[IN1],
                                                  in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5],
                                                  in6: Manifest[IN6]) =
    Method(typeRef(owner), typeRef(out), name, typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6))

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7](name: String)
                                                      (implicit owner: Manifest[OWNER], out: Manifest[OUT], in1: Manifest[IN1],
                                                       in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5],
                                                       in6: Manifest[IN6], in7: Manifest[IN7]) =
    Method(typeRef(owner), typeRef(out), name, typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7))

  def methodDeclaration[OUT](name: String,
                             body: IntermediateRepresentation,
                             locals: () => Seq[LocalVariable],
                             parameters: Parameter*)(implicit out: Manifest[OUT]) =
    MethodDeclaration(name, typeRef(out), parameters, body, locals)

  def param[TYPE](name: String)(implicit typ: Manifest[TYPE]): Parameter = Parameter(typeRef(typ), name)

  def param(name: String, typeReference: TypeReference): Parameter = Parameter(typeReference, name)

  def parameterizedType(base: TypeReference, typ: TypeReference):TypeReference =
    TypeReference.parameterizedType(base, typ)

  def typeParam(name: String): TypeReference = TypeReference.typeParameter(name)

  def extending[TYPE] (implicit typ: Manifest[TYPE]): TypeReference.Bound = TypeReference.extending(typeRef(typ))

  def constructor[OWNER](implicit owner: Manifest[OWNER]) = Constructor(typeRef(owner), Seq.empty)

  def constructor[OWNER, IN](implicit owner: Manifest[OWNER],  in: Manifest[IN]) =
    Constructor(typeRef(owner), Seq(typeRef(in)))

  def constructor[OWNER, IN1, IN2](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2)))

  def constructor[OWNER, IN1, IN2, IN3](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3)))

  def constructor[OWNER, IN1, IN2, IN3, IN4](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5], in6: Manifest[IN6]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5), typeRef(in6)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5], in6: Manifest[IN6],  in7: Manifest[IN7]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5], in6: Manifest[IN6],  in7: Manifest[IN7],  in8: Manifest[IN8]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7), typeRef(in8)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5], in6: Manifest[IN6],  in7: Manifest[IN7],  in8: Manifest[IN8],  in9: Manifest[IN9]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7), typeRef(in8), typeRef(in9)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5], in6: Manifest[IN6],  in7: Manifest[IN7],  in8: Manifest[IN8],  in9: Manifest[IN9],  in10: Manifest[IN10]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7), typeRef(in8), typeRef(in9), typeRef(in10)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5], in6: Manifest[IN6],  in7: Manifest[IN7],  in8: Manifest[IN8],  in9: Manifest[IN9],  in10: Manifest[IN10],  in11: Manifest[IN11]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7), typeRef(in8), typeRef(in9), typeRef(in10), typeRef(in11)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11, IN12](implicit owner: Manifest[OWNER],  in1: Manifest[IN1], in2: Manifest[IN2], in3: Manifest[IN3], in4: Manifest[IN4], in5: Manifest[IN5], in6: Manifest[IN6],  in7: Manifest[IN7],  in8: Manifest[IN8],  in9: Manifest[IN9],  in10: Manifest[IN10],  in11: Manifest[IN11],  in12: Manifest[IN12]) =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2),  typeRef(in3),  typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7), typeRef(in8), typeRef(in9), typeRef(in10), typeRef(in11), typeRef(in12)))

  def invokeStatic(method: Method, params: IntermediateRepresentation*): IntermediateRepresentation = InvokeStatic(
    method, params)

  def invoke(owner: IntermediateRepresentation, method: Method,
             params: IntermediateRepresentation*): IntermediateRepresentation =
    if (method.returnType == org.neo4j.codegen.TypeReference.VOID)
      InvokeSideEffect(owner, method, params)
    else
      Invoke(owner, method, params)

  def invokeSideEffect(owner: IntermediateRepresentation, method: Method,
                       params: IntermediateRepresentation*): IntermediateRepresentation =
    InvokeSideEffect(owner, method, params)

  def load(variable: String): IntermediateRepresentation = Load(variable)

  def load(variable: LocalVariable): IntermediateRepresentation = Load(variable.name)

  def cast[TO](expression: IntermediateRepresentation)(implicit to: Manifest[TO]) = Cast(typeRef(to), expression)

  def instanceOf[T](expression: IntermediateRepresentation)(implicit t: Manifest[T]) = InstanceOf(typeRef(t), expression)

  def loadField(field: Field): IntermediateRepresentation = LoadField(field)

  def setField(field: Field, value: IntermediateRepresentation): IntermediateRepresentation = SetField(field, value)

  def getStatic[OUT](name: String)(implicit out: Manifest[OUT]) = GetStatic(None, typeRef(out), name)

  def getStatic[OWNER, OUT](name: String)(implicit owner: Manifest[OWNER], out: Manifest[OUT]) =
    GetStatic(Some(typeRef(owner)), typeRef(out), name)

  def noValue: IntermediateRepresentation = getStatic[Values, Value]("NO_VALUE")

  def trueValue: IntermediateRepresentation = getStatic[Values, BooleanValue]("TRUE")

  def falseValue: IntermediateRepresentation = getStatic[Values, BooleanValue]("FALSE")

  def isNaN(value: IntermediateRepresentation): IntermediateRepresentation = and(instanceOf[FloatingPointValue](value), invoke(cast[FloatingPointValue](value), method[FloatingPointValue, Boolean]("isNaN")))

  def constant(value: Any): IntermediateRepresentation = Constant(value)

  def arrayOf[T](values: IntermediateRepresentation*)(implicit t: Manifest[T]): IntermediateRepresentation =
    ArrayLiteral(typeRef(t), values.toArray)

  def arrayLoad(array: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    ArrayLoad(array, offset)

  def arraySet(array: IntermediateRepresentation, offset: IntermediateRepresentation, value: IntermediateRepresentation): IntermediateRepresentation =
    ArraySet(array, offset, value)

  def arraySet(array: IntermediateRepresentation, offset: Int, value: IntermediateRepresentation): IntermediateRepresentation =
    ArraySet(array, constant(offset), value)

  def arrayLength(array: IntermediateRepresentation): IntermediateRepresentation = ArrayLength(array)

  def ternary(condition: IntermediateRepresentation,
              onTrue: IntermediateRepresentation,
              onFalse: IntermediateRepresentation): IntermediateRepresentation = Ternary(condition, onTrue, onFalse)

  def add(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Add(lhs, rhs)

  def subtract(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Subtract(lhs, rhs)

  def multiply(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Multiply(lhs, rhs)

  def lessThan(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Lt(lhs, rhs)

  def lessThanOrEqual(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Lte(lhs, rhs)

  def greaterThan(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Gt(lhs, rhs)

  def greaterThanOrEqual(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Gte(lhs, rhs)

  def equal(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Eq(lhs, rhs)

  def notEqual(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    NotEq(lhs, rhs)

  def block(ops: IntermediateRepresentation*): IntermediateRepresentation = Block(ops)

  def noop(): IntermediateRepresentation = Noop

  def condition(test: IntermediateRepresentation)
               (onTrue: IntermediateRepresentation): IntermediateRepresentation = Condition(test, onTrue)

  def ifElse(test: IntermediateRepresentation)
               (onTrue: IntermediateRepresentation)
               (onFalse: IntermediateRepresentation): IntermediateRepresentation = Condition(test, onTrue, Some(onFalse))

  def loop(test: IntermediateRepresentation)
               (body: IntermediateRepresentation): IntermediateRepresentation = Loop(test, body, labelName = null)

  def labeledLoop(labelName: String, test: IntermediateRepresentation)
                 (body: IntermediateRepresentation): IntermediateRepresentation = Loop(test, body, labelName)

  def break(labelName: String): IntermediateRepresentation = Break(labelName)

  def declare[TYPE](name: String)(implicit typ: Manifest[TYPE]) = DeclareLocalVariable(typeRef(typ), name)

  def declare(typeReference: codegen.TypeReference, name: String) = DeclareLocalVariable(typeReference, name)

  def assign(name: String, value: IntermediateRepresentation) = AssignToLocalVariable(name, value)

  def assign(variable: LocalVariable, value: IntermediateRepresentation) = AssignToLocalVariable(variable.name, value)

  def declareAndAssign(typeReference: TypeReference, name: String, value: IntermediateRepresentation): IntermediateRepresentation =
    block(declare(typeReference, name), assign(name, value))

  def returns(value: IntermediateRepresentation): IntermediateRepresentation = Returns(value)

  def tryCatch[E](name: String)(ops: IntermediateRepresentation)(onError: IntermediateRepresentation)
                 (implicit typ: Manifest[E]) =
    TryCatch(ops, onError, typeRef(typ), name)

  def fail(error: IntermediateRepresentation) = Throw(error)

  def and(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) = BooleanAnd(lhs, rhs)

  def and(ands: Seq[IntermediateRepresentation]): IntermediateRepresentation = {
    if (ands.isEmpty) constant(true)
    else if (ands.size == 1) ands.head
    else ands.reduceLeft((acc, current) => and(acc, current))
  }

  def or(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation = BooleanOr(lhs, rhs)

  def or(ors: Seq[IntermediateRepresentation]): IntermediateRepresentation = {
    if (ors.isEmpty) constant(true)
    else ors.reduceLeft((acc, current) => or(acc, current))
  }

  def isNull(test: IntermediateRepresentation): IntermediateRepresentation = IsNull(test)

  def isNotNull(test: IntermediateRepresentation): IntermediateRepresentation = Not(IsNull(test))

  def newInstance(constructor: Constructor, params: IntermediateRepresentation*) = NewInstance(constructor, params)

  def newInstance(inner: ExtendClass, params: IntermediateRepresentation*) = NewInstanceInnerClass(inner, params)

  def newArray(baseType: codegen.TypeReference, size: Int) = NewArray(baseType, size)

  def not(test: IntermediateRepresentation) = Not(test)

  def oneTime(expression: IntermediateRepresentation): IntermediateRepresentation = OneTime(expression)(used = false)

  def print(value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(getStatic[System, PrintStream]("out"), method[PrintStream, Unit, Object]("println"), value )

  def box(expression: IntermediateRepresentation) = Box(expression)

  def unbox(expression: IntermediateRepresentation) = Unbox(expression)

  def self(): IntermediateRepresentation = Self
}
