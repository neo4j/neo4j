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

import org.neo4j.codegen
import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.api.CodeOptimization.BooleanValueFcn
import org.neo4j.codegen.api.CodeOptimization.InFcn
import org.neo4j.codegen.api.CodeOptimization.LongValueFcn
import org.neo4j.codegen.api.CodeOptimization.NotFcn
import org.neo4j.codegen.api.CodeOptimization.simplifyPredicates
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.LongValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

import java.io.ObjectInputStream.GetField
import java.io.PrintStream
import java.lang.reflect.Modifier

/**
 * IntermediateRepresentation is an intermediate step between pure byte code and the operator/expression
 *
 * The representation is intended to be quite low level and fairly close to the actual bytecode representation.
 */
sealed trait IntermediateRepresentation extends Foldable {
  def typeReference: TypeReference
}

/**
 * Invoke a static method
 *
 * @param method the method to invoke
 * @param params the parameter to the static method
 */
case class InvokeStatic(method: Method, params: Seq[IntermediateRepresentation]) extends IntermediateRepresentation {
  override def typeReference: TypeReference = method.returnType
}

/**
 * Invoke a static method with side effects
 *
 * @param method the method to invoke
 * @param params the parameter to the static method
 */
case class InvokeStaticSideEffect(method: Method, params: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = method.returnType
}

/**
 * Invoke a method
 *
 * @param target the target to call the method on
 * @param method the method to invoke
 * @param params the parameter to the method
 */
case class Invoke(target: IntermediateRepresentation, method: Method, params: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = method.returnType
}

/**
 * Invoke a local instance method
 *
 * @param method the method to invoke
 * @param params the parameter to the method
 */
case class InvokeLocal(method: PrivateMethod, params: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = method.returnType
}

/**
 * Invoke a void method or a non-void method but ignoring the output
 *
 * @param target the target to call the method on
 * @param method the method to invoke
 * @param params the parameter to the method
 */
case class InvokeSideEffect(target: IntermediateRepresentation, method: Method, params: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * Invoke a local instance void method or a local non-void instance method but ignoring the output
 *
 * @param method the method to invoke
 * @param params the parameter to the method
 */
case class InvokeLocalSideEffect(method: PrivateMethod, params: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * Load a local variable by name
 *
 * @param variable the name of the variable
 */
case class Load(variable: String, typeReference: TypeReference) extends IntermediateRepresentation

/**
 * Load a field
 *
 * @param field the field to load
 */
case class LoadField(field: Field) extends IntermediateRepresentation {
  override def typeReference: TypeReference = field.typ
}

/**
 * Set a field to a value
 *
 * @param field the field to set
 * @param value the value to set
 */
case class SetField(field: Field, value: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * Constant java value
 *
 * @param value the constant value
 */
case class Constant(value: Any) extends IntermediateRepresentation {

  override def typeReference: TypeReference = {
    val typ = TypeReference.typeReference(value.getClass)
    if (typ.simpleName() == "String") typ else TypeReference.toUnboxedType(typ)
  }
}

/**
 * Loads an array literal of the given inputs
 *
 * @param values the values of the array
 */
case class ArrayLiteral(typ: codegen.TypeReference, values: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.arrayOf(typ)
}

/**
 * Load a value from an array
 *
 * @param array array to load from
 * @param offset offset to load from
 */
case class ArrayLoad(array: IntermediateRepresentation, offset: IntermediateRepresentation)
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = array.typeReference.elementOfArray()
}

/**
 * Set a value in an array at the given offset
 *
 * @param array array to set value in
 * @param offset offset to set at
 * @param value value to set
 */
case class ArraySet(
  array: IntermediateRepresentation,
  offset: IntermediateRepresentation,
  value: IntermediateRepresentation
) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * Returns the lenght of an array
 * @param array the length of the array
 */
case class ArrayLength(array: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.INT
}

/**
 * Defines ternary expression, i.e. {{{condition ? onTrue : onFalse}}}
 *
 * @param condition the condition to test
 * @param onTrue    will be evaluted if condition is true
 * @param onFalse   will be evaluated if condition is false
 */
case class Ternary(
  condition: IntermediateRepresentation,
  onTrue: IntermediateRepresentation,
  onFalse: IntermediateRepresentation
) extends IntermediateRepresentation {

  override def typeReference: TypeReference = {
    if (onTrue.typeReference == onFalse.typeReference) onTrue.typeReference
    else TypeReference.OBJECT
  }
}

/**
 * Defines {{{lhs + rhs}}}
 *
 * @param lhs the left-hand side to add
 * @param rhs the right-hand side to add
 */
case class Add(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation {

  override def typeReference: TypeReference = {
    require(lhs.typeReference == rhs.typeReference)
    lhs.typeReference
  }
}

/**
 * Defines {{{lhs - rhs}}}
 *
 * @param lhs the left-hand side to subtract from
 * @param rhs the right-hand side to subtract
 */
case class Subtract(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation)
    extends IntermediateRepresentation {

  override def typeReference: TypeReference = {
    require(lhs.typeReference == rhs.typeReference)
    lhs.typeReference
  }
}

/**
 * Defines {{{lhs * rhs}}}
 *
 * @param lhs the left-hand side to multiply
 * @param rhs the right-hand side to multiply
 */
case class Multiply(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation)
    extends IntermediateRepresentation {

  override def typeReference: TypeReference = {
    require(lhs.typeReference == rhs.typeReference)
    lhs.typeReference
  }
}

/**
 * Defines {{{lhs < rhs}}}
 *
 * @param lhs the left-hand side to compare
 * @param rhs the right-hand side to compare
 */
case class Lt(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation {
  require(lhs.typeReference == rhs.typeReference)
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Defines {{{lhs <= rhs}}}
 *
 * @param lhs the left-hand side to compare
 * @param rhs the right-hand side to compare
 */
case class Lte(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation {
  require(lhs.typeReference == rhs.typeReference)
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Defines {{{lhs > rhs}}}
 *
 * @param lhs the left-hand side to compare
 * @param rhs the right-hand side to compare
 */
case class Gt(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation {
  require(lhs.typeReference == rhs.typeReference)
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Defines {{{lhs >= rhs}}}
 *
 * @param lhs the left-hand side to compare
 * @param rhs the right-hand side to compare
 */
case class Gte(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation {
  require(lhs.typeReference == rhs.typeReference)
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Defines equality or identy, i.e. {{{lhs == rhs}}}
 *
 * @param lhs the left-hand side to check
 * @param rhs the right-hand side to check
 */
case class Eq(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Defines  {{{lhs != rhs}}}
 *
 * @param lhs the left-hand side to check
 * @param rhs the right-hand side to check
 */
case class NotEq(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Defines  !test
 *
 * @param test the expression to check
 */
case class Not(test: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.BOOLEAN

}

/**
 * Checks if expression is null
 */
case class IsNull(test: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.BOOLEAN

}

/**
 * A block is a sequence of operations where the block evaluates to the last expression
 * @param ops the operations to perform in the block
 */
case class Block(ops: collection.Seq[IntermediateRepresentation]) extends IntermediateRepresentation {
  override def typeReference: TypeReference = ops.last.typeReference
}

/**
 * Noop does absolutely nothing.
 */
case object Noop extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

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
case class Condition(
  test: IntermediateRepresentation,
  onTrue: IntermediateRepresentation,
  onFalse: Option[IntermediateRepresentation] = None
) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * A loop runs body while the provided test is true
 * @param test the body will run while this evaluates to true
 * @param body the body to run on each iteration
 */
case class Loop(test: IntermediateRepresentation, body: IntermediateRepresentation, labelName: String)
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

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
case class Break(labelName: String) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * Declare a local variable of the given type.
 *
 * {{{
 * typ name;
 * }}}
 * @param typ the type of the variable
 * @param name the name of the variable
 */
case class DeclareLocalVariable(typ: codegen.TypeReference, name: String) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * Assign a variable to a value.
 *
 * {{{
 * name = value;
 * }}}
 * @param name the name of the variable
 * @param value the value to assign to the variable
 */
case class AssignToLocalVariable(name: String, value: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

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
case class TryCatch(
  ops: IntermediateRepresentation,
  onError: IntermediateRepresentation,
  exception: codegen.TypeReference,
  name: String
) extends IntermediateRepresentation {
  override def typeReference: TypeReference = ops.typeReference
}

/**
 * Throw an error
 * @param error the error to throw
 */
case class Throw(error: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

/**
 * Boolean && operator
 * {{{
 *   a1 && a2 && ...;
 * }}}
 * @param as the arguments to and
 */
case class BooleanAnd(as: Seq[IntermediateRepresentation]) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Boolean || operator
 * {{{
 *   a1 || a2 || ...;
 * }}}
 * @param as the arguments to add
 */
case class BooleanOr(as: Seq[IntermediateRepresentation]) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Loads a static field
 * @param owner Either the owning class or None if it is a local static field
 * @param output The type of the static field
 * @param name The name of the static field
 */
case class GetStatic(owner: Option[codegen.TypeReference], output: codegen.TypeReference, name: String)
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = output
}

/**
 * Instantiate a new object
 * @param constructor the constructor to call
 * @param params the parameter to the constructor
 */
case class NewInstance(constructor: Constructor, params: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = constructor.owner
}

/**
 * Instantiate a new instance of an inner class
 *
 * @param clazz     the inner-class to instantiate
 * @param arguments the arguments to the constructor
 */
case class NewInstanceInnerClass(clazz: ExtendClass, arguments: Seq[IntermediateRepresentation])
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = clazz.overrides
}

/**
 * Instantiate a new array
 * @param baseType the type of the array elements
 * @param size the size of the array.
 */
case class NewArray(baseType: codegen.TypeReference, size: Int) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.arrayOf(baseType)
}

case class NewArrayDynamicSize(baseType: codegen.TypeReference, size: IntermediateRepresentation)
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.arrayOf(baseType)
}

case class Returns(representation: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.VOID
}

case class OneTime(inner: IntermediateRepresentation)(private var used: Boolean) extends IntermediateRepresentation {
  def isUsed: Boolean = used

  def use(): Unit = {
    used = true
  }

  override def typeReference: TypeReference = inner.typeReference
}

/**
 * Box a primitive value
 * @param expression the value to box
 */
case class Box(expression: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.toBoxedType(expression.typeReference)
}

/**
 * Unbox a value to a primitive
 * @param expression the value to unbox
 */
case class Unbox(expression: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.toUnboxedType(expression.typeReference)
}

/**
 * Defines a constructor
 * @param owner the owner of the constructor, or the object to be instantiated
 * @param params the parameter to the constructor
 */
case class Constructor(owner: codegen.TypeReference, params: Seq[codegen.TypeReference]) {

  def asReference: codegen.MethodReference =
    if (params.isEmpty) codegen.MethodReference.constructorReference(owner)
    else codegen.MethodReference.constructorReference(owner, params: _*)
}

/**
 * Cast the given expression to the given type
 * @param to the type to cast to
 * @param expression the expression to cast
 */
case class Cast(to: codegen.TypeReference, expression: IntermediateRepresentation) extends IntermediateRepresentation {
  override def typeReference: TypeReference = to
}

/**
 * Instance of check if the given expression has the given type
 * @param typ does expression have this type
 * @param expression the expression to check
 */
case class InstanceOf(typ: codegen.TypeReference, expression: IntermediateRepresentation)
    extends IntermediateRepresentation {
  override def typeReference: TypeReference = TypeReference.BOOLEAN
}

/**
 * Returns `this`
 */
case class Self(typeReference: TypeReference) extends IntermediateRepresentation {}

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
case class ExtendClass(
  name: String,
  overrides: TypeReference,
  parameters: Seq[Parameter],
  methods: Seq[MethodDeclaration],
  fields: collection.Seq[Field]
)

/**
 * Defines a method
 *
 * @param owner  the owner of the method
 * @param returnType output type to the method
 * @param name   the name of the method
 * @param params the parameter types of the method
 */
case class Method(
  owner: codegen.TypeReference,
  returnType: codegen.TypeReference,
  name: String,
  params: Seq[codegen.TypeReference]
) {

  def asReference: codegen.MethodReference =
    codegen.MethodReference.methodReference(owner, returnType, name, params: _*)
}

/**
 * Defines a private method that is local to the current class
 * @param returnType output type to the method
 * @param name   the name of the method
 * @param params the parameter types of the method
 */
case class PrivateMethod(returnType: codegen.TypeReference, name: String, params: Seq[codegen.TypeReference])

case class Parameter(typ: codegen.TypeReference, name: String) {
  def asCodeGen: codegen.Parameter = codegen.Parameter.param(typ, name)
}

case class ClassDeclaration[T](
  packageName: String,
  className: String,
  extendsClass: Option[codegen.TypeReference],
  implementsInterfaces: collection.Seq[codegen.TypeReference],
  constructorParameters: collection.Seq[Parameter],
  initializationCode: IntermediateRepresentation,
  genFields: () => collection.Seq[Field],
  methods: Seq[MethodDeclaration]
) {

  def fields: collection.Seq[Field] = genFields()
}

case class MethodDeclaration(
  methodName: String,
  returnType: codegen.TypeReference,
  parameters: Seq[Parameter],
  body: IntermediateRepresentation,
  genLocalVariables: () => Seq[LocalVariable] = () => Seq.empty,
  parameterizedWith: Option[(String, codegen.TypeReference.Bound)] = None,
  throws: Option[TypeReference] = None,
  modifiers: Int = Modifier.PUBLIC
) {
  def localVariables: Seq[LocalVariable] = genLocalVariables()
}

case class ConstructorDeclaration(constructor: Constructor, body: IntermediateRepresentation)

sealed trait Field {
  def typ: codegen.TypeReference
  def name: String
}

case class InstanceField(
  typ: codegen.TypeReference,
  name: String
)(val initializer: Option[() => IntermediateRepresentation] = None) extends Field

case class StaticField(typ: codegen.TypeReference, name: String, value: Option[Any] = None) extends Field

case class LocalVariable(typ: codegen.TypeReference, name: String, value: IntermediateRepresentation)

/**
 * Defines a simple dsl to facilitate constructing intermediate representation
 */
object IntermediateRepresentation {

  def typeRef(manifest: Manifest[_]): codegen.TypeReference = {
    // this is a sign that we forgot to provide the type in e.g. load[C]("foo")
    require(manifest.runtimeClass != classOf[Nothing], "missing type information")
    val arguments = manifest.typeArguments
    val base = codegen.TypeReference.typeReference(manifest.runtimeClass)
    if (arguments.nonEmpty && !manifest.runtimeClass.isArray) {
      codegen.TypeReference.parameterizedType(base, arguments.map(typeRef): _*)
    } else {
      base
    }
  }

  def typeRefOf[TYPE](implicit typ: Manifest[TYPE]): codegen.TypeReference = typeRef(typ)

  def nonGenericTypeRefOf[TYPE](implicit typ: Manifest[TYPE]): codegen.TypeReference =
    codegen.TypeReference.typeReference(manifest.runtimeClass)

  def field[TYPE](name: String)(implicit typ: Manifest[TYPE]): InstanceField = InstanceField(typeRef(typ), name)(None)

  def field[TYPE](name: String, initializer: IntermediateRepresentation)(implicit typ: Manifest[TYPE]): InstanceField =
    InstanceField(typeRef(typ), name)(Some(() => initializer))

  def lazyField[TYPE](name: String, initializer: () => IntermediateRepresentation)(implicit
  typ: Manifest[TYPE]): InstanceField =
    InstanceField(typeRef(typ), name)(Some(initializer))

  def staticConstant[TYPE](name: String, value: AnyRef)(implicit typ: Manifest[TYPE]): StaticField =
    StaticField(typeRef(typ), name, Some(value))

  def variable[TYPE](name: String, value: IntermediateRepresentation)(implicit typ: Manifest[TYPE]): LocalVariable =
    LocalVariable(typeRef(typ), name, value)

  def method[OWNER, OUT](name: String)(implicit owner: Manifest[OWNER], out: Manifest[OUT]): Method =
    Method(typeRef(owner), typeRef(out), name, Seq.empty)

  def method[OWNER, OUT, IN](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in: Manifest[IN]
  ): Method =
    Method(typeRef(owner), typeRef(out), name, Seq(typeRef(in)))

  def method[OWNER, OUT, IN1, IN2](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2]
  ): Method =
    Method(typeRef(owner), typeRef(out), name, Seq(typeRef(in1), typeRef(in2)))

  def method[OWNER, OUT, IN1, IN2, IN3](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3]
  ): Method =
    Method(typeRef(owner), typeRef(out), name, Seq(typeRef(in1), typeRef(in2), typeRef(in3)))

  def method[OWNER, OUT, IN1, IN2, IN3, IN4](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4]
  ): Method =
    Method(typeRef(owner), typeRef(out), name, Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4)))

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5]
  ): Method =
    Method(
      typeRef(owner),
      typeRef(out),
      name,
      Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5))
    )

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6]
  ): Method =
    Method(
      typeRef(owner),
      typeRef(out),
      name,
      Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6))
    )

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7]
  ): Method =
    Method(
      typeRef(owner),
      typeRef(out),
      name,
      Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7))
    )

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8]
  ): Method =
    Method(
      typeRef(owner),
      typeRef(out),
      name,
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8)
      )
    )

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9]
  ): Method =
    Method(
      typeRef(owner),
      typeRef(out),
      name,
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9)
      )
    )

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9],
    in10: Manifest[IN10]
  ): Method =
    Method(
      typeRef(owner),
      typeRef(out),
      name,
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9),
        typeRef(in10)
      )
    )

  def method[OWNER, OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11](name: String)(
    implicit owner: Manifest[OWNER],
    out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9],
    in10: Manifest[IN10],
    in11: Manifest[IN11]
  ): Method =
    Method(
      typeRef(owner),
      typeRef(out),
      name,
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9),
        typeRef(in10),
        typeRef(in11)
      )
    )

  def methodDeclaration[OUT](
    name: String,
    body: IntermediateRepresentation,
    locals: () => Seq[LocalVariable],
    parameters: Parameter*
  )(implicit out: Manifest[OUT]): MethodDeclaration =
    MethodDeclaration(name, typeRef(out), parameters, body, locals)

  def privateMethod[OUT](name: String)(implicit out: Manifest[OUT]): PrivateMethod =
    PrivateMethod(typeRef(out), name, Seq.empty)

  def privateMethod[OUT, IN](name: String)(implicit out: Manifest[OUT], in: Manifest[IN]): PrivateMethod =
    PrivateMethod(typeRef(out), name, Seq(typeRef(in)))

  def privateMethod[OUT, IN1, IN2](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2]
  ): PrivateMethod =
    PrivateMethod(typeRef(out), name, Seq(typeRef(in1), typeRef(in2)))

  def privateMethod[OUT, IN1, IN2, IN3](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3]
  ): PrivateMethod =
    PrivateMethod(typeRef(out), name, Seq(typeRef(in1), typeRef(in2), typeRef(in3)))

  def privateMethod[OUT, IN1, IN2, IN3, IN4](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4]
  ): PrivateMethod =
    PrivateMethod(typeRef(out), name, Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4)))

  def privateMethod[OUT, IN1, IN2, IN3, IN4, IN5](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5]
  ): PrivateMethod =
    PrivateMethod(typeRef(out), name, Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5)))

  def privateMethod[OUT, IN1, IN2, IN3, IN4, IN5, IN6](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6]
  ): PrivateMethod =
    PrivateMethod(
      typeRef(out),
      name,
      Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6))
    )

  def privateMethod[OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7]
  ): PrivateMethod =
    PrivateMethod(
      typeRef(out),
      name,
      Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7))
    )

  def privateMethod[OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8]
  ): PrivateMethod =
    PrivateMethod(
      typeRef(out),
      name,
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8)
      )
    )

  def privateMethod[OUT, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9](name: String)(
    implicit out: Manifest[OUT],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9]
  ): PrivateMethod =
    PrivateMethod(
      typeRef(out),
      name,
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9)
      )
    )

  def param[TYPE](name: String)(implicit typ: Manifest[TYPE]): Parameter = Parameter(typeRef(typ), name)

  def param(name: String, typeReference: TypeReference): Parameter = Parameter(typeReference, name)

  def parameterizedType(base: TypeReference, typ: TypeReference): TypeReference =
    TypeReference.parameterizedType(base, typ)

  def typeParam(name: String): TypeReference = TypeReference.typeParameter(name)

  def extending[TYPE](implicit typ: Manifest[TYPE]): TypeReference.Bound = TypeReference.extending(typeRef(typ))

  def constructor[OWNER](implicit owner: Manifest[OWNER]): Constructor = Constructor(typeRef(owner), Seq.empty)

  def constructor[OWNER, IN](implicit owner: Manifest[OWNER], in: Manifest[IN]): Constructor =
    Constructor(typeRef(owner), Seq(typeRef(in)))

  def constructor[OWNER, IN1, IN2](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2]
  ): Constructor =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2)))

  def constructor[OWNER, IN1, IN2, IN3](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3]
  ): Constructor =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2), typeRef(in3)))

  def constructor[OWNER, IN1, IN2, IN3, IN4](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4]
  ): Constructor =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5]
  ): Constructor =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6]
  ): Constructor =
    Constructor(typeRef(owner), Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6)))

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7]
  ): Constructor =
    Constructor(
      typeRef(owner),
      Seq(typeRef(in1), typeRef(in2), typeRef(in3), typeRef(in4), typeRef(in5), typeRef(in6), typeRef(in7))
    )

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8]
  ): Constructor =
    Constructor(
      typeRef(owner),
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8)
      )
    )

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9]
  ): Constructor =
    Constructor(
      typeRef(owner),
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9)
      )
    )

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9],
    in10: Manifest[IN10]
  ): Constructor =
    Constructor(
      typeRef(owner),
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9),
        typeRef(in10)
      )
    )

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9],
    in10: Manifest[IN10],
    in11: Manifest[IN11]
  ): Constructor =
    Constructor(
      typeRef(owner),
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9),
        typeRef(in10),
        typeRef(in11)
      )
    )

  def constructor[OWNER, IN1, IN2, IN3, IN4, IN5, IN6, IN7, IN8, IN9, IN10, IN11, IN12](
    implicit owner: Manifest[OWNER],
    in1: Manifest[IN1],
    in2: Manifest[IN2],
    in3: Manifest[IN3],
    in4: Manifest[IN4],
    in5: Manifest[IN5],
    in6: Manifest[IN6],
    in7: Manifest[IN7],
    in8: Manifest[IN8],
    in9: Manifest[IN9],
    in10: Manifest[IN10],
    in11: Manifest[IN11],
    in12: Manifest[IN12]
  ): Constructor =
    Constructor(
      typeRef(owner),
      Seq(
        typeRef(in1),
        typeRef(in2),
        typeRef(in3),
        typeRef(in4),
        typeRef(in5),
        typeRef(in6),
        typeRef(in7),
        typeRef(in8),
        typeRef(in9),
        typeRef(in10),
        typeRef(in11),
        typeRef(in12)
      )
    )

  def invokeStatic(method: Method, params: IntermediateRepresentation*): IntermediateRepresentation =
    if (method.returnType == org.neo4j.codegen.TypeReference.VOID) InvokeStaticSideEffect(method, params)
    else InvokeStatic(method, params)

  def invokeStaticSideEffect(method: Method, params: IntermediateRepresentation*): IntermediateRepresentation =
    InvokeStaticSideEffect(method, params)

  def invoke(
    owner: IntermediateRepresentation,
    method: Method,
    params: IntermediateRepresentation*
  ): IntermediateRepresentation =
    if (method.returnType == org.neo4j.codegen.TypeReference.VOID)
      InvokeSideEffect(owner, method, params)
    else
      Invoke(owner, method, params)

  def invoke(ownerVar: String, method: Method, params: IntermediateRepresentation*): IntermediateRepresentation =
    invoke(Load(ownerVar, method.owner), method, params: _*)

  def invokeLocal(method: PrivateMethod, params: IntermediateRepresentation*): IntermediateRepresentation =
    if (method.returnType == org.neo4j.codegen.TypeReference.VOID)
      InvokeLocalSideEffect(method, params)
    else
      InvokeLocal(method, params)

  def invokeSideEffect(
    owner: IntermediateRepresentation,
    method: Method,
    params: IntermediateRepresentation*
  ): IntermediateRepresentation =
    InvokeSideEffect(owner, method, params)

  def invokeLocalSideEffect(method: PrivateMethod, params: IntermediateRepresentation*): IntermediateRepresentation =
    InvokeLocalSideEffect(method, params)

  def invokeSideEffect(
    ownerVar: String,
    method: Method,
    params: IntermediateRepresentation*
  ): IntermediateRepresentation =
    invokeSideEffect(Load(ownerVar, method.owner), method, params: _*)

  def load[TYPE](variable: String)(implicit typ: Manifest[TYPE]): Load = Load(variable, typeRef(typ))

  def load(variable: LocalVariable): Load = Load(variable.name, variable.typ)

  def load(parameter: Parameter): Load = Load(parameter.name, parameter.typ)

  def cast[TO](expression: IntermediateRepresentation)(implicit to: Manifest[TO]): IntermediateRepresentation = {
    val toType = typeRef(to)
    if (expression.typeReference == toType) {
      expression
    } else {
      Cast(toType, expression)
    }
  }

  def instanceOf[T](expression: IntermediateRepresentation)(implicit t: Manifest[T]): InstanceOf =
    InstanceOf(typeRef(t), expression)

  def loadField(field: Field): IntermediateRepresentation = LoadField(field)

  def setField(field: Field, value: IntermediateRepresentation): IntermediateRepresentation = SetField(field, value)

  def getStatic[OUT](name: String)(implicit out: Manifest[OUT]): GetStatic = GetStatic(None, typeRef(out), name)

  def getStatic[OWNER, OUT](name: String)(implicit owner: Manifest[OWNER], out: Manifest[OUT]): GetStatic =
    GetStatic(Some(typeRef(owner)), typeRef(out), name)

  def getStatic(field: StaticField): GetStatic = GetStatic(None, field.typ, field.name)

  def noValue: IntermediateRepresentation = getStatic[Values, Value]("NO_VALUE")

  def trueValue: IntermediateRepresentation = getStatic[Values, BooleanValue]("TRUE")

  def falseValue: IntermediateRepresentation = getStatic[Values, BooleanValue]("FALSE")

  def InfinityValue: IntermediateRepresentation = getStatic[Values, DoubleValue]("Infinity")

  def NaNValue: IntermediateRepresentation = getStatic[Values, DoubleValue]("NaN")

  def isNaN(value: IntermediateRepresentation): IntermediateRepresentation = and(
    instanceOf[FloatingPointValue](value),
    invoke(cast[FloatingPointValue](value), method[FloatingPointValue, Boolean]("isNaN"))
  )

  def constant(value: Any): IntermediateRepresentation = Constant(value)

  def arrayOf[T](values: IntermediateRepresentation*)(implicit t: Manifest[T]): IntermediateRepresentation =
    ArrayLiteral(typeRef(t), values)

  def arrayLoad(array: IntermediateRepresentation, offset: Int): IntermediateRepresentation =
    arrayLoad(array, constant(offset))

  def arrayLoad(array: IntermediateRepresentation, offset: IntermediateRepresentation): IntermediateRepresentation =
    ArrayLoad(array, offset)

  def arraySet(
    array: IntermediateRepresentation,
    offset: IntermediateRepresentation,
    value: IntermediateRepresentation
  ): IntermediateRepresentation =
    ArraySet(array, offset, value)

  def arraySet(
    array: IntermediateRepresentation,
    offset: Int,
    value: IntermediateRepresentation
  ): IntermediateRepresentation =
    ArraySet(array, constant(offset), value)

  def arrayLength(array: IntermediateRepresentation): IntermediateRepresentation = ArrayLength(array)

  def ternary(
    condition: IntermediateRepresentation,
    onTrue: IntermediateRepresentation,
    onFalse: IntermediateRepresentation
  ): IntermediateRepresentation = {
    simplifyPredicates(Ternary(condition, onTrue, onFalse))
  }

  def add(lhs: IntermediateRepresentation, rhs: Int): IntermediateRepresentation =
    Add(lhs, constant(rhs))

  def add(lhs: IntermediateRepresentation, rhs: Long): IntermediateRepresentation =
    Add(lhs, constant(rhs))

  def add(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Add(lhs, rhs)

  /**
   * NOTE: assumes Int
   */
  def increment(value: String): IntermediateRepresentation = assign(value, add(load[Int](value), constant(1)))

  def increment(value: InstanceField): IntermediateRepresentation = setField(value, add(loadField(value), constant(1)))

  def subtract(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Subtract(lhs, rhs)

  def multiply(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Multiply(lhs, rhs)

  def lessThan(lhsVar: String, rhs: IntermediateRepresentation): IntermediateRepresentation =
    lessThan(Load(lhsVar, rhs.typeReference), rhs)

  def lessThan(lhs: IntermediateRepresentation, rhsVar: String): IntermediateRepresentation =
    lessThan(lhs, Load(rhsVar, lhs.typeReference))

  def lessThan(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Lt(lhs, rhs)

  def lessThanOrEqual(lhsVar: String, rhs: IntermediateRepresentation): IntermediateRepresentation =
    lessThanOrEqual(Load(lhsVar, rhs.typeReference), rhs)

  def lessThanOrEqual(lhs: IntermediateRepresentation, rhsVar: String): IntermediateRepresentation =
    lessThanOrEqual(lhs, Load(rhsVar, lhs.typeReference))

  def lessThanOrEqual(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Lte(lhs, rhs)

  def greaterThan(lhsVar: String, rhs: IntermediateRepresentation): IntermediateRepresentation =
    greaterThan(Load(lhsVar, rhs.typeReference), rhs)

  def greaterThan(lhs: IntermediateRepresentation, rhsVar: String): IntermediateRepresentation =
    greaterThan(lhs, Load(rhsVar, lhs.typeReference))

  def greaterThan(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Gt(lhs, rhs)

  def greaterThanOrEqual(lhsVar: String, rhs: IntermediateRepresentation): IntermediateRepresentation =
    greaterThanOrEqual(Load(lhsVar, rhs.typeReference), rhs)

  def greaterThanOrEqual(lhs: IntermediateRepresentation, rhsVar: String): IntermediateRepresentation =
    greaterThanOrEqual(lhs, Load(rhsVar, lhs.typeReference))

  def greaterThanOrEqual(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Gte(lhs, rhs)

  def equal(lhsVar: String, rhs: IntermediateRepresentation): IntermediateRepresentation =
    equal(Load(lhsVar, rhs.typeReference), rhs)

  def equal(lhs: IntermediateRepresentation, rhsVar: String): IntermediateRepresentation =
    equal(lhs, Load(rhsVar, lhs.typeReference))

  def equal(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    Eq(lhs, rhs)

  def notEqual(lhsVar: String, rhs: IntermediateRepresentation): IntermediateRepresentation =
    notEqual(Load(lhsVar, rhs.typeReference), rhs)

  def notEqual(lhs: IntermediateRepresentation, rhsVar: String): IntermediateRepresentation =
    notEqual(lhs, Load(rhsVar, lhs.typeReference))

  def notEqual(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation =
    NotEq(lhs, rhs)

  def block(ops: IntermediateRepresentation*): IntermediateRepresentation = {
    val reducedOps = ops.filter(_ != Noop)
    if (reducedOps.isEmpty) noop()
    else if (reducedOps.length == 1) reducedOps.head
    else Block(reducedOps)
  }

  def block(ops: collection.Seq[IntermediateRepresentation]): IntermediateRepresentation = {
    if (ops.isEmpty) noop()
    else if (ops.length == 1) ops.head
    else Block(ops)
  }

  def noop(): IntermediateRepresentation = Noop

  def isEmpty(ir: IntermediateRepresentation): Boolean = ir match {
    case Noop       => true
    case Block(ops) => ops.isEmpty || ops.forall(isEmpty)
    case _          => false
  }

  def condition(test: IntermediateRepresentation)(onTrue: IntermediateRepresentation): IntermediateRepresentation = {
    if (isEmpty(onTrue)) noop()
    else simplifyPredicates(Condition(test, onTrue))
  }

  def ifElse(test: IntermediateRepresentation)(onTrue: IntermediateRepresentation)(onFalse: IntermediateRepresentation)
    : IntermediateRepresentation = {
    if (isEmpty(onFalse)) condition(test)(onTrue)
    else if (isEmpty(onTrue)) condition(not(test))(onFalse)
    else simplifyPredicates(Condition(test, onTrue, Some(onFalse)))
  }

  def loop(test: IntermediateRepresentation)(body: IntermediateRepresentation): IntermediateRepresentation =
    Loop(simplifyPredicates(test), body, labelName = null)

  def labeledLoop(
    labelName: String,
    test: IntermediateRepresentation
  )(body: IntermediateRepresentation): IntermediateRepresentation = Loop(simplifyPredicates(test), body, labelName)

  def break(labelName: String): IntermediateRepresentation = Break(labelName)

  def declare[TYPE](name: String)(implicit typ: Manifest[TYPE]): DeclareLocalVariable =
    DeclareLocalVariable(typeRef(typ), name)

  def declare(typeReference: codegen.TypeReference, name: String): DeclareLocalVariable =
    DeclareLocalVariable(typeReference, name)

  def assign(name: String, value: IntermediateRepresentation): AssignToLocalVariable =
    AssignToLocalVariable(name, value)

  def assign(variable: LocalVariable, value: IntermediateRepresentation): AssignToLocalVariable =
    AssignToLocalVariable(variable.name, value)

  def declareAndAssign(name: String, value: IntermediateRepresentation): IntermediateRepresentation =
    block(declareAndAssignList(value.typeReference, name, value): _*)

  def declareAndAssign(load: Load, value: IntermediateRepresentation): IntermediateRepresentation =
    block(declareAndAssignList(value.typeReference, load.variable, value): _*)

  def declareAndAssign(
    typeReference: TypeReference,
    load: Load,
    value: IntermediateRepresentation
  ): IntermediateRepresentation =
    declareAndAssign(typeReference, load.variable, value)

  def declareAndAssign(
    typeReference: TypeReference,
    name: String,
    value: IntermediateRepresentation
  ): IntermediateRepresentation =
    block(declareAndAssignList(typeReference, name, value): _*)

  def declareAndAssignList(
    typeReference: TypeReference,
    name: String,
    value: IntermediateRepresentation
  ): Seq[IntermediateRepresentation] =
    Seq(declare(typeReference, name), assign(name, value))

  def returns(value: IntermediateRepresentation): IntermediateRepresentation = Returns(value)

  def tryCatch[E](name: String)(ops: IntermediateRepresentation)(onError: IntermediateRepresentation)(implicit
  typ: Manifest[E]): TryCatch =
    TryCatch(ops, onError, typeRef(typ), name)

  def tryCatchIfNecessary[E](name: String)(ops: IntermediateRepresentation)(
    onError: IntermediateRepresentation
  )(implicit typ: Manifest[E]): IntermediateRepresentation = {
    // This is by no means a complete list of "safe operations" but it removes some common expressions
    def cannotFail(in: IntermediateRepresentation): Boolean = {
      in.folder.treeForall {
        case _: Eq | _: NotEq | BooleanValueFcn(_) | LongValueFcn(_) | NotFcn(_) | InFcn() |
          _: Constant | _: Block | _: Load | _: ArrayLoad | _: ArraySet | _: ArrayLength | _: ArrayLiteral | _: AssignToLocalVariable | _: GetStatic | _: GetField |
          _: DeclareLocalVariable =>
          true
        case _: IntermediateRepresentation => false
        case _                             => true
      }
    }
    if (cannotFail(ops)) {
      ops
    } else {
      tryCatch[E](name)(ops)(onError)
    }
  }

  def fail(error: IntermediateRepresentation): Throw = Throw(error)

  def and(lhsVar: String, rhs: IntermediateRepresentation): IntermediateRepresentation =
    and(Load(lhsVar, rhs.typeReference), rhs)

  def and(lhs: IntermediateRepresentation, rhsVar: String): IntermediateRepresentation =
    and(lhs, Load(rhsVar, lhs.typeReference))

  def and(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation = {
    simplifyPredicates(BooleanAnd(Seq(lhs, rhs)))
  }

  def and(ands: collection.Seq[IntermediateRepresentation]): IntermediateRepresentation = {
    simplifyPredicates(BooleanAnd(ands.toSeq))
  }

  def or(lhs: IntermediateRepresentation, rhs: IntermediateRepresentation): IntermediateRepresentation = {
    simplifyPredicates(BooleanOr(Seq(lhs, rhs)))
  }

  def or(ors: collection.Seq[IntermediateRepresentation]): IntermediateRepresentation = {
    simplifyPredicates(BooleanOr(ors.toSeq))
  }

  def isNull(test: IntermediateRepresentation): IntermediateRepresentation = IsNull(test)

  def isNotNull(test: IntermediateRepresentation): IntermediateRepresentation = Not(IsNull(test))

  def newInstance(constructor: Constructor, params: IntermediateRepresentation*): NewInstance =
    NewInstance(constructor, params)

  def newInstance(inner: ExtendClass, params: IntermediateRepresentation*): NewInstanceInnerClass =
    NewInstanceInnerClass(inner, params)

  def newArray(baseType: codegen.TypeReference, size: Int): NewArray = NewArray(baseType, size)

  def newDynamicallySizedArray(baseType: codegen.TypeReference, size: IntermediateRepresentation): NewArrayDynamicSize =
    NewArrayDynamicSize(baseType, size)

  def not(test: IntermediateRepresentation): IntermediateRepresentation = {
    simplifyPredicates(Not(test))
  }

  def oneTime(expression: IntermediateRepresentation): IntermediateRepresentation = OneTime(expression)(used = false)

  def print(value: IntermediateRepresentation): IntermediateRepresentation =
    invokeSideEffect(getStatic[System, PrintStream]("out"), method[PrintStream, Unit, Object]("println"), value)

  def box(expression: IntermediateRepresentation): Box = Box(expression)

  def unbox(expression: IntermediateRepresentation): Unbox = Unbox(expression)

  def self[TYPE](implicit typ: Manifest[TYPE]): IntermediateRepresentation = Self(typeRef(typ))

  def booleanValue(ir: IntermediateRepresentation): IntermediateRepresentation = {
    simplifyPredicates(invokeStatic(method[Values, BooleanValue, Boolean]("booleanValue"), ir))
  }

  def longValue(ir: IntermediateRepresentation): IntermediateRepresentation =
    invokeStatic(method[Values, LongValue, Long]("longValue"), ir)

  def scalaObjectInstance(instance: AnyRef): IntermediateRepresentation = {
    val typeReference = codegen.TypeReference.typeReference(instance.getClass)
    GetStatic(Some(typeReference), typeReference, "MODULE$")
  }

}
