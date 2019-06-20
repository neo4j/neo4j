/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.codegen
import org.neo4j.codegen.CodeGenerator.generateCode
import org.neo4j.codegen.Expression._
import org.neo4j.codegen.FieldReference.{field, staticField}
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen.TypeReference
import org.neo4j.codegen.TypeReference.OBJECT
import org.neo4j.codegen.bytecode.ByteCode.BYTECODE
import org.neo4j.codegen.source.SourceCode.{PRINT_SOURCE, SOURCECODE}

/**
  * Produces runnable code from an IntermediateRepresentation
  */
object CodeGeneration {

  private val DEBUG = true

  def compileClass[T](c: ClassDeclaration[T]): Class[T] = {
    val handle = compileClassDeclaration(c)
    val clazz = handle.loadAnonymousClass()
    setConstants(clazz, c.fields)
    clazz.asInstanceOf[Class[T]]
  }

  private def setConstants(clazz: Class[_], fields: Seq[Field]): Unit = {
    fields.distinct.foreach {
      case StaticField(_, name, Some(value)) =>
        clazz.getDeclaredField(name).set(null, value)
      case _ =>
    }
  }

  private def beginBlock[Block <: AutoCloseable, T](block: Block)(exhaustBlock: Block => T): T = {
    /*
     * In the java API we are using try-with-resources for this. This is slightly problematic since we
     * are then always calling close which potentially will hide errors thrown in code generation.
     */
    val result = exhaustBlock(block)
    block.close()
    result
  }

  private def generateConstructor(clazz: codegen.ClassGenerator, fields: Seq[Field], params: Seq[Parameter] = Seq.empty,
                          initializationCode: codegen.CodeBlock => codegen.Expression = _ => codegen.Expression.EMPTY,
                          parent: Option[TypeReference] = None): Unit = {
    beginBlock(clazz.generateConstructor(params.map(_.asCodeGen): _*)) { block =>
      block.expression(invokeSuper(parent.getOrElse(OBJECT)))
      fields.distinct.foreach {
        case InstanceField(typ, name, initializer) =>
          val reference = clazz.field(typ, name)
          initializer.map(ir => compileExpression(ir, block)).foreach { value =>
            block.put(block.self(), reference, value)
          }
        case StaticField(typ, name, _) =>
          clazz.publicStaticField(typ, name)
      }
      initializationCode(block)
    }
  }

  private def generator = {
    if (DEBUG) generateCode(classOf[IntermediateRepresentation].getClassLoader, SOURCECODE, PRINT_SOURCE)
    else generateCode(classOf[IntermediateRepresentation].getClassLoader, BYTECODE)
  }

  private def compileExpression(ir: IntermediateRepresentation, block: codegen.CodeBlock): codegen.Expression = ir match {
    //Foo.method(p1, p2,...)
    case InvokeStatic(method, params) =>
      invoke(method.asReference, params.map(p => compileExpression(p, block)): _*)
    //target.method(p1,p2,...)
    case Invoke(target, method, params) =>
      invoke(compileExpression(target, block), method.asReference, params.map(p => compileExpression(p, block)): _*)
    //target.method(p1,p2,...)
    case InvokeSideEffect(target, method, params) =>
      val invocation = invoke(compileExpression(target, block), method.asReference,
                              params.map(p => compileExpression(p, block)): _*)

      if (method.returnType.isVoid) block.expression(invocation)
      else block.expression(codegen.Expression.pop(invocation))
      codegen.Expression.EMPTY

    //loads local variable by name
    case Load(variable) => block.load(variable)

    //loads field
    case LoadField(f) =>
      codegen.Expression.get(block.self(), field(block.owner(), f.typ, f.name))

    //sets a field
    case SetField(f, v) =>
      block.put(block.self(), field(block.owner(), f.typ, f.name), compileExpression(v, block))
      codegen.Expression.EMPTY

     //loads a given constant
    case Constant(value) => constant(value)

    //new ArrayValue[]{p1, p2,...}
    case ArrayLiteral(typ, values) => newInitializedArray(typ, values.map(v => compileExpression(v, block)): _*)

    // array[offset] = value
    case ArraySet(array, offset, value) =>
      block.expression(codegen.Expression.arraySet(compileExpression(array, block), compileExpression(offset, block), compileExpression(value, block)))
      codegen.Expression.EMPTY

    //array.length
    case ArrayLength(array) =>
     codegen.Expression.arrayLength(compileExpression(array, block))

    // array[offset]
    case ArrayLoad(array, offset) => codegen.Expression.arrayLoad(compileExpression(array, block), constant(offset))

    //Foo.BAR
    case GetStatic(owner, typ, name) => getStatic(staticField(owner.getOrElse(block.owner()), typ, name))

    //condition ? onTrue : onFalse
    case Ternary(condition, onTrue, onFalse) =>
      codegen.Expression.ternary(compileExpression(condition, block),
                         compileExpression(onTrue, block),
                         compileExpression(onFalse, block))

    //lhs + rhs
    case Add(lhs, rhs) =>
      codegen.Expression.add(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs - rhs
    case Subtract(lhs, rhs) =>
      codegen.Expression.subtract(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs < rhs
    case Lt(lhs, rhs) =>
      codegen.Expression.lt(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs <= rhs
    case Lte(lhs, rhs) =>
      codegen.Expression.lte(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs > rhs
    case Gt(lhs, rhs) =>
      codegen.Expression.gt(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs > rhs
    case Gte(lhs, rhs) =>
      codegen.Expression.gte(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs == rhs
    case Eq(lhs, rhs) =>
      codegen.Expression.equal(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs != rhs
    case NotEq(lhs, rhs) =>
      codegen.Expression.notEqual(compileExpression(lhs, block), compileExpression(rhs, block))

    //test == null
    case IsNull(test) => codegen.Expression.isNull(compileExpression(test, block))

    //run multiple ops in a block, the value of the block is the last expression
    case Block(ops) =>
      if (ops.isEmpty) codegen.Expression.EMPTY else ops.map(compileExpression(_, block)).last

    //if (test) {onTrue}
    case Condition(test, onTrue, None) =>
      beginBlock(block.ifStatement(compileExpression(test, block)))(compileExpression(onTrue, _))

    case Condition(test, onTrue, Some(onFalse)) =>
      block.ifElseStatement(compileExpression(test, block), (t: codegen.CodeBlock) => compileExpression(onTrue, t),
                            (f: codegen.CodeBlock) => compileExpression(onFalse, f))
      codegen.Expression.EMPTY

    //typ name;
    case DeclareLocalVariable(typ, name) =>
      block.declare(typ, name)

    //name = value;
    case AssignToLocalVariable(name, value) =>
      block.assign(block.local(name), compileExpression(value, block))
      codegen.Expression.EMPTY

    //try {ops} catch(exception name)(onError)
    case TryCatch(ops, onError, exception, name) =>
      block.tryCatch((mainBlock: codegen.CodeBlock) => compileExpression(ops, mainBlock),
                     (errorBlock: codegen.CodeBlock) => compileExpression(onError, errorBlock),
                     param(exception, name))
      codegen.Expression.EMPTY

    //throw error
    case Throw(error) =>
      block.throwException(compileExpression(error, block))
      codegen.Expression.EMPTY

    //lhs && rhs
    case BooleanAnd(lhs, rhs) =>
      codegen.Expression.and(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs && rhs
    case BooleanOr(lhs, rhs) =>
      codegen.Expression.or(compileExpression(lhs, block), compileExpression(rhs, block))

    //new Foo(args[0], args[1], ...)
    case NewInstance(constructor, args) =>
      codegen.Expression.invoke(codegen.Expression.newInstance(constructor.owner), constructor.asReference, args.map(compileExpression(_, block)):_*)

    //new Foo[5]
    case NewArray(baseType, size) =>
      codegen.Expression.newArray(baseType, size)

    case Returns(value: IntermediateRepresentation) =>
      block.returns(compileExpression(value, block))
      codegen.Expression.EMPTY

    //while(test) { body }
    case Loop(test, body) =>
      beginBlock(block.whileLoop(compileExpression(test, block)))(compileExpression(body, _))

    // (to) expressions
    case Cast(to, expression) => codegen.Expression.cast(to, compileExpression(expression, block))

    //  expressions instance of t
    case InstanceOf(typ, expression) => codegen.Expression.instanceOf(typ, compileExpression(expression, block))

    case Not(test) => codegen.Expression.not(compileExpression(test, block))

    case e@OneTime(inner) =>
      if (!e.isUsed) {
        e.use()
        compileExpression(inner, block)
      } else codegen.Expression.EMPTY

    case Noop =>
      codegen.Expression.EMPTY
  }

  private def compileClassDeclaration(c: ClassDeclaration[_]): codegen.ClassHandle = {
    val handle = beginBlock(generator.generateClass(c.extendsClass.getOrElse(codegen.TypeReference.OBJECT), c.packageName, c.className, c.implementsInterfaces: _*)) { clazz: codegen.ClassGenerator =>
      //clazz.generateConstructor(generateParametersWithNames(c.constructor.constructor.params): _*)
      generateConstructor(clazz,
                          c.fields,
                          c.constructorParameters,
                          block => compileExpression(c.initializationCode, block),
                          c.extendsClass)
      c.methods.foreach { m =>
        compileMethodDeclaration(clazz, m)
      }
      clazz.handle()
    }
    handle
  }

  private def compileMethodDeclaration(clazz: codegen.ClassGenerator, m: MethodDeclaration): Unit = {
    val method = codegen.MethodDeclaration.method(m.returnType, m.methodName,
                                     m.parameters.map(_.asCodeGen): _*)
    m.parameterizedWith.foreach {
      case (name, bound) => method.parameterizedWith(name, bound)
    }
    m.throws.foreach(method.throwsException)

    beginBlock(clazz.generate(method)) { block =>
      m.localVariables.distinct.foreach { v =>
        block.assign(v.typ, v.name, compileExpression(v.value, block))
      }
      if (m.returnType == codegen.TypeReference.VOID) {
        block.expression(compileExpression(m.body, block))
      }
      else {
        block.returns(compileExpression(m.body, block))
      }
    }
  }
}
