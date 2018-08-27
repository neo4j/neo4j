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

import java.util.function.Consumer

import org.neo4j.codegen
import org.neo4j.codegen.CodeGenerator.generateCode
import org.neo4j.codegen.Expression.{constant, getStatic, invoke, invokeSuper, newArray}
import org.neo4j.codegen.FieldReference.{field, staticField}
import org.neo4j.codegen.MethodDeclaration.method
import org.neo4j.codegen.MethodReference.methodReference
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen.TypeReference.{OBJECT, typeReference}
import org.neo4j.codegen._
import org.neo4j.codegen.bytecode.ByteCode.BYTECODE
import org.neo4j.codegen.source.SourceCode.{PRINT_SOURCE, SOURCECODE}
import org.neo4j.cypher.internal.runtime.DbAccess
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.frontend.helpers.using

/**
  * Produces runnable code from an IntermediateRepresentation
  */
object CodeGeneration {

  private val DEBUG = false
  private val VALUES = classOf[Values]
  private val LONG = classOf[LongValue]
  private val DOUBLE = classOf[DoubleValue]
  private val TEXT = classOf[TextValue]
  private val PACKAGE_NAME = "org.neo4j.cypher.internal.compiler.v3_5.generated"
  private val EXPRESSION = classOf[CompiledExpression]
  private val PROJECTION = classOf[CompiledProjection]
  private val COMPUTE_METHOD: MethodDeclaration.Builder = method(classOf[AnyValue], "evaluate",
                                                                 param(classOf[ExecutionContext], "context"),
                                                                 param(classOf[DbAccess], "dbAccess"),
                                                                 param(classOf[MapValue], "params"))
  private val PROJECT_METHOD: MethodDeclaration.Builder = method(classOf[Unit], "project",
                                                                 param(classOf[ExecutionContext], "context"),
                                                                 param(classOf[DbAccess], "dbAccess"),
                                                                 param(classOf[MapValue], "params"))

  private def className(): String = "Expression" + System.nanoTime()

  def compileExpression(expression: IntermediateExpression): CompiledExpression = {
    val handle = using(generator.generateClass(PACKAGE_NAME, className(), EXPRESSION)) { clazz: ClassGenerator =>

      generateConstructor(clazz, expression)
      using(clazz.generate(COMPUTE_METHOD)) { block =>
        expression.variables.distinct.foreach{ v =>
          block.assign(v.typ, v.name, compileExpression(v.value, block))
        }
        val noValue = getStatic(staticField(classOf[Values], classOf[Value], "NO_VALUE"))
        if (expression.nullCheck.nonEmpty) {
          val test = expression.nullCheck.map(e => compileExpression(e, block))
            .reduceLeft((acc, current) => Expression.or(acc, current))

          block.returns(Expression.ternary(test, noValue, compileExpression(expression.ir, block)))
        } else block.returns(compileExpression(expression.ir, block))
      }
      clazz.handle()
    }

    handle.loadClass().newInstance().asInstanceOf[CompiledExpression]
  }

  def compileProjection(expression: IntermediateExpression): CompiledProjection = {
    val handle = using(generator.generateClass(PACKAGE_NAME, className(), PROJECTION)) { clazz: ClassGenerator =>

      generateConstructor(clazz, expression)
      using(clazz.generate(PROJECT_METHOD)) { block =>
        expression.variables.distinct.foreach{ v =>
          block.assign(v.typ, v.name, compileExpression(v.value, block))
        }
        block.expression(compileExpression(expression.ir, block))
      }
      clazz.handle()
    }

    handle.loadClass().newInstance().asInstanceOf[CompiledProjection]
  }

  private def generateConstructor(clazz: ClassGenerator, expression: IntermediateExpression): Unit = {
    using(clazz.generateConstructor()) { block =>
      block.expression(invokeSuper(OBJECT))
      expression.fields.foreach { f =>
        val reference = clazz.field(f.typ, f.name)
        //if fields has initializer set them in the constructor
        val initializer = f.initializer.map(ir => compileExpression(ir, block))
        initializer.foreach { value =>
          block.put(block.self(), reference, value)
        }
      }
    }
  }

  private def generator = {
    if (DEBUG) generateCode(classOf[CompiledExpression].getClassLoader, SOURCECODE, PRINT_SOURCE)
    else generateCode(classOf[CompiledExpression].getClassLoader, BYTECODE)
  }

  private def compileExpression(ir: IntermediateRepresentation, block: CodeBlock): codegen.Expression = ir match {
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

      if (method.output == TypeReference.VOID) block.expression(invocation)
      else block.expression(Expression.pop(invocation))
      Expression.EMPTY

    //loads local variable by name
    case Load(variable) => block.load(variable)
    //loads field
    case LoadField(f) => Expression.get(block.self(), field(block.owner(), f.typ, f.name))
    //sets a field
    case SetField(f, v) =>
      block.put(block.self(), field(block.owner(), f.typ, f.name), compileExpression(v, block))
      Expression.EMPTY
    //Values.longValue(value)
    case Integer(value) =>
      invoke(methodReference(VALUES, LONG,
                             "longValue", classOf[Long]), constant(value.longValue()))
    //Values.doubleValue(value)
    case Float(value) =>
      invoke(methodReference(VALUES, DOUBLE,
                             "doubleValue", classOf[Double]), constant(value.doubleValue()))
    //Values.stringValue(value)
    case StringLiteral(value) =>
      invoke(methodReference(VALUES, TEXT,
                             "stringValue", classOf[String]), constant(value.stringValue()))
    //loads a given constant
    case Constant(value) => constant(value)

    //new ArrayValue[]{p1, p2,...}
    case ArrayLiteral(values) => newArray(typeReference(classOf[AnyValue]),
                                          values.map(v => compileExpression(v, block)): _*)

    //Foo.BAR
    case GetStatic(owner, typ, name) => getStatic(staticField(owner, typ, name))

    //condition ? onTrue : onFalse
    case Ternary(condition, onTrue, onFalse) =>
      Expression.ternary(compileExpression(condition, block),
                         compileExpression(onTrue, block),
                         compileExpression(onFalse, block))
    //lhs == rhs
    case Eq(lhs, rhs) =>
      Expression.equal(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs != rhs
    case NotEq(lhs, rhs) =>
      Expression.notEqual(compileExpression(lhs, block), compileExpression(rhs, block))

    //test == null
    case IsNull(test) => Expression.isNull(compileExpression(test, block))

    //run multiple ops in a block, the value of the block is the last expression
    case Block(ops) =>
      if (ops.isEmpty) Expression.EMPTY else ops.map(compileExpression(_, block)).last

    //if (test) {onTrue}
    case Condition(test, onTrue) =>
      using(block.ifStatement(compileExpression(test, block)))(compileExpression(onTrue, _))

    //typ name;
    case DeclareLocalVariable(typ, name) =>
      block.declare(typ, name)

    //name = value;
    case AssignToLocalVariable(name, value) =>
      block.assign(block.local(name), compileExpression(value, block))
      Expression.EMPTY

    //try {ops} catch(exception name)(onError)
    case TryCatch(ops, onError, exception, name) =>
      block.tryCatch(new Consumer[CodeBlock] {
        override def accept(mainBlock: CodeBlock): Unit = compileExpression(ops, mainBlock)
      }, new Consumer[CodeBlock] {
        override def accept(errorBlock: CodeBlock): Unit = compileExpression(onError, errorBlock)
      }, Parameter.param(exception, name))
      Expression.EMPTY

    //throw error
    case Throw(error) =>
      block.throwException(compileExpression(error, block))
      Expression.EMPTY

    //lhs && rhs
    case BooleanAnd(lhs, rhs) =>
      Expression.and(compileExpression(lhs, block), compileExpression(rhs, block))

    //lhs && rhs
    case BooleanOr(lhs, rhs) =>
      Expression.or(compileExpression(lhs, block), compileExpression(rhs, block))

    //new Foo(args[0], args[1], ...)
    case NewInstance(constructor, args) =>
      Expression.invoke(Expression.newInstance(constructor.owner), constructor.asReference, args.map(compileExpression(_, block)):_*)

    //while(test) { body }
    case Loop(test, body) =>
      using(block.whileLoop(compileExpression(test, block)))(compileExpression(body, _))

    // (to) expressions
    case Cast(to, expression) => Expression.cast(to, compileExpression(expression, block))
  }
}
