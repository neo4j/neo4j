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
import org.neo4j.codegen.Expression.{constant, getStatic, invoke, newArray}
import org.neo4j.codegen.FieldReference.staticField
import org.neo4j.codegen.MethodDeclaration.method
import org.neo4j.codegen.MethodReference.methodReference
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen.TypeReference.typeReference
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
  private val VALUE = classOf[Value]
  private val LONG = classOf[LongValue]
  private val DOUBLE = classOf[DoubleValue]
  private val TEXT = classOf[TextValue]
  private val PACKAGE_NAME = "org.neo4j.cypher.internal.compiler.v3_5.generated"
  private val INTERFACE = classOf[CompiledExpression]
  private val COMPUTE_METHOD = method(classOf[AnyValue], "evaluate",
                                      param(classOf[ExecutionContext], "context"),
                                      param(classOf[DbAccess], "dbAccess"),
                                      param(classOf[MapValue], "params"))

  private def className(): String = "Expression" + System.nanoTime()

  def compile(ir: IntermediateRepresentation): CompiledExpression = {
    val handle = using(generator) { clazz =>
      using(clazz.generate(COMPUTE_METHOD)) { block =>
        block.returns(compileExpression(ir, block))
      }
      clazz.handle()
    }

    handle.loadClass().newInstance().asInstanceOf[CompiledExpression]
  }

  private def generator =
    if (DEBUG) generateCode(SOURCECODE, PRINT_SOURCE).generateClass(PACKAGE_NAME, className(), INTERFACE)
    else generateCode(BYTECODE).generateClass(PACKAGE_NAME, className(), INTERFACE)

  private def compileExpression(ir: IntermediateRepresentation, block: CodeBlock): codegen.Expression = ir match {
    //Foo.method(p1, p2,...)
    case InvokeStatic(method, params) =>
      invoke(method.asReference, params.map(p => compileExpression(p, block)): _*)
    //target.method(p1,p2,...)
    case Invoke(target, method, params) =>
      invoke(compileExpression(target, block), method.asReference, params.map(p => compileExpression(p, block)): _*)
    //loads local variable by name
    case Load(variable) => block.load(variable)
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
    //Values.NO_VALUE
    case NULL => getStatic(staticField(VALUES, VALUE, "NO_VALUE"))
    //Values.TRUE
    case TRUE => getStatic(staticField(VALUES, classOf[BooleanValue], "TRUE"))
    //Values.FALSE
    case FALSE => getStatic(staticField(VALUES, classOf[BooleanValue], "FALSE"))
    //new ArrayValue[]{p1, p2,...}
    case ArrayLiteral(values) => newArray(typeReference(classOf[AnyValue]),
                                          values.map(v => compileExpression(v, block)): _*)

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

    //run multiple ops in a block, the value of the block is the last expression
    case Block(ops) => ops.map(compileExpression(_, block)).last

    //if (test) {onTrue}
    case Condition(test, onTrue) =>
      using(block.ifStatement(compileExpression(test, block)))(compileExpression(onTrue, _))

    //typ name;
    case DeclareLocalVariable(typ, name) =>
      block.declare(typeReference(typ), name)

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
  }
}
