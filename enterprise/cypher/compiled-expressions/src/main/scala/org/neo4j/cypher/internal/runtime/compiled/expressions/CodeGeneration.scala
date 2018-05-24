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

import org.neo4j.codegen
import org.neo4j.codegen.CodeGenerator.generateCode
import org.neo4j.codegen._
import org.neo4j.codegen.bytecode.ByteCode
import org.neo4j.codegen.Expression.{getStatic, invoke}
import org.neo4j.codegen.FieldReference.staticField
import org.neo4j.codegen.MethodDeclaration.method
import org.neo4j.codegen.MethodReference.methodReference
import org.neo4j.codegen.Parameter.param
import org.neo4j.codegen.bytecode.ByteCode.BYTECODE
import org.neo4j.codegen.source.SourceCode
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.frontend.helpers.using

object CodeGeneration {

  private val PACKAGE_NAME = "org.neo4j.cypher.internal.compiler.v3_5.generated"
  private val INTERFACE = classOf[CompiledExpression]
  private val COMPUTE_METHOD = method(classOf[AnyValue], "compute", param(classOf[MapValue], "params"))

  private def className(): String = "Expression" + System.nanoTime()

  def compile(ir: IntermediateRepresentation): CompiledExpression = {
    val handle = using(generateCode(BYTECODE).generateClass(PACKAGE_NAME, className(), INTERFACE)) { clazz =>
      using(clazz.generate(COMPUTE_METHOD)) { block =>
        block.returns(compileExpression(ir, block))
      }
     clazz.handle()
    }

   handle.loadClass().newInstance().asInstanceOf[CompiledExpression]
  }

  private def compileExpression(ir: IntermediateRepresentation, block: CodeBlock): codegen.Expression = ir match {
    case InvokeStatic(method, params) =>
      invoke(method.asReference, params.map(p => compileExpression(p, block)): _*)
    case Invoke(target, method, params) =>
      invoke(compileExpression(target, block), method.asReference, params.map(p => compileExpression(p, block)): _*)
    case Load(variable) => block.load(variable)
    case Integer(value) =>
      invoke(methodReference(classOf[Values],
                             classOf[LongValue],
                             "longValue", classOf[Long]), Expression.constant(value.longValue()))
    case Float(value) =>
      invoke(methodReference(classOf[Values],
                             classOf[DoubleValue],
                             "doubleValue", classOf[Double]), Expression.constant(value.doubleValue()))

    case StringLiteral(value) =>
      invoke(methodReference(classOf[Values],
                             classOf[TextValue],
                             "stringValue", classOf[String]), Expression.constant(value.stringValue()))

    case Constant(value) => Expression.constant(value)

    case NULL => getStatic(staticField(classOf[Values], classOf[Value], "NO_VALUE"))
    case TRUE => getStatic(staticField(classOf[Values], classOf[BooleanValue], "TRUE"))
    case FALSE => getStatic(staticField(classOf[Values], classOf[BooleanValue], "FALSE"))

  }


}
