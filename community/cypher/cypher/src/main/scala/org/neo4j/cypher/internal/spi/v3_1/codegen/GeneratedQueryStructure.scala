/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.spi.v3_1.codegen

import java.lang.reflect.Modifier
import java.util

import org.neo4j.codegen.CodeGeneratorOption._
import org.neo4j.codegen.TypeReference._
import org.neo4j.codegen.source.{SourceCode, SourceVisitor}
import org.neo4j.codegen.{CodeGenerator, _}
import org.neo4j.cypher.internal.compiler.v3_1.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions.{CodeGenType, FloatType, IntType, ReferenceType}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan._
import org.neo4j.cypher.internal.compiler.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_1.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_1.spi.{InternalResultVisitor, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_1.{ExecutionMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_1.symbols
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.impl.core.NodeManager

object GeneratedQueryStructure extends CodeStructure[GeneratedQuery] {

  import Expression.{constant, invoke, newInstance}
  import MethodReference.constructorReference

  case class GeneratedQueryStructureResult(query: GeneratedQuery, source: Option[(String, String)])
    extends CodeStructureResult[GeneratedQuery]

  private def createGenerator(conf: CodeGenConfiguration, source: (Option[(String, String)]) => Unit) = {
    val mode = conf.mode match {
      case SourceCodeMode => SourceCode.SOURCECODE
      case ByteCodeMode => SourceCode.BYTECODE
    }
    val option = if (conf.saveSource) new SourceVisitor {
      override protected def visitSource(reference: TypeReference,
                                         sourceCode: CharSequence) = source(Some(reference.name(), sourceCode.toString))
    } else {
      source(None)
      BLANK_OPTION
    }

    try {
      CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, mode, option)
    } catch {
      case e: Exception => throw new CantCompileQueryException(e.getMessage, e)
    }
  }

  class SourceSaver extends ((Option[(String, String)]) => Unit) {

    private var _source: Option[(String, String)] = None

    override def apply(v1: Option[(String, String)]) =  _source = v1

    def source: Option[(String, String)] = _source
  }

  override def generateQuery(className: String, columns: Seq[String], operatorIds: Map[String, Id],
                             conf: CodeGenConfiguration)
                            (block: MethodStructure[_] => Unit)(implicit codeGenContext: CodeGenContext) = {

    val sourceSaver = new SourceSaver
    val generator = createGenerator(conf, sourceSaver)
    val execution = using(
      generator.generateClass(conf.packageName, className + "Execution", typeRef[GeneratedQueryExecution],
                              typeRef[SuccessfulCloseable])) { clazz =>
      // fields
      val fields = Fields(
        closer = clazz.field(typeRef[TaskCloser], "closer"),
        ro = clazz.field(typeRef[ReadOperations], "ro"),
        entityAccessor = clazz.field(typeRef[NodeManager], "nodeManager"),
        executionMode = clazz.field(typeRef[ExecutionMode], "executionMode"),
        description = clazz.field(typeRef[Provider[InternalPlanDescription]], "description"),
        tracer = clazz.field(typeRef[QueryExecutionTracer], "tracer"),
        params = clazz.field(typeRef[util.Map[String, Object]], "params"),
        closeable = clazz.field(typeRef[SuccessfulCloseable], "closeable"),
        success = clazz.generate(Templates.success(clazz.handle())),
        close = clazz.generate(Templates.close(clazz.handle())))
        // the "COLUMNS" static field
        clazz.staticField(typeRef[util.List[String]], "COLUMNS", Templates.asList[String](
        columns.map(key => constant(key))))

      // the operator id fields
      operatorIds.keys.foreach { opId =>
        clazz.staticField(typeRef[Id], opId)
      }

      // simple methods
      clazz.generate(Templates.constructor(clazz.handle()))
      clazz.generate(Templates.setSuccessfulCloseable(clazz.handle()))
      clazz.generate(Templates.executionMode(clazz.handle()))
      clazz.generate(Templates.executionPlanDescription(clazz.handle()))
      clazz.generate(Templates.JAVA_COLUMNS)

      using(clazz.generate(MethodDeclaration.method(typeRef[Unit], "accept",
                                                    Parameter.param(parameterizedType(classOf[InternalResultVisitor[_]],
                                                                                      typeParameter("E")), "visitor")).
        parameterizedWith("E", extending(typeRef[Exception])).
        throwsException(typeParameter("E")))) { method =>
        method.assign(typeRef[ResultRowImpl], "row", Templates.newResultRow)
        block(GeneratedMethodStructure(fields, method, new AuxGenerator(conf.packageName, generator)))
        method.expression(invoke(method.self(), fields.success))
        method.expression(invoke(method.self(), fields.close))
      }
      clazz.handle()
    }
    val query = using(generator.generateClass(conf.packageName, className, typeRef[GeneratedQuery])) { clazz =>
      using(clazz.generateMethod(typeRef[GeneratedQueryExecution], "execute",
                                 param[TaskCloser]("closer"),
                                 param[QueryContext]("queryContext"),
                                 param[ExecutionMode]("executionMode"),
                                 param[Provider[InternalPlanDescription]]("description"),
                                 param[QueryExecutionTracer]("tracer"),
                                 param[util.Map[String, Object]]("params"))) { execute =>
        execute.returns(
          invoke(
            newInstance(execution),
            constructorReference(execution,
                                 typeRef[TaskCloser],
                                 typeRef[QueryContext],
                                 typeRef[ExecutionMode],
                                 typeRef[Provider[InternalPlanDescription]],
                                 typeRef[QueryExecutionTracer],
                                 typeRef[util.Map[String, Object]]),
                                  execute.load("closer"),
                                  execute.load("queryContext"),
                                  execute.load("executionMode"),
                                  execute.load("description"),
                                  execute.load("tracer"),
                                  execute.load("params")))
      }
      clazz.handle()
    }.newInstance().asInstanceOf[GeneratedQuery]
    val clazz: Class[_] = execution.loadClass()
    operatorIds.foreach {
      case (key, id) => setStaticField(clazz, key, id)
    }

    GeneratedQueryStructureResult(query, sourceSaver.source)
  }

  def method[O <: AnyRef, R](name: String, params: TypeReference*)
                            (implicit owner: Manifest[O], returns: Manifest[R]): MethodReference =
    MethodReference.methodReference(typeReference(owner), typeReference(returns), name, Modifier.PUBLIC, params: _*)

  def staticField[O <: AnyRef, R](name: String)(implicit owner: Manifest[O], fieldType: Manifest[R]): FieldReference =
    FieldReference.staticField(typeReference(owner), typeReference(fieldType), name)

  def param[T <: AnyRef](name: String)(implicit manifest: Manifest[T]): Parameter =
    Parameter.param(typeReference(manifest), name)

  def typeRef[T](implicit manifest: Manifest[T]): TypeReference = typeReference(manifest)

  def typeReference(manifest: Manifest[_]): TypeReference = {
    val arguments = manifest.typeArguments
    val base = TypeReference.typeReference(manifest.runtimeClass)
    if (arguments.nonEmpty) {
      TypeReference.parameterizedType(base, arguments.map(typeReference): _*)
    } else {
      base
    }
  }

  def lowerType(cType: CodeGenType): TypeReference = cType match {
    case CodeGenType(symbols.CTNode, IntType) => typeRef[Long]
    case CodeGenType(symbols.CTRelationship, IntType) => typeRef[Long]
    case CodeGenType(symbols.CTInteger, IntType) => typeRef[Long]
    case CodeGenType(symbols.CTFloat, FloatType) => typeRef[Double]
    case CodeGenType(symbols.CTString, ReferenceType) => typeRef[String]
    case _ => typeRef[Object]
  }

  def nullValue(cType: CodeGenType) = cType match {
    case CodeGenType(symbols.CTNode, IntType) => constant(-1L)
    case CodeGenType(symbols.CTRelationship, IntType) => constant(-1L)
    case _ => constant(null)
  }
}