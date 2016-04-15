/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.function.Consumer

import org.neo4j.codegen.CodeGeneratorOption._
import org.neo4j.codegen.ExpressionTemplate._
import org.neo4j.codegen.MethodReference._
import org.neo4j.codegen.TypeReference._
import org.neo4j.codegen.source.{SourceCode, SourceVisitor}
import org.neo4j.codegen.{CodeGenerator, _}
import org.neo4j.collection.primitive.{Primitive, PrimitiveLongIntMap, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.compiler.v3_1.codegen.{CodeGenContext, CodeStructure, MethodStructure, QueryExecutionTracer, ResultRowImpl, setStaticField}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan._
import org.neo4j.cypher.internal.compiler.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.{Id, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_1.planner.CantCompileQueryException
import org.neo4j.cypher.internal.compiler.v3_1.spi.{InternalResultVisitor, QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.compiler.v3_1.{ExecutionMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_1.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v3_1.{CypherExecutionException, symbols}
import org.neo4j.graphdb.Direction
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.{ReadOperations, StatementTokenNameLookup, TokenNameLookup}
import org.neo4j.kernel.impl.api.RelationshipDataExtractor
import org.neo4j.kernel.impl.core.NodeManager


object GeneratedQueryStructure extends CodeStructure[GeneratedQuery] {

  import Expression.{constant, invoke, newInstance}

  override def generateQuery(packageName: String, className: String, columns: Seq[String], operatorIds: Map[String, Id],
                             sourceSink: Option[SourceSink])
                            (block: MethodStructure[_] => Unit)(implicit codeGenContext: CodeGenContext) = {
    val generator: CodeGenerator = try {
      CodeGenerator.generateCode(classOf[CodeStructure[_]].getClassLoader, SourceCode.BYTECODE, SourceCode.PRINT_SOURCE,
                                 sourceSink.map(sink => new SourceVisitor {
                                   override protected def visitSource(reference: TypeReference,
                                                                      sourceCode: CharSequence): Unit =
                                     sink.apply(reference.name(), sourceCode.toString)
                                 }).getOrElse(BLANK_OPTION))
    } catch {
      case e: Exception => throw new CantCompileQueryException(e.getMessage, e)
    }
    val execution = using(
      generator.generateClass(packageName, className + "Execution", typeRef[GeneratedQueryExecution],
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
        success = clazz.generate(Templates.SUCCESS),
        close = clazz.generate(Templates.CLOSE))
        // the "COLUMNS" static field
        clazz.staticField(typeRef[util.List[String]], "COLUMNS", Templates.asList[String](
        columns.map(key => constant(key))))

      // the operator id fields
      operatorIds.keys.foreach { opId =>
        clazz.staticField(typeRef[Id], opId)
      }

      // simple methods
      clazz.generate(Templates.CONSTRUCTOR)
      clazz.generate(Templates.SET_SUCCESSFUL_CLOSEABLE)
      clazz.generate(Templates.EXECUTION_MODE)
      clazz.generate(Templates.EXECUTION_PLAN_DESCRIPTION)
      clazz.generate(Templates.JAVA_COLUMNS)

      using(clazz.generate(MethodDeclaration.method(typeRef[Unit], "accept",
                                                    Parameter.param(parameterizedType(classOf[InternalResultVisitor[_]],
                                                                                      typeParameter("E")), "visitor")).
        parameterizedWith("E", extending(typeRef[Exception])).
        throwsException(typeParameter("E")))) { method =>
        method.assign(typeRef[ResultRowImpl], "row", Templates.newResultRow)
        block(GeneratedMethodStructure(fields, method, new AuxGenerator(packageName, generator)))
        method.expression(invoke(method.self(), fields.success))
        method.expression(invoke(method.self(), fields.close))
      }
      clazz.handle()
    }
    val query = using(generator.generateClass(packageName, className, typeRef[GeneratedQuery])) { clazz =>
      using(clazz.generateMethod(typeRef[GeneratedQueryExecution], "execute",
                                 param[TaskCloser]("closer"),
                                 param[QueryContext]("queryContext"),
                                 param[ExecutionMode]("executionMode"),
                                 param[Provider[InternalPlanDescription]]("description"),
                                 param[QueryExecutionTracer]("tracer"),
                                 param[util.Map[String, Object]]("params"))) { execute =>
        execute.returns(Expression
                          .invoke(newInstance(execution), MethodReference.constructorReference(execution,
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
    query
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

  def lowerType(cType: CypherType): TypeReference = cType match {
    case symbols.CTNode => typeRef[Long]
    case symbols.CTRelationship => typeRef[Long]
    case symbols.CTString => typeRef[String]
    case symbols.CTAny => typeRef[Object]
  }

  def nullValue(cType: CypherType) = cType match {
    case symbols.CTNode => constant(-1L)
    case symbols.CTRelationship => constant(-1L)
    case symbols.CTString => constant(null)
    case symbols.CTAny => constant(null)
  }
}

private object Templates {

  import GeneratedQueryStructure.{method, param, staticField, typeRef}

  def newInstance(valueType: TypeReference, args: Expression*): Expression = {
    Expression.invoke(Expression.newInstance(valueType), MethodReference.constructorReference(valueType), args: _*)
  }

  val newLongObjectMap = Expression.invoke(method[Primitive, PrimitiveLongObjectMap[_]]("longObjectMap"))
  val newCountingMap = Expression.invoke(method[Primitive, PrimitiveLongIntMap]("longIntMap"))

  def asList[T](values: Seq[Expression])(implicit manifest: Manifest[T]): Expression = Expression.invoke(
    methodReference(typeRef[util.Arrays], typeRef[util.List[T]], "asList", typeRef[Array[Object]]),
    Expression.newArray(typeRef[T], values: _*))

  def handleKernelExceptions[V](generate: CodeBlock, ro: FieldReference, close: MethodReference)
                         (block: CodeBlock => V): V = {
    var result = null.asInstanceOf[V]

    generate.tryCatch(new Consumer[CodeBlock] {
      override def accept(body: CodeBlock) = {
        result = block(body)
      }
    }, new Consumer[CodeBlock]() {
      override def accept(handle: CodeBlock) = {
                handle.throwException(Expression.invoke(
                  Expression.newInstance(typeRef[CypherExecutionException]),
                  MethodReference.constructorReference(typeRef[CypherExecutionException], typeRef[String], typeRef[Throwable]),
                  Expression.invoke(handle.load("e"), method[KernelException, String]("getUserMessage", typeRef[TokenNameLookup]),
                                    Expression.invoke(
                                      Expression.newInstance(typeRef[StatementTokenNameLookup]),
                                      MethodReference
                                        .constructorReference(typeRef[StatementTokenNameLookup], typeRef[ReadOperations]),
                                      Expression.get(handle.self(), ro))), handle.load("e")
                ))
      }
    }, param[KernelException]("e"))

    result
  }

  val incoming = Expression.get(staticField[Direction, Direction](Direction.INCOMING.name()))
  val outgoing = Expression.get(staticField[Direction, Direction](Direction.OUTGOING.name()))
  val both = Expression.get(staticField[Direction, Direction](Direction.BOTH.name()))
  val newResultRow = Expression
    .invoke(Expression.newInstance(typeRef[ResultRowImpl]),
            MethodReference.constructorReference(typeRef[ResultRowImpl]))
  val newRelationshipDataExtractor = Expression
    .invoke(Expression.newInstance(typeRef[RelationshipDataExtractor]),
            MethodReference.constructorReference(typeRef[RelationshipDataExtractor]))

  val CONSTRUCTOR = MethodTemplate.constructor(
    param[TaskCloser]("closer"),
    param[QueryContext]("queryContext"),
    param[ExecutionMode]("executionMode"),
    param[Provider[InternalPlanDescription]]("description"),
    param[QueryExecutionTracer]("tracer"),

    param[util.Map[String, Object]]("params")).
    put(self(), typeRef[TaskCloser], "closer", load("closer")).
    put(self(), typeRef[ReadOperations], "ro",
        cast(classOf[ReadOperations], invoke(
          invoke(load("queryContext"), method[QueryContext, QueryTransactionalContext]("transactionalContext")),
          method[QueryTransactionalContext, Object]("readOperations")))).
    put(self(), typeRef[ExecutionMode], "executionMode", load("executionMode")).
    put(self(), typeRef[Provider[InternalPlanDescription]], "description", load("description")).
    put(self(), typeRef[QueryExecutionTracer], "tracer", load("tracer")).
    put(self(), typeRef[util.Map[String, Object]], "params", load("params")).
    put(self(), typeRef[NodeManager], "nodeManager",
        cast(typeRef[NodeManager],
             invoke(load("queryContext"), method[QueryContext, Object]("entityAccessor")))).
    build()

  val SET_SUCCESSFUL_CLOSEABLE = MethodTemplate.method(typeRef[Unit], "setSuccessfulCloseable",
                                                       param[SuccessfulCloseable]("closeable")).
    put(self(), typeRef[SuccessfulCloseable], "closeable", load("closeable")).
    build()
  val SUCCESS = MethodTemplate.method(typeRef[Unit], "success").
    expression(
      invoke(get(self(), typeRef[SuccessfulCloseable], "closeable"), method[SuccessfulCloseable, Unit]("success"))).
    build()
  val CLOSE = MethodTemplate.method(typeRef[Unit], "close").
    expression(
      invoke(get(self(), typeRef[SuccessfulCloseable], "closeable"), method[SuccessfulCloseable, Unit]("close"))).
    build()
  val EXECUTION_MODE = MethodTemplate.method(typeRef[ExecutionMode], "executionMode").
    returns(get(self(), typeRef[ExecutionMode], "executionMode")).
    build()
  val EXECUTION_PLAN_DESCRIPTION = MethodTemplate.method(typeRef[InternalPlanDescription], "executionPlanDescription").
    returns(invoke(get(self(), typeRef[Provider[InternalPlanDescription]], "description"),
                   method[Provider[InternalPlanDescription], InternalPlanDescription]("get"))).
    build()
  val JAVA_COLUMNS = MethodTemplate.method(typeRef[util.List[String]], "javaColumns").
    returns(get(typeRef[util.List[String]], "COLUMNS")).
    build()
}