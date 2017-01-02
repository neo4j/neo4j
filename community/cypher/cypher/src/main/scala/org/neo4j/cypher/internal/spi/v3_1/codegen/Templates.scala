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
import java.util
import java.util.function.Consumer

import org.neo4j.codegen.ExpressionTemplate._
import org.neo4j.codegen.MethodReference._
import org.neo4j.codegen._
import org.neo4j.collection.primitive.{Primitive, PrimitiveLongIntMap, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.compiler.v3_1.codegen._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_1.spi.{QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.compiler.v3_1.{ExecutionMode, TaskCloser}
import org.neo4j.cypher.internal.frontend.v3_1.CypherExecutionException
import org.neo4j.graphdb.Direction
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.{ReadOperations, StatementTokenNameLookup, TokenNameLookup}
import org.neo4j.kernel.impl.api.RelationshipDataExtractor
import org.neo4j.kernel.impl.core.NodeManager

/**
  * Contains common code generation constructs.
  */
object Templates {

  import GeneratedQueryStructure.{method, param, staticField, typeRef}

  def createNewInstance(valueType: TypeReference, args: (TypeReference,Expression)*): Expression = {
    val argTypes = args.map(_._1)
    val argExpression = args.map(_._2)
    Expression.invoke(Expression.newInstance(valueType),
                      MethodReference.constructorReference(valueType, argTypes: _*), argExpression:_*)
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
        handle.expression(Expression.invoke(handle.self(), close))
        handle.throwException(Expression.invoke(
          Expression.newInstance(typeRef[CypherExecutionException]),
          MethodReference.constructorReference(typeRef[CypherExecutionException], typeRef[String], typeRef[Throwable]),
          Expression
            .invoke(handle.load("e"), method[KernelException, String]("getUserMessage", typeRef[TokenNameLookup]),
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

  def tryCatch(generate: CodeBlock)(tryBlock :CodeBlock => Unit)(exception: Parameter)(catchBlock :CodeBlock => Unit): Unit = {
    generate.tryCatch(new Consumer[CodeBlock] {
      override def accept(body: CodeBlock) = tryBlock(body)
    }, new Consumer[CodeBlock]() {
      override def accept(handle: CodeBlock) = catchBlock(handle)
    }, exception)
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
    invokeSuper().
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
    returns(cast( typeRef[InternalPlanDescription],
      invoke(get(self(), typeRef[Provider[InternalPlanDescription]], "description"),
                   method[Provider[InternalPlanDescription], Object]("get")))).
    build()
  val JAVA_COLUMNS = MethodTemplate.method(typeRef[util.List[String]], "javaColumns").
    returns(get(typeRef[util.List[String]], "COLUMNS")).
    build()
}
