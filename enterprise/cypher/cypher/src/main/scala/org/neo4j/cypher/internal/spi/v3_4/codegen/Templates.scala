/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.spi.v3_4.codegen

import java.util
import java.util.Comparator
import java.util.function.Consumer
import java.util.stream.{DoubleStream, IntStream, LongStream}

import org.neo4j.codegen.ExpressionTemplate._
import org.neo4j.codegen.MethodDeclaration.Builder
import org.neo4j.codegen.MethodReference._
import org.neo4j.codegen._
import org.neo4j.collection.primitive.{Primitive, PrimitiveLongIntMap, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.codegen.{PrimitiveNodeStream, PrimitiveRelationshipStream}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{Completable, Provider}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.using
import org.neo4j.cypher.internal.javacompat.ResultRowImpl
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.{ExecutionMode, QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.spi.v3_4.codegen.Methods.{newNodeProxyById, newRelationshipProxyById}
import org.neo4j.cypher.internal.util.v3_4.CypherExecutionException
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer
import org.neo4j.graphdb.{Direction, Node, Relationship}
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.exceptions.{EntityNotFoundException, KernelException}
import org.neo4j.kernel.api.SilentTokenNameLookup
import org.neo4j.kernel.impl.api.RelationshipDataExtractor
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.{Value, ValueComparator, Values}
import org.neo4j.values.virtual._
import org.neo4j.values.{AnyValue, AnyValues}

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

  def createNewNodeReference(expression: Expression): Expression =
    Expression.invoke(method[VirtualValues, NodeReference]("node", typeRef[Long]), expression)

  def createNewRelationshipReference(expression: Expression): Expression =
    Expression.invoke(method[VirtualValues, RelationshipReference]("relationship", typeRef[Long]), expression)

  def createNewNodeValueFromPrimitive(proxySpi: Expression, expression: Expression) =
    Expression.invoke(method[ValueUtils, NodeValue]("fromNodeProxy", typeRef[Node]),
      Expression.invoke(proxySpi, newNodeProxyById, expression))

  def createNewRelationshipValueFromPrimitive(proxySpi: Expression, expression: Expression) =
    Expression.invoke(method[ValueUtils, RelationshipValue]("fromRelationshipProxy", typeRef[Relationship]),
      Expression.invoke(proxySpi, newRelationshipProxyById, expression))

  def asList[T](values: Seq[Expression])(implicit manifest: Manifest[T]): Expression = Expression.invoke(
    methodReference(typeRef[util.Arrays], typeRef[util.List[T]], "asList", typeRef[Array[Object]]),
    Expression.newArray(typeRef[T], values: _*))

  def asAnyValueList(values: Seq[Expression]): Expression = Expression.invoke(
    methodReference(typeRef[VirtualValues], typeRef[ListValue], "list", typeRef[Array[AnyValue]]),
    Expression.newArray(typeRef[AnyValue], values: _*))

  def asPrimitiveNodeStream(values: Seq[Expression]): Expression = Expression.invoke(
    methodReference(typeRef[PrimitiveNodeStream], typeRef[PrimitiveNodeStream], "of", typeRef[Array[Long]]),
    Expression.newArray(typeRef[Long], values: _*))

  def asPrimitiveRelationshipStream(values: Seq[Expression]): Expression = Expression.invoke(
    methodReference(typeRef[PrimitiveRelationshipStream], typeRef[PrimitiveRelationshipStream], "of", typeRef[Array[Long]]),
    Expression.newArray(typeRef[Long], values: _*))

  def asLongStream(values: Seq[Expression]): Expression = Expression.invoke(
    methodReference(typeRef[LongStream], typeRef[LongStream], "of", typeRef[Array[Long]]),
    Expression.newArray(typeRef[Long], values: _*))

  def asDoubleStream(values: Seq[Expression]): Expression = Expression.invoke(
    methodReference(typeRef[DoubleStream], typeRef[DoubleStream], "of", typeRef[Array[Double]]),
    Expression.newArray(typeRef[Double], values: _*))

  def asIntStream(values: Seq[Expression]): Expression = Expression.invoke(
    methodReference(typeRef[IntStream], typeRef[IntStream], "of", typeRef[Array[Int]]),
    Expression.newArray(typeRef[Int], values: _*))

  def handleEntityNotFound[V](generate: CodeBlock, fields: Fields, finalizers: Seq[Boolean => CodeBlock => Unit])
                             (happyPath: CodeBlock => V)(onFailure: CodeBlock => V): V = {
    var result = null.asInstanceOf[V]

    generate.tryCatch(new Consumer[CodeBlock] {
      override def accept(innerBody: CodeBlock): Unit = result = happyPath(innerBody)
    }, new Consumer[CodeBlock] {
      override def accept(innerError: CodeBlock): Unit = {
        result = onFailure(innerError)
        innerError.continueIfPossible()
      }
    }, param[EntityNotFoundException]("enf"))
    result
  }

  def handleKernelExceptions[V](generate: CodeBlock, fields: Fields, finalizers: Seq[Boolean => CodeBlock => Unit])
                         (block: CodeBlock => V): V = {
    var result = null.asInstanceOf[V]

    generate.tryCatch(new Consumer[CodeBlock] {
      override def accept(body: CodeBlock) = {
        result = block(body)
      }
    }, new Consumer[CodeBlock]() {
      override def accept(handle: CodeBlock) = {
        finalizers.foreach(block => block(false)(handle))
        handle.throwException(Expression.invoke(
          Expression.newInstance(typeRef[CypherExecutionException]),
          MethodReference.constructorReference(typeRef[CypherExecutionException], typeRef[String], typeRef[Throwable]),
          Expression
            .invoke(handle.load("e"), method[KernelException, String]("getUserMessage", typeRef[TokenNameLookup]),
                    Expression.invoke(
                      Expression.newInstance(typeRef[SilentTokenNameLookup]),
                      MethodReference
                        .constructorReference(typeRef[SilentTokenNameLookup], typeRef[TokenRead]),
                      Expression.get(handle.self(), fields.tokenRead))), handle.load("e")
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

  val noValue = Expression.getStatic(staticField[Values, Value]("NO_VALUE"))
  val incoming = Expression.getStatic(staticField[Direction, Direction](Direction.INCOMING.name()))
  val outgoing = Expression.getStatic(staticField[Direction, Direction](Direction.OUTGOING.name()))
  val both = Expression.getStatic(staticField[Direction, Direction](Direction.BOTH.name()))
  val newResultRow = Expression
    .invoke(Expression.newInstance(typeRef[ResultRowImpl]),
            MethodReference.constructorReference(typeRef[ResultRowImpl]))
  val newRelationshipDataExtractor = Expression
    .invoke(Expression.newInstance(typeRef[RelationshipDataExtractor]),
            MethodReference.constructorReference(typeRef[RelationshipDataExtractor]))
  val valueComparator = Expression.getStatic(staticField[Values, ValueComparator]("COMPARATOR"))
  val anyValueComparator = Expression.getStatic(staticField[AnyValues, Comparator[AnyValue]]("COMPARATOR"))

  def constructor(classHandle: ClassHandle) = MethodTemplate.constructor(
    param[QueryContext]("queryContext"),
    param[ExecutionMode]("executionMode"),
    param[Provider[InternalPlanDescription]]("description"),
    param[QueryExecutionTracer]("tracer"),

    param[MapValue]("params")).
    invokeSuper().
    put(self(classHandle), typeRef[QueryContext], "queryContext", load("queryContext", typeRef[QueryContext])).
    put(self(classHandle), typeRef[ExecutionMode], "executionMode", load("executionMode", typeRef[ExecutionMode])).
    put(self(classHandle), typeRef[Provider[InternalPlanDescription]], "description", load("description", typeRef[InternalPlanDescription])).
    put(self(classHandle), typeRef[QueryExecutionTracer], "tracer", load("tracer", typeRef[QueryExecutionTracer])).
    put(self(classHandle), typeRef[MapValue], "params", load("params", typeRef[MapValue])).
    put(self(classHandle), typeRef[EmbeddedProxySPI], "proxySpi",
             invoke(load("queryContext", typeRef[QueryContext]), method[QueryContext, EmbeddedProxySPI]("entityAccessor"))).
    build()

  def getOrLoadCursors(clazz: ClassGenerator, fields: Fields) = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[CursorFactory], "getOrLoadCursors")
    using(clazz.generate(methodBuilder)) { generate =>
      val cursors = Expression.get(generate.self(), fields.cursors)
      using(generate.ifStatement(Expression.isNull(cursors))) { block =>
        val transactionalContext: MethodReference = method[QueryContext, QueryTransactionalContext]("transactionalContext")
        val cursors: MethodReference = method[QueryTransactionalContext, CursorFactory]("cursors")
        val queryContext = Expression.get(block.self(), fields.queryContext)
        block.put(block.self(), fields.cursors,
                  Expression.invoke(Expression.invoke(queryContext, transactionalContext), cursors))
      }
      generate.returns(cursors)
    }
  }

  def getOrLoadDataRead(clazz: ClassGenerator, fields: Fields) = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[Read], "getOrLoadDataRead")
    using(clazz.generate(methodBuilder)) { generate =>
      val dataRead = Expression.get(generate.self(), fields.dataRead)
      using(generate.ifStatement(Expression.isNull(dataRead))) { block =>
        val transactionalContext: MethodReference = method[QueryContext, QueryTransactionalContext]("transactionalContext")
        val dataRead: MethodReference = method[QueryTransactionalContext, Read]("dataRead")
        val queryContext = Expression.get(block.self(), fields.queryContext)
        block.put(block.self(), fields.dataRead,
                  Expression.invoke(Expression.invoke(queryContext, transactionalContext), dataRead))
      }
      generate.returns(dataRead)
    }
  }

  def getOrLoadTokenRead(clazz: ClassGenerator, fields: Fields) = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[TokenRead], "getOrLoadTokenRead")
    using(clazz.generate(methodBuilder)) { generate =>
      val tokenRead = Expression.get(generate.self(), fields.tokenRead)
      using(generate.ifStatement(Expression.isNull(tokenRead))) { block =>
        val transactionalContext: MethodReference = method[QueryContext, QueryTransactionalContext]("transactionalContext")
        val tokenRead: MethodReference = method[QueryTransactionalContext, TokenRead]("tokenRead")
        val queryContext = Expression.get(block.self(), fields.queryContext)
        block.put(block.self(), fields.tokenRead,
                  Expression.invoke(Expression.invoke(queryContext, transactionalContext), tokenRead))
      }
      generate.returns(tokenRead)
    }
  }

  def getOrLoadSchemaRead(clazz: ClassGenerator, fields: Fields) = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[SchemaRead], "getOrLoadSchemaRead")
    using(clazz.generate(methodBuilder)) { generate =>
      val schemaRead = Expression.get(generate.self(), fields.schemaRead)
      using(generate.ifStatement(Expression.isNull(schemaRead))) { block =>
        val transactionalContext: MethodReference = method[QueryContext, QueryTransactionalContext]("transactionalContext")
        val schemaRead: MethodReference = method[QueryTransactionalContext, SchemaRead]("schemaRead")
        val queryContext = Expression.get(block.self(), fields.queryContext)
        block.put(block.self(), fields.schemaRead,
                  Expression.invoke(Expression.invoke(queryContext, transactionalContext), schemaRead))
      }
      generate.returns(schemaRead)
    }
  }

  def setCompletable(classHandle: ClassHandle) = MethodTemplate.method(typeRef[Unit], "setCompletable",
                                                                               param[Completable]("closeable")).
    put(self(classHandle), typeRef[Completable], "closeable", load("closeable", typeRef[Completable])).
    build()

  def executionMode(classHandle: ClassHandle) = MethodTemplate.method(typeRef[ExecutionMode], "executionMode").
    returns(get(self(classHandle), typeRef[ExecutionMode], "executionMode")).
    build()

  def executionPlanDescription(classHandle: ClassHandle) = MethodTemplate.method(typeRef[InternalPlanDescription], "executionPlanDescription").
    returns(cast( typeRef[InternalPlanDescription],
      invoke(get(self(classHandle), typeRef[Provider[InternalPlanDescription]], "description"),
                   method[Provider[InternalPlanDescription], Object]("get")))).
    build()

  def nodeCursor(clazz: ClassGenerator,  fields: Fields): Unit = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[NodeCursor], "nodeCursor")
    using(clazz.generate(methodBuilder)) { generate =>
      val nodeCursor = Expression.get(generate.self(), fields.nodeCursor)
        Expression.get(generate.self(), fields.cursors)
      val cursors = Expression.invoke(generate.self(),
                                      methodReference(generate.owner(), typeRef[CursorFactory], "getOrLoadCursors" ))
      using(generate.ifStatement(Expression.isNull(nodeCursor))) { block =>
        block.put(block.self(), fields.nodeCursor,
        Expression.invoke(cursors, method[CursorFactory, NodeCursor]("allocateNodeCursor")))

      }
      generate.returns(nodeCursor)
    }
  }

  def relationshipScanCursor(clazz: ClassGenerator,  fields: Fields): Unit = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[RelationshipScanCursor], "relationshipScanCursor")
    using(clazz.generate(methodBuilder)) { generate =>
      val relationshipCursor = Expression.get(generate.self(), fields.relationshipScanCursor)
      Expression.get(generate.self(), fields.cursors)
      val cursors = Expression.invoke(generate.self(),
                                      methodReference(generate.owner(), typeRef[CursorFactory], "getOrLoadCursors" ))
      using(generate.ifStatement(Expression.isNull(relationshipCursor))) { block =>
        block.put(block.self(), fields.relationshipScanCursor,
                  Expression.invoke(cursors, method[CursorFactory, RelationshipScanCursor]("allocateRelationshipScanCursor")))

      }
      generate.returns(relationshipCursor)
    }
  }

  def closeCursors(clazz: ClassGenerator, fields: Fields): Unit = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[Unit], "closeCursors")
    using(clazz.generate(methodBuilder)) { generate =>
      val nodeCursor = Expression.get(generate.self(), fields.nodeCursor)
      using(generate.ifStatement(Expression.notNull(nodeCursor))) { block =>
        block.expression(Expression.invoke(nodeCursor, method[NodeCursor, Unit]("close")))
      }
      val relationshipCursor = Expression.get(generate.self(), fields.relationshipScanCursor)
      using(generate.ifStatement(Expression.notNull(relationshipCursor))) { block =>
        block.expression(Expression.invoke(relationshipCursor, method[RelationshipScanCursor, Unit]("close")))
      }
      val propertyCursor = Expression.get(generate.self(), fields.propertyCursor)
      using(generate.ifStatement(Expression.notNull(propertyCursor))) { block =>
        block.expression(Expression.invoke(propertyCursor, method[PropertyCursor, Unit]("close")))
      }
    }
  }

  def propertyCursor(clazz: ClassGenerator,  fields: Fields): Unit = {
    val methodBuilder: Builder = MethodDeclaration.method(typeRef[PropertyCursor], "propertyCursor")
    using(clazz.generate(methodBuilder)) { generate =>
      val propertyCursor = Expression.get(generate.self(), fields.propertyCursor)
      val cursors = Expression.invoke(generate.self(),
                                      methodReference(generate.owner(), typeRef[CursorFactory], "getOrLoadCursors" ))
      using(generate.ifStatement(Expression.isNull(propertyCursor))) { block =>
        block.put(block.self(), fields.propertyCursor,
                  Expression.invoke(cursors, method[CursorFactory, PropertyCursor]("allocatePropertyCursor")))

      }
      generate.returns(propertyCursor)
    }
  }

  val FIELD_NAMES = MethodTemplate.method(TypeReference.typeReference(classOf[Array[String]]), "fieldNames").
    returns(get(TypeReference.typeReference(classOf[Array[String]]), "COLUMNS")).
    build()
}
