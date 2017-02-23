/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.spi.v3_2.codegen

import java.util
import java.util.PrimitiveIterator
import java.util.stream.{DoubleStream, IntStream, LongStream}

import org.neo4j.codegen.Expression.{not, or, _}
import org.neo4j.codegen.MethodReference.methodReference
import org.neo4j.codegen._
import org.neo4j.collection.primitive._
import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.CompositeKey
import org.neo4j.cypher.internal.codegen._
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.expressions.{BoolType,  FloatType, IntType, LongType, ReferenceType, Parameter => _, _}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi._
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.{CodeGenContext, QueryExecutionEvent}
import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.commands.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_2.spi.{NodeIdWrapper, RelationshipIdWrapper}
import org.neo4j.cypher.internal.frontend.v3_2.helpers._
import org.neo4j.cypher.internal.frontend.v3_2.symbols.{CTNode, CTRelationship, ListType}
import org.neo4j.cypher.internal.frontend.v3_2.{ParameterNotFoundException, SemanticDirection, symbols}
import org.neo4j.cypher.internal.spi.v3_2.codegen.GeneratedMethodStructure.CompletableFinalizer
import org.neo4j.cypher.internal.spi.v3_2.codegen.Methods._
import org.neo4j.cypher.internal.spi.v3_2.codegen.Templates.{createNewInstance, handleKernelExceptions, newRelationshipDataExtractor, tryCatch}
import org.neo4j.graphdb.Direction
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.api.schema_new.index.{NewIndexDescriptor, NewIndexDescriptorFactory}
import org.neo4j.kernel.api.schema_new.{IndexQuery, LabelSchemaDescriptor}
import org.neo4j.kernel.impl.api.RelationshipDataExtractor
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.expressions.CodeGenType

import scala.collection.mutable

object GeneratedMethodStructure {
  type CompletableFinalizer = Boolean => CodeBlock => Unit
}


class GeneratedMethodStructure(val fields: Fields, val generator: CodeBlock, aux: AuxGenerator, tracing: Boolean = true,
                               events: List[String] = List.empty,
                               onClose: Seq[CompletableFinalizer] = Seq.empty,
                               locals: mutable.Map[String, LocalVariable] = mutable.Map.empty
                              )(implicit context: CodeGenContext)
  extends MethodStructure[Expression] {

  import GeneratedQueryStructure._
  import TypeReference.parameterizedType

  private val _finalizers: mutable.ArrayBuffer[CompletableFinalizer] = mutable.ArrayBuffer()
  _finalizers.appendAll(onClose)

  def finalizers: Seq[CompletableFinalizer] = _finalizers

  private def copy(fields: Fields = fields,
                   generator: CodeBlock = generator,
                   aux: AuxGenerator = aux,
                   tracing: Boolean = tracing,
                   events: List[String] = events,
                   onClose: Seq[CompletableFinalizer] = _finalizers,
                   locals: mutable.Map[String, LocalVariable] = locals): GeneratedMethodStructure = new GeneratedMethodStructure(
    fields, generator, aux, tracing, events, onClose, locals)

  private case class HashTable(valueType: TypeReference, listType: TypeReference, tableType: TypeReference,
                               get: MethodReference, put: MethodReference, add: MethodReference)

  private def extractHashTable(tableType: RecordingJoinTableType): HashTable = tableType match {
    case LongToListTable(tupleDescriptor, localMap) =>
      // compute the participating types
      val valueType = aux.typeReference(tupleDescriptor)
      val listType = parameterizedType(classOf[util.ArrayList[_]], valueType)
      val tableType = parameterizedType(classOf[PrimitiveLongObjectMap[_]], valueType)
      // the methods we use on those types
      val get = methodReference(tableType, typeRef[Object], "get", typeRef[Long])
      val put = methodReference(tableType, typeRef[Object], "put", typeRef[Long], typeRef[Object])
      val add = methodReference(listType, typeRef[Boolean], "add", typeRef[Object])

      HashTable(valueType, listType, tableType, get, put, add)

    case LongsToListTable(tupleDescriptor, localMap) =>
      // compute the participating types
      val valueType = aux.typeReference(tupleDescriptor)
      val listType = parameterizedType(classOf[util.ArrayList[_]], valueType)
      val tableType = parameterizedType(classOf[util.HashMap[_, _]], typeRef[CompositeKey], valueType)
      // the methods we use on those types
      val get = methodReference(tableType, typeRef[Object], "get", typeRef[Object])
      val put = methodReference(tableType, typeRef[Object], "put", typeRef[Object], typeRef[Object])
      val add = methodReference(listType, typeRef[Boolean], "add", typeRef[Object])
      HashTable(valueType, listType, tableType, get, put, add)
  }

  override def nextNode(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, invoke(generator.load(iterVar), nextLong))

  override def createRelExtractor(relVar: String) =
    generator.assign(typeRef[RelationshipDataExtractor], relExtractor(relVar), newRelationshipDataExtractor)

  override def nextRelationshipAndNode(toNodeVar: String, iterVar: String, direction: SemanticDirection,
                                       fromNodeVar: String,
                                       relVar: String) = {
    val extractor = relExtractor(relVar)
    val start = invoke(generator.load(extractor), startNode)
    val end = invoke(generator.load(extractor), endNode)

    generator.expression(
      pop(
        invoke(generator.load(iterVar), relationshipVisit,
               invoke(generator.load(iterVar), fetchNextRelationship),
               generator.load(extractor))))
    generator.assign(typeRef[Long], toNodeVar, toGraphDb(direction) match {
      case Direction.INCOMING => start
      case Direction.OUTGOING => end
      case Direction.BOTH => ternary(equal(start, generator.load(fromNodeVar)), end, start)
    })
    generator.assign(typeRef[Long], relVar, invoke(generator.load(extractor), getRelationship))
  }

  private def relExtractor(relVar: String) = s"${relVar}Extractor"

  override def nextRelationship(iterVar: String, ignored: SemanticDirection, relVar: String) = {
    val extractor = relExtractor(relVar)
    generator.expression(
      pop(
        invoke(generator.load(iterVar), relationshipVisit,
               invoke(generator.load(iterVar), fetchNextRelationship),
               generator.load(extractor))))
    generator.assign(typeRef[Long], relVar, invoke(generator.load(extractor), getRelationship))
  }

  override def allNodesScan(iterVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar, invoke(readOperations, nodesGetAll))

  override def labelScan(iterVar: String, labelIdVar: String) =
    generator.assign(typeRef[PrimitiveLongIterator], iterVar,
                     invoke(readOperations, nodesGetForLabel, generator.load(labelIdVar)))

  override def lookupLabelId(labelIdVar: String, labelName: String) =
    generator.assign(typeRef[Int], labelIdVar,
                     invoke(readOperations, labelGetForName, constant(labelName)))

  override def lookupLabelIdE(labelName: String): Expression =
    invoke(readOperations, labelGetForName, constant(labelName))

  override def lookupRelationshipTypeId(typeIdVar: String, typeName: String) =
    generator.assign(typeRef[Int], typeIdVar, invoke(readOperations, relationshipTypeGetForName, constant(typeName)))

  override def lookupRelationshipTypeIdE(typeName: String) =
    invoke(readOperations, relationshipTypeGetForName, constant(typeName))

  override def hasNextNode(iterVar: String) =
    invoke(generator.load(iterVar), hasNextLong)

  override def hasNextRelationship(iterVar: String) =
    invoke(generator.load(iterVar), hasMoreRelationship)

  override def whileLoop(test: Expression)(block: MethodStructure[Expression] => Unit) =
    using(generator.whileLoop(test)) { body =>
      block(copy(generator = body))
    }

  override def forEach(varName: String, codeGenType: CodeGenType, iterable: Expression)
                      (block: MethodStructure[Expression] => Unit) =
    using(generator.forEach(Parameter.param(lowerType(codeGenType), varName), iterable)) { body =>
      block(copy(generator = body))
    }

  override def ifStatement(test: Expression)(block: (MethodStructure[Expression]) => Unit) = {
    using(generator.ifStatement(test)) { body =>
      block(copy(generator = body))
    }
  }

  override def ifNotStatement(test: Expression)(block: (MethodStructure[Expression]) => Unit) = {
    using(generator.ifStatement(not(test))) { body =>
      block(copy(generator = body))
    }
  }

  override def ifNonNullStatement(test: Expression)(block: (MethodStructure[Expression]) => Unit) = {
    using(generator.ifStatement(Expression.notNull(test))) { body =>
      block(copy(generator = body))
    }
  }

  override def ternaryOperator(test: Expression, onTrue: Expression, onFalse: Expression): Expression =
    ternary(test, onTrue, onFalse)

  override def returnSuccessfully() {
    //close all outstanding events
    for (event <- events) {
      generator.expression(
        invoke(generator.load(event),
               method[QueryExecutionEvent, Unit]("close")))
    }
    _finalizers.foreach(codeBlock => codeBlock(true)(generator))
    generator.returns()
  }

  override def declareCounter(name: String, initialValue: Expression): Unit = {
    val variable = generator.declare(typeRef[Long], name)
    locals += (name -> variable)
    generator.assign(variable, invoke(mathCastToLong, initialValue))
  }

  override def decrementInteger(name: String) = {
    val local = locals(name)
    generator.assign(local, subtract(local, constant(1L)))
  }

  override def incrementInteger(name: String): Unit =
    incrementInteger(name, constant(1L))

  override def incrementInteger(name: String, value: Expression): Unit = {
    val local = locals(name)
    generator.assign(local, add(local, value))
  }

  override def checkInteger(name: String, comparator: Comparator, value: Long): Expression = {
    val local = locals(name)
    comparator match {
      case Equal => equal(local, constant(value))
      case LessThan => lt(local, constant(value))
      case LessThanEqual => lte(local, constant(value))
      case GreaterThan => gt(local, constant(value))
      case GreaterThanEqual => gte(local, constant(value))
    }
  }

  override def setInRow(column: String, value: Expression) =
    generator.expression(invoke(resultRow, set, constant(column), value))

  override def visitorAccept() = tryCatch(generator) { onSuccess =>
    using(
      onSuccess.ifStatement(
        not(invoke(onSuccess.load("visitor"),
               visit, onSuccess.load("row"))))) { body =>
      // NOTE: we are in this if-block if the visitor decided to terminate early (by returning false)
      //close all outstanding events
      for (event <- events) {
        body.expression(invoke(generator.load(event),
                               method[QueryExecutionEvent, Unit]("close")))
      }
      _finalizers.foreach(block => block(true)(body))
      body.returns()
    }
  }(exception = param[Throwable]("e")) { onError =>
    for (event <- events) {
      onError.expression(
        invoke(onError.load(event),
               method[QueryExecutionEvent, Unit]("close")))
    }
    _finalizers.foreach(block => block(false)(onError))
    onError.throwException(onError.load("e"))
  }


  override def materializeNode(nodeIdVar: String, codeGenType: CodeGenType) =
    if (codeGenType.isPrimitive)
      invoke(nodeManager, newNodeProxyById, generator.load(nodeIdVar))
    else
      invoke(nodeManager, newNodeProxyById,
        invoke(cast(typeRef[NodeIdWrapper], generator.load(nodeIdVar)), nodeId))

  override def node(nodeIdVar: String, codeGenType: CodeGenType) =
    generator.load(nodeIdVar)

  override def nullablePrimitive(varName: String, codeGenType: CodeGenType, onSuccess: Expression) = codeGenType match {
    case CypherCodeGenType(CTNode, LongType) | CypherCodeGenType(CTRelationship, LongType) =>
      ternary(
        equal(nullValue(codeGenType), generator.load(varName)),
        nullValue(codeGenType),
        onSuccess)
    case _ => ternary(Expression.isNull(generator.load(varName)), constant(null), onSuccess)
  }

  override def nullableReference(varName: String, codeGenType: CodeGenType, onSuccess: Expression) = codeGenType match {
    case CypherCodeGenType(CTNode, LongType) | CypherCodeGenType(CTRelationship, LongType) =>
      ternary(
        equal(nullValue(codeGenType), generator.load(varName)),
        constant(null),
        onSuccess)
    case _ => ternary(Expression.isNull(generator.load(varName)), constant(null), onSuccess)
  }

  override def materializeRelationship(relIdVar: String, codeGenType: CodeGenType) =
    if (codeGenType.isPrimitive)
      invoke(nodeManager, newRelationshipProxyById, generator.load(relIdVar))
    else
      invoke(nodeManager, newRelationshipProxyById,
        invoke(cast(typeRef[RelationshipIdWrapper], generator.load(relIdVar)), relId))

  override def relationship(relIdVar: String, codeGenType: CodeGenType) =
    generator.load(relIdVar)

  override def materializeAny(variable: String) =
    invoke(materializeAnyResult, nodeManager, generator.load(variable))

  override def trace[V](planStepId: String, maybeSuffix: Option[String] = None)(block: MethodStructure[Expression] => V) = if (!tracing) block(this)
  else {
    val suffix = maybeSuffix.map("_" +_ ).getOrElse("")
    val eventName = s"event_$planStepId${suffix}"
    generator.assign(typeRef[QueryExecutionEvent], eventName, traceEvent(planStepId))
    val result = block(copy(events = eventName :: events, generator = generator))
    generator.expression(invoke(generator.load(eventName), method[QueryExecutionEvent, Unit]("close")))
    result
  }

  private def traceEvent(planStepId: String) =
    invoke(tracer, executeOperator,
           getStatic(FieldReference.staticField(generator.owner(), typeRef[Id], planStepId)))

  override def incrementDbHits() = if (tracing) generator.expression(invoke(loadEvent, Methods.dbHit))

  override def incrementRows() = if (tracing) generator.expression(invoke(loadEvent, Methods.row))

  private def loadEvent = generator
    .load(events.headOption.getOrElse(throw new IllegalStateException("no current trace event")))

  override def expectParameter(key: String, variableName: String, codeGenType: CodeGenType) = {
    using(
      generator.ifStatement(not(invoke(params, mapContains, constant(key))))) { block =>
      block.throwException(parameterNotFoundException(key))
    }
    val invokeLoadParameter = invoke(loadParameter, invoke(params, mapGet, constantExpression(key)))
    generator.assign(lowerType(codeGenType), variableName,
      // We assume the value in the parameter map will always be boxed, so if we are declaring
      // a primitive variable we need to force it to be unboxed
      codeGenType match {
        case CodeGenType.primitiveNode =>
          unbox(Expression.cast(typeRef[NodeIdWrapper], invokeLoadParameter),
            CypherCodeGenType(symbols.CTNode, ReferenceType))
        case CodeGenType.primitiveRel =>
          unbox(Expression.cast(typeRef[RelationshipIdWrapper], invokeLoadParameter),
            CypherCodeGenType(symbols.CTRelationship, ReferenceType))
        case CodeGenType.primitiveInt =>
          Expression.unbox(Expression.cast(typeRef[java.lang.Long], invokeLoadParameter))
        case CodeGenType.primitiveFloat =>
          Expression.unbox(Expression.cast(typeRef[java.lang.Double], invokeLoadParameter))
        case CodeGenType.primitiveBool =>
          Expression.unbox(Expression.cast(typeRef[java.lang.Boolean], invokeLoadParameter))
        case CypherCodeGenType(_, ListReferenceType(repr)) if RepresentationType.isPrimitive(repr) =>
          asPrimitiveStream(invokeLoadParameter, codeGenType)
        case _ => invokeLoadParameter
      }
    )
  }

  override def mapGetExpression(mapName: String, key: String): Expression = {
    invoke(cast(typeRef[java.util.Map[String, Object]], generator.load(mapName)), mapGet, constantExpression(key))
  }

  override def constantExpression(value: AnyRef) = constant(value)

  override def notExpression(value: Expression): Expression = not(value)

  override def threeValuedNotExpression(value: Expression): Expression = invoke(Methods.not, value)

  override def threeValuedEqualsExpression(lhs: Expression, rhs: Expression) = invoke(Methods.ternaryEquals, lhs, rhs)

  override def threeValuedPrimitiveEqualsExpression(lhs: Expression, rhs: Expression, codeGenType: CodeGenType) = {
    // This is only for primitive nodes and relationships
    assert(codeGenType == CodeGenType.primitiveNode || codeGenType == CodeGenType.primitiveRel)
    ternary(
      or(equal(nullValue(codeGenType), lhs),
         equal(nullValue(codeGenType), rhs)),
      constant(null),
      box(equal(lhs, rhs))
    )
  }

  override def equalityExpression(lhs: Expression, rhs: Expression, codeGenType: CodeGenType) =
    if (codeGenType.isPrimitive) equal(lhs, rhs)
    else invoke(lhs, Methods.equals, rhs)

  override def primitiveEquals(lhs: Expression, rhs: Expression): Expression = equal(lhs, rhs)

  override def orExpression(lhs: Expression, rhs: Expression) = or(lhs, rhs)

  override def threeValuedOrExpression(lhs: Expression, rhs: Expression) = invoke(Methods.or, lhs, rhs)

  override def markAsNull(varName: String, codeGenType: CodeGenType) =
    generator.assign(lowerType(codeGenType), varName, nullValue(codeGenType))


  override def isNull(varName: String, codeGenType: CodeGenType) =
    equal(nullValue(codeGenType), generator.load(varName))

  override def notNull(varName: String, codeGenType: CodeGenType) = not(isNull(varName, codeGenType))

  override def box(expression: Expression, codeGenType: CodeGenType) = codeGenType match {
    case CypherCodeGenType(symbols.CTNode, IntType) =>
      createNewInstance(typeRef[NodeIdWrapperImpl], (typeRef[Long], expression))
    case CypherCodeGenType(symbols.CTRelationship, IntType) =>
      createNewInstance(typeRef[RelationshipIdWrapperImpl], (typeRef[Long], expression))
    case _ => Expression.box(expression)
  }

  override def unbox(expression: Expression, codeGenType: CodeGenType) = codeGenType match {
    case c if c.isPrimitive => expression
    case CypherCodeGenType(symbols.CTNode, ReferenceType) => invoke(Methods.unboxNode, expression)
    case CypherCodeGenType(symbols.CTRelationship, ReferenceType) => invoke(Methods.unboxRel, expression)
    case _ => Expression.unbox(expression)
  }

  override def toFloat(expression: Expression) = toDouble(expression)

  override def nodeGetRelationshipsWithDirection(iterVar: String, nodeVar: String, direction: SemanticDirection) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local, invoke(readOperations, Methods.nodeGetRelationshipsWithDirection, body.load(nodeVar), dir(direction)))
    }
  }

  override def nodeGetRelationshipsWithDirectionAndTypes(iterVar: String, nodeVar: String, direction: SemanticDirection,
                                                         typeVars: Seq[String]) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local, invoke(readOperations, Methods.nodeGetRelationshipsWithDirectionAndTypes,
                                body.load(nodeVar), dir(direction),
                                newArray(typeRef[Int], typeVars.map(body.load): _*)))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection,
                                       toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local, invoke(Methods.allConnectingRelationships,
                                readOperations, body.load(fromNode), dir(direction),
                                body.load(toNode)))
    }
  }

  override def connectingRelationships(iterVar: String, fromNode: String, direction: SemanticDirection,
                                       typeVars: Seq[String], toNode: String) = {
    val local = generator.declare(typeRef[RelationshipIterator], iterVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local, invoke(Methods.connectingRelationships, readOperations, body.load(fromNode), dir(direction),
                                body.load(toNode),
                                newArray(typeRef[Int], typeVars.map(body.load): _*)))
    }
  }

  override def loadVariable(varName: String) = generator.load(varName)

  override def addExpression(lhs: Expression, rhs: Expression) = math(Methods.mathAdd, lhs, rhs)

  override def subtractExpression(lhs: Expression, rhs: Expression) = math(Methods.mathSub, lhs, rhs)

  override def multiplyExpression(lhs: Expression, rhs: Expression) = math(Methods.mathMul, lhs, rhs)

  override def divideExpression(lhs: Expression, rhs: Expression) = math(Methods.mathDiv, lhs, rhs)

  override def modulusExpression(lhs: Expression, rhs: Expression) = math(Methods.mathMod, lhs, rhs)

  private def math(method: MethodReference, lhs: Expression, rhs: Expression): Expression =
    invoke(method, lhs, rhs)

  private val getOrLoadReadOperations: MethodReference =
    methodReference(generator.owner(), typeRef[ReadOperations], "getOrLoadReadOperations")

  private def readOperations: Expression =
    invoke(generator.self(), getOrLoadReadOperations)

  private def nodeManager = get(generator.self(), fields.entityAccessor)

  private def resultRow = generator.load("row")

  private def tracer = get(generator.self(), fields.tracer)

  private def params = get(generator.self(), fields.params)

  private def parameterNotFoundException(key: String) =
    invoke(newInstance(typeRef[ParameterNotFoundException]),
           MethodReference.constructorReference(typeRef[ParameterNotFoundException], typeRef[String]),
           constant(s"Expected a parameter named $key"))

  private def dir(dir: SemanticDirection): Expression = dir match {
    case SemanticDirection.INCOMING => Templates.incoming
    case SemanticDirection.OUTGOING => Templates.outgoing
    case SemanticDirection.BOTH => Templates.both
  }

  override def asList(values: Seq[Expression]) = Templates.asList[Object](values)

  override def asPrimitiveStream(publicTypeList: Expression, codeGenType: CodeGenType) = {
    codeGenType match {
      case CypherCodeGenType(ListType(CTNode), ListReferenceType(LongType)) =>
        Expression.invoke(methodReference(typeRef[PrimitiveNodeStream], typeRef[PrimitiveNodeStream], "of", typeRef[Object]), publicTypeList)
      case CypherCodeGenType(ListType(CTRelationship), ListReferenceType(LongType)) =>
        Expression.invoke(methodReference(typeRef[PrimitiveRelationshipStream], typeRef[PrimitiveRelationshipStream], "of", typeRef[Object]), publicTypeList)
      case CypherCodeGenType(_, ListReferenceType(LongType)) =>
        // byte[]
        // short[]
        // int[]
        // long[]
        // Object[]
        Expression.invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[LongStream], "toLongStream", typeRef[Object]), publicTypeList)
      case CypherCodeGenType(_, ListReferenceType(FloatType)) =>
        // float[]
        // double[]
        // Object[]
        Expression.invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[DoubleStream], "toDoubleStream", typeRef[Object]), publicTypeList)
      case CypherCodeGenType(_, ListReferenceType(BoolType)) =>
        // boolean[]
        // Object[]
        Expression.invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[IntStream], "toBooleanStream", typeRef[Object]), publicTypeList)
      case _ =>
        throw new IllegalArgumentException(s"CodeGenType $codeGenType not supported as primitive stream")
    }
  }

  override def asPrimitiveStream(values: Seq[Expression], codeGenType: CodeGenType) = {
    codeGenType match {
      case CypherCodeGenType(ListType(CTNode), ListReferenceType(LongType)) =>
        Templates.asPrimitiveNodeStream(values)
      case CypherCodeGenType(ListType(CTRelationship), ListReferenceType(LongType)) =>
        Templates.asPrimitiveRelationshipStream(values)
      case CypherCodeGenType(_, ListReferenceType(LongType)) =>
        Templates.asLongStream(values)
      case CypherCodeGenType(_, ListReferenceType(FloatType)) =>
        Templates.asDoubleStream(values)
      case CypherCodeGenType(_, ListReferenceType(BoolType)) =>
        // There are no primitive streams for booleans, so we use an IntStream with value conversions
        // 0 = false, 1 = true
        Templates.asIntStream(values.map(Expression.ternary(_, Expression.constant(1), Expression.constant(0))))
      case _ =>
        throw new IllegalArgumentException(s"CodeGenType $codeGenType not supported as primitive stream")
    }
  }

  override def declarePrimitiveIterator(name: String, iterableCodeGenType: CodeGenType): Unit = {
    val variable = iterableCodeGenType match {
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(LongType)) =>
        generator.declare(typeRef[PrimitiveIterator.OfLong], name)
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(FloatType)) =>
        generator.declare(typeRef[PrimitiveIterator.OfDouble], name)
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(BoolType)) =>
        generator.declare(typeRef[PrimitiveIterator.OfInt], name)
      case _ => throw new IllegalArgumentException(s"CodeGenType $iterableCodeGenType not supported as primitive iterator")
    }
    locals += (name -> variable)
  }

  override def primitiveIteratorFrom(iterable: Expression, iterableCodeGenType: CodeGenType) =
    iterableCodeGenType match {
      case CypherCodeGenType(symbols.ListType(symbols.CTNode), ListReferenceType(LongType)) =>
        invoke(cast(typeRef[PrimitiveNodeStream], iterable), method[PrimitiveNodeStream, PrimitiveIterator.OfLong]("primitiveIterator"))
      case CypherCodeGenType(symbols.ListType(symbols.CTRelationship), ListReferenceType(LongType)) =>
        invoke(cast(typeRef[PrimitiveRelationshipStream], iterable), method[PrimitiveRelationshipStream, PrimitiveIterator.OfLong]("primitiveIterator"))
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(LongType)) =>
        invoke(cast(typeRef[LongStream], iterable), method[LongStream, PrimitiveIterator.OfLong]("iterator"))
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(FloatType)) =>
        invoke(cast(typeRef[DoubleStream], iterable), method[DoubleStream, PrimitiveIterator.OfDouble]("iterator"))
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(BoolType)) =>
        invoke(cast(typeRef[IntStream], iterable), method[IntStream, PrimitiveIterator.OfInt]("iterator"))
      case _ => throw new IllegalArgumentException(s"CodeGenType $iterableCodeGenType not supported as primitive iterator")
    }

  override def primitiveIteratorNext(iterator: Expression, iterableCodeGenType: CodeGenType) =
    iterableCodeGenType match {
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(LongType)) =>
        // Nodes & Relationships are also covered by this case
        invoke(iterator, method[PrimitiveIterator.OfLong, Long]("nextLong"))
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(FloatType)) =>
        invoke(iterator, method[PrimitiveIterator.OfDouble, Double]("nextDouble"))
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(BoolType)) =>
        not(equal(constant(0), invoke(iterator, method[PrimitiveIterator.OfInt, Int]("nextInt"))))
      case _ => throw new IllegalArgumentException(s"CodeGenType $iterableCodeGenType not supported as primitive iterator")
    }

  override def primitiveIteratorHasNext(iterator: Expression, iterableCodeGenType: CodeGenType) =
    iterableCodeGenType match {
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(LongType)) =>
        // Nodes & Relationships are also covered by this case
        invoke(iterator, method[PrimitiveIterator.OfLong, Long]("hasNext"))
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(FloatType)) =>
        invoke(iterator, method[PrimitiveIterator.OfDouble, Double]("hasNext"))
      case CypherCodeGenType(symbols.ListType(_), ListReferenceType(BoolType)) =>
        invoke(iterator, method[PrimitiveIterator.OfInt, Int]("hasNext"))
      case _ => throw new IllegalArgumentException(s"CodeGenType $iterableCodeGenType not supported as primitive iterator")
    }

  override def declareIterator(name: String): Unit = {
    val variable = generator.declare(typeRef[java.util.Iterator[Object]], name)
    locals += (name -> variable)
  }

  override def iteratorFrom(iterable: Expression) =
    invoke(method[CompiledConversionUtils, java.util.Iterator[_]]("iteratorFrom", typeRef[Object]), iterable)

  override def iteratorNext(iterator: Expression) =
    invoke(iterator, method[java.util.Iterator[_], Object]("next"))

  override def iteratorHasNext(iterator: Expression) =
    invoke(iterator, method[java.util.Iterator[_], Boolean]("hasNext"))

  override def toSet(value: Expression) =
    createNewInstance(typeRef[util.HashSet[Object]], (typeRef[util.Collection[_]], value))

  override def newDistinctSet(name: String, codeGenTypes: Iterable[CodeGenType]) = {
    if (codeGenTypes.size == 1 && codeGenTypes.head.repr == LongType) {
      generator.assign(generator.declare(typeRef[PrimitiveLongSet], name),
                       invoke(method[Primitive, PrimitiveLongSet]("offHeapLongSet")))
      _finalizers.append((_: Boolean) => (block) =>
                           block.expression(
                             invoke(block.load(name), method[PrimitiveLongSet, Unit]("close"))))

    } else {
      generator.assign(generator.declare(typeRef[util.HashSet[Object]], name),
                       createNewInstance(typeRef[util.HashSet[Object]]))
    }
  }

  override def distinctSetIfNotContains(name: String, structure: Map[String,(CodeGenType,Expression)])
                                       (block: MethodStructure[Expression] => Unit) = {
    if (structure.size == 1 && structure.head._2._1.repr == LongType) {
      val (_, (_, value)) = structure.head
      using(generator.ifStatement(not(invoke(generator.load(name),
                                             method[PrimitiveLongSet, Boolean]("contains", typeRef[Long]), value)))) { body =>
        body.expression(pop(invoke(generator.load(name), method[PrimitiveLongSet, Boolean]("add", typeRef[Long]), value)))
        block(copy(generator = body))
      }
    } else {
      val tmpName = context.namer.newVarName()
      newUniqueAggregationKey(tmpName, structure)
      using(generator.ifStatement(not(invoke(generator.load(name), Methods.setContains, generator.load(tmpName))))) { body =>
        body.expression(pop(invoke(loadVariable(name), Methods.setAdd, generator.load(tmpName))))
        block(copy(generator = body))
      }
    }
  }
  override def distinctSetIterate(name: String, keyTupleDescriptor: HashableTupleDescriptor)
                                    (block: (MethodStructure[Expression]) => Unit) = {
    val key = keyTupleDescriptor.structure
    if (key.size == 1 && key.head._2.repr == LongType) {
      val (keyName, keyType) = key.head
      val localName = context.namer.newVarName()
      val variable = generator.declare(typeRef[PrimitiveLongIterator], localName)
      generator.assign(variable, invoke(generator.load(name),
                                        method[PrimitiveLongSet, PrimitiveLongIterator]("iterator")))
      using(generator.whileLoop(
        invoke(generator.load(localName), method[PrimitiveLongIterator, Boolean]("hasNext")))) { body =>

        body.assign(body.declare(typeRef[Long], keyName),
                    invoke(body.load(localName), method[PrimitiveLongIterator, Long]("next")))
        block(copy(generator = body))
      }
    } else {
      val localName = context.namer.newVarName()
      val next = context.namer.newVarName()
      val variable = generator.declare(typeRef[java.util.Iterator[Object]], localName)
      val keyStruct = aux.hashableTypeReference(keyTupleDescriptor)
      generator.assign(variable,
                       invoke(generator.load(name),method[util.HashSet[Object], util.Iterator[Object]]("iterator")))
      using(generator.whileLoop(
        invoke(generator.load(localName),
               method[java.util.Iterator[Object], Boolean]("hasNext")))) { body =>
        body.assign(body.declare(keyStruct, next),
                    cast(keyStruct,
                         invoke(body.load(localName),
                                method[util.Iterator[Object], Object]("next"))))
        key.foreach {
          case (keyName, keyType) =>

            body.assign(body.declare(lowerType(keyType), keyName),
                        Expression.get(body.load(next),
                                       FieldReference.field(keyStruct, lowerType(keyType), keyName)))
        }
        block(copy(generator = body))
      }
    }
  }

  override def newUniqueAggregationKey(varName: String, structure: Map[String, (CodeGenType, Expression)]) = {
    val typ = aux.hashableTypeReference(HashableTupleDescriptor(structure.map {
      case (n, (t, _)) => n -> t
    }))
    val local = generator.declare(typ, varName)
    locals += varName -> local
    generator.assign(local, createNewInstance(typ))
    structure.foreach {
      case (n, (t, e)) =>
        val field = FieldReference.field(typ, lowerType(t), n)
        generator.put(generator.load(varName), field, e)
    }
    if (structure.size == 1) {
      generator.put(generator.load(varName), FieldReference.field(typ, typeRef[Int], "hashCode"),
                    invoke(method[CompiledEquivalenceUtils, Int]("hashCode", typeRef[Object]),
                           box(structure.values.head._2)))
    } else {
      generator.put(generator.load(varName), FieldReference.field(typ, typeRef[Int], "hashCode"),
                    invoke(method[CompiledEquivalenceUtils, Int]("hashCode", typeRef[Array[Object]]),
                           newArray(typeRef[Object], structure.values.map(_._2).toSeq: _*)))
    }
  }

  override def newAggregationMap(name: String, keyTypes: IndexedSeq[CodeGenType]) = {
    if (keyTypes.size == 1 && keyTypes.head.repr == LongType) {
      generator.assign(generator.declare(typeRef[PrimitiveLongLongMap], name),
                       invoke(method[Primitive, PrimitiveLongLongMap]("offHeapLongLongMap")))
      _finalizers.append((_: Boolean) => (block) =>
                           block.expression(
                             invoke(block.load(name), method[PrimitiveLongLongMap, Unit]("close"))))
    } else {
      val local = generator.declare(typeRef[util.HashMap[Object, java.lang.Long]], name)
      generator.assign(local, createNewInstance(typeRef[util.HashMap[Object, java.lang.Long]]))
    }
  }

  override def newMapOfSets(name: String, keyTypes: IndexedSeq[CodeGenType], elementType: CodeGenType) = {

    val setType = if (elementType.repr == LongType) typeRef[PrimitiveLongSet] else typeRef[util.HashSet[Object]]
    if (keyTypes.size == 1 && keyTypes.head.repr == LongType) {
      val typ =  TypeReference.parameterizedType(typeRef[PrimitiveLongObjectMap[_]], setType)
      generator.assign(generator.declare(typ, name),
                       invoke(method[Primitive, PrimitiveLongObjectMap[PrimitiveLongSet]]("longObjectMap")))

    } else {
      val typ =  TypeReference.parameterizedType(typeRef[util.HashMap[_,_]], typeRef[Object], setType)

      generator.assign(generator.declare(typ, name ), createNewInstance(typ))
    }
  }

  override def allocateSortTable(name: String, tableDescriptor: SortTableDescriptor, count: Expression): Unit = {
    val tableType = sortTableType(tableDescriptor)
    val localVariable = generator.declare(tableType, name)
    locals += name -> localVariable
    generator.assign(localVariable, createNewInstance(tableType, (typeRef[Int], invoke(Methods.mathCastToInt, box(count)))))
  }

  override def sortTableAdd(name: String, tableDescriptor: SortTableDescriptor, value: Expression): Unit = {
    val tableType = sortTableType(tableDescriptor)
    generator.expression(pop(invoke(generator.load(name),
      methodReference(tableType, typeRef[Boolean], "add", typeRef[Object]),
      box(value))))
  }

  override def sortTableSort(name: String, tableDescriptor: SortTableDescriptor): Unit = {
    val tableType = sortTableType(tableDescriptor)
    generator.expression(invoke(generator.load(name),
      methodReference(tableType, typeRef[Unit], "sort")))
  }

  override def sortTableIterate(tableName: String, tableDescriptor: SortTableDescriptor,
                                varNameToField: Map[String, String])
                               (block: (MethodStructure[Expression]) => Unit): Unit = {
    val tupleDescriptor = tableDescriptor.tupleDescriptor
    val tupleType = aux.typeReference(tupleDescriptor)
    val elementName = context.namer.newVarName()

    using(generator.forEach(Parameter.param(tupleType, elementName), generator.load(tableName))) { body =>
      varNameToField.foreach {
        case (localName, fieldName) =>
          val fieldType = lowerType(tupleDescriptor.structure(fieldName))

          val localVariable: LocalVariable = body.declare(fieldType, localName)
          locals += localName -> localVariable

          body.assign(localVariable,
            get(body.load(elementName),
              FieldReference.field(tupleType, fieldType, fieldName))
          )
      }
      block(copy(generator = body))
    }
  }

  override def aggregationMapGet(mapName: String, valueVarName: String, key: Map[String, (CodeGenType, Expression)],
                                 keyVar: String) = {
    val local = generator.declare(typeRef[Long], valueVarName)
    locals += valueVarName -> local

    if (key.size == 1 && key.head._2._1.repr == LongType) {
      val (_, (_, keyExpression)) = key.head
      generator.assign(local, invoke(generator.load(mapName), method[PrimitiveLongLongMap, Long]("get", typeRef[
        Long]), keyExpression))
      using(generator.ifStatement(equal(generator.load(valueVarName), constant(Long.box(-1L))))) { body =>
        body.assign(local, constant(Long.box(0L)))
      }
    } else {
      newUniqueAggregationKey(keyVar, key)
      generator.assign(local, unbox(
        cast(typeRef[java.lang.Long],
             invoke(generator.load(mapName),
                    method[util.HashMap[Object, java.lang.Long], Object]("getOrDefault", typeRef[Object],
                                                                         typeRef[Object]),
                    generator.load(keyVar), box(constant(Long.box(0L))))),
        CypherCodeGenType(symbols.CTInteger, ReferenceType)))
    }
  }

  override def checkDistinct(name: String, key: Map[String, (CodeGenType, Expression)], keyVar: String,
                             value: Expression, valueType: CodeGenType)(block: MethodStructure[Expression] => Unit) = {
    if (key.size == 1 && key.head._2._1.repr == LongType) {
      val (_, (_, keyExpression)) = key.head
      val tmp = context.namer.newVarName()

      if (valueType.repr == LongType) {
        val localVariable = generator.declare(typeRef[PrimitiveLongSet], tmp)
        generator.assign(localVariable,
                         cast(typeRef[PrimitiveLongSet],
                              invoke(generator.load(name),
                                     method[PrimitiveLongObjectMap[Object], Object]("get", typeRef[Long]),
                                     keyExpression)))


        using(generator.ifStatement(Expression.isNull(generator.load(tmp)))) { inner =>
          inner.assign(localVariable, invoke(method[Primitive, PrimitiveLongSet]("longSet")))
          inner.expression(pop(invoke(generator.load(name),
                                      method[PrimitiveLongObjectMap[Object], Object]("put", typeRef[Long],
                                                                                     typeRef[Object]),
                                      keyExpression, inner.load(tmp))))
        }
        using(generator.ifStatement(not(invoke(generator.load(tmp),
                                               method[PrimitiveLongSet, Boolean]("contains", typeRef[Long]),
                                               value)))) { inner =>
          block(copy(generator = inner))
        }
        generator.expression(pop(invoke(generator.load(tmp),
                                        method[PrimitiveLongSet, Boolean]("add", typeRef[Long]), value)))
      } else {
        val localVariable = generator.declare(typeRef[util.HashSet[Object]], tmp)
        generator.assign(localVariable,
                         cast(typeRef[util.HashSet[Object]],
                              invoke(generator.load(name),
                                     method[PrimitiveLongObjectMap[Object], Object]("get", typeRef[Long]),
                                     keyExpression)))
        using(generator.ifStatement(Expression.isNull(generator.load(tmp)))) { inner =>
          inner.assign(localVariable, createNewInstance(typeRef[util.HashSet[Object]]))
          inner.expression(pop(invoke(generator.load(name),
                                      method[PrimitiveLongObjectMap[Object], Object]("put", typeRef[Long],
                                                                                     typeRef[Object]),
                                      keyExpression, inner.load(tmp))))
        }
        using(generator.ifStatement(not(invoke(generator.load(tmp),
                                               method[util.HashSet[Object], Boolean]("contains", typeRef[Object]),
                                               value)))) { inner =>
          block(copy(generator = inner))
        }
        generator.expression(pop(invoke(generator.load(tmp),
                                        method[util.HashSet[Object], Boolean]("add", typeRef[Object]), value)))
      }
    } else {
      val setVar = context.namer.newVarName()
      if (valueType.repr == LongType) {
        val localVariable = generator.declare(typeRef[PrimitiveLongSet], setVar)
        if (!locals.contains(keyVar)) newUniqueAggregationKey(keyVar, key)

        generator.assign(localVariable,
                         cast(typeRef[PrimitiveLongSet],
                              invoke(generator.load(name),
                                     method[util.HashMap[Object, PrimitiveLongSet], Object]("get", typeRef[Object]),
                                     generator.load(keyVar))))
        using(generator.ifStatement(Expression.isNull(generator.load(setVar)))) { inner =>

          inner.assign(localVariable, invoke(method[Primitive, PrimitiveLongSet]("longSet")))
          inner.expression(pop(invoke(generator.load(name),
                                      method[util.HashMap[Object, PrimitiveLongSet], Object]("put", typeRef[Object],
                                                                                             typeRef[Object]),
                                      generator.load(keyVar), inner.load(setVar))))
        }

        using(generator.ifStatement(not(invoke(generator.load(setVar),
                                               method[PrimitiveLongSet, Boolean]("contains", typeRef[Long]),
                                               value)))) { inner =>
          block(copy(generator = inner))
          inner.expression(pop(invoke(generator.load(setVar),
                                      method[PrimitiveLongSet, Boolean]("add", typeRef[Long]),
                                      value)))
        }
      } else {
        val localVariable = generator.declare(typeRef[util.HashSet[Object]], setVar)
        if (!locals.contains(keyVar)) newUniqueAggregationKey(keyVar, key)

        generator.assign(localVariable,
                         cast(typeRef[util.HashSet[Object]],
                              invoke(generator.load(name),
                                     method[util.HashMap[Object, util.HashSet[Object]], Object]("get", typeRef[Object]),
                                     generator.load(keyVar))))
        using(generator.ifStatement(Expression.isNull(generator.load(setVar)))) { inner =>

          inner.assign(localVariable, createNewInstance(typeRef[util.HashSet[Object]]))
          inner.expression(pop(invoke(generator.load(name),
                                      method[util.HashMap[Object, util.HashSet[Object]], Object]("put", typeRef[Object],
                                                                                                 typeRef[Object]),
                                      generator.load(keyVar), inner.load(setVar))))
        }
        val valueVar = context.namer.newVarName()
        newUniqueAggregationKey(valueVar, Map(context.namer.newVarName() -> (valueType -> value)))

        using(generator.ifStatement(not(invoke(generator.load(setVar),
                                               method[util.HashSet[Object], Boolean]("contains", typeRef[Object]),
                                               generator.load(valueVar))))) { inner =>
          block(copy(generator = inner))
          inner.expression(pop(invoke(generator.load(setVar),
                                      method[util.HashSet[Object], Boolean]("add", typeRef[Object]),
                                      generator.load(valueVar))))
        }
      }
    }
  }

  override def aggregationMapPut(name: String, key: Map[String, (CodeGenType, Expression)], keyVar: String,
                                 value: Expression) = {
    if (key.size == 1 && key.head._2._1.repr == LongType) {
      val (_, (_, keyExpression)) = key.head
      generator.expression(pop(invoke(generator.load(name),
                                      method[PrimitiveLongLongMap, Long]("put", typeRef[Long], typeRef[Long]),
                                      keyExpression, value)))
    } else {

      if (!locals.contains(keyVar)) newUniqueAggregationKey(keyVar, key)
      generator.expression(pop(invoke(generator.load(name),
                                      method[util.HashMap[Object, java.lang.Long], Object]("put", typeRef[Object],
                                                                                           typeRef[Object]),
                                      generator.load(keyVar), box(value))))
    }
  }

  override def aggregationMapIterate(name: String, keyTupleDescriptor: HashableTupleDescriptor, valueVar: String)
                                    (block: (MethodStructure[Expression]) => Unit) = {
    val key = keyTupleDescriptor.structure
    if (key.size == 1 && key.head._2.repr == LongType) {
      val (keyName, keyType) = key.head
      val localName = context.namer.newVarName()
      val variable = generator.declare(typeRef[PrimitiveLongIterator], localName)
      generator.assign(variable, invoke(generator.load(name),
                                        method[PrimitiveLongLongMap, PrimitiveLongIterator]("iterator")))
      using(generator.whileLoop(
        invoke(generator.load(localName), method[PrimitiveLongIterator, Boolean]("hasNext")))) { body =>

        body.assign(body.declare(typeRef[Long], keyName),
                    invoke(body.load(localName), method[PrimitiveLongIterator, Long]("next")))
        body.assign(body.declare(typeRef[Long], valueVar),
                    invoke(body.load(name), method[PrimitiveLongLongMap, Long]("get", typeRef[Long]),
                           body.load(keyName)))
        block(copy(generator = body))
      }
    } else {
      val localName = context.namer.newVarName()
      val next = context.namer.newVarName()
      val variable = generator
        .declare(typeRef[java.util.Iterator[java.util.Map.Entry[Object, java.lang.Long]]], localName)
      val keyStruct = aux.hashableTypeReference(keyTupleDescriptor)
      generator.assign(variable,
                       invoke(invoke(generator.load(name),
                                     method[util.HashMap[Object, java.lang.Long],
                                       java.util.Set[java.util.Map.Entry[Object, java.lang.Long]]]("entrySet")),
                              method[java.util.Set[java.util.Map.Entry[Object, java.lang.Long]], java.util.Iterator[java.util.Map.Entry[Object, java.lang.Long]]](
                                "iterator")))
      using(generator.whileLoop(
        invoke(generator.load(localName),
               method[java.util.Iterator[java.util.Map.Entry[Object, java.lang.Long]], Boolean]("hasNext")))) { body =>
        body.assign(body.declare(typeRef[java.util.Map.Entry[Object, java.lang.Long]], next),
                    cast(typeRef[util.Map.Entry[Object, java.lang.Long]],
                         invoke(body.load(localName),
                                method[java.util.Iterator[java.util.Map.Entry[Object, java.lang.Long]], Object](
                                  "next"))))
        key.foreach {
          case (keyName, keyType) =>

            body.assign(body.declare(lowerType(keyType), keyName),
                        Expression.get(
                          cast(keyStruct,
                               invoke(body.load(next),
                                      method[java.util.Map.Entry[Object, java.lang.Long], Object]("getKey"))),
                          FieldReference.field(keyStruct, lowerType(keyType), keyName)))
        }

        body.assign(body.declare(typeRef[Long], valueVar),
                    unbox(cast(typeRef[java.lang.Long],
                               invoke(body.load(next),
                                      method[java.util.Map.Entry[Object, java.lang.Long], Object]("getValue"))),
                          CypherCodeGenType(symbols.CTInteger, ReferenceType)))
        block(copy(generator = body))
      }
    }
  }

  override def castToCollection(value: Expression) = invoke(Methods.toCollection, value)

  override def asMap(map: Map[String, Expression]) = {
    invoke(Methods.createMap,
           newArray(typeRef[Object], map.flatMap {
             case (key, value) => Seq(constant(key), value)
           }.toSeq: _*))
  }

  override def invokeMethod(resultType: JoinTableType, resultVar: String, methodName: String)
                           (block: MethodStructure[Expression] => Unit) = {
    val returnType: TypeReference = joinTableType(resultType)
    generator.assign(returnType, resultVar,
                     invoke(generator.self(), methodReference(generator.owner(), returnType, methodName)))
    using(generator.classGenerator().generateMethod(returnType, methodName)) { body =>
      block(copy(generator = body, events = List.empty))
      body.returns(body.load(resultVar))
    }
  }

  override def allocateProbeTable(tableVar: String, tableType: JoinTableType) =
    generator.assign(joinTableType(tableType), tableVar, allocate(tableType))

  private def joinTableType(resultType: JoinTableType): TypeReference = {
    val returnType = resultType match {
      case LongToCountTable =>
        typeRef[PrimitiveLongIntMap]
      case LongsToCountTable =>
        parameterizedType(classOf[util.HashMap[_, _]], classOf[CompositeKey], classOf[java.lang.Integer])
      case LongToListTable(tupleDescriptor, _) =>
        parameterizedType(classOf[PrimitiveLongObjectMap[_]],
                          parameterizedType(classOf[util.ArrayList[_]],
                                            aux.typeReference(tupleDescriptor)))
      case LongsToListTable(tupleDescriptor, _) =>
        parameterizedType(classOf[util.HashMap[_, _]],
                          typeRef[CompositeKey],
                          parameterizedType(classOf[util.ArrayList[_]],
                                            aux.typeReference(tupleDescriptor)))
    }
    returnType
  }

  private def allocate(resultType: JoinTableType): Expression = resultType match {
    case LongToCountTable => Templates.newCountingMap
    case LongToListTable(_, _) => Templates.newLongObjectMap
    case LongsToCountTable => createNewInstance(joinTableType(LongsToCountTable))
    case typ: LongsToListTable => createNewInstance(joinTableType(typ))
  }

  override def updateProbeTableCount(tableVar: String, tableType: CountingJoinTableType,
                                     keyVars: Seq[String]) = tableType match {
    case LongToCountTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val countName = context.namer.newVarName()
      generator.assign(typeRef[Int], countName,
                       invoke(generator.load(tableVar), countingTableGet, generator.load(keyVar)))
      generator.expression(
        pop(
          invoke(generator.load(tableVar), countingTablePut, generator.load(keyVar),
                 ternary(
                   equal(generator.load(countName), getStatic(staticField[LongKeyIntValueTable, Int]("NULL"))),
                   constant(1), add(generator.load(countName), constant(1))))))

    case LongsToCountTable =>
      val countName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      generator.assign(typeRef[CompositeKey], keyName,
                       invoke(compositeKey,
                              newArray(typeRef[Long], keyVars.map(generator.load): _*)))
      generator.assign(typeRef[java.lang.Integer], countName,
                       cast(typeRef[java.lang.Integer],
                            invoke(generator.load(tableVar), countingTableCompositeKeyGet,
                                   generator.load(keyName))
                       ))
      generator.expression(
        pop(
          invoke(generator.load(tableVar), countingTableCompositeKeyPut,
                 generator.load(keyName),
                 ternary(Expression.isNull(generator.load(countName)),
                               box(constant(1)), box(add(invoke(generator.load(countName), unboxInteger), constant(1)))))))
  }

  override def probe(tableVar: String, tableType: JoinTableType, keyVars: Seq[String])
                    (block: MethodStructure[Expression] => Unit) = tableType match {
    case LongToCountTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      generator.assign(times, invoke(generator.load(tableVar), countingTableGet, generator.load(keyVar)))
      using(generator.whileLoop(gt(times, constant(0)))) { body =>
        block(copy(generator = body))
        body.assign(times, subtract(times, constant(1)))
      }
    case LongsToCountTable =>
      val times = generator.declare(typeRef[Int], context.namer.newVarName())
      val intermediate = generator.declare(typeRef[java.lang.Integer], context.namer.newVarName())
      generator.assign(intermediate,
                       cast(typeRef[Integer],
                            invoke(generator.load(tableVar),
                                   countingTableCompositeKeyGet,
                                   invoke(compositeKey,
                                          newArray(typeRef[Long],
                                                   keyVars.map(generator.load): _*)))))
      generator.assign(times,
                       ternary(
                         Expression.isNull(intermediate),
                         constant(-1),
                         invoke(intermediate, unboxInteger)))

      using(generator.whileLoop(gt(times, constant(0)))) { body =>
        block(copy(generator = body))
        body.assign(times, subtract(times, constant(1)))
      }

    case tableType@LongToListTable(tupleDescriptor, localVars) =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head

      val hashTable = extractHashTable(tableType)
      // generate the code
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()
      generator.assign(list, invoke(generator.load(tableVar), hashTable.get, generator.load(keyVar)))
      using(generator.ifStatement(Expression.notNull(list))) { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (l, f) =>
              val fieldType = lowerType(tupleDescriptor.structure(f))
              forEach.assign(fieldType, l, get(forEach.load(elementName),
                                               field(tupleDescriptor, f)))
          }
          block(copy(generator = forEach))
        }
      }

    case tableType@LongsToListTable(tupleDescriptor, localVars) =>
      val hashTable = extractHashTable(tableType)
      val list = generator.declare(hashTable.listType, context.namer.newVarName())
      val elementName = context.namer.newVarName()

      generator.assign(list,
                       cast(hashTable.listType,
                            invoke(generator.load(tableVar), hashTable.get,
                                   invoke(compositeKey,
                                          newArray(typeRef[Long],
                                                   keyVars.map(generator.load): _*))
                            )))
      using(generator.ifStatement(Expression.notNull(list))) { onTrue =>
        using(onTrue.forEach(Parameter.param(hashTable.valueType, elementName), list)) { forEach =>
          localVars.foreach {
            case (l, f) =>
              val fieldType = lowerType(tupleDescriptor.structure(f))
              forEach.assign(fieldType, l, get(forEach.load(elementName),
                                               field(tupleDescriptor, f)))
          }
          block(copy(generator = forEach))
        }
      }
  }

  override def putField(tupleDescriptor: TupleDescriptor, value: Expression,
                        fieldName: String,
                        localVar: String) = {
    generator.put(value,
      field(tupleDescriptor, fieldName),
      generator.load(localVar))
  }

  override def updateProbeTable(tupleDescriptor: TupleDescriptor, tableVar: String,
                                tableType: RecordingJoinTableType,
                                keyVars: Seq[String], element: Expression) = tableType match {
    case _: LongToListTable =>
      assert(keyVars.size == 1)
      val keyVar = keyVars.head
      val hashTable = extractHashTable(tableType)
      // generate the code
      val listName = context.namer.newVarName()
      val list = generator.declare(hashTable.listType, listName) // ProbeTable list;
      // list = tableVar.get(keyVar);
      generator.assign(list,
                       cast(hashTable.listType,
                            invoke(
                              generator.load(tableVar), hashTable.get,
                              generator.load(keyVar))))
      using(generator.ifStatement(Expression.isNull(list))) { onTrue => // if (null == list)
        // list = new ListType();
        onTrue.assign(list, createNewInstance(hashTable.listType))
        onTrue.expression(
          // tableVar.put(keyVar, list);
          pop(
            invoke(
              generator.load(tableVar), hashTable.put, generator.load(keyVar), generator.load(listName))))
      }
      // list.add( element );
      generator.expression(
        pop(
          invoke(list, hashTable.add, element))
      )

    case _: LongsToListTable =>
      val hashTable = extractHashTable(tableType)
      // generate the code
      val listName = context.namer.newVarName()
      val keyName = context.namer.newVarName()
      val list = generator.declare(hashTable.listType, listName) // ProbeTable list;
      generator.assign(typeRef[CompositeKey], keyName,
                       invoke(compositeKey,
                              newArray(typeRef[Long], keyVars.map(generator.load): _*)))
      // list = tableVar.get(keyVar);
      generator.assign(list,
                       cast(hashTable.listType,
                            invoke(generator.load(tableVar), hashTable.get, generator.load(keyName))))
      using(generator.ifStatement(Expression.isNull(generator.load(listName)))) { onTrue => // if (null == list)
        // list = new ListType();
        onTrue.assign(list, createNewInstance(hashTable.listType))
        // tableVar.put(keyVar, list);
        onTrue.expression(
          pop(
            invoke(generator.load(tableVar), hashTable.put, generator.load(keyName),
                   generator.load(listName))))
      }
      // list.add( element );
      generator.expression(
        pop(
          invoke(list, hashTable.add, element)))
  }

  override def declareProperty(propertyVar: String) = {
    val localVariable = generator.declare(typeRef[Object], propertyVar)
    locals += (propertyVar -> localVariable)
    generator.assign(localVariable, constant(null))
  }

  override def declareAndInitialize(varName: String, codeGenType: CodeGenType) = {
    val localVariable = generator.declare(lowerType(codeGenType), varName)
    locals += (varName -> localVariable)
    codeGenType match {
      case CypherCodeGenType(symbols.CTInteger, LongType) => constant(0L)
      case CypherCodeGenType(symbols.CTFloat, FloatType) => constant(0.0)
      case CypherCodeGenType(symbols.CTBoolean, BoolType) => constant(false)
      case _ => generator.assign(localVariable, nullValue(codeGenType))
    }
  }

  override def declare(varName: String, codeGenType: CodeGenType) = {
    val localVariable = generator.declare(lowerType(codeGenType), varName)
    locals += (varName -> localVariable)
  }

  override def assign(varName: String, codeGenType: CodeGenType, expression: Expression) = {
    val maybeVariable: Option[LocalVariable] = locals.get(varName)
    if (maybeVariable.nonEmpty) generator.assign(maybeVariable.get, expression)
    else {
      val variable = generator.declare(lowerType(codeGenType), varName)
      locals += (varName -> variable)
      generator.assign(variable, expression)
    }
  }

  override def hasLabel(nodeVar: String, labelVar: String, predVar: String) = {
    val local = locals(predVar)

    handleKernelExceptions(generator, fields.ro, _finalizers) { inner =>
      val invoke = Expression.invoke(readOperations, nodeHasLabel, inner.load(nodeVar), inner.load(labelVar))
      inner.assign(local, invoke)
      generator.load(predVar)
    }
  }

  override def relType(relVar: String, typeVar: String) = {
    val variable = locals(typeVar)
    val typeOfRel = invoke(generator.load(relExtractor(relVar)), typeOf)
    handleKernelExceptions(generator, fields.ro, _finalizers) { inner =>
      val res = invoke(readOperations, relationshipTypeGetName, typeOfRel)
      inner.assign(variable, res)
      generator.load(variable.name())
    }
  }

  override def declareFlag(name: String, initialValue: Boolean) = {
    val localVariable = generator.declare(typeRef[Boolean], name)
    locals += (name -> localVariable)
    generator.assign(localVariable, constant(initialValue))
  }

  override def updateFlag(name: String, newValue: Boolean) = {
    generator.assign(locals(name), constant(newValue))
  }

  override def declarePredicate(name: String) = {
    locals += (name -> generator.declare(typeRef[Boolean], name))
  }

  override def nodeGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local, invoke(readOperations, nodeGetProperty, body.load(nodeIdVar), body.load(propIdVar)))
    }
  }

  override def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local, invoke(readOperations, nodeGetProperty, body.load(nodeIdVar), constant(propId)))
    }
  }

  override def nodeIdSeek(nodeIdVar: String, expression: Expression, codeGenType: CodeGenType)(block: MethodStructure[Expression] => Unit) = {
    codeGenType match {
      case CypherCodeGenType(symbols.CTInteger, LongType) =>
        generator.assign(typeRef[Long], nodeIdVar, expression)
      case CypherCodeGenType(symbols.CTInteger, ReferenceType) =>
        generator.assign(typeRef[Long], nodeIdVar, invoke(Methods.mathCastToLong, expression))
      case _ =>
        throw new IllegalArgumentException(s"CodeGenType $codeGenType can not be converted to long")
    }
    using(generator.ifStatement(and(
      gt(generator.load(nodeIdVar), constant(-1L)),
      invoke(readOperations, nodeExists, generator.load(nodeIdVar))
    ))) { ifBody =>
      block(copy(generator = ifBody))
    }
  }

  override def relationshipGetPropertyForVar(relIdVar: String, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local,
                  invoke(readOperations, relationshipGetProperty, body.load(relIdVar), body.load(propIdVar)))
    }
  }

  override def relationshipGetPropertyById(relIdVar: String, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      body.assign(local, invoke(readOperations, relationshipGetProperty, body.load(relIdVar), constant(propId)))
    }
  }

  override def lookupPropertyKey(propName: String, propIdVar: String) =
    generator.assign(typeRef[Int], propIdVar, invoke(readOperations, propertyKeyGetForName, constant(propName)))

  override def newIndexDescriptor(descriptorVar: String, labelVar: String, propKeyVar: String) = {
    val getIndexDescriptor = method[NewIndexDescriptorFactory, NewIndexDescriptor]("forLabel", typeRef[Int], typeRef[Array[Int]])
    val propertyIdsExpr = Expression.newArray(typeRef[Int], generator.load(propKeyVar))
    generator.assign(typeRef[NewIndexDescriptor], descriptorVar,
                      invoke(getIndexDescriptor, generator.load(labelVar), propertyIdsExpr ))
  }

  override def indexSeek(iterVar: String, descriptorVar: String, value: Expression, codeGenType: CodeGenType) = {
    val predicate = generator.declare(typeRef[IndexQuery], s"${iterVar}Query")
    val local = generator.declare(typeRef[PrimitiveLongIterator], iterVar)
    val boxedValue = if (codeGenType.isPrimitive) Expression.box(value) else value
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      val descriptor = body.load(descriptorVar)
      val schema = invoke(descriptor, method[NewIndexDescriptor, LabelSchemaDescriptor]("schema"))
      val propertyKeyId = invoke(schema, method[LabelSchemaDescriptor, Int]("getPropertyId"))
      body.assign(predicate, invoke(indexQueryExact, propertyKeyId, boxedValue))
      body.assign(local, invoke(readOperations, indexQuery, descriptor,
        newArray(typeRef[IndexQuery], predicate)))
    }
  }

  override def indexUniqueSeek(nodeVar: String, descriptorVar: String, value: Expression, codeGenType: CodeGenType) = {
    val predicate = generator.declare(typeRef[IndexQuery.ExactPredicate], s"${nodeVar}Query")
    val local = generator.declare(typeRef[Long], nodeVar)
    val boxedValue = if (codeGenType.isPrimitive) Expression.box(value) else value
    handleKernelExceptions(generator, fields.ro, _finalizers) { body =>
      val descriptor = body.load(descriptorVar)
      val schema = invoke(descriptor, method[NewIndexDescriptor, LabelSchemaDescriptor]("schema"))
      val propertyKeyId = invoke(schema, method[LabelSchemaDescriptor, Int]("getPropertyId"))
      body.assign(predicate, invoke(indexQueryExact, propertyKeyId, boxedValue))
      body.assign(local, invoke(readOperations, nodeGetUniqueFromIndexLookup, descriptor,
        newArray(typeRef[IndexQuery.ExactPredicate], predicate)))
    }
  }

  def token(t: Int) = Expression.constant(t)

  def wildCardToken = Expression.constant(-1)

  override def nodeCountFromCountStore(expression: Expression): Expression =
    invoke(readOperations, countsForNode, expression )

  override def relCountFromCountStore(start: Expression, end: Expression, types: Expression*): Expression =
    if (types.isEmpty) invoke(readOperations, Methods.countsForRel, start, wildCardToken, end )
    else types.map(invoke(readOperations, Methods.countsForRel, start, _, end )).reduceLeft(Expression.add)

  override def coerceToBoolean(propertyExpression: Expression): Expression =
    invoke(coerceToPredicate, propertyExpression)

  override def newTableValue(targetVar: String, tupleDescriptor: TupleDescriptor) = {
    val tupleType = aux.typeReference(tupleDescriptor)
    generator.assign(tupleType, targetVar, createNewInstance(tupleType))
    generator.load(targetVar)
  }

  private def field(tupleDescriptor: TupleDescriptor, fieldName: String) = {
    val fieldType: CodeGenType = tupleDescriptor.structure(fieldName)
    FieldReference.field(aux.typeReference(tupleDescriptor), lowerType(fieldType), fieldName)
  }

  private def sortTableType(tableDescriptor: SortTableDescriptor): TypeReference = {
    tableDescriptor match {
      case FullSortTableDescriptor(tupleDescriptor) =>
        val tupleType = aux.comparableTypeReference(tupleDescriptor)
        TypeReference.parameterizedType(classOf[DefaultFullSortTable[_]], tupleType)

      case TopTableDescriptor(tupleDescriptor) =>
        val tupleType = aux.comparableTypeReference(tupleDescriptor)
        TypeReference.parameterizedType(classOf[DefaultTopTable[_]], tupleType)
    }
  }
}
