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

import java.util.stream.{DoubleStream, IntStream, LongStream}
import java.util.{PrimitiveIterator, ArrayList => JArrayList, HashMap => JHashMap, HashSet => JHashSet, Iterator => JIterator, Map => JMap, Set => JSet}

import org.neo4j.codegen.Expression.{invoke, not, or, _}
import org.neo4j.codegen.MethodReference.methodReference
import org.neo4j.codegen._
import org.neo4j.collection.primitive._
import org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.CompositeKey
import org.neo4j.cypher.internal.codegen._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{AnyValueType, BoolType, CodeGenType, CypherCodeGenType, FloatType, ListReferenceType, LongType, ReferenceType, RepresentationType, Parameter => _}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, QueryExecutionEvent}
import org.neo4j.cypher.internal.frontend.v3_4.helpers._
import org.neo4j.cypher.internal.spi.v3_4.codegen.GeneratedMethodStructure.CompletableFinalizer
import org.neo4j.cypher.internal.spi.v3_4.codegen.Methods._
import org.neo4j.cypher.internal.spi.v3_4.codegen.Templates._
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTInteger, CTNode, CTRelationship, ListType}
import org.neo4j.cypher.internal.util.v3_4.{ParameterNotFoundException, symbols}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api._
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual._

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
      val listType = parameterizedType(classOf[JArrayList[_]], valueType)
      val tableType = parameterizedType(classOf[PrimitiveLongObjectMap[_]], valueType)
      // the methods we use on those types
      val get = methodReference(tableType, typeRef[Object], "get", typeRef[Long])
      val put = methodReference(tableType, typeRef[Object], "put", typeRef[Long], typeRef[Object])
      val add = methodReference(listType, typeRef[Boolean], "add", typeRef[Object])

      HashTable(valueType, listType, tableType, get, put, add)

    case LongsToListTable(tupleDescriptor, localMap) =>
      // compute the participating types
      val valueType = aux.typeReference(tupleDescriptor)
      val listType = parameterizedType(classOf[JArrayList[_]], valueType)
      val tableType = parameterizedType(classOf[JHashMap[_, _]], typeRef[CompositeKey], valueType)
      // the methods we use on those types
      val get = methodReference(tableType, typeRef[Object], "get", typeRef[Object])
      val put = methodReference(tableType, typeRef[Object], "put", typeRef[Object], typeRef[Object])
      val add = methodReference(listType, typeRef[Boolean], "add", typeRef[Object])
      HashTable(valueType, listType, tableType, get, put, add)
  }

  override def nodeFromNodeValueIndexCursor(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, invoke(generator.load(iterVar),
                                                      method[NodeValueIndexCursor, Long]("nodeReference")))

  override def nodeFromNodeCursor(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, invoke(generator.load(iterVar), method[NodeCursor, Long]("nodeReference")))

  override def nodeFromNodeLabelIndexCursor(targetVar: String, iterVar: String) =
    generator.assign(typeRef[Long], targetVar, invoke(generator.load(iterVar), method[NodeLabelIndexCursor, Long]("nodeReference")))

  override def nextRelationshipAndNode(toNodeVar: String, iterVar: String, direction: SemanticDirection,
                                       fromNodeVar: String,
                                       relVar: String) = {
    val cursor = relCursor(relVar)
    generator.assign(typeRef[Long], toNodeVar, invoke(generator.load(cursor),
                                                   method[RelationshipSelectionCursor, Long]("otherNodeReference")))
    generator.assign(typeRef[Long], relVar, invoke(generator.load(cursor),
                                                   method[RelationshipSelectionCursor, Long]("relationshipReference")))
  }
  private def relCursor(relVar: String) = s"${relVar}Iter"

  override def nextRelationship(cursorName: String, ignored: SemanticDirection, relVar: String) = {
    generator.assign(typeRef[Long], relVar, invoke(generator.load(relCursor(relVar)),
                                                   method[RelationshipSelectionCursor, Long]("relationshipReference")))
  }

  override def allNodesScan(cursorName: String) = {
    generator.assign(typeRef[NodeCursor], cursorName, invoke(cursors, method[CursorFactory, NodeCursor]("allocateNodeCursor")))
    _finalizers.append((_: Boolean) => (block) =>
      block.expression(
        invoke(block.load(cursorName), method[NodeCursor, Unit]("close"))))
    generator.expression(invoke(dataRead, method[Read, Unit]("allNodesScan", typeRef[NodeCursor]), generator.load(cursorName) ))
  }

  override def labelScan(cursorName: String, labelIdVar: String) = {
    generator.assign(typeRef[NodeLabelIndexCursor], cursorName, invoke(cursors, method[CursorFactory, NodeLabelIndexCursor]("allocateNodeLabelIndexCursor")))
    _finalizers.append((_: Boolean) => (block) =>
      block.expression(
        invoke(block.load(cursorName), method[NodeLabelIndexCursor, Unit]("close"))))
    generator.expression(invoke(dataRead, method[Read, Unit]("nodeLabelScan", typeRef[Int], typeRef[NodeLabelIndexCursor]),
                                generator.load(labelIdVar), generator.load(cursorName) ))
  }

  override def lookupLabelId(labelIdVar: String, labelName: String) =
    generator.assign(typeRef[Int], labelIdVar,
                     invoke(tokenRead, labelGetForName, constant(labelName)))

  override def lookupLabelIdE(labelName: String): Expression =
    invoke(tokenRead, labelGetForName, constant(labelName))

  override def lookupRelationshipTypeId(typeIdVar: String, typeName: String) =
    generator.assign(typeRef[Int], typeIdVar, invoke(tokenRead, relationshipTypeGetForName, constant(typeName)))

  override def lookupRelationshipTypeIdE(typeName: String) =
    invoke(tokenRead, relationshipTypeGetForName, constant(typeName))

  override def advanceNodeCursor(cursorName: String) =
    invoke(generator.load(cursorName), method[NodeCursor, Boolean]("next"))

  override def closeNodeCursor(cursorName: String) =
    generator.expression(invoke(generator.load(cursorName), method[NodeCursor, Unit]("close")))

  override def advanceNodeLabelIndexCursor(cursorName: String) =
    invoke(generator.load(cursorName), method[NodeLabelIndexCursor, Boolean]("next"))

  override def closeNodeLabelIndexCursor(cursorName: String) =
    generator.expression(invoke(generator.load(cursorName), method[NodeLabelIndexCursor, Unit]("close")))

  override def advanceRelationshipSelectionCursor(cursorName: String) =
    invoke(generator.load(cursorName), method[RelationshipSelectionCursor, Boolean]("next"))

  override def closeRelationshipSelectionCursor(cursorName: String) =
    generator.expression(invoke(generator.load(cursorName), method[RelationshipSelectionCursor, Unit]("close")))

  override def advanceNodeValueIndexCursor(cursorName: String) =
    invoke(generator.load(cursorName), method[NodeValueIndexCursor, Boolean]("next"))

  override def closeNodeValueIndexCursor(cursorName: String) =
    using(generator.ifStatement(Expression.notNull(generator.load(cursorName)))) { inner =>
      inner.expression(
        invoke(inner.load(cursorName), method[NodeValueIndexCursor, Unit]("close")))
    }

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

  override def ifNonNullStatement(test: Expression, codeGenType: CodeGenType)(block: (MethodStructure[Expression]) => Unit) = {
    val notNullExpression: Expression =
      codeGenType match {
        case CodeGenType.primitiveNode | CodeGenType.primitiveRel | CypherCodeGenType(_, _: AnyValueType) =>
          Expression.notEqual(test, nullValue(codeGenType))

        case CypherCodeGenType(_, ReferenceType) =>
          Expression.and(Expression.notNull(test), Expression.notEqual(test, noValue()))

        case _ =>
          throw new IllegalArgumentException(s"CodeGenType $codeGenType does not have a null value")
      }
    using(generator.ifStatement(notNullExpression)) { body =>
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

  override def setInRow(column: Int, value: Expression) =
    generator.expression(invoke(resultRow, set, constant(column), value))

  override def toMaterializedAnyValue(expression: Expression, codeGenType: CodeGenType): Expression =
    toAnyValue(expression, codeGenType, materializeEntities = true)

  override def toAnyValue(expression: Expression, codeGenType: CodeGenType): Expression =
    toAnyValue(expression, codeGenType, materializeEntities = false)

  // NOTE: This method assumes that code for converting from null to NoValue for nullable expressions has already been generated outside it
  private def toAnyValue(expression: Expression, codeGenType: CodeGenType, materializeEntities: Boolean): Expression = codeGenType match {
    case CypherCodeGenType(_, _: AnyValueType) =>
      cast(typeRef[AnyValue], expression) // Already an AnyValue

    // == Node ==
    case CodeGenType.primitiveNode =>
      if (materializeEntities)
        createNewNodeValueFromPrimitive(nodeManager, expression)
      else
        createNewNodeReference(expression)

    case CypherCodeGenType(CTNode, _) => // NOTE: This may already be a NodeValue
      invoke(method[ValueUtils, NodeValue]("asNodeValue", typeRef[Object]), expression)

    // == Relationship ==
    case CodeGenType.primitiveRel =>
      if (materializeEntities)
        createNewRelationshipValueFromPrimitive(nodeManager, expression)
      else
        createNewRelationshipReference(expression)

    case CypherCodeGenType(CTRelationship, _) => // NOTE: This may already be a RelationshipValue
      invoke(method[ValueUtils, RelationshipValue]("asRelationshipValue", typeRef[Object]), expression)

    // == Integer ==
    case CodeGenType.primitiveInt =>
      invoke(method[Values, LongValue]("longValue", typeRef[Long]), expression)

    case CypherCodeGenType(CTInteger, _) => // NOTE: This may already be a LongValue
      invoke(method[ValueUtils, LongValue]("asLongValue", typeRef[Object]), expression)

    // == Float ==
    case CodeGenType.primitiveFloat =>
      invoke(method[Values, DoubleValue]("doubleValue", typeRef[Double]), expression)

    case CypherCodeGenType(symbols.CTFloat, _) => // NOTE: This may already be a DoubleValue
      invoke(method[ValueUtils, DoubleValue]("asDoubleValue", typeRef[Object]), expression)

    // == Boolean ==
    case CodeGenType.primitiveBool =>
      invoke(method[Values, BooleanValue]("booleanValue", typeRef[Boolean]), expression)

    case CypherCodeGenType(symbols.CTBoolean, _) => // NOTE: This may already be a BooleanValue
      invoke(method[ValueUtils, BooleanValue]("asBooleanValue", typeRef[Object]), expression)

    // == String ==
    case CypherCodeGenType(symbols.CTString, _) => // NOTE: This may already be a StringValue
      invoke(method[ValueUtils, TextValue]("asTextValue", typeRef[Object]), expression)

    // TODO: Primitive list values
    //    case CypherCodeGenType(ListType(CTNode), ListReferenceType(LongType)) =>
    //    case CypherCodeGenType(ListType(CTRelationship), ListReferenceType(LongType)) =>
    //    case CypherCodeGenType(_, ListReferenceType(LongType)) =>
    //    case CypherCodeGenType(_, ListReferenceType(FloatType)) =>
    //    case CypherCodeGenType(_, ListReferenceType(BoolType)) =>

    case _ =>
      // This also allows things that are already AnyValue (as opposed to ValueUtils.of). We do not always know this at compile time.
      invoke(method[ValueUtils, AnyValue]("asAnyValue", typeRef[Object]), expression)

  }

  override def visitorAccept() = tryCatch(generator) { onSuccess => {
    using(onSuccess.ifStatement(not(invoke(onSuccess.load("visitor"),
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
    else if (codeGenType.isAnyValue)
      invoke(materializeNodeValue, nodeManager, generator.load(nodeIdVar))
    else
      invoke(nodeManager, newNodeProxyById,
        invoke(cast(typeRef[VirtualNodeValue], generator.load(nodeIdVar)), nodeId))

  override def node(nodeIdVar: String, codeGenType: CodeGenType) =
    if (codeGenType.isPrimitive) generator.load(nodeIdVar)
    else invoke(cast(typeRef[VirtualNodeValue], generator.load(nodeIdVar)), nodeId)

  // Unused in production
  override def nullablePrimitive(varName: String, codeGenType: CodeGenType, onSuccess: Expression) = codeGenType match {
    case CypherCodeGenType(CTNode, LongType) | CypherCodeGenType(CTRelationship, LongType) =>
      ternary(
        equal(nullValue(codeGenType), generator.load(varName)),
        nullValue(codeGenType),
        onSuccess)
    case _ => ternary(Expression.isNull(generator.load(varName)), constant(null), onSuccess)
  }

  override def nullableReference(varName: String, codeGenType: CodeGenType, onSuccess: Expression) = codeGenType match {
    case CypherCodeGenType(CTNode, LongType) | CypherCodeGenType(CTRelationship, LongType) | CypherCodeGenType(_, _: AnyValueType) =>
      ternary(
        equal(nullValue(codeGenType), generator.load(varName)),
        constant(null),
        onSuccess)
    case _ => ternary(Expression.isNull(generator.load(varName)), constant(null), onSuccess)
  }

  override def materializeRelationship(relIdVar: String, codeGenType: CodeGenType) =
    if (codeGenType.isPrimitive)
      invoke(nodeManager, newRelationshipProxyById, generator.load(relIdVar))
    else if (codeGenType.isAnyValue)
      invoke(materializeRelationshipValue, nodeManager, generator.load(relIdVar))
    else
      invoke(nodeManager, newRelationshipProxyById,
        invoke(cast(typeRef[VirtualRelationshipValue], generator.load(relIdVar)), relId))

  override def relationship(relIdVar: String, codeGenType: CodeGenType) =
    generator.load(relIdVar)

  override def materializeAny(expression: Expression, codeGenType: CodeGenType) =
    codeGenType match {
      case CypherCodeGenType(_, _: AnyValueType) =>
        invoke(materializeAnyValueResult, nodeManager, expression)
      case _ =>
        invoke(materializeAnyResult, nodeManager, expression)
    }

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
      generator.ifStatement(not(invoke(params,  method[MapValue, Boolean]("containsKey", typeRef[String]), constant(key))))) { block =>
      block.throwException(parameterNotFoundException(key))
    }
    val invokeLoadParameter = invoke(params, method[MapValue, AnyValue]("get", typeRef[String]), constantExpression(key))

    generator.assign(lowerType(codeGenType), variableName,
      // We assume the value in the parameter map will always be boxed, so if we are declaring
      // a primitive variable we need to force it to be unboxed
      codeGenType match {
        case CodeGenType.primitiveNode =>
          unbox(Expression.cast(typeRef[VirtualNodeValue], invokeLoadParameter),
            CypherCodeGenType(symbols.CTNode, ReferenceType))
        case CodeGenType.primitiveRel =>
          unbox(Expression.cast(typeRef[VirtualRelationshipValue], invokeLoadParameter),
            CypherCodeGenType(symbols.CTRelationship, ReferenceType))
        case CodeGenType.primitiveInt =>
          Expression.unbox(Expression.cast(typeRef[java.lang.Long], invokeLoadParameter))
        case CodeGenType.primitiveFloat =>
          Expression.unbox(Expression.cast(typeRef[java.lang.Double], invokeLoadParameter))
        case CodeGenType.primitiveBool =>
          Expression.unbox(Expression.cast(typeRef[java.lang.Boolean], invokeLoadParameter))
        case CypherCodeGenType(_, ListReferenceType(repr)) if RepresentationType.isPrimitive(repr) =>
          asPrimitiveStream(invokeLoadParameter, codeGenType)
        case CypherCodeGenType(_, _: AnyValueType) =>
          Expression.cast(typeRef[AnyValue], invokeLoadParameter)
        case _ => invokeLoadParameter
      }
    )
  }

  override def mapGetExpression(map: Expression, key: String): Expression = {
    invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[Object], "mapGetProperty", typeRef[Object],
                           typeRef[String]), map, constantExpression(key))
  }

  override def constantExpression(value: AnyRef) = constant(value)

  override def constantValueExpression(value: AnyRef, codeGenType: CodeGenType) =
    if (codeGenType.isPrimitive)
      invoke(method[Values,Value]("of", typeRef[AnyRef]), box(constant(value), codeGenType))
    else {
      codeGenType match {
        case CypherCodeGenType(symbols.CTString, _) =>
          invoke(method[Values,TextValue]("stringValue", typeRef[String]), constant(value))
        case _ =>
          invoke(method[Values,Value]("of", typeRef[AnyRef]), constant(value))
      }
    }

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
      box(equal(lhs, rhs), CodeGenType.primitiveBool)
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


  override def isNull(varName: String, codeGenType: CodeGenType) = isNull(generator.load(varName), codeGenType)

  override def isNull(expr: Expression, codeGenType: CodeGenType) = {
    equal(nullValue(codeGenType), expr)
  }

  override def notNull(expr: Expression, codeGenType: CodeGenType) = not(isNull(expr, codeGenType))

  override def notNull(varName: String, codeGenType: CodeGenType) = notNull(generator.load(varName), codeGenType)

  override def ifNullThenNoValue(expr: Expression) = {
    ternary(Expression.isNull(expr), noValue(), expr)
  }

  override def box(expression: Expression, codeGenType: CodeGenType) = codeGenType match {
    case CypherCodeGenType(symbols.CTNode, LongType) =>
      createNewNodeReference(expression)
    case CypherCodeGenType(symbols.CTRelationship, LongType) =>
      createNewRelationshipReference(expression)
    case _ => Expression.box(expression)
  }

  override def unbox(expression: Expression, codeGenType: CodeGenType) = codeGenType match {
    case c if c.isPrimitive => expression
    case CypherCodeGenType(symbols.CTNode, ReferenceType) => invoke(Methods.unboxNode, expression)
    case CypherCodeGenType(symbols.CTRelationship, ReferenceType) => invoke(Methods.unboxRel, expression)
    case _ => Expression.unbox(expression)
  }

  override def toFloat(expression: Expression) = toDouble(expression)

  override def nodeGetRelationshipsWithDirection(iterVar: String, nodeVar: String, nodeVarType: CodeGenType, direction: SemanticDirection) = {
    generator.assign(typeRef[RelationshipSelectionCursor], iterVar,
                     invoke(
                       methodReference(typeRef[CompiledCursorUtils], typeRef[RelationshipSelectionCursor],
                                       "nodeGetRelationships",  typeRef[Read], typeRef[CursorFactory],
                                       typeRef[NodeCursor], typeRef[Long], typeRef[Direction]),
                       dataRead, cursors, nodeCursor, forceLong(nodeVar, nodeVarType), dir(direction))
                     )
    _finalizers.append((_: Boolean) => (block) =>
      block.expression(
        invoke(block.load(iterVar), method[RelationshipSelectionCursor, Unit]("close"))))
  }

  override def nodeGetRelationshipsWithDirectionAndTypes(iterVar: String, nodeVar: String, nodeVarType: CodeGenType,
                                                         direction: SemanticDirection,

                                                         typeVars: Seq[String]) = {
    generator.assign(typeRef[RelationshipSelectionCursor], iterVar,
                     invoke(
                       methodReference(typeRef[CompiledCursorUtils], typeRef[RelationshipSelectionCursor],
                                       "nodeGetRelationships",  typeRef[Read], typeRef[CursorFactory],
                                       typeRef[NodeCursor], typeRef[Long], typeRef[Direction], typeRef[Array[Int]]),
                       dataRead, cursors, nodeCursor, forceLong(nodeVar, nodeVarType), dir(direction),
                       newArray(typeRef[Int], typeVars.map(generator.load): _*)) )
    _finalizers.append((_: Boolean) => (block) =>
      block.expression(
        invoke(block.load(iterVar), method[RelationshipSelectionCursor, Unit]("close"))))
  }

  override def connectingRelationships(iterVar: String, fromNode: String, fromNodeType: CodeGenType, direction: SemanticDirection,
                                       toNode: String, toNodeType: CodeGenType) = {
    generator.assign(typeRef[RelationshipSelectionCursor], iterVar, invoke(Methods.allConnectingRelationships,
                                                                           dataRead, cursors, nodeCursor,
                                                                           forceLong(fromNode, fromNodeType),
                                                                           dir(direction),  forceLong(toNode, toNodeType)))
    _finalizers.append((_: Boolean) => (block) =>
      block.expression(
        invoke(block.load(iterVar), method[RelationshipSelectionCursor, Unit]("close"))))
  }

  override def connectingRelationships(iterVar: String, fromNode: String, fromNodeType: CodeGenType, direction: SemanticDirection,

                                       typeVars: Seq[String], toNode: String, toNodeType: CodeGenType) = {
      generator.assign(typeRef[RelationshipSelectionCursor], iterVar, invoke(Methods.connectingRelationships,
                                                                             dataRead, cursors, nodeCursor,
                                                                             forceLong(fromNode, fromNodeType),
                                                                             dir(direction),  forceLong(toNode, toNodeType),
                                                                             newArray(typeRef[Int], typeVars.map(generator.load): _*)))
    _finalizers.append((_: Boolean) => (block) =>
      block.expression(
        invoke(block.load(iterVar), method[RelationshipSelectionCursor, Unit]("close"))))
  }

  override def loadVariable(varName: String) = generator.load(varName)

  override def multiplyPrimitive(lhs: Expression, rhs: Expression) = multiply(lhs, rhs)

  override def addExpression(lhs: Expression, rhs: Expression) = math(Methods.mathAdd, lhs, rhs)

  override def subtractExpression(lhs: Expression, rhs: Expression) = math(Methods.mathSub, lhs, rhs)

  override def multiplyExpression(lhs: Expression, rhs: Expression) = math(Methods.mathMul, lhs, rhs)

  override def divideExpression(lhs: Expression, rhs: Expression) = math(Methods.mathDiv, lhs, rhs)

  override def modulusExpression(lhs: Expression, rhs: Expression) = math(Methods.mathMod, lhs, rhs)

  private def math(method: MethodReference, lhs: Expression, rhs: Expression): Expression =
    invoke(method, lhs, rhs)

  private def dataRead: Expression =
    invoke(generator.self(), methodReference(generator.owner(), typeRef[Read], "getOrLoadDataRead"))

  private def tokenRead: Expression =
    invoke(generator.self(), methodReference(generator.owner(), typeRef[TokenRead], "getOrLoadTokenRead"))

  private def schemaRead: Expression =
    invoke(generator.self(), methodReference(generator.owner(), typeRef[SchemaRead], "getOrLoadSchemaRead"))

  private def cursors: Expression =
    invoke(generator.self(), methodReference(generator.owner(), typeRef[CursorFactory], "getOrLoadCursors"))

  private def nodeCursor: Expression =
    invoke(generator.self(), methodReference(generator.owner(), typeRef[NodeCursor], "nodeCursor"))

  private def relationshipScanCursor: Expression =
    invoke(generator.self(), methodReference(generator.owner(), typeRef[RelationshipScanCursor], "relationshipScanCursor"))

  private def propertyCursor: Expression =
    invoke(generator.self(), methodReference(generator.owner(), typeRef[PropertyCursor], "propertyCursor"))

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

  // Unused
  override def asList(values: Seq[Expression]) = Templates.asList[Object](values)

  override def asAnyValueList(values: Seq[Expression]) = Templates.asAnyValueList(values)

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
        Expression.invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[LongStream], "toLongStream", typeRef[Object]), publicTypeList)
      case CypherCodeGenType(_, ListReferenceType(FloatType)) =>
        // float[]
        // double[]
        Expression.invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[DoubleStream], "toDoubleStream", typeRef[Object]), publicTypeList)
      case CypherCodeGenType(_, ListReferenceType(BoolType)) =>
        // boolean[]
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

  // Unused
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
    val variable = generator.declare(typeRef[JIterator[Object]], name)
    locals += (name -> variable)
  }

  override def declareIterator(name: String, elementCodeGenType: CodeGenType): Unit = {
    val variable = generator.declare(parameterizedType(classOf[JIterator[_]], lowerType(elementCodeGenType)), name)
    locals += (name -> variable)
  }

  override def iteratorFrom(iterable: Expression) =
    invoke(method[CompiledConversionUtils, JIterator[_]]("iteratorFrom", typeRef[Object]), iterable)

  override def iteratorNext(iterator: Expression, codeGenType: CodeGenType) =
    Expression.cast(lowerType(codeGenType), invoke(iterator, method[JIterator[_], Object]("next")))

  override def iteratorHasNext(iterator: Expression) =
    invoke(iterator, method[JIterator[_], Boolean]("hasNext"))

  override def toSet(value: Expression) =
    invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[JSet[Object]], "toSet", typeRef[Object]),
           value)

  override def newDistinctSet(name: String, codeGenTypes: Iterable[CodeGenType]) = {
    if (codeGenTypes.size == 1 && codeGenTypes.head.repr == LongType) {
      generator.assign(generator.declare(typeRef[PrimitiveLongSet], name),
                       invoke(method[Primitive, PrimitiveLongSet]("longSet")))
      _finalizers.append((_: Boolean) => (block) =>
                           block.expression(
                             invoke(block.load(name), method[PrimitiveLongSet, Unit]("close"))))

    } else {
      generator.assign(generator.declare(typeRef[JHashSet[Object]], name),
                       createNewInstance(typeRef[JHashSet[Object]]))
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
      val variable = generator.declare(typeRef[JIterator[Object]], localName)
      val keyStruct = aux.hashableTypeReference(keyTupleDescriptor)
      generator.assign(variable,
                       invoke(generator.load(name),method[JHashSet[Object], JIterator[Object]]
                         ("iterator")))
      using(generator.whileLoop(
        invoke(generator.load(localName),
               method[JIterator[Object], Boolean]("hasNext")))) { body =>
        body.assign(body.declare(keyStruct, next),
                    cast(keyStruct,
                         invoke(body.load(localName),
                                method[JIterator[Object], Object]("next"))))
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
      val (cgType, expression) = structure.values.head
      generator.put(generator.load(varName), FieldReference.field(typ, typeRef[Int], "hashCode"),
                    invoke(method[CompiledEquivalenceUtils, Int]("hashCode", typeRef[Object]),
                           box(expression, cgType)))
    } else {
      val elementType = deriveCommonType(structure.values.map(_._1))
      generator.put(generator.load(varName), FieldReference.field(typ, typeRef[Int], "hashCode"),
        invoke(method[CompiledEquivalenceUtils, Int]("hashCode", TypeReference.arrayOf(elementType)),
          if (elementType.isPrimitive)
            newArray(elementType, structure.values.map(_._2).toSeq: _*)
          else
            newArray(elementType, structure.values.map(e => Expression.box(e._2)).toSeq: _*)
        ))
    }
  }

  override def newAggregationMap(name: String, keyTypes: IndexedSeq[CodeGenType]) = {
    if (keyTypes.size == 1 && keyTypes.head.repr == LongType) {
      generator.assign(generator.declare(typeRef[PrimitiveLongLongMap], name),
                       invoke(method[Primitive, PrimitiveLongLongMap]("longLongMap")))
      _finalizers.append((_: Boolean) => (block) =>
                           block.expression(
                             invoke(block.load(name), method[PrimitiveLongLongMap, Unit]("close"))))
    } else {
      val local = generator.declare(typeRef[JHashMap[Object, java.lang.Long]], name)
      generator.assign(local, createNewInstance(typeRef[JHashMap[Object, java.lang.Long]]))
    }
  }

  override def newMapOfSets(name: String, keyTypes: IndexedSeq[CodeGenType], elementType: CodeGenType) = {

    val setType = if (elementType.repr == LongType) typeRef[PrimitiveLongSet] else typeRef[JHashSet[Object]]
    if (keyTypes.size == 1 && keyTypes.head.repr == LongType) {
      val typ =  TypeReference.parameterizedType(typeRef[PrimitiveLongObjectMap[_]], setType)
      generator.assign(generator.declare(typ, name),
                       invoke(method[Primitive, PrimitiveLongObjectMap[PrimitiveLongSet]]("longObjectMap")))

    } else {
      val typ =  TypeReference.parameterizedType(typeRef[JHashMap[_,_]], typeRef[Object], setType)

      generator.assign(generator.declare(typ, name ), createNewInstance(typ))
    }
  }

  override def allocateSortTable(name: String, tableDescriptor: SortTableDescriptor, count: Expression): Unit = {
    val tableType = sortTableType(tableDescriptor)
    val localVariable = generator.declare(tableType, name)
    locals += name -> localVariable
    val boxedInteger = box(count, CodeGenType.Any) // TODO: we shouldn't need to box here, we know it's either 'int' or 'long'
    generator.assign(localVariable, createNewInstance(tableType, (typeRef[Int], invoke(Methods.mathCastToInt, boxedInteger))))
  }

  override def sortTableAdd(name: String, tableDescriptor: SortTableDescriptor, value: Expression): Unit = {
    val tableType = sortTableType(tableDescriptor)
    generator.expression(pop(invoke(generator.load(name),
      methodReference(tableType, typeRef[Boolean], "add", typeRef[Object]),
      box(value, CodeGenType.Any)))) // TODO: this boxing seems completely unnecessary
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
                    method[JHashMap[Object, java.lang.Long], Object]("getOrDefault", typeRef[Object],
                                                                         typeRef[Object]),
                    generator.load(keyVar), box(constantLong(0L), CodeGenType.javaLong))),
        CypherCodeGenType(CTInteger, ReferenceType)))
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
        val localVariable = generator.declare(typeRef[JHashSet[Object]], tmp)
        generator.assign(localVariable,
                         cast(typeRef[JHashSet[Object]],
                              invoke(generator.load(name),
                                     method[PrimitiveLongObjectMap[Object], Object]("get", typeRef[Long]),
                                     keyExpression)))
        using(generator.ifStatement(Expression.isNull(generator.load(tmp)))) { inner =>
          inner.assign(localVariable, createNewInstance(typeRef[JHashSet[Object]]))
          inner.expression(pop(invoke(generator.load(name),
                                      method[PrimitiveLongObjectMap[Object], Object]("put", typeRef[Long],
                                                                                     typeRef[Object]),
                                      keyExpression, inner.load(tmp))))
        }
        using(generator.ifStatement(not(invoke(generator.load(tmp),
                                               method[JHashSet[Object], Boolean]("contains", typeRef[Object]),
                                               value)))) { inner =>
          block(copy(generator = inner))
        }
        generator.expression(pop(invoke(generator.load(tmp),
                                        method[JHashSet[Object], Boolean]("add", typeRef[Object]), value)))
      }
    } else {
      val setVar = context.namer.newVarName()
      if (valueType.repr == LongType) {
        val localVariable = generator.declare(typeRef[PrimitiveLongSet], setVar)
        if (!locals.contains(keyVar)) newUniqueAggregationKey(keyVar, key)

        generator.assign(localVariable,
                         cast(typeRef[PrimitiveLongSet],
                              invoke(generator.load(name),
                                     method[JHashMap[Object, PrimitiveLongSet], Object]("get", typeRef[Object]),
                                     generator.load(keyVar))))
        using(generator.ifStatement(Expression.isNull(generator.load(setVar)))) { inner =>

          inner.assign(localVariable, invoke(method[Primitive, PrimitiveLongSet]("longSet")))
          inner.expression(pop(invoke(generator.load(name),
                                      method[JHashMap[Object, PrimitiveLongSet], Object]("put", typeRef[Object],
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
        val localVariable = generator.declare(typeRef[JHashSet[Object]], setVar)
        if (!locals.contains(keyVar)) newUniqueAggregationKey(keyVar, key)

        generator.assign(localVariable,
                         cast(typeRef[JHashSet[Object]],
                              invoke(generator.load(name),
                                     method[JHashMap[Object, JHashSet[Object]], Object]("get", typeRef[Object]),
                                     generator.load(keyVar))))
        using(generator.ifStatement(Expression.isNull(generator.load(setVar)))) { inner =>

          inner.assign(localVariable, createNewInstance(typeRef[JHashSet[Object]]))
          inner.expression(pop(invoke(generator.load(name),
                                      method[JHashMap[Object, JHashSet[Object]], Object]("put", typeRef[Object],
                                                                                                 typeRef[Object]),
                                      generator.load(keyVar), inner.load(setVar))))
        }
        val valueVar = context.namer.newVarName()
        newUniqueAggregationKey(valueVar, Map(context.namer.newVarName() -> (valueType -> value)))

        using(generator.ifStatement(not(invoke(generator.load(setVar),
                                               method[JHashSet[Object], Boolean]("contains", typeRef[Object]),
                                               generator.load(valueVar))))) { inner =>
          block(copy(generator = inner))
          inner.expression(pop(invoke(generator.load(setVar),
                                      method[JHashSet[Object], Boolean]("add", typeRef[Object]),
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
                                      method[JHashMap[Object, java.lang.Long], Object]("put", typeRef[Object],
                                                                                           typeRef[Object]),
                                      generator.load(keyVar), box(value, CodeGenType.javaLong))))
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
        .declare(typeRef[JIterator[JMap.Entry[Object, java.lang.Long]]], localName)
      val keyStruct = aux.hashableTypeReference(keyTupleDescriptor)
      generator.assign(variable,
                       invoke(invoke(generator.load(name),
                              method[JHashMap[Object, java.lang.Long], JSet[JMap.Entry[Object, java.lang.Long]]]("entrySet")),
                              method[JSet[JMap.Entry[Object, java.lang.Long]], JIterator[java.util.Map.Entry[Object, java.lang.Long]]]("iterator")))
      using(generator.whileLoop(
        invoke(generator.load(localName),
               method[JIterator[JMap.Entry[Object, java.lang.Long]], Boolean]("hasNext")))) { body =>
        body.assign(body.declare(typeRef[JMap.Entry[Object, java.lang.Long]], next),
                    cast(typeRef[JMap.Entry[Object, java.lang.Long]],
                         invoke(body.load(localName),
                                method[JIterator[JMap.Entry[Object, java.lang.Long]], Object]("next"))))
        key.foreach {
          case (keyName, keyType) =>

            body.assign(body.declare(lowerType(keyType), keyName),
                        Expression.get(
                          cast(keyStruct,
                               invoke(body.load(next),
                                      method[JMap.Entry[Object, java.lang.Long], Object]("getKey"))),
                          FieldReference.field(keyStruct, lowerType(keyType), keyName)))
        }

        body.assign(body.declare(typeRef[Long], valueVar),
                    unbox(cast(typeRef[java.lang.Long],
                               invoke(body.load(next),
                                      method[JMap.Entry[Object, java.lang.Long], Object]("getValue"))),
                          CypherCodeGenType(CTInteger, ReferenceType)))
        block(copy(generator = body))
      }
    }
  }

  // This version is for when maps contains AnyValues
  override def asMap(map: Map[String, Expression]) = {
    invoke(method[VirtualValues, MapValue]("map", typeRef[java.util.Map[String,AnyValue]]),
      invoke(Methods.createAnyValueMap,
             newArray(typeRef[Object], map.flatMap {
               case (key, value) => Seq(constant(key), value)
             }.toSeq: _*)))
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
        parameterizedType(classOf[JHashMap[_, _]], classOf[CompositeKey], classOf[java.lang.Integer])
      case LongToListTable(tupleDescriptor, _) =>
        parameterizedType(classOf[PrimitiveLongObjectMap[_]],
                          parameterizedType(classOf[JArrayList[_]],
                                            aux.typeReference(tupleDescriptor)))
      case LongsToListTable(tupleDescriptor, _) =>
        parameterizedType(classOf[JHashMap[_, _]],
                          typeRef[CompositeKey],
                          parameterizedType(classOf[JArrayList[_]],
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
                               box(constantInt(1), CodeGenType.javaInt), box(add(invoke(generator.load(countName), unboxInteger), constantInt(1)), CodeGenType.javaInt)))))
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
    val localVariable = generator.declare(typeRef[Value], propertyVar)
    locals += (propertyVar -> localVariable)
    generator.assign(localVariable, noValue)
  }

  override def declareAndInitialize(varName: String, codeGenType: CodeGenType) = {
    val localVariable = generator.declare(lowerType(codeGenType), varName)
    locals += (varName -> localVariable)
    codeGenType match {
      case CypherCodeGenType(CTInteger, LongType) => constant(0L)
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

    handleEntityNotFound(generator, fields, _finalizers) { inner =>
      val invoked =
        invoke(
          methodReference(typeRef[CompiledCursorUtils],
                          typeRef[Boolean], "nodeHasLabel",
                          typeRef[Read], typeRef[NodeCursor], typeRef[Long],
                          typeRef[Int]),
          dataRead, nodeCursor, inner.load(nodeVar), inner.load(labelVar))
      inner.assign(local, invoked)
      generator.load(predVar)
    } { fail =>
      fail.assign(local, constant(false))
      generator.load(predVar)
    }
  }

  override def relType(relVar: String, typeVar: String) = {
    val variable = locals(typeVar)
    val typeOfRel = invoke(generator.load(relCursor(relVar)),  method[RelationshipSelectionCursor, Int]("type"))
    handleKernelExceptions(generator, fields, _finalizers) { inner =>
      val res = invoke(tokenRead, relationshipTypeGetName, typeOfRel)
      inner.assign(variable, res)
      generator.load(variable.name())
    }
  }

  override def localVariable(variable: String, e: Expression, codeGenType: CodeGenType): Unit = {
    val local = generator.declare(lowerType(codeGenType), variable)
    generator.assign(local, e)
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

  override def nodeGetPropertyForVar(nodeVar: String, nodeVarType: CodeGenType, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    handleEntityNotFound(generator, fields, _finalizers) { body =>
      body.assign(local,
                    invoke(
                      methodReference(typeRef[CompiledCursorUtils],
                                      typeRef[Value], "nodeGetProperty",
                                      typeRef[Read], typeRef[NodeCursor], typeRef[Long],
                                      typeRef[PropertyCursor], typeRef[Int]),
                      dataRead, nodeCursor, forceLong(nodeVar, nodeVarType), propertyCursor, body.load(propIdVar))
      )
    } { fail =>
      fail.assign(local, noValue())
    }
  }

  override def nodeGetPropertyById(nodeVar: String, nodeVarType: CodeGenType, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    handleEntityNotFound(generator, fields, _finalizers) { body =>
      body.assign(local,
                    invoke(
                      methodReference(typeRef[CompiledCursorUtils],
                                      typeRef[Value], "nodeGetProperty",
                                      typeRef[Read], typeRef[NodeCursor], typeRef[Long],
                                      typeRef[PropertyCursor], typeRef[Int]),
                      dataRead, nodeCursor, forceLong(nodeVar, nodeVarType),  propertyCursor, constant(propId))
      )
    }{ fail =>
      fail.assign(local, noValue())
    }
  }

  override def nodeIdSeek(nodeIdVar: String, expression: Expression, codeGenType: CodeGenType)(block: MethodStructure[Expression] => Unit) = {
    codeGenType match {
      case CypherCodeGenType(CTInteger, LongType) =>
        generator.assign(typeRef[Long], nodeIdVar, expression)
      case CypherCodeGenType(CTInteger, _: AnyValueType) =>
        generator.assign(typeRef[Long], nodeIdVar, invoke(cast(typeRef[NumberValue], expression), method[NumberValue, Long]("longValue")))
      case CypherCodeGenType(CTInteger, ReferenceType) =>
        generator.assign(typeRef[Long], nodeIdVar, invoke(Methods.mathCastToLong, expression))
      case _ =>
        throw new IllegalArgumentException(s"CodeGenType $codeGenType can not be converted to long")
    }
    using(generator.ifStatement(and(
      gt(generator.load(nodeIdVar), constant(-1L)),
      invoke(dataRead, nodeExists, generator.load(nodeIdVar))
    ))) { ifBody =>
      block(copy(generator = ifBody))
    }
  }

  override def relationshipGetPropertyForVar(relIdVar: String, relVarType: CodeGenType, propIdVar: String, propValueVar: String) = {
    val local = locals(propValueVar)
    handleEntityNotFound(generator, fields, _finalizers) { body =>
      body.assign(local,
                    invoke(
                      methodReference(typeRef[CompiledCursorUtils],
                                      typeRef[Value], "relationshipGetProperty",
                                      typeRef[Read], typeRef[RelationshipScanCursor], typeRef[Long],
                                      typeRef[PropertyCursor], typeRef[Int]),
                      dataRead, relationshipScanCursor, forceLong(relIdVar, relVarType), propertyCursor, body.load(propIdVar))
      )
    } { fail =>
      fail.assign(local, noValue())
    }
  }

  override def relationshipGetPropertyById(relIdVar: String, relVarType: CodeGenType, propId: Int, propValueVar: String) = {
    val local = locals(propValueVar)
    handleEntityNotFound(generator, fields, _finalizers) { body =>
      body.assign(local,
                    invoke(
                      methodReference(typeRef[CompiledCursorUtils],
                                      typeRef[Value], "relationshipGetProperty",
                                      typeRef[Read], typeRef[RelationshipScanCursor], typeRef[Long],
                                      typeRef[PropertyCursor], typeRef[Int]),
                      dataRead, relationshipScanCursor, forceLong(relIdVar, relVarType),  propertyCursor, constant(propId))
      )
    }{ fail =>
      fail.assign(local, noValue())
    }
  }

  override def lookupPropertyKey(propName: String, propIdVar: String) =
    generator.assign(typeRef[Int], propIdVar, invoke(tokenRead, propertyKeyGetForName, constant(propName)))

  override def newIndexReference(referenceVar: String, labelVar: String, propKeyVar: String) = {
    val propertyIdsExpr = Expression.newArray(typeRef[Int], generator.load(propKeyVar))

    generator.assign(typeRef[CapableIndexReference], referenceVar,
                     invoke(schemaRead,
                           method[SchemaRead, CapableIndexReference]("index", typeRef[Int], typeRef[Array[Int]]),
                            generator.load(labelVar), propertyIdsExpr)
    )
  }

  override def indexSeek(cursorName: String, indexReference: String, value: Expression, codeGenType: CodeGenType) = {
    val local = generator.declare(typeRef[NodeValueIndexCursor], cursorName)
    generator.assign(local, constant(null))
    _finalizers.append((_: Boolean) => (block) =>
      using(block.ifStatement(Expression.notNull(block.load(cursorName)))) { inner =>
        inner.expression(
          invoke(inner.load(cursorName), method[NodeValueIndexCursor, Unit]("close")))
      })
    val boxedValue =
      if (codeGenType.isPrimitive) Expression.box(value) else value
    handleKernelExceptions(generator, fields, _finalizers) { body =>
      val index = body.load(indexReference)
      body.assign(local,
                  invoke(
                    methodReference(typeRef[CompiledIndexUtils], typeRef[NodeValueIndexCursor], "indexSeek",
                                    typeRef[Read], typeRef[CursorFactory], typeRef[CapableIndexReference], typeRef[AnyRef]),
                    dataRead, cursors, index, boxedValue)
      )
    }
  }

  def token(t: Int) = Expression.constant(t)

  def wildCardToken = Expression.constant(-1)

  override def nodeCountFromCountStore(expression: Expression): Expression =
    invoke(dataRead, countsForNode, expression )

  override def relCountFromCountStore(start: Expression, end: Expression, types: Expression*): Expression =
    if (types.isEmpty) invoke(dataRead, Methods.countsForRel, start, wildCardToken, end )
    else types.map(invoke(dataRead, Methods.countsForRel, start, _, end )).reduceLeft(Expression.add)

  override def coerceToBoolean(propertyExpression: Expression): Expression =
    invoke(coerceToPredicate, propertyExpression)

  override def newTableValue(targetVar: String, tupleDescriptor: TupleDescriptor) = {
    val tupleType = aux.typeReference(tupleDescriptor)
    generator.assign(tupleType, targetVar, createNewInstance(tupleType))
    generator.load(targetVar)
  }

  override def noValue(): Expression = Templates.noValue

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

  //Forces variable to be of type long or fails accordingly
  private def forceLong(name: String, typ: CodeGenType): Expression = typ.repr match {
    case LongType => generator.load(name)
    case _: ReferenceType =>
      invoke(methodReference(typeRef[CompiledConversionUtils], typeRef[Long], "extractLong", typeRef[Object]), generator.load(name))
    case _ => throw new IllegalStateException(s"$name has type $typ which cannot be represented as a long")
  }

  // Lowers a sequence of types and returns either a type that all elements share or Object if any types are different
  private def deriveCommonType(codeGenTypes: Iterable[CodeGenType]): TypeReference = {
    val lowerTypes = codeGenTypes.map(lowerTypeScalarSubset) // We use a subset version of lowerType that does not give us the specializations for sequence values (CTList)
    lowerTypes.reduce((a, b) => if (a == b) a else typeRef[Object])
  }
}
