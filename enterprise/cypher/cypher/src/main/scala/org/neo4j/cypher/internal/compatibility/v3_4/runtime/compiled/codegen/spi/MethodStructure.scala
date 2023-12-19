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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.Variable
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.values.AnyValue

/**
  * Describes the SPI for generating a method.
  *
  * In principle you can think of it as you have a method, e.g.
  *
  * {{{
  *   public void foo
  *   {
  *   ...
  *   }
  * }}}
  *
  * This SPI describes the operations that can be put in that method.
  */
trait MethodStructure[E] {


  // misc
  def localVariable(variable: String, e: E, codeGenType: CodeGenType): Unit
  def declareFlag(name: String, initialValue: Boolean)
  def updateFlag(name: String, newValue: Boolean)
  def declarePredicate(name: String): Unit
  def assign(varName: String, codeGenType: CodeGenType, value: E): Unit
  def assign(v: Variable, value: E): Unit = assign(v.name, v.codeGenType, value)
  def declareAndInitialize(varName: String, codeGenType: CodeGenType): Unit
  def declare(varName: String, codeGenType: CodeGenType): Unit
  def declareProperty(name: String): Unit
  def declareCounter(name: String, initialValue: E): Unit
  def putField(tupleDescriptor: TupleDescriptor, value: E, fieldName: String, localVar: String): Unit
  def updateProbeTable(tupleDescriptor: TupleDescriptor, tableVar: String, tableType: RecordingJoinTableType, keyVars: Seq[String], element: E): Unit
  def probe(tableVar: String, tableType: JoinTableType, keyVars: Seq[String])(block: MethodStructure[E]=>Unit): Unit
  def updateProbeTableCount(tableVar: String, tableType: CountingJoinTableType, keyVar: Seq[String]): Unit
  def allocateProbeTable(tableVar: String, tableType: JoinTableType): Unit
  def invokeMethod(resultType: JoinTableType, resultVar: String, methodName: String)(block: MethodStructure[E]=>Unit): Unit
  def coerceToBoolean(propertyExpression: E): E

  def incrementInteger(name: String): Unit
  def decrementInteger(name: String): Unit
  def incrementInteger(name: String, value: E): Unit
  def checkInteger(variableName: String, comparator: Comparator, value: Long): E
  def newTableValue(targetVar: String, tupleDescriptor: TupleDescriptor): E
  def noValue(): E
  def constantExpression(value: AnyRef): E
  def constantValueExpression(value: AnyRef, codeGenType: CodeGenType): E
  def constantPrimitiveExpression(value: AnyVal): E = constantExpression(value.asInstanceOf[AnyRef])
  def asMap(map: Map[String, E]): E
  def asList(values: Seq[E]): E
  def asAnyValueList(values: Seq[E]): E
  def asPrimitiveStream(values: E, codeGenType: CodeGenType): E
  def asPrimitiveStream(values: Seq[E], codeGenType: CodeGenType): E

  def declarePrimitiveIterator(name: String, iterableCodeGenType: CodeGenType): Unit
  def primitiveIteratorFrom(iterable: E, iterableCodeGenType: CodeGenType): E
  def primitiveIteratorNext(iterator: E, iterableCodeGenType: CodeGenType): E
  def primitiveIteratorHasNext(iterator: E, iterableCodeGenType: CodeGenType): E

  def declareIterator(name: String): Unit
  def declareIterator(name: String, codeGenType: CodeGenType): Unit
  def iteratorFrom(iterable: E): E
  def iteratorNext(iterator: E, codeGenType: CodeGenType): E
  def iteratorHasNext(iterator: E): E

  def toSet(value: E): E
  def newDistinctSet(name: String, codeGenTypes: Iterable[CodeGenType])
  def distinctSetIfNotContains(name: String, structure: Map[String,(CodeGenType,E)])(block: MethodStructure[E] => Unit)
  def distinctSetIterate(name: String, key: HashableTupleDescriptor)(block: (MethodStructure[E]) => Unit)
  def newUniqueAggregationKey(varName: String, structure: Map[String, (CodeGenType,E)]): Unit
  def newAggregationMap(name: String, keyTypes: IndexedSeq[CodeGenType]): Unit
  def aggregationMapGet(name: String, varName: String, key: Map[String,(CodeGenType,E)], keyVar: String)
  def aggregationMapPut(name: String, key: Map[String,(CodeGenType,E)], keyVar: String, value: E): Unit
  def aggregationMapIterate(name: String, key: HashableTupleDescriptor, valueVar: String)(block: MethodStructure[E] => Unit): Unit
  def newMapOfSets(name: String, keyTypes: IndexedSeq[CodeGenType], elementType: CodeGenType)
  def checkDistinct(name: String, key: Map[String,(CodeGenType, E)], keyVar: String, value: E, valueType: CodeGenType)(block: MethodStructure[E] => Unit)

  def allocateSortTable(name: String, tableDescriptor: SortTableDescriptor, count: E): Unit
  def sortTableAdd(name: String, tableDescriptor: SortTableDescriptor, value: E): Unit
  def sortTableSort(name: String, tableDescriptor: SortTableDescriptor): Unit
  def sortTableIterate(name: String, tableDescriptor: SortTableDescriptor,
                       varNameToField: Map[String, String])
                      (block: (MethodStructure[E]) => Unit): Unit

  def loadVariable(varName: String): E

  // arithmetic
  def multiplyPrimitive(lhs: E, rhs: E): E
  def addExpression(lhs: E, rhs: E): E
  def subtractExpression(lhs: E, rhs: E): E
  def multiplyExpression(lhs: E, rhs: E): E
  def divideExpression(lhs: E, rhs: E): E
  def modulusExpression(lhs: E, rhs: E): E

  // predicates
  def threeValuedNotExpression(value: E): E
  def notExpression(value: E): E
  def threeValuedEqualsExpression(lhs: E, rhs: E): E
  def threeValuedPrimitiveEqualsExpression(lhs: E, rhs: E, codeGenType: CodeGenType): E
  def equalityExpression(lhs: E, rhs: E, codeGenType: CodeGenType): E
  def primitiveEquals(lhs: E, rhs: E): E
  def orExpression(lhs: E, rhs: E): E
  def threeValuedOrExpression(lhs: E, rhs: E): E

  // object handling
  def markAsNull(varName: String, codeGenType: CodeGenType): Unit
  def nullablePrimitive(varName: String, codeGenType: CodeGenType, onSuccess: E): E
  def nullableReference(varName: String, codeGenType: CodeGenType, onSuccess: E): E
  def isNull(expr: E, codeGenType: CodeGenType): E
  def isNull(name: String, codeGenType: CodeGenType): E
  def notNull(expr: E, codeGenType: CodeGenType): E
  def notNull(name: String, codeGenType: CodeGenType): E
  def ifNullThenNoValue(expr: E): E // NOTE: Only for non-primitives
  def box(expression:E, codeGenType: CodeGenType): E
  def unbox(expression:E, codeGenType: CodeGenType): E
  def toFloat(expression:E): E

  // parameters, and external data loading
  def expectParameter(key: String, variableName: String, codeGenType: CodeGenType): Unit

  // map
  def mapGetExpression(map: E, key: String): E

  // tracing
  def trace[V](planStepId: String, maybeSuffix: Option[String] = None)(block: MethodStructure[E] => V): V
  def incrementDbHits(): Unit
  def incrementRows(): Unit

  // db access
  def labelScan(iterVar: String, labelIdVar: String): Unit
  def hasLabel(nodeVar: String, labelVar: String, predVar: String): E
  def allNodesScan(iterVar: String): Unit
  def lookupLabelId(labelIdVar: String, labelName: String): Unit
  def lookupLabelIdE(labelName: String): E
  def lookupRelationshipTypeId(typeIdVar: String, typeName: String): Unit
  def lookupRelationshipTypeIdE(typeName: String): E
  def nodeGetRelationshipsWithDirection(iterVar: String, nodeVar: String, nodeVarType: CodeGenType, direction: SemanticDirection): Unit
  def nodeGetRelationshipsWithDirectionAndTypes(iterVar: String, nodeVar: String, nodeVarType: CodeGenType, direction: SemanticDirection, typeVars: Seq[String]): Unit
  def connectingRelationships(iterVar: String, fromNode: String, fromNodeType: CodeGenType, dir: SemanticDirection, toNode:String, toNodeType: CodeGenType)
  def connectingRelationships(iterVar: String, fromNode: String, fromNodeType: CodeGenType, dir: SemanticDirection, types: Seq[String], toNode: String, toNodeType: CodeGenType)
  def nodeFromNodeValueIndexCursor(targetVar: String, iterVar: String): Unit
  def nodeFromNodeCursor(targetVar: String, iterVar: String): Unit
  def nodeFromNodeLabelIndexCursor(targetVar: String, iterVar: String): Unit
  def nextRelationshipAndNode(toNodeVar: String, iterVar: String, direction: SemanticDirection, fromNodeVar: String, relVar: String): Unit
  def nextRelationship(iterVar: String, direction: SemanticDirection, relVar: String): Unit

  def advanceNodeCursor(cursorName: String): E
  def closeNodeCursor(cursorName: String): Unit
  def advanceNodeLabelIndexCursor(cursorName: String): E
  def closeNodeLabelIndexCursor(cursorName: String): Unit
  def advanceRelationshipSelectionCursor(cursorName: String): E
  def closeRelationshipSelectionCursor(cursorName: String): Unit
  def advanceNodeValueIndexCursor(cursorName: String): E
  def closeNodeValueIndexCursor(cursorName: String): Unit

  def nodeGetPropertyById(nodeVar: String, nodeVarType: CodeGenType, propId: Int, propValueVar: String): Unit
  def nodeGetPropertyForVar(nodeVar: String, nodeVarType: CodeGenType, propIdVar: String, propValueVar: String): Unit
  def nodeIdSeek(nodeIdVar: String, expression: E, codeGenType: CodeGenType)(block: MethodStructure[E] => Unit): Unit
  def relationshipGetPropertyById(relIdVar: String, relVarType: CodeGenType, propId: Int, propValueVar: String): Unit
  def relationshipGetPropertyForVar(relIdVar: String, relVarType: CodeGenType, propIdVar: String, propValueVar: String): Unit
  def lookupPropertyKey(propName: String, propVar: String)
  def indexSeek(iterVar: String, descriptorVar: String, value: E, codeGenType: CodeGenType): Unit
  def relType(relIdVar: String, typeVar: String): Unit
  def newIndexReference(descriptorVar: String, labelVar: String, propKeyVar: String): Unit
  def nodeCountFromCountStore(expression: E): E
  def relCountFromCountStore(start: E, end: E, types: E*): E
  def token(t: Int): E
  def wildCardToken: E

  // code structure
  def whileLoop(test: E)(block: MethodStructure[E] => Unit): Unit
  def forEach(varName: String, codeGenType: CodeGenType, iterable: E)(block: MethodStructure[E] => Unit): Unit
  def ifStatement(test: E)(block: MethodStructure[E] => Unit): Unit
  def ifNotStatement(test: E)(block: MethodStructure[E] => Unit): Unit
  def ifNonNullStatement(test: E, codeGenType: CodeGenType)(block: MethodStructure[E] => Unit): Unit
  def ternaryOperator(test: E, onTrue: E, onFalse: E): E
  def returnSuccessfully(): Unit

  // results
  def materializeNode(nodeIdVar: String, codeGenType: CodeGenType): E
  def node(nodeIdVar: String, codeGenType: CodeGenType): E
  def materializeRelationship(relIdVar: String, codeGenType: CodeGenType): E
  def relationship(relIdVar: String, codeGenType: CodeGenType): E
  def materializeAny(expression: E, codeGenType: CodeGenType): E
  /** Feed single row to the given visitor */
  def visitorAccept(): Unit
  def setInRow(column: Int, value: E): Unit
  def toAnyValue(e: E, t: CodeGenType): E
  def toMaterializedAnyValue(e: E, t: CodeGenType): E // Like toAnyValue, but will materialize nodes and relationships into proxies
}

sealed trait Comparator
case object Equal extends Comparator
case object LessThan extends Comparator
case object LessThanEqual extends Comparator
case object GreaterThan extends Comparator
case object GreaterThanEqual extends Comparator

sealed trait JoinTableType
sealed trait CountingJoinTableType extends JoinTableType
sealed trait RecordingJoinTableType extends JoinTableType

case object LongToCountTable extends CountingJoinTableType
case object LongsToCountTable extends CountingJoinTableType
case class LongToListTable(tupleDescriptor: TupleDescriptor, localMap: Map[String, String]) extends RecordingJoinTableType
case class LongsToListTable(tupleDescriptor: TupleDescriptor, localMap: Map[String, String]) extends RecordingJoinTableType

sealed trait SortOrder
case object Ascending extends SortOrder
case object Descending extends SortOrder

case class SortItem(fieldName: String, sortOrder: SortOrder)

/**
  * What we call tuple here is a fixed length collection of query variable values,
  * e.g. a result row (or intermediate row).
  * The TupleDescriptor carries type information used to generate a materialization of this,
  * e.g. a class with fields.
  * The order of individual fields is not specified, but is left as an implementation detail.
  */
sealed trait TupleDescriptor {
  val structure: Map[String, CodeGenType]
}

case class SimpleTupleDescriptor(structure: Map[String, CodeGenType]) extends TupleDescriptor
case class HashableTupleDescriptor(structure: Map[String, CodeGenType]) extends TupleDescriptor
case class OrderableTupleDescriptor(structure: Map[String, CodeGenType],
                                    sortItems: Iterable[SortItem]) extends TupleDescriptor

sealed trait SortTableDescriptor {
  val tupleDescriptor: TupleDescriptor
}

case class FullSortTableDescriptor(tupleDescriptor: OrderableTupleDescriptor) extends SortTableDescriptor
case class TopTableDescriptor(tupleDescriptor: OrderableTupleDescriptor) extends SortTableDescriptor
