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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.spi

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection

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
  def projectVariable(variableName: String, value: E)
  def declareFlag(name: String, initialValue: Boolean)
  def updateFlag(name: String, newValue: Boolean)
  def declarePredicate(name: String): Unit
  def assign(varName: String, codeGenType: CodeGenType, value: E): Unit
  def declare(varName: String, codeGenType: CodeGenType): Unit
  def declareProperty(name: String): Unit
  def declareCounter(name: String, initialValue: E): Unit
  def putField(structure: Map[String, CodeGenType], value: E, fieldType: CodeGenType, fieldName: String, localVar: String): Unit
  def updateProbeTable(structure: Map[String, CodeGenType], tableVar: String, tableType: RecordingJoinTableType, keyVars: Seq[String], element: E): Unit
  def probe(tableVar: String, tableType: JoinTableType, keyVars: Seq[String])(block: MethodStructure[E]=>Unit): Unit
  def updateProbeTableCount(tableVar: String, tableType: CountingJoinTableType, keyVar: Seq[String]): Unit
  def allocateProbeTable(tableVar: String, tableType: JoinTableType): Unit
  def invokeMethod(resultType: JoinTableType, resultVar: String, methodName: String)(block: MethodStructure[E]=>Unit): Unit
  def coerceToBoolean(propertyExpression: E): E

  def incrementInteger(name: String): Unit
  def decrementInteger(name: String): Unit
  def checkInteger(variableName: String, comparator: Comparator, value: Long): E
  def newTableValue(targetVar: String, structure: Map[String, CodeGenType]): E
  def constantExpression(value: Object): E
  def asMap(map: Map[String, E]): E
  def asList(values: Seq[E]): E

  def toSet(value: E): E
  def newSet(name: String, codeGenType: CodeGenType)
  def setContains(name: String, value: E, codeGenType: CodeGenType): E
  def addToSet(name: String, value: E, codeGenType: CodeGenType): Unit
  def newUniqueAggregationKey(varName: String, structure: Map[String, (CodeGenType,E)]): Unit
  def newAggregationMap(name: String, keyTypes: IndexedSeq[CodeGenType])
  def aggregationMapGet(name: String, varName: String, key: Map[String,(CodeGenType,E)], keyVar: String)
  def aggregationMapPut(name: String, key: Map[String,(CodeGenType,E)], keyVar: String, value: E): Unit
  def aggregationMapIterate(name: String, key: Map[String,CodeGenType], valueVar: String)(block: MethodStructure[E] => Unit): Unit
  def newMapOfSets(name: String, keyTypes: IndexedSeq[CodeGenType], elementType: CodeGenType)
  def checkDistinct(name: String, key: Map[String,(CodeGenType, E)], keyVar: String, value: E, valueType: CodeGenType)(block: MethodStructure[E] => Unit)

  def castToCollection(value: E): E

  def loadVariable(varName: String): E

  // arithmetic
  def addExpression(lhs: E, rhs: E): E
  def subtractExpression(lhs: E, rhs: E): E
  def multiplyExpression(lhs: E, rhs: E): E
  def divideExpression(lhs: E, rhs: E): E
  def modulusExpression(lhs: E, rhs: E): E

  // predicates
  def threeValuedNotExpression(value: E): E
  def notExpression(value: E): E
  def threeValuedEqualsExpression(lhs: E, rhs: E): E
  def equalityExpression(lhs: E, rhs: E, codeGenType: CodeGenType): E
  def orExpression(lhs: E, rhs: E): E
  def threeValuedOrExpression(lhs: E, rhs: E): E

  // object handling
  def markAsNull(varName: String, codeGenType: CodeGenType): Unit
  def nullablePrimitive(varName: String, codegenType: CodeGenType, onSuccess: E): E
  def nullableReference(varName: String, codeGenType: CodeGenType, onSuccess: E): E
  def isNull(name: String, codeGenType: CodeGenType): E
  def notNull(name: String, codeGenType: CodeGenType): E
  def box(expression:E): E
  def unbox(expression:E, codeGenType: CodeGenType): E
  def toFloat(expression:E): E

  // parameters
  def expectParameter(key: String, variableName: String): Unit

  // tracing
  def trace[V](planStepId: String)(block: MethodStructure[E] => V): V
  def incrementDbHits(): Unit
  def incrementRows(): Unit

  // db access
  def labelScan(iterVar: String, labelIdVar: String): Unit
  def hasLabel(nodeVar: String, labelVar: String, predVar: String): E
  def allNodesScan(iterVar: String): Unit
  def lookupLabelId(labelIdVar: String, labelName: String): Unit
  def lookupRelationshipTypeId(typeIdVar: String, typeName: String): Unit
  def nodeGetAllRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection): Unit
  def nodeGetRelationships(iterVar: String, nodeVar: String, direction: SemanticDirection, typeVars: Seq[String]): Unit
  def connectingRelationships(iterVar: String, fromNode: String, dir: SemanticDirection, toNode:String)
  def connectingRelationships(iterVar: String, fromNode: String, dir: SemanticDirection, types: Seq[String], toNode: String)
  def nextNode(targetVar: String, iterVar: String): Unit
  def nextRelationshipAndNode(toNodeVar: String, iterVar: String, direction: SemanticDirection, fromNodeVar: String, relVar: String): Unit
  def nextRelationship(iterVar: String, direction: SemanticDirection, relVar: String): Unit
  def hasNextNode(iterVar: String): E
  def hasNextRelationship(iterVar: String): E
  def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String): Unit
  def nodeGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String): Unit
  def nodeIdSeek(nodeIdVar: String, expression: E)(block: MethodStructure[E] => Unit): Unit
  def relationshipGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String): Unit
  def relationshipGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String): Unit
  def lookupPropertyKey(propName: String, propVar: String)
  def indexSeek(iterVar: String, descriptorVar: String, value: E): Unit
  def indexUniqueSeek(name: String, descriptorVar: String, value: E)
  def relType(relIdVar: String, typeVar: String): Unit
  def newIndexDescriptor(descriptorVar: String, labelVar: String, propKeyVar: String): Unit
  def createRelExtractor(extractorName: String): Unit
  def nodeCountFromCountStore(expression: E): E
  def relCountFromCountStore(start: E, end: E, types: E*): E
  def token(t: Int): E
  def wildCardToken: E

  // code structure
  def whileLoop(test: E)(block: MethodStructure[E] => Unit): Unit
  def forEach(varName: String, codeGenType: CodeGenType, iterable: E)(block: MethodStructure[E] => Unit): Unit
  def ifStatement(test: E)(block: MethodStructure[E] => Unit): Unit
  def ifNotStatement(test: E)(block: MethodStructure[E] => Unit): Unit
  def ifNonNullStatement(test: E)(block: MethodStructure[E] => Unit): Unit
  def ternaryOperator(test:E, onSuccess:E, onError: E): E
  def returnSuccessfully(): Unit

  // results
  def materializeNode(nodeIdVar: String): E
  def node(nodeIdVar: String): E
  def materializeRelationship(relIdVar: String): E
  def relationship(relIdVar: String): E
  /** Feed single row to the given visitor */
  def visitorAccept(): Unit
  def setInRow(column: String, value: E): Unit
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
case class LongToListTable(structure: Map[String, CodeGenType], localMap: Map[String, String]) extends RecordingJoinTableType
case class LongsToListTable(structure: Map[String, CodeGenType], localMap: Map[String, String]) extends RecordingJoinTableType
