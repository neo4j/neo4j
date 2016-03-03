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
package org.neo4j.cypher.internal.compiler.v3_0.codegen

import org.neo4j.cypher.internal.compiler.v3_0.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.symbols.CypherType

/**
 * This constitutes the SPI for code generation.
 */
trait CodeStructure[T] {
  type SourceSink = (String, String) => Unit
  def generateQuery(packageName: String, className: String, columns: Seq[String], operatorIds: Map[String, Id], sourceSink: Option[SourceSink])
                   (block: MethodStructure[_] => Unit)(implicit codeGenContext: CodeGenContext): T
}

sealed trait JoinTableType
sealed trait CountingJoinTableType extends JoinTableType
sealed trait RecordingJoinTableType extends JoinTableType

case object LongToCountTable extends CountingJoinTableType
case object LongsToCountTable extends CountingJoinTableType
case class LongToListTable(structure: Map[String, CypherType], localMap: Map[String, String]) extends RecordingJoinTableType
case class LongsToListTable(structure: Map[String, CypherType], localMap: Map[String, String]) extends RecordingJoinTableType

trait MethodStructure[E] {


  // misc
  def projectVariable(variableName: String, value: E)
  def declareFlag(name: String, initialValue: Boolean)
  def updateFlag(name: String, newValue: Boolean)
  def declarePredicate(name: String): Unit
  def declare(varName: String, cypherType: CypherType): Unit
  def declareProperty(name: String): Unit
  def declareCounter(name: String, initialValue: E): Unit
  def putField(structure: Map[String, CypherType], value: E, fieldType: CypherType, fieldName: String, localVar: String): Unit
  def updateProbeTable(structure: Map[String, CypherType], tableVar: String, tableType: RecordingJoinTableType, keyVars: Seq[String], element: E): Unit
  def probe(tableVar: String, tableType: JoinTableType, keyVars: Seq[String])(block: MethodStructure[E]=>Unit): Unit
  def updateProbeTableCount(tableVar: String, tableType: CountingJoinTableType, keyVar: Seq[String]): Unit
  def allocateProbeTable(tableVar: String, tableType: JoinTableType): Unit
  def method(resultType: JoinTableType, resultVar: String, methodName: String)(block: MethodStructure[E]=>Unit): Unit
  def coerceToBoolean(propertyExpression: E): E

  // expressions
  def decreaseCounterAndCheckForZero(name: String): E
  def counterEqualsZero(variableName: String): E
  def newTableValue(targetVar: String, structure: Map[String, CypherType]): E
  def constant(value: Object): E
  def asMap(map: Map[String, E]): E
  def asList(values: Seq[E]): E

  def toSet(value: E): E

  def castToCollection(value: E): E

  def load(varName: String): E

  // arithmetic
  def add(lhs: E, rhs: E): E
  def sub(lhs: E, rhs: E): E
  def mul(lhs: E, rhs: E): E
  def div(lhs: E, rhs: E): E
  def mod(lhs: E, rhs: E): E

  // predicates
  def threeValuedNot(value: E): E
  def not(value: E): E
  def threeValuedEquals(lhs: E, rhs: E): E
  def eq(lhs: E, rhs: E): E
  def or(lhs: E, rhs: E): E
  def threeValuedOr(lhs: E, rhs: E): E

  // null handling
  def markAsNull(varName: String, cypherType: CypherType): Unit
  def nullable(varName: String, cypherType: CypherType, onSuccess: E): E
  def notNull(name: String, cypherType: CypherType): E

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
  def hasNext(iterVar: String): E
  def nodeGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String): Unit
  def nodeGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String): Unit
  def relationshipGetPropertyById(nodeIdVar: String, propId: Int, propValueVar: String): Unit
  def relationshipGetPropertyForVar(nodeIdVar: String, propIdVar: String, propValueVar: String): Unit
  def lookupPropertyKey(propName: String, propVar: String)
  def indexSeek(iterVar: String, descriptorVar: String, value: E): Unit
  def indexUniqueSeek(name: String, descriptorVar: String, value: E)
  def relType(relIdVar: String, typeVar: String): Unit
  def newIndexDescriptor(descriptorVar: String, labelVar: String, propKeyVar: String): Unit
  def createRelExtractor(extractorName: String): Unit


  // code structure
  def whileLoop(test: E)(block: MethodStructure[E] => Unit): Unit
  def forEach(varName: String, cypherType: CypherType, iterable: E)(block: MethodStructure[E] => Unit): Unit
  def ifStatement(test: E)(block: MethodStructure[E] => Unit): Unit
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
