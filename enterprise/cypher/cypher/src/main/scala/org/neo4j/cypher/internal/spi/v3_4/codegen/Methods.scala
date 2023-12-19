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

import org.neo4j.codegen.MethodReference
import org.neo4j.collection.primitive.{PrimitiveLongIntMap, PrimitiveLongIterator}
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.CompositeKey
import org.neo4j.cypher.internal.codegen._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.QueryExecutionEvent
import org.neo4j.cypher.internal.javacompat.ResultRecord
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.codegen.QueryExecutionTracer
import org.neo4j.cypher.result.QueryResult.{QueryResultVisitor, Record}
import org.neo4j.graphdb.Direction
import org.neo4j.helpers.collection.MapUtil
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor
import org.neo4j.internal.kernel.api.{Read, TokenRead, _}
import org.neo4j.kernel.impl.api.store.RelationshipIterator
import org.neo4j.kernel.impl.api.{RelationshipDataExtractor, RelationshipVisitor}
import org.neo4j.kernel.impl.core.{EmbeddedProxySPI, NodeProxy, RelationshipProxy}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{NodeValue, RelationshipValue, VirtualNodeValue, VirtualRelationshipValue}

object Methods {

  import GeneratedQueryStructure.{method, typeRef}
  val countingTablePut: MethodReference = method[PrimitiveLongIntMap, Int]("put", typeRef[Long], typeRef[Int])
  val countingTableCompositeKeyPut: MethodReference = method[util.HashMap[CompositeKey, Integer], Object]("put", typeRef[Object], typeRef[Object])
  val countingTableGet: MethodReference = method[PrimitiveLongIntMap, Int]("get", typeRef[Long])
  val countingTableCompositeKeyGet: MethodReference = method[util.HashMap[CompositeKey, Integer], Object]("get", typeRef[Object])
  val compositeKey: MethodReference = method[CompiledConversionUtils, CompositeKey]("compositeKey", typeRef[Array[Long]])
  val hasNextLong: MethodReference = method[PrimitiveLongIterator, Boolean]("hasNext")
  val hasMoreRelationship: MethodReference = method[RelationshipIterator, Boolean]("hasNext")
  val createMap: MethodReference = method[MapUtil, util.Map[String, Object]]("map", typeRef[Array[Object]]) // Unused
  val createAnyValueMap: MethodReference = method[MapUtil, util.Map[String, AnyValue]]("genericMap", typeRef[Array[Object]])
  val format: MethodReference = method[String, String]("format", typeRef[String], typeRef[Array[Object]])
  val relationshipVisit: MethodReference = method[RelationshipIterator, Boolean]("relationshipVisit", typeRef[Long], typeRef[RelationshipVisitor[RuntimeException]])
  val getRelationship: MethodReference = method[RelationshipDataExtractor, Long]("relationship")
  val startNode: MethodReference = method[RelationshipDataExtractor, Long]("startNode")
  val endNode: MethodReference = method[RelationshipDataExtractor, Long]("endNode")
  val typeOf: MethodReference = method[RelationshipDataExtractor, Int]("type")
  val allConnectingRelationships: MethodReference = method[CompiledExpandUtils, RelationshipSelectionCursor]("connectingRelationships",
                                                                                                             typeRef[Read],
                                                                                                             typeRef[CursorFactory],
                                                                                                             typeRef[NodeCursor],
                                                                                                             typeRef[Long],
                                                                                                             typeRef[Direction],
                                                                                                             typeRef[Long])
  val connectingRelationships: MethodReference = method[CompiledExpandUtils, RelationshipSelectionCursor]("connectingRelationships",
                                                                                                          typeRef[Read],
                                                                                                          typeRef[CursorFactory],
                                                                                                          typeRef[NodeCursor],
                                                                                                          typeRef[Long],
                                                                                                          typeRef[Direction],
                                                                                                          typeRef[Long],
                                                                                                          typeRef[Array[Int]])

  val mathAdd: MethodReference = method[CompiledMathHelper, Object]("add", typeRef[Object], typeRef[Object])
  val mathSub: MethodReference = method[CompiledMathHelper, Object]("subtract", typeRef[Object], typeRef[Object])
  val mathMul: MethodReference = method[CompiledMathHelper, Object]("multiply", typeRef[Object], typeRef[Object])
  val mathDiv: MethodReference = method[CompiledMathHelper, Object]("divide", typeRef[Object], typeRef[Object])
  val mathMod: MethodReference = method[CompiledMathHelper, Object]("modulo", typeRef[Object], typeRef[Object])
  val mathCastToInt: MethodReference = method[CompiledMathHelper, Int]("transformToInt", typeRef[Object])
  val mathCastToLong: MethodReference = method[CompiledMathHelper, Long]("transformToLong", typeRef[Object])
  val mapGet: MethodReference = method[util.Map[String, Object], Object]("get", typeRef[Object])
  val mapContains: MethodReference = method[util.Map[String, Object], Boolean]("containsKey", typeRef[Object])
  val setContains: MethodReference = method[util.Set[Object], Boolean]("contains", typeRef[Object])
  val setAdd: MethodReference = method[util.Set[Object], Boolean]("add", typeRef[Object])
  val labelGetForName: MethodReference = method[TokenRead, Int]("nodeLabel", typeRef[String])
  val propertyKeyGetForName: MethodReference = method[TokenRead, Int]("propertyKey", typeRef[String])
  val coerceToPredicate: MethodReference = method[CompiledConversionUtils, Boolean]("coerceToPredicate", typeRef[Object])
  val ternaryEquals: MethodReference = method[CompiledConversionUtils, java.lang.Boolean]("equals", typeRef[Object], typeRef[Object])
  val equals: MethodReference = method[Object, Boolean]("equals", typeRef[Object])
  val or: MethodReference = method[CompiledConversionUtils, java.lang.Boolean]("or", typeRef[Object], typeRef[Object])
  val not: MethodReference = method[CompiledConversionUtils, java.lang.Boolean]("not", typeRef[Object])
  val relationshipTypeGetForName: MethodReference = method[TokenRead, Int]("relationshipType", typeRef[String])
  val relationshipTypeGetName: MethodReference = method[TokenRead, String]("relationshipTypeName", typeRef[Int])
  val nodeExists: MethodReference = method[Read, Boolean]("nodeExists", typeRef[Long])
  val countsForNode: MethodReference = method[Read, Long]("countsForNode", typeRef[Int])
  val countsForRel: MethodReference = method[Read, Long]("countsForRelationship", typeRef[Int], typeRef[Int], typeRef[Int])
  val nextLong: MethodReference = method[PrimitiveLongIterator, Long]("next")
  val fetchNextRelationship: MethodReference = method[RelationshipIterator, Long]("next")
  val newNodeProxyById: MethodReference = method[EmbeddedProxySPI, NodeProxy]("newNodeProxy", typeRef[Long])
  val newRelationshipProxyById: MethodReference = method[EmbeddedProxySPI, RelationshipProxy]("newRelationshipProxy", typeRef[Long])
  val materializeAnyResult: MethodReference = method[CompiledConversionUtils, AnyValue]("materializeAnyResult", typeRef[EmbeddedProxySPI], typeRef[Object])
  val materializeAnyValueResult: MethodReference = method[CompiledConversionUtils, AnyValue]("materializeAnyValueResult", typeRef[EmbeddedProxySPI], typeRef[Object])
  val materializeNodeValue: MethodReference = method[CompiledConversionUtils, NodeValue]("materializeNodeValue", typeRef[EmbeddedProxySPI], typeRef[Object])
  val materializeRelationshipValue: MethodReference =
    method[CompiledConversionUtils, RelationshipValue]("materializeRelationshipValue", typeRef[EmbeddedProxySPI], typeRef[Object])
  val nodeId: MethodReference = method[VirtualNodeValue, Long]("id")
  val relId: MethodReference = method[VirtualRelationshipValue, Long]("id")
  val set: MethodReference = method[ResultRecord, Unit]("set", typeRef[Int], typeRef[AnyValue])
  val visit: MethodReference = method[QueryResultVisitor[_], Boolean]("visit", typeRef[Record])
  val executeOperator: MethodReference = method[QueryExecutionTracer, QueryExecutionEvent]("executeOperator", typeRef[Id])
  val dbHit: MethodReference = method[QueryExecutionEvent, Unit]("dbHit")
  val row: MethodReference = method[QueryExecutionEvent, Unit]("row")
  val unboxInteger: MethodReference = method[java.lang.Integer, Int]("intValue")
  val unboxBoolean: MethodReference = method[java.lang.Boolean, Boolean]("booleanValue")
  val unboxLong: MethodReference = method[java.lang.Long, Long]("longValue")
  val unboxDouble: MethodReference = method[java.lang.Double, Double]("doubleValue")
  val unboxNode: MethodReference = method[CompiledConversionUtils, Long]("unboxNodeOrNull", typeRef[VirtualNodeValue])
  val unboxRel: MethodReference = method[CompiledConversionUtils, Long]("unboxRelationshipOrNull", typeRef[VirtualRelationshipValue])
  val reboxValue: MethodReference = method[Values, Object]("asObject", typeRef[Value])
}
