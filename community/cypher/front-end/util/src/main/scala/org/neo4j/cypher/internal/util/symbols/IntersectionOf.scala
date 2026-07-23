/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.symbols

import org.neo4j.cypher.internal.util.InputPosition

object IntersectionOf {
  private val pos = InputPosition.NONE

  def apply(a: CypherType, b: CypherType): CypherType = (a, b) match {
    case (mt: MapType, rt: RecordType)      => nullabilityIntersected(rt, mt)
    case (rt: RecordType, mt: MapType)      => nullabilityIntersected(rt, mt)
    case (rtA: RecordType, rtB: RecordType) => intersectRecordTypes(rtA, rtB)

    case (nt: NodeType, rt: RecordType) if rt.isBaseTypeOpen => intersectNodeTypeAndRecordType(nt, rt)
    case (rt: RecordType, nt: NodeType) if rt.isBaseTypeOpen => intersectNodeTypeAndRecordType(nt, rt)
    case (nt: NodeReferenceValueType, rt: RecordType) if rt.isBaseTypeOpen =>
      intersectNodeReferenceValueTypeAndRecordType(nt, rt)
    case (rt: RecordType, nt: NodeReferenceValueType) if rt.isBaseTypeOpen =>
      intersectNodeReferenceValueTypeAndRecordType(nt, rt)

    case (relT: RelationshipType, rt: RecordType) if rt.isBaseTypeOpen =>
      intersectRelationshipTypeAndRecordType(relT, rt)
    case (rt: RecordType, relT: RelationshipType) if rt.isBaseTypeOpen =>
      intersectRelationshipTypeAndRecordType(relT, rt)
    case (rrt: RelationshipReferenceValueType, rt: RecordType) if rt.isBaseTypeOpen =>
      intersectRelationshipReferenceValueTypeAndRecordType(rrt, rt)
    case (rt: RecordType, rrt: RelationshipReferenceValueType) if rt.isBaseTypeOpen =>
      intersectRelationshipReferenceValueTypeAndRecordType(rrt, rt)

    case (nt: NodeType, nrt: NodeReferenceValueType)                => nullabilityIntersected(nrt, nt)
    case (nrt: NodeReferenceValueType, nt: NodeType)                => nullabilityIntersected(nrt, nt)
    case (ntA: NodeReferenceValueType, ntB: NodeReferenceValueType) => intersectNodeReferenceValueTypes(ntA, ntB)

    case (rt: RelationshipType, rrt: RelationshipReferenceValueType) => nullabilityIntersected(rrt, rt)
    case (rrt: RelationshipReferenceValueType, rt: RelationshipType) => nullabilityIntersected(rrt, rt)
    case (rtA: RelationshipReferenceValueType, rtB: RelationshipReferenceValueType) =>
      intersectRelationshipReferenceValueTypes(rtA, rtB)

    case (lA: ListType, lB: ListType) => intersectListTypes(lA, lB)

    case (du: ClosedDynamicUnionType, o) => intersectDynamicUnionType(du, o)
    case (o, du: ClosedDynamicUnionType) => intersectDynamicUnionType(du, o)

    case _ if IsSubtypeOf(a, b) => a
    case _ if IsSubtypeOf(b, a) => b

    case _ if a.isNullable && b.isNullable => CTNull
    case _                                 => CTNothing
  }

  private inline def intersectAbstractRecordTypes(
    a: AbstractRecordType,
    b: AbstractRecordType
  ): RecordType | NullType | NothingType = {
    val defaultFieldType = IntersectionOf(a.defaultFieldType, b.defaultFieldType)
    val aExclusiveFieldNames = a.fields.keySet -- b.fields.keySet
    val bExclusiveFieldNames = b.fields.keySet -- a.fields.keySet
    val commonFieldNames = a.fields.keySet intersect b.fields.keySet

    val aExclusiveFields =
      aExclusiveFieldNames.map(name => name -> IntersectionOf(a.fieldType(name), b.defaultFieldType))
    val bExclusiveFields =
      bExclusiveFieldNames.map(name => name -> IntersectionOf(b.fieldType(name), a.defaultFieldType))
    val commonFields = commonFieldNames.map(name =>
      name -> IntersectionOf(a.fieldType(name), b.fieldType(name))
    )
    val fields = (aExclusiveFields union bExclusiveFields union commonFields).toMap

    if (fields.exists(_._2.isNothing)) {
      if (intersectNullability(a, b)) {
        CTNull
      } else {
        CTNothing
      }
    } else {
      RecordType(fields, defaultFieldType, isBaseTypeOpen = true, intersectNullability(a, b))(pos)
    }
  }

  private inline def intersectRecordTypes(a: RecordType, b: RecordType): RecordType | NullType | NothingType = {
    intersectAbstractRecordTypes(a, b) match {
      case rtIntersection: RecordType => rtIntersection.copy(isBaseTypeOpen = a.isBaseTypeOpen && b.isBaseTypeOpen)(pos)
      case n: (NullType | NothingType) => n
    }
  }

  private inline def intersectNodeTypeAndRecordType(nt: NodeType, rt: RecordType): NodeReferenceValueType = {
    NodeReferenceValueType(Set.empty, rt.fields, rt.defaultFieldType, intersectNullability(nt, rt))(pos)
  }

  private inline def intersectNodeReferenceValueTypeAndRecordType(
    nt: NodeReferenceValueType,
    rt: RecordType
  ): NodeReferenceValueType | NullType | NothingType = {
    intersectAbstractRecordTypes(nt, rt) match {
      case rtIntersection: RecordType =>
        NodeReferenceValueType(
          nt.labels,
          rtIntersection.fields,
          rtIntersection.defaultFieldType,
          rtIntersection.isNullable
        )(pos)
      case n: (NullType | NothingType) => n
    }
  }

  private inline def intersectNodeReferenceValueTypes(
    a: NodeReferenceValueType,
    b: NodeReferenceValueType
  ): NodeReferenceValueType | NullType | NothingType = {
    val aExclusiveLabels = a.labels -- b.labels
    val bExclusiveLabels = b.labels -- a.labels
    intersectAbstractRecordTypes(a, b) match {
      case rt: RecordType if (b.isOpen || aExclusiveLabels.isEmpty) && (a.isOpen || bExclusiveLabels.isEmpty) =>
        NodeReferenceValueType(a.labels union b.labels, rt.fields, rt.defaultFieldType, rt.isNullable)(pos)
      case _ =>
        if (intersectNullability(a, b)) {
          CTNull
        } else {
          CTNothing
        }
    }
  }

  private inline def intersectRelationshipTypeAndRecordType(
    relT: RelationshipType,
    rt: RecordType
  ): RelationshipReferenceValueType = {
    val endpoint = NodeReferenceValueType.any(false)(pos)
    RelationshipReferenceValueType(
      Option.empty,
      rt.fields,
      rt.defaultFieldType,
      endpoint,
      endpoint,
      intersectNullability(relT, rt)
    )(pos)
  }

  private inline def intersectRelationshipReferenceValueTypeAndRecordType(
    rrt: RelationshipReferenceValueType,
    rt: RecordType
  ): RelationshipReferenceValueType | NullType | NothingType = {
    intersectAbstractRecordTypes(rrt, rt) match {
      case rtIntersection: RecordType =>
        RelationshipReferenceValueType(
          rrt.label,
          rtIntersection.fields,
          rtIntersection.defaultFieldType,
          rrt.source,
          rrt.destination,
          rtIntersection.isNullable
        )(pos)
      case n: (NullType | NothingType) => n
    }
  }

  private inline def intersectRelationshipReferenceValueTypes(
    a: RelationshipReferenceValueType,
    b: RelationshipReferenceValueType
  ): RelationshipReferenceValueType | NullType | NothingType = {
    val source = intersectNodeReferenceValueTypes(a.source, b.source)
    val destination = intersectNodeReferenceValueTypes(a.destination, b.destination)
    val record = intersectAbstractRecordTypes(a, b)
    (record, source, destination) match {
      case (rt: RecordType, s: NodeReferenceValueType, d: NodeReferenceValueType)
        if (a.isOpen || b.isOpen || a.label == b.label) =>
        RelationshipReferenceValueType(
          a.label.orElse(b.label),
          rt.fields,
          rt.defaultFieldType,
          s,
          d,
          rt.isNullable
        )(pos)
      case _ =>
        if (intersectNullability(a, b)) {
          CTNull
        } else {
          CTNothing
        }
    }
  }

  private inline def intersectListTypes(a: ListType, b: ListType): ListType = {
    ListType(IntersectionOf(a.innerType, b.innerType), intersectNullability(a, b))(pos)
  }

  private inline def intersectDynamicUnionType(du: ClosedDynamicUnionType, other: CypherType): CypherType = {
    du.innerTypes.map(innerType => IntersectionOf(innerType, other)).filterNot(_.isNothing) match {
      case set if set.isEmpty   => NothingType()(pos)
      case set if set.size == 1 => set.head
      case set                  => CypherType.normalizeTypes(ClosedDynamicUnionType(set)(pos))
    }
  }

  /*
   * Returns the first type set nullable only nullable if both types (first and second) are nullable.
   */
  private inline def nullabilityIntersected(t: CypherType, o: CypherType): CypherType =
    t.withIsNullable(intersectNullability(t, o))

  /*
   * Returns true if both types (first and second) are nullable.
   */
  private inline def intersectNullability(a: CypherType, b: CypherType): Boolean = a.isNullable && b.isNullable
}
