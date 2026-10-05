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

object IsSubtypeOf extends TypeAssignabilityRelationship {

  override def apply(sub: CypherType, sup: CypherType): Boolean = (sub, sup) match {
    // basics
    case (NothingType(), _) => true
    case (NullType(), _)    => sup.isNullable

    case (sub, sup) if IsEqualTo.ignoringNullability(sub, sup) =>
      isNullableSub(sub, sup)
    // case (sub, sup) if !sub.isNullable && sup.isNullable && sub == sup.withIsNullable(false) => true

    case (_, AnyType(_)) => isNullableSub(sub, sup)

    // number types
    case (Integer8Type(_), Integer16Type(_) | Integer32Type(_) | IntegerType(_) | NumberType(_)) =>
      isNullableSub(sub, sup)
    case (Integer16Type(_), Integer32Type(_) | IntegerType(_) | NumberType(_)) => isNullableSub(sub, sup)
    case (Integer32Type(_), IntegerType(_) | NumberType(_))                    => isNullableSub(sub, sup)
    case (IntegerType(_), NumberType(_))                                       => isNullableSub(sub, sup)

    case (Float16Type(_), Float32Type(_) | FloatType(_) | NumberType(_))  => isNullableSub(sub, sup)
    case (BFloat16Type(_), Float32Type(_) | FloatType(_) | NumberType(_)) => isNullableSub(sub, sup)
    case (Float32Type(_), FloatType(_) | NumberType(_))                   => isNullableSub(sub, sup)
    case (FloatType(_), NumberType(_))                                    => isNullableSub(sub, sup)

    // property value types
    case (_, PropertyValueType(_)) => sub.canBeStoredInProperty && isNullableSub(sub, sup)

    // vector types
    case (VectorType(_, _, _), VectorType(None, None, _)) => isNullableSub(sub, sup)
    case (VectorType(_, Some(subDim), _), VectorType(None, Some(supDim), _)) =>
      subDim == supDim && isNullableSub(sub, sup)
    case (VectorType(Some(subInnerType), _, _), VectorType(Some(supInnerType), None, _)) =>
      subInnerType.withIsNullable(false) == supInnerType.withIsNullable(false) && isNullableSub(sub, sup)
    case (VectorType(Some(subInnerType), Some(subDim), _), VectorType(Some(supInnerType), Some(supDim), _)) =>
      subDim == supDim && subInnerType.withIsNullable(false) == supInnerType.withIsNullable(false) && isNullableSub(
        sub,
        sup
      )

    // list types
    case (ListType(subInner, _), ListType(supInner, _)) =>
      IsSubtypeOf(subInner, supInner) && isNullableSub(sub, sup)

    // dynamic union types
    case (ClosedDynamicUnionType(innerTypes), _) => innerTypes.forall(inner => IsSubtypeOf(inner, sup))
    case (_, ClosedDynamicUnionType(innerTypes)) => innerTypes.exists(inner => IsSubtypeOf(sub, inner))

    // record types
    case (sub: RecordType, sup: RecordType) => isAbstractRecordSub(sub, sup)
    case (_: RecordType, MapType(_))        => isNullableSub(sub, sup)

    // node reference value types and legacy node types
    case (sub: NodeReferenceValueType, sup: NodeReferenceValueType) => isNodeReferenceSub(sub, sup)
    case (NodeReferenceValueType(_, _, _, _), NodeType(_))          => isNullableSub(sub, sup)

    // relationship reference value types
    case (sub: RelationshipReferenceValueType, sup: RelationshipReferenceValueType) =>
      isRelationshipReferenceSub(sub, sup)
    case (RelationshipReferenceValueType(_, _, _, _, _, _), RelationshipType(_)) => isNullableSub(sub, sup)

    // default
    case (_, _) => false
  }

  override protected inline def opennessRequirementForGraphElementLabels(
    sup: NodeReferenceValueType | RelationshipReferenceValueType
  ): Boolean = sup.isOpen

  override protected inline def opennessRequirementForAbstractRecords(
    sub: AbstractRecordType,
    sup: AbstractRecordType
  ): Boolean = (sup.isOpen || !sub.isOpen)

  override protected inline def defaultFieldTypeRequirementForAbstractRecords(
    sub: AbstractRecordType,
    sup: AbstractRecordType
  ): Boolean =
    IsSubtypeOf(sub.defaultFieldType, sup.defaultFieldType)

  override protected inline def fillSup(sup: AbstractRecordType, allSubFieldNames: Set[String]): AbstractRecordType = {
    val fieldsFilled = allSubFieldNames.map(name => name -> sup.fields.getOrElse(name, sup.defaultFieldType)).toMap
    val filled = sup.withFields(fieldsFilled)
    filled
  }
}
