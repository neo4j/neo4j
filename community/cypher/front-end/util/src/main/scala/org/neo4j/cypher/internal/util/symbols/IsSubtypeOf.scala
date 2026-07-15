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

object IsSubtypeOf {

  def apply(sub: CypherType, sup: CypherType): Boolean = (sub, sup) match {
    // basics
    case (NothingType(), _) => true
    case (NullType(), _)    => sup.isNullable

    case (sub, sup) if IsEqualTo.ignoringNullability(sub, sup) =>
      isNullableSubtype(sub, sup)
    // case (sub, sup) if !sub.isNullable && sup.isNullable && sub == sup.withIsNullable(false) => true

    case (_, AnyType(_)) => isNullableSubtype(sub, sup)

    // number types
    case (Integer8Type(_), Integer16Type(_) | Integer32Type(_) | IntegerType(_) | NumberType(_)) =>
      isNullableSubtype(sub, sup)
    case (Integer16Type(_), Integer32Type(_) | IntegerType(_) | NumberType(_)) => isNullableSubtype(sub, sup)
    case (Integer32Type(_), IntegerType(_) | NumberType(_))                    => isNullableSubtype(sub, sup)
    case (IntegerType(_), NumberType(_))                                       => isNullableSubtype(sub, sup)

    case (Float32Type(_), FloatType(_) | NumberType(_)) => isNullableSubtype(sub, sup)
    case (FloatType(_), NumberType(_))                  => isNullableSubtype(sub, sup)

    // property value types
    case (_, PropertyValueType(_)) => sub.canBeStoredInProperty && isNullableSubtype(sub, sup)

    // vector types
    case (VectorType(_, _, _), VectorType(None, None, _)) => isNullableSubtype(sub, sup)
    case (VectorType(_, Some(subDim), _), VectorType(None, Some(supDim), _)) =>
      subDim == supDim && isNullableSubtype(sub, sup)
    case (VectorType(Some(subInnerType), _, _), VectorType(Some(supInnerType), None, _)) =>
      subInnerType.withIsNullable(false) == supInnerType.withIsNullable(false) && isNullableSubtype(sub, sup)
    case (VectorType(Some(subInnerType), Some(subDim), _), VectorType(Some(supInnerType), Some(supDim), _)) =>
      subDim == supDim && subInnerType.withIsNullable(false) == supInnerType.withIsNullable(false) && isNullableSubtype(
        sub,
        sup
      )

    // list types
    case (ListType(subInner, _), ListType(supInner, _)) =>
      IsSubtypeOf(subInner, supInner) && isNullableSubtype(sub, sup)

    // dynamic union types
    case (ClosedDynamicUnionType(innerTypes), _) => innerTypes.forall(inner => IsSubtypeOf(inner, sup))
    case (_, ClosedDynamicUnionType(innerTypes)) => innerTypes.exists(inner => IsSubtypeOf(sub, inner))

    // record types
    case (sub: RecordType, sup: RecordType) =>
      (sup.isBaseTypeOpen || !sub.isBaseTypeOpen) && isAbstractRecordSubtype(sub, sup)
    case (_: RecordType, MapType(_)) => isNullableSubtype(sub, sup)

    // node reference value types and legacy node types
    case (
        sub @ NodeReferenceValueType(subLabels, _, _, _),
        sup @ NodeReferenceValueType(supLabels, _, _, _)
      ) =>
      val labelsOk = if (sup.isOpen) supLabels subsetOf subLabels else supLabels == subLabels
      labelsOk && isAbstractRecordSubtype(sub, sup)
    case (sub: NodeReferenceValueType, sup: RecordType) if sup.isBaseTypeOpen => isAbstractRecordSubtype(sub, sup)
    case (NodeReferenceValueType(_, _, _, _), NodeType(_) | MapType(_))       => isNullableSubtype(sub, sup)
    case (NodeType(_), MapType(_) | RecordType.Any(_))                        => isNullableSubtype(sub, sup)

    // relationship reference value types
    case (
        sub @ RelationshipReferenceValueType(subLabel, _, _, subSource, subDestination, _),
        sup @ RelationshipReferenceValueType(supLabel, _, _, supSource, supDestination, _)
      ) =>
      val labelsOk = (sup.isOpen && sup.label.isEmpty) || supLabel == subLabel
      labelsOk && isAbstractRecordSubtype(sub, sup) && IsSubtypeOf(subSource, supSource) && IsSubtypeOf(
        subDestination,
        supDestination
      )
    case (sub: RelationshipReferenceValueType, sup: RecordType) if sup.isBaseTypeOpen =>
      isAbstractRecordSubtype(sub, sup)
    case (RelationshipReferenceValueType(_, _, _, _, _, _), RelationshipType(_) | MapType(_)) =>
      isNullableSubtype(sub, sup)
    case (RelationshipType(_), MapType(_) | RecordType.Any(_)) => isNullableSubtype(sub, sup)

    // default
    case (_, _) => false
  }

  private def isAbstractRecordSubtype(sub: AbstractRecordType, sup: AbstractRecordType): Boolean = {
    inline def fill(r: AbstractRecordType, allFieldNames: Set[String]): AbstractRecordType = {
      val fieldsFilled = allFieldNames.map(name => name -> r.fields.getOrElse(name, r.defaultFieldType)).toMap
      val filled = sup.withFields(fieldsFilled)
      filled
    }

    isNullableSubtype(sub, sup) &&
    (sup.isFieldOpen || !sub.isFieldOpen) &&
    (sup.fields.keySet subsetOf sub.fields.keySet) &&
    IsSubtypeOf(sub.defaultFieldType, sup.defaultFieldType) && {
      val supFilled = fill(sup, sub.fields.keySet)
      supFilled.fields.forall {
        case (name, supFieldType) =>
          IsSubtypeOf(sub.fields(name), supFieldType)
      }
    }
  }

  /* Decides whether sub is a subtype of sup purely regarding the isNullable flag.
   * sub is a subtype of sup if one of the following is true:
   * 1) sup is nullable (it does not matter if sub is nullable or not in this case)
   * 2) if sub is not nullable (it does not matter if sup is nullable or not in this case)
   */
  private inline def isNullableSubtype(sub: CypherType, sup: CypherType): Boolean = sup.isNullable || !sub.isNullable
}
