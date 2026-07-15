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
package org.neo4j.cypher.internal.util.types

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.CypherType.normalizeTypes
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeReferenceValueType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.RecordType
import org.neo4j.cypher.internal.util.symbols.RelationshipReferenceValueType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.BuildFrom
import scala.util.Random

trait CypherTypeTestSuite extends CypherFunSuite {

  protected val pos = InputPosition.NONE

  protected val nullableAnyType = AnyType(true)(pos)
  protected val notNullableAnyType = AnyType(false)(pos)

  protected val nothingType = NothingType()(pos)
  protected val nullType = NothingType()(pos)

  /*
   * randomization helpers
   */

  protected def pickNFrom[T](n: Int, xs: Iterable[T])(using rand: Random): Iterable[T] = rand.shuffle(xs).take(n)

  protected def pickSubset[T](set: Set[T])(using rand: Random): Set[T] =
    pickNFrom(rand.nextInt(set.size + 1), set).toSet

  /*
   * type predicates
   */

  protected def noListType(t: CypherType): Boolean = normalizeTypes(t) match {
    case _: ListType               => false
    case u: ClosedDynamicUnionType => u.innerTypes.forall(noListType)
    case _                         => true
  }

  protected def noRecordType(t: CypherType): Boolean = normalizeTypes(t) match {
    case _: RecordType             => false
    case _: MapType                => false
    case u: ClosedDynamicUnionType => u.innerTypes.forall(noRecordType)
    case _                         => true
  }

  /*
   * type construction helpers
   */

  protected inline def rt(fields: (String, CypherType)*): RecordType =
    RecordType(fields.toMap, isFieldOpen = true, isBaseTypeOpen = true, isNullable = true)(pos)
  protected inline def rt(fields: Set[(String, CypherType)]): RecordType = rt(fields.toSeq: _*)

  protected inline def nrt(labels: Set[String], fields: (String, CypherType)*): NodeReferenceValueType =
    NodeReferenceValueType(labels, fields.toMap, isFieldOpen = true, isNullable = true)(pos)

  protected inline def rrt(
    label: Option[String],
    source: NodeReferenceValueType,
    destination: NodeReferenceValueType,
    fields: (String, CypherType)*
  ): RelationshipReferenceValueType =
    RelationshipReferenceValueType(label, fields.toMap, isFieldOpen = true, source, destination, isNullable = true)(pos)

  protected inline def u(types: Set[CypherType]): ClosedDynamicUnionType | NothingType = {
    if (types.isEmpty) NothingType()(pos)
    else ClosedDynamicUnionType(types)(pos)
  }

  protected inline def l(elementType: CypherType): ListType = ListType(elementType, isNullable = true)(pos)

  extension (t: CypherType) {
    inline def notNull: CypherType = t.withIsNullable(isNullable = false)
    inline def nullable: CypherType = t.withIsNullable(isNullable = true)
    inline def |(o: CypherType): ClosedDynamicUnionType = ClosedDynamicUnionType(Set(t, o))(pos)
  }

  extension (du: ClosedDynamicUnionType) {

    def |(o: CypherType): ClosedDynamicUnionType = o match {
      case ou: ClosedDynamicUnionType => ClosedDynamicUnionType(du.innerTypes union ou.innerTypes)(pos)
      case _                          => ClosedDynamicUnionType(du.innerTypes + o)(pos)
    }
    inline def norm: CypherType = CypherType.normalizeTypes(du)
  }

  extension (rt: RecordType) {
    inline def fieldClosed: RecordType = rt.copy(defaultFieldType = NothingType()(pos))(pos)
    inline def baseTypeClosed: RecordType = rt.copy(isBaseTypeOpen = false)(pos)
    inline def default(cypherType: CypherType): RecordType = rt.copy(defaultFieldType = cypherType)(pos)
  }

  extension (nrt: NodeReferenceValueType) {
    inline def closed: NodeReferenceValueType = nrt.copy(defaultFieldType = NothingType()(pos))(pos)
    inline def default(cypherType: CypherType): NodeReferenceValueType = nrt.copy(defaultFieldType = cypherType)(pos)
  }

  extension (set: Set[String]) {
    inline def &(o: String): Set[String] = set + o
  }

  extension (s: String) {
    inline def ::(t: CypherType): (String, CypherType) = (s, t)
    inline def &(o: String): Set[String] = Set(s, o)
    inline def label: Set[String] = Set(s)
  }
}
