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

object IsEqualTo {

  inline def apply(a: CypherType, b: CypherType): Boolean =
    ignoringNullability(b, a) && a.isNullable == b.isNullable

  inline def ignoringNullability(a: CypherType, b: CypherType): Boolean =
    a == b || equalIgnoringNullability(a, b) || equalIgnoringNullability(b, a)

  /*
   * This needs to list each pair only once, since apply calls it both ways.
   */
  private inline def equalIgnoringNullability(a: CypherType, b: CypherType): Boolean = (a, b) match {
    // when ignoring nullability
    // case (NothingType(), NullType()) => true
    case (a, b) if !a.isNullable && b.isNullable && a == b.withIsNullable(false) => true

    // legacy map type
    case (MapType(aNullable), RecordType.Any(bNullable)) => true

    // legacy node type
    case (NodeType(aNullable), NodeReferenceValueType.Any(bNullable)) => true

    // legacy relationship type
    case (RelationshipType(aNullable), RelationshipReferenceValueType.Any(bNullable)) => true

    // default
    case _ => false
  }
}
