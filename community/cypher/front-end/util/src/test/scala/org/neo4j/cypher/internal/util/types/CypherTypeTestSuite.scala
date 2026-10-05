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
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTAnyNotNull
import org.neo4j.cypher.internal.util.symbols.CTBFloat16
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTFloat16
import org.neo4j.cypher.internal.util.symbols.CTFloat32
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTInteger16
import org.neo4j.cypher.internal.util.symbols.CTInteger32
import org.neo4j.cypher.internal.util.symbols.CTInteger8
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNothing
import org.neo4j.cypher.internal.util.symbols.CTNull
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTStringNotNull
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CTUUID
import org.neo4j.cypher.internal.util.symbols.CTVector
import org.neo4j.cypher.internal.util.symbols.CTZonedDateTime
import org.neo4j.cypher.internal.util.symbols.CTZonedTime
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.CypherType.normalizeTypes
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeReferenceValueType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.RecordType
import org.neo4j.cypher.internal.util.symbols.RelationshipReferenceValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.BuildFrom
import scala.language.implicitConversions
import scala.util.Random

trait CypherTypeTestSuite extends CypherFunSuite {

  protected val pos: InputPosition.Range = InputPosition.NONE

  protected val baseTypeRepresentatives_Legacy: Set[CypherType] = Set(
    CTBoolean,
    CTInteger,
    CTFloat,
    CTVector,
    CTString,
    CTList(CTAny),
    CTMap,
    CTNode,
    CTRelationship,
    CTPath,
    CTPoint,
    CTTime,
    CTLocalTime,
    CTZonedTime,
    CTDate,
    CTDateTime,
    CTLocalDateTime,
    CTZonedDateTime,
    CTDuration,
    CTUUID
  )

  protected val baseTypeRepresentatives: Set[CypherType] = Set(
    CTBoolean,
    CTInteger,
    CTFloat,
    CTVector,
    CTString,
    CTList(CTAny),
    rt("z" :: CTBoolean),
    nrt(Set.empty),
    rrt(None, nrt(Set.empty).notNull, nrt(Set.empty).notNull),
    CTPath,
    CTPoint,
    CTTime,
    CTLocalTime,
    CTZonedTime,
    CTDate,
    CTDateTime,
    CTLocalDateTime,
    CTZonedDateTime,
    CTDuration,
    CTUUID
  )

  /*
   * lattices
   */

  case class Lattice(name: String, edges: ((CypherType, CypherType), Boolean)*) {
    def coercionEdges: Seq[(CypherType, CypherType)] = edges.filter(_._2).map(_._1)
    def edgesWithCoercion: Seq[(CypherType, CypherType)] = edges.map(_._1)
    def edgesWithoutCoercion: Seq[(CypherType, CypherType)] = edges.filterNot(_._2).map(_._1)

    def allTypes: Set[CypherType] = edges.flatMap {
      case ((a, b), _) => Set(a, b)
    }.toSet
  }

  def coercing(edge: (CypherType, CypherType)): ((CypherType, CypherType), Boolean) = (edge, true)

  given CypherTypeCypherType2CypherTypeCypherTypeBoolean
    : Conversion[(CypherType, CypherType), ((CypherType, CypherType), Boolean)] with
    def apply(edge: (CypherType, CypherType)): ((CypherType, CypherType), Boolean) = (edge, false)

  object Lattice {

    /*
     * Subtype lattice — arcs point upward (subtype below -> supertype above).
     *
     *   CTAny
     *     |
     *   CTNull
     *     |
     *   CTNothing
     */
    val basics: Lattice = Lattice(
      "basics",
      CTNothing -> CTNull,
      CTNull -> CTAny
    )

    /*
     * Subtype lattice — arcs point upward (subtype below -> supertype above).
     *
     *            CTAny
     *           /     \
     *     CTString   CTAnyNotNull
     *      /     \    /
     *  CTNull  CTStringNotNull
     *      \     /
     *     CTNothing
     */
    val basics2: Lattice = Lattice(
      "basics 2",
      CTNothing -> CTNull,
      CTNothing -> CTStringNotNull,
      CTStringNotNull -> CTString,
      CTStringNotNull -> CTAnyNotNull,
      CTNull -> CTString,
      CTString -> CTAny,
      CTAnyNotNull -> CTAny
    )

    /*
     * Subtype lattice — arcs point upward (subtype below -> supertype above)
     *           CTNumber
     *          /        \
     *   CTInteger      CTFloat
     *         |          |
     *   CTInteger32    CTFloat32
     *         |          /    \
     *   CTInteger16  CTFloat16 CTBFloat16
     *         |
     *   CTInteger8
     */
    val numericTypes: Lattice = Lattice(
      "numeric types",
      CTInteger8 -> CTInteger16,
      CTInteger16 -> CTInteger32,
      CTInteger32 -> CTInteger,
      CTFloat16 -> CTFloat32,
      CTBFloat16 -> CTFloat32,
      CTFloat32 -> CTFloat,
      CTInteger -> CTNumber,
      CTFloat -> CTNumber
    )

    val vectorTypes: Lattice = {
      val v = VectorType(None, None, isNullable = true)(pos)
      val vF32 = VectorType(Some(CTFloat32), None, isNullable = true)(pos)
      val vF = VectorType(Some(CTFloat), None, isNullable = true)(pos)
      val vF32D3 = VectorType(Some(CTFloat32), Some(3), isNullable = true)(pos)
      val vFD3 = VectorType(Some(CTFloat), Some(3), isNullable = true)(pos)
      val vF32D100 = VectorType(Some(CTFloat32), Some(100), isNullable = true)(pos)
      val vFD100 = VectorType(Some(CTFloat), Some(100), isNullable = true)(pos)
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       *
       *                  v
       *                /   \
       *              /       \
       *            /           \
       *       vF32                vF
       *     /      \           /      \
       *  vF32D3  vF32D100   vFD3    vFD100
       */
      Lattice(
        "vector types",
        vF32D3 -> vF32,
        vF32D100 -> vF32,
        vFD3 -> vF,
        vFD100 -> vF,
        vF32 -> v,
        vF -> v
      )
    }

    val recordTypes: Lattice = {
      val mt = CTMap
      val aIbS_o = rt("a" :: CTInteger.notNull, "b" :: CTString.notNull)
      val aIbS_S = aIbS_o.default(CTString.notNull)
      val aIbS_c = aIbS_o.closed
      val aI_o = rt("a" :: CTInteger.notNull)
      val aI_S = aI_o.default(CTString.notNull)
      val aI_c = aI_o.closed
      val bS_o = rt("b" :: CTString.notNull)
      val bS_S = bS_o.default(CTString.notNull)
      val bS_c = bS_o.closed
      val empty_o = rt()
      val empty_c = empty_o.closed
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       * Coercion — dashed arrows (source type -> target type)
       *
       *             empty_o == mt
       *             /    | ┊    \
       *            /     | ↓     \
       *  ╭╌╌╌╌╌ aI_o   empty_c   bS_o ╌╌╌╌╌╮
       *  ┊     /    \           /    \     ┊
       *  ┊    /      \         /      \    ┊
       *  ┊  aI_S        aIbS_o       bS_S  ┊
       *  ┊   |   \       |  ┊          |   ┊
       *  ┊   |     \     |  ┊          |   ┊
       *  ┊   |       \   |  ┊          |   ┊
       *  ╰→ aI_c     aIbS_S ┊        bS_c ←╯
       *                  |  ┊
       *                  |  ↓
       *                 aIbS_c
       */
      Lattice(
        "record types",
        aIbS_c -> aIbS_S,
        aIbS_S -> aIbS_o,
        aIbS_S -> aI_S,
        // aIbS_S -> bS_S, // not because bS_S mandates all other fields to be STRING, i.e. field a cannot be INTEGER as in aIbS_S
        aIbS_o -> aI_o,
        aIbS_o -> bS_o,
        coercing(aIbS_o -> aIbS_c),
        aI_c -> aI_S,
        aI_S -> aI_o,
        bS_c -> bS_S,
        bS_S -> bS_o,
        aI_o -> empty_o,
        coercing(aI_o -> aI_c),
        bS_o -> empty_o,
        coercing(bS_o -> bS_c),
        empty_c -> empty_o,
        coercing(empty_o -> empty_c),
        empty_o -> mt,
        mt -> empty_o
      )
    }

    val recordTypes_withNullableFields: Lattice = {
      val mt = CTMap
      val aNull_o = rt("a" :: CTNull)
      val aNull_S = aNull_o.default(CTString)
      val aNull_c = aNull_o.closed
      val aI_o = rt("a" :: CTInteger)
      val aI_S = aI_o.default(CTString)
      val aI_c = aI_o.closed
      val aS_o = rt("a" :: CTString)
      val aS_S = aS_o.default(CTString)
      val aS_c = aS_o.closed
      val empty_o = rt()
      val empty_c = empty_o.closed
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       * Coercion — dashed arrows (source type -> target type)
       *
       *           empty_o == mt
       *            /   | ┊   \
       *           /    | ↓    \
       *  ╭╌╌╌ aI_o   empty_c   aS_o ╌╌╌╮
       *  ┊    /   \           /   \    ┊
       *  ┊   /     \         /     \   ┊
       *  ┊  aI_S     aNull_o     aS_S  ┊
       *  ┊  |   \      | ┊      /   |  ┊
       *  ┊  |     \    | ┊    /     |  ┊
       *  ┊  |       \  | ┊  /       |  ┊
       *  ╰→ aI_c     aNull_S     aS_c ←╯
       *          \     | ┊     /
       *            \   | ↓   /
       *              aNull_c
       */
      Lattice(
        "record types with nullable fields",
        aNull_c -> aI_c,
        aNull_c -> aNull_S,
        aNull_c -> aS_c,
        aNull_S -> aI_S,
        aNull_S -> aNull_o,
        aNull_S -> aS_S,
        aNull_o -> aI_o,
        aNull_o -> aS_o,
        coercing(aNull_o -> aNull_c),
        aI_c -> aI_S,
        aI_S -> aI_o,
        aS_c -> aS_S,
        aS_S -> aS_o,
        aI_o -> empty_o,
        coercing(aI_o -> aI_c),
        aS_o -> empty_o,
        coercing(aS_o -> aS_c),
        empty_c -> empty_o,
        coercing(empty_o -> empty_c),
        empty_o -> mt,
        mt -> empty_o
      )
    }

    val recordTypesRandomized: Lattice = {
      val recordTypesRandomizedSeed = Random.nextInt(1024)

      given Random = new Random(recordTypesRandomizedSeed)

      val fieldTypes = baseTypeRepresentatives.zipWithIndex.map { case (t, i) => s"f$i" -> t }

      val a = pickSubset(fieldTypes)
      val b = pickSubset(fieldTypes)
      val c = pickSubset(fieldTypes)
      val aXbXc = a union b union c
      val aXb = a union b
      val aXc = a union c
      val bXc = b union c
      val aUb = a intersect b
      val aUc = a intersect c
      val bUc = b intersect c
      val aUbUc = a intersect b intersect c
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above).
       * Every label is wrapped in rt(...);  X = two edges crossing (not a node).
       *
       *        aUbUc
       *         /|\
       *       /  |  \
       *     /    |    \
       *   aUb   aUc   bUc
       *    |\   / \   /|
       *    |  X     X  |
       *    |/   \ /   \|
       *    a     b     c
       *    |\   / \   /|
       *    |  X     X  |
       *    |/   \ /   \|
       *   aXb   aXc   bXc
       *     \    |    /
       *       \  |  /
       *         \|/
       *        aXbXc
       */
      Lattice(
        s"record types randomized (seed: $recordTypesRandomizedSeed)",
        rt(aXbXc) -> rt(aXb),
        rt(aXbXc) -> rt(aXc),
        rt(aXbXc) -> rt(bXc),
        rt(aXb) -> rt(a),
        rt(aXb) -> rt(b),
        rt(aXc) -> rt(a),
        rt(aXc) -> rt(c),
        rt(bXc) -> rt(b),
        rt(bXc) -> rt(c),
        rt(a) -> rt(aUb),
        rt(b) -> rt(aUb),
        rt(a) -> rt(aUc),
        rt(c) -> rt(aUc),
        rt(b) -> rt(bUc),
        rt(c) -> rt(bUc),
        rt(aUb) -> rt(aUbUc),
        rt(aUc) -> rt(aUbUc),
        rt(bUc) -> rt(aUbUc)
      )
    }

    val nodeReferenceValueTypes_labelsOnly: Lattice = {
      val nt = CTNode
      val lAB_o = nrt("A" & "B")
      val lAB_c = lAB_o.closed
      val lA_o = nrt("A".label)
      val lA_c = lA_o.closed
      val lB_o = nrt("B".label)
      val lB_c = lB_o.closed
      val empty_o = nrt(Set.empty)
      val empty_c = empty_o.closed
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       * Coercion — dashed arrows (source type -> target type)
       *
       *          empty_o == nt
       *            /  | ┊  \
       *           /   | ↓   \
       *       lA_o  empty_c  lB_o
       *       ┊ | \         / | ┊
       *       ↓ |  \       /  | ↓
       *       lA_c   lAB_o   lB_c
       *               | ┊
       *               | ↓
       *              lAB_c
       */
      Lattice(
        "node reference value types — labels only",
        lAB_c -> lAB_o,
        coercing(lAB_o -> lAB_c),
        lAB_o -> lA_o,
        lAB_o -> lB_o,
        lA_c -> lA_o,
        coercing(lA_o -> lA_c),
        lA_o -> empty_o,
        lB_c -> lB_o,
        coercing(lB_o -> lB_c),
        lB_o -> empty_o,
        empty_c -> empty_o,
        coercing(empty_o -> empty_c),
        empty_o -> nt,
        nt -> empty_o
      )
    }

    val nodeReferenceValueTypes_propertiesOnly_openAndClosed: Lattice = {
      val nt = CTNode
      val aIbS_o = nrt(Set.empty, "a" :: CTInteger.notNull, "b" :: CTString.notNull)
      val aIbS_S = aIbS_o.default(CTString.notNull)
      val aIbS_c = aIbS_o.closed
      val aI_o = nrt(Set.empty, "a" :: CTInteger.notNull)
      val aI_S = aI_o.default(CTString.notNull)
      val aI_c = aI_o.closed
      val bS_o = nrt(Set.empty, "b" :: CTString.notNull)
      val bS_S = bS_o.default(CTString.notNull)
      val bS_c = bS_o.closed
      val empty_o = nrt(Set.empty)
      val empty_c = empty_o.closed
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       * Coercion — dashed arrows (source type -> target type)
       *
       *             empty_o == nt
       *             /    | ┊    \
       *            /     | ↓     \
       *  ╭╌╌╌╌╌ aI_o   empty_c   bS_o ╌╌╌╌╌╮
       *  ┊     /    \           /    \     ┊
       *  ┊    /      \         /      \    ┊
       *  ┊  aI_S        aIbS_o       bS_S  ┊
       *  ┊   |   \       |  ┊          |   ┊
       *  ┊   |     \     |  ┊          |   ┊
       *  ┊   |       \   |  ┊          |   ┊
       *  ╰→ aI_c     aIbS_S ┊        bS_c ←╯
       *                  |  ┊
       *                  |  ↓
       *                 aIbS_c
       */
      Lattice(
        "node reference value types — properties only, open and closed",
        aIbS_c -> aIbS_S,
        aIbS_S -> aIbS_o,
        aIbS_S -> aI_S,
        // aIbS_S -> bS_S, // not because bS_S mandates all other fields to be STRING, i.e. field a cannot be INTEGER as in aIbS_S
        aIbS_o -> aI_o,
        aIbS_o -> bS_o,
        coercing(aIbS_o -> aIbS_c),
        aI_c -> aI_S,
        aI_S -> aI_o,
        bS_c -> bS_S,
        bS_S -> bS_o,
        aI_o -> empty_o,
        coercing(aI_o -> aI_c),
        bS_o -> empty_o,
        coercing(bS_o -> bS_c),
        empty_c -> empty_o,
        coercing(empty_o -> empty_c),
        empty_o -> nt,
        nt -> empty_o
      )
    }

    val nodeReferenceValueTypes_withNullableFields: Lattice = {
      val nt = CTNode
      val aNull_o = nrt(Set.empty, "a" :: CTNull)
      val aNull_S = aNull_o.default(CTString)
      val aNull_c = aNull_o.closed
      val aI_o = nrt(Set.empty, "a" :: CTInteger)
      val aI_S = aI_o.default(CTString)
      val aI_c = aI_o.closed
      val aS_o = nrt(Set.empty, "a" :: CTString)
      val aS_S = aS_o.default(CTString)
      val aS_c = aS_o.closed
      val empty_o = nrt(Set.empty)
      val empty_c = empty_o.closed
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       * Coercion — dashed arrows (source type -> target type)
       *
       *           empty_o == nt
       *            /   | ┊   \
       *           /    | ↓    \
       *  ╭╌╌╌ aI_o   empty_c   aS_o ╌╌╌╮
       *  ┊    /   \           /   \    ┊
       *  ┊   /     \         /     \   ┊
       *  ┊  aI_S     aNull_o     aS_S  ┊
       *  ┊  |   \      | ┊      /   |  ┊
       *  ┊  |     \    | ┊    /     |  ┊
       *  ┊  |       \  | ┊  /       |  ┊
       *  ╰→ aI_c     aNull_S     aS_c ←╯
       *          \     | ┊     /
       *            \   | ↓   /
       *              aNull_c
       */
      Lattice(
        "node reference value types with nullable fields",
        aNull_c -> aI_c,
        aNull_c -> aNull_S,
        aNull_c -> aS_c,
        aNull_S -> aI_S,
        aNull_S -> aNull_o,
        aNull_S -> aS_S,
        aNull_o -> aI_o,
        aNull_o -> aS_o,
        coercing(aNull_o -> aNull_c),
        aI_c -> aI_S,
        aI_S -> aI_o,
        aS_c -> aS_S,
        aS_S -> aS_o,
        aI_o -> empty_o,
        coercing(aI_o -> aI_c),
        aS_o -> empty_o,
        coercing(aS_o -> aS_c),
        empty_c -> empty_o,
        coercing(empty_o -> empty_c),
        empty_o -> nt,
        nt -> empty_o
      )
    }

    val nodeReferenceValueTypes_mixed: Lattice = {
      val nt = CTNode
      val lA_a = nrt("A".label, "a" :: CTInteger)
      val lA = nrt("A".label)
      val empty_a = nrt(Set.empty, "a" :: CTInteger)
      val empty = nrt(Set.empty)
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       *
       *    empty = nt
       *      /    \
       *     lA   empty_a
       *      \    /
       *       lA_a
       */
      Lattice(
        "node reference value types — mixed",
        lA_a -> lA,
        lA_a -> empty_a,
        lA -> empty,
        empty_a -> empty,
        empty -> nt,
        nt -> empty
      )
    }

    val nodeReferenceValueTypesAndRecordTypes: Lattice = {
      val n_lA_a = nrt("A".label, "a" :: CTInteger)
      val n_lA = nrt("A".label)
      val n_empty_a = nrt(Set.empty, "a" :: CTInteger)
      val n_empty = nrt(Set.empty)

      val nt = CTNode

      val r_a_o = rt("a" :: CTInteger)
      val r_a_c = r_a_o.closed
      val r_o = rt()
      val r_c = r_o.closed
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       * Coercion — dashed arrows (source type -> target type)
       *
       *    n_empty == nt ╌╌╌╌╌╌╌╌╌╌╌╌→ r_o ╌╌╌╌╌╮
       *        /    \                 /   \     ┊
       *       /      \               /     \    ┊
       *    n_lA  n_empty_a ╌╌╌→ r_a_o ╌╌╮  r_c ←╯
       *       \      /               \  ┊
       *        \    /                 \ ↓
       *        n_lA_a                 r_a_c
       */
      Lattice(
        "node reference value types and record types",
        n_lA_a -> n_lA,
        n_lA_a -> n_empty_a,
        n_lA -> n_empty,
        n_empty_a -> n_empty,
        coercing(n_empty_a -> r_a_o),
        n_empty -> nt,
        nt -> n_empty,
        coercing(nt -> r_o),
        r_a_c -> r_a_o,
        r_a_o -> r_o,
        coercing(r_a_o -> r_a_c),
        r_c -> r_o,
        coercing(r_o -> r_c)
      )
    }

    val relationshipReferenceValueTypes_endpoints: Lattice = {
      val lA_a = nrt("A".label, "a" :: CTInteger).notNull
      val lA = nrt("A".label).notNull
      val lB_a = nrt("B".label, "a" :: CTInteger).notNull
      val lB = nrt("B".label).notNull
      val empty_a = nrt(Set.empty, "a" :: CTInteger).notNull
      val empty = nrt(Set.empty).notNull

      val rt = CTRelationship
      val l = Some("R")
      val r__lA_a__lB_a = rrt(l, lA_a, lB_a)
      val r__lA_a__lB = rrt(l, lA_a, lB)
      val r__lA_a__empty_a = rrt(l, lA_a, empty_a)
      val r__lA_a__empty = rrt(l, lA_a, empty)
      val r__lA__lB_a = rrt(l, lA, lB_a)
      val r__lA__lB = rrt(l, lA, lB)
      val r__lA__empty_a = rrt(l, lA, empty_a)
      val r__lA__empty = rrt(l, lA, empty)
      val r__empty_a__lB_a = rrt(l, empty_a, lB_a)
      val r__empty_a__lB = rrt(l, empty_a, lB)
      val r__empty_a__empty_a = rrt(l, empty_a, empty_a)
      val r__empty_a__empty = rrt(l, empty_a, empty)
      val r__empty__lB_a = rrt(l, empty, lB_a)
      val r__empty__lB = rrt(l, empty, lB)
      val r__empty__empty_a = rrt(l, empty, empty_a)
      val r__empty__empty = rrt(l, empty, empty)
      /*
       * Relationship reference value types form a lattice.  A REL type :R has a
       * left and a right node-reference endpoint; each endpoint is a diamond over
       * {has label?, has property?}, so the whole type is the product of two
       * diamonds -- a 4-cube.
       * Arcs point upward (subtype below -> supertype above):
       *
       * top = most general (r__empty__empty), bottom = most specific (r__lA_a__lB_a).
       *
       * Lattice node = L|R,  L = left endpoint, R = right endpoint:
       *   left : Aa=(:A {a})  A_=(:A)  _a=({a})  __=()
       *   right: Ba=(:B {a})  B_=(:B)  _a=({a})  __=()
       *   e.g. Aa|Ba = r__lA_a__lB_a,  __|B_ = r__empty__lB
       *
       *                            rt
       *                            │
       *                          __|__
       *             ╭─────────┬────┴────┬─────────╮
       *           A_|__     _a|__     __|B_     __|_a
       *    ╭──────┴─│─│─────╯ │ ╰─────│─┤ ╰─────│─│─┴───────╮
       *    │        │ │       ╰───────│─│───────│─┤         │
       *    │        ├─│───────────────╯ │       │ │         │
       *    │        │ ╰───────┬─────────│───────╯ │         │
       *  Aa|__    A_|B_     A_|_a     _a|B_     _a|_a     __|Ba
       *    │ │      │ ╰───────│─┴─────│─┬─│─────│─│───────╯ │
       *    │ ╰──────│─────────┼───────│─│─│─────╯ │         │
       *    ╰────────┼─────────│───────╯ │ ╰───────┼─────────╯
       *           Aa|B_     Aa|_a     A_|Ba     _a|Ba
       *             ╰─────────┴────┬────┴─────────╯
       *                          Aa|Ba
       */
      Lattice(
        "relationship reference value types — endpoints",
        r__lA_a__lB_a -> r__empty_a__lB_a,
        r__lA_a__lB_a -> r__lA__lB_a,
        r__lA_a__lB_a -> r__lA_a__lB,
        r__lA_a__lB_a -> r__lA_a__empty_a,
        r__lA__lB_a -> r__lA__lB,
        r__lA__lB_a -> r__lA__empty_a,
        r__lA__lB_a -> r__empty__lB_a,
        r__lA_a__lB -> r__lA__lB,
        r__lA_a__lB -> r__lA_a__empty,
        r__lA_a__lB -> r__empty_a__lB,
        r__lA__lB -> r__lA__empty,
        r__lA__lB -> r__empty__lB,
        r__lA_a__empty_a -> r__empty_a__empty_a,
        r__lA_a__empty_a -> r__lA__empty_a,
        r__lA_a__empty_a -> r__lA_a__empty,
        r__empty_a__lB_a -> r__empty__lB_a,
        r__empty_a__lB_a -> r__empty_a__empty_a,
        r__empty_a__lB_a -> r__empty_a__lB,
        r__lA_a__empty -> r__empty_a__empty,
        r__lA_a__empty -> r__lA__empty,
        r__empty_a__lB -> r__empty_a__empty,
        r__empty_a__lB -> r__empty__lB,
        r__empty__lB_a -> r__empty__empty_a,
        r__empty__lB_a -> r__empty__lB,
        r__lA__empty_a -> r__empty__empty_a,
        r__lA__empty_a -> r__lA__empty,
        r__lA__empty -> r__empty__empty,
        r__empty__lB -> r__empty__empty,
        r__empty_a__empty_a -> r__empty_a__empty,
        r__empty_a__empty_a -> r__empty__empty_a,
        r__empty_a__empty -> r__empty__empty,
        r__empty__empty_a -> r__empty__empty,
        r__empty__empty -> rt
      )
    }

    val relationshipReferenceValueTypesAndRecordTypes: Lattice = {
      val n = nrt(Set.empty).notNull
      val n_lA = nrt("A".label).notNull
      val n_lB = nrt("B".label).notNull

      val rel_AB_lA_a = rrt(Some("A"), n_lA, n_lB, "a" :: CTInteger)
      val rel_AB_lA = rrt(Some("A"), n_lA, n_lB)
      val rel_AB_empty_a = rrt(Option.empty, n_lA, n_lB, "a" :: CTInteger)
      val rel_AB_empty = rrt(Option.empty, n_lA, n_lB)
      val rel_empty_a = rrt(Option.empty, n, n, "a" :: CTInteger)
      val rel_empty = rrt(Option.empty, n, n)

      val relt = CTRelationship

      val r_a_o = rt("a" :: CTInteger)
      val r_a_c = r_a_o.closed
      val r_o = rt()
      val r_c = r_o.closed
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above)
       * Coercion — dashed arrows (source type -> target type)
       *
       *       rel_empty = relt ╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌→ r_o ╌╌╌╌╌╮
       *          /        \                    /   \     ┊
       *         /          \                  /     \    ┊
       *  rel_AB_empty   rel_empty_a ╌╌╌→ r_a_o ╌╌╮  r_c ←╯
       *        |     \      |                 \  ┊
       *        |      \     |                  \ ↓
       *        |       \    |                  r_a_c
       *    rel_AB_lA   rel_AB_empty_a
       *          \        /
       *           \      /
       *          rel_AB_lA_a
       */
      Lattice(
        "relationship reference value types and record types",
        rel_AB_lA_a -> rel_AB_lA,
        rel_AB_lA_a -> rel_AB_empty_a,
        rel_AB_lA -> rel_AB_empty,
        rel_AB_empty_a -> rel_AB_empty,
        rel_AB_empty_a -> rel_empty_a,
        rel_AB_empty -> relt,
        rel_AB_empty -> rel_empty,
        rel_empty_a -> rel_empty,
        coercing(rel_empty_a -> r_a_o),
        rel_empty -> relt,
        relt -> rel_empty,
        coercing(relt -> r_o),
        r_a_c -> r_a_o,
        r_a_o -> r_o,
        coercing(r_a_o -> r_a_c),
        r_c -> r_o,
        coercing(r_o -> r_c)
      )
    }

    val dynamicUnionTypes: Lattice = {
      val uIS = CTInteger | CTString
      val uNS = CTNumber | CTString
      val uID = CTInteger | CTDate
      val uSD = CTString | CTDate
      val uISD = CTInteger | CTString | CTDate
      val uNSD = CTNumber | CTString | CTDate
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above).
       * X = two edges crossing (not a node).
       *
       *                  uNSD
       *                /      \
       *              uNS      uISD
       *            /  |     /   | \
       *           /   |    /    |   \
       *          /    |   /     |     \
       *         /     |  /      |       \
       *        /     uIS       uID      uSD
       *       /      |   \   /     \   /  |
       *   CTNUmber   |     X         X    |
       *        \     |   /   \     /   \  |
       *         \    |  /     \   /     \ |
       *         CTInteger   CTString   CTDate
       */
      Lattice(
        "dynamic union types",
        CTInteger -> CTNumber,
        CTInteger -> uIS,
        CTInteger -> uID,
        CTString -> uIS,
        CTString -> uSD,
        CTDate -> uID,
        CTDate -> uSD,
        CTNumber -> uNS,
        uIS -> uNS,
        uIS -> uISD,
        uID -> uISD,
        uSD -> uISD,
        uNS -> uNSD,
        uISD -> uNSD
      )
    }

    val coercibleDynamicUnionTypes: Lattice = {
      val rec = rt()
      val nod = nrt(Set.empty)
      val rel = rrt(Option.empty, nod.notNull, nod.notNull)
      val uRecNod = rec | nod
      val uRecRel = rec | rel
      val uRecInt = rec | CTInteger
      val uNodRel = nod | rel
      val uNodInt = nod | CTInteger
      val uRelInt = rel | CTInteger
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above).
       * Coercion — dashed arrows (source type -> target type).
       * X = two edges crossing (not a node).
       *
       *                                    uNodInt             uRelInt
       *                                     │ │ ┊               ┊ │ │
       *      uRecNod   uNodRel   uRecRel    │ │ ╰╌╌→ uRecInt ←╌╌╯ │ │
       *       ┊ │ │     ┊ │ │     │ │ ┊     │ ╰────────│─┴────┬───╯ │
       *       ┊ │ │     ┊ │ ╰─────│─┴─┊─────│─────┬────│──────│─────╯
       *       ┊ │ ╰─────┊─│─────┬─┴───┊─────│─────│────╯      │
       *       ┊ ╰───┬───┊─┴─────│─────┊─────╯     │           │
       *       ┊    nod  ┊       │     ┊          rel         Int
       *       ┊     ┊   ┊       │     ┊           ┊
       *       ╰╌╌╌╌╌┴╌╌╌┴╌╌╌╌→ rec ←╌╌┴╌╌╌╌╌╌╌╌╌╌╌╯
       */
      Lattice(
        "coercible dynamic union types",
        rec -> uRecNod,
        rec -> uRecRel,
        rec -> uRecInt,
        nod -> uRecNod,
        nod -> uNodRel,
        nod -> uNodInt,
        coercing(nod -> rec),
        rel -> uRecRel,
        rel -> uNodRel,
        rel -> uRelInt,
        coercing(rel -> rec),
        CTInteger -> uRecInt,
        CTInteger -> uNodInt,
        CTInteger -> uRelInt,
        coercing(uRecNod -> rec),
        coercing(uRecRel -> rec),
        coercing(uNodRel -> rec),
        coercing(uNodInt -> uRecInt),
        coercing(uRelInt -> uRecInt)
      )
    }

    val dynamicUnionTypesRandomized: Lattice = {
      val dynamicUnionsRandomizedSeed = Random.nextInt(1024)

      given Random = new Random(dynamicUnionsRandomizedSeed)

      val a = pickSubset(baseTypeRepresentatives)
      val b = pickSubset(baseTypeRepresentatives)
      val c = pickSubset(baseTypeRepresentatives)
      val aXbXc = a intersect b intersect c
      val aXb = a intersect b
      val aXc = a intersect c
      val bXc = b intersect c
      val aUb = a union b
      val aUc = a union c
      val bUc = b union c
      val aUbUc = a union b union c
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above).
       * Every label is wrapped in u(...);  X = two edges crossing (not a node).
       *
       *        aUbUc
       *         /|\
       *       /  |  \
       *     /    |    \
       *   aUb   aUc   bUc
       *    |\   / \   /|
       *    |  X     X  |
       *    |/   \ /   \|
       *    a     b     c
       *    |\   / \   /|
       *    |  X     X  |
       *    |/   \ /   \|
       *   aXb   aXc   bXc
       *     \    |    /
       *       \  |  /
       *         \|/
       *        aXbXc
       */
      Lattice(
        s"dynamic union types randomized (seed: $dynamicUnionsRandomizedSeed)",
        u(aXbXc) -> u(aXb),
        u(aXbXc) -> u(aXc),
        u(aXbXc) -> u(bXc),
        u(aXb) -> u(a),
        u(aXb) -> u(b),
        u(aXc) -> u(a),
        u(aXc) -> u(c),
        u(bXc) -> u(b),
        u(bXc) -> u(c),
        u(a) -> u(aUb),
        u(b) -> u(aUb),
        u(a) -> u(aUc),
        u(c) -> u(aUc),
        u(b) -> u(bUc),
        u(c) -> u(bUc),
        u(aUb) -> u(aUbUc),
        u(aUc) -> u(aUbUc),
        u(bUc) -> u(aUbUc)
      )
    }

    val listTypesAndDynamicUnionTypesRandomized: Lattice = {
      val listTypesAndDynamicUnionsRandomizedSeed = Random.nextInt(1024)

      given Random = new Random(listTypesAndDynamicUnionsRandomizedSeed)

      val elementTypes = pickNFrom(3, baseTypeRepresentatives).toSet
      val Seq(a, b, c) = elementTypes.toSeq

      val l_null = l(CTNull)
      val l_a = l(a)
      val l_b = l(b)
      val l_c = l(c)
      val l_ab = l(a | b)
      val l_ac = l(a | c)
      val l_bc = l(b | c)
      val l_abc = l(a | b | c)

      val u_lalb = l_a | l_b
      val u_lalc = l_a | l_c
      val u_lblc = l_b | l_c
      val u_lalblcl = l_a | l_b | l_c
      /*
       * Subtype lattice — arcs point upward (subtype below -> supertype above).
       * X = two edges crossing (not a node).
       *
       *                l_abc
       *              / /  | \
       *           /   /   |    \
       *        /     /    |       \
       *    l_ab  l_ac  u_lalblcl   l_bc
       *       |    \   /  |    \     |
       *       |      X    |     \    |
       *       |    /   \  |      \   |
       *      u_lalb    u_lalc    u_lblc
       *       |   \   /      \   /   |
       *       |     X          X     |
       *       |   /   \      /   \   |
       *       l_a       l_b        l_c
       *           \       |      /
       *             \     |    /
       *                l_null
       */
      Lattice(
        s"list types and dynamic union types randomized (seed: $listTypesAndDynamicUnionsRandomizedSeed)",
        l_null -> l_a,
        l_null -> l_b,
        l_null -> l_c,
        l_a -> u_lalb,
        l_a -> u_lalc,
        l_b -> u_lalb,
        l_b -> u_lblc,
        l_c -> u_lalc,
        l_c -> u_lblc,
        l_ab -> l_abc,
        l_ac -> l_abc,
        l_bc -> l_abc,
        u_lalb -> l_ab,
        u_lalc -> l_ac,
        u_lblc -> l_bc,
        u_lalb -> u_lalblcl,
        u_lalc -> u_lalblcl,
        u_lblc -> u_lalblcl,
        u_lalblcl -> l_abc
      )
    }

    val allLattices: Seq[Lattice] = Seq(
      basics,
      basics2,
      numericTypes,
      vectorTypes,
      recordTypes,
      recordTypes_withNullableFields,
      recordTypesRandomized,
      nodeReferenceValueTypes_labelsOnly,
      nodeReferenceValueTypes_propertiesOnly_openAndClosed,
      nodeReferenceValueTypes_withNullableFields,
      nodeReferenceValueTypes_mixed,
      nodeReferenceValueTypesAndRecordTypes,
      relationshipReferenceValueTypes_endpoints,
      relationshipReferenceValueTypesAndRecordTypes,
      dynamicUnionTypes,
      coercibleDynamicUnionTypes,
      dynamicUnionTypesRandomized,
      listTypesAndDynamicUnionTypesRandomized
    )
  }

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

  protected def noNodeReferenceValueType(t: CypherType): Boolean = normalizeTypes(t) match {
    case _: NodeReferenceValueType => false
    case _: NodeType               => false
    case u: ClosedDynamicUnionType => u.innerTypes.forall(noNodeReferenceValueType)
    case _                         => true
  }

  protected def noRelationshipReferenceValueType(t: CypherType): Boolean = normalizeTypes(t) match {
    case _: RelationshipReferenceValueType => false
    case _: RelationshipType               => false
    case u: ClosedDynamicUnionType         => u.innerTypes.forall(noRelationshipReferenceValueType)
    case _                                 => true
  }

  protected def notAny(t: CypherType): Boolean = normalizeTypes(t) match {
    case _: AnyType                => false
    case u: ClosedDynamicUnionType => u.innerTypes.forall(notAny)
    case _                         => true
  }

  protected inline def isAny(t: CypherType): Boolean = !notAny(t)

  /*
   * type construction helpers
   */

  protected inline def rt(fields: (String, CypherType)*): RecordType =
    RecordType(fields.toMap, isOpen = true, isNullable = true)(pos)
  protected inline def rt(fields: Set[(String, CypherType)]): RecordType = rt(fields.toSeq: _*)

  protected inline def nrt(labels: Set[String], fields: (String, CypherType)*): NodeReferenceValueType =
    NodeReferenceValueType(labels, fields.toMap, isOpen = true, isNullable = true)(pos)

  protected inline def rrt(
    label: Option[String],
    source: NodeReferenceValueType,
    destination: NodeReferenceValueType,
    fields: (String, CypherType)*
  ): RelationshipReferenceValueType =
    RelationshipReferenceValueType(label, fields.toMap, isOpen = true, source, destination, isNullable = true)(pos)

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
    inline def closed: RecordType = rt.copy(defaultFieldType = NothingType()(pos))(pos)
    inline def default(cypherType: CypherType): RecordType = rt.copy(defaultFieldType = cypherType)(pos)
  }

  extension (nrt: NodeReferenceValueType) {
    inline def notNull: NodeReferenceValueType = nrt.withIsNullable(isNullable = false)
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

  /*
   * lattice gymnastics
   */

  protected def taggedTransitiveClosure[A](edges: Seq[((A, A), Boolean)]): Set[((A, A), Boolean)] = {
    @annotation.tailrec
    def loop(closure: Set[((A, A), Boolean)]): Set[((A, A), Boolean)] = {
      val next =
        closure ++
          (for {
            ((a, b), x) <- closure
            ((c, d), y) <- closure
            if b == c
          } yield ((a, d), x || y))
      if next == closure then {
        closure
      } else {
        loop(next)
      }
    }
    loop(edges.toSet)
  }

  protected def computeTaggedMeets[T](latticeEdges: Seq[((T, T), Boolean)]): Seq[(T, T, Option[(T, Boolean)])] = {
    // 1. Extract all unique nodes in the graph
    val nodes = latticeEdges.flatMap { case ((a, b), _) => Seq(a, b) }.distinct

    // 2. Compute reflexive transitive closure
    val tc = taggedTransitiveClosure(latticeEdges) union nodes.map(t => ((t, t), false)).toSet

    // 3. Compute the meet for every possible pair
    for {
      from <- nodes
      to <- nodes
      if from != to
    } yield {
      // Find all Z such that Z <= FROM and Z <~ TO
      val commonLowerBounds = nodes.collect {
        case z if tc.contains(((z, from), false)) && tc.contains(((z, to), false)) => (z, false)
        case z if tc.contains(((z, from), false)) && tc.contains(((z, to), true))  => (z, true)
      }
      // The meet is a lower bound M where every other lower bound Z satisfies Z <= M
      def findMeet(commonLowerBounds: Seq[(T, Boolean)]): Option[(T, Boolean)] = {
        commonLowerBounds.find(m =>
          commonLowerBounds.forall(z => tc.contains(((z._1, m._1), false)))
        )
      }
      // for the meet, we prefer one with the lower tag
      val meet = findMeet(commonLowerBounds.filterNot(_._2)).orElse(findMeet(commonLowerBounds))

      (from, to, meet)
    }
  }

  protected def transitiveClosure[A](edges: Seq[(A, A)]): Set[(A, A)] = {
    @annotation.tailrec
    def loop(closure: Set[(A, A)]): Set[(A, A)] = {
      val next =
        closure ++
          (for {
            (a, b) <- closure
            (c, d) <- closure
            if b == c
          } yield (a, d))
      if next == closure then {
        closure
      } else {
        loop(next)
      }
    }
    loop(edges.toSet)
  }

  protected def computeMeets[T](latticeEdges: Seq[(T, T)]): Seq[(T, T, Option[T])] = {
    // 1. Extract all unique nodes in the graph
    val nodes = latticeEdges.flatMap((a, b) => Seq(a, b)).distinct

    // 2. Compute reflexive transitive closure
    val tc = transitiveClosure(latticeEdges) union nodes.map(t => (t, t)).toSet

    // 3. Compute the meet for every possible pair
    for {
      x <- nodes
      y <- nodes
      if x != y
    } yield {
      // Find all Z such that Z <= X and Z <= Y
      val commonLowerBounds = nodes.filter(z => tc.contains((z, x)) && tc.contains((z, y)))

      // The meet is a lower bound M where every other lower bound Z satisfies Z <= M
      val meet = commonLowerBounds.find(m =>
        commonLowerBounds.forall(z => tc.contains((z, m)))
      )

      (x, y, meet)
    }
  }

  @annotation.tailrec
  private def traverse[T](stack: List[T], visited: Set[T])(using steps: Map[T, Seq[T]]): Set[T] = {
    stack match
      case Nil => visited
      case current :: rest =>
        if (visited.contains(current)) {
          traverse(rest, visited)
        } else {
          val reached = steps.getOrElse(current, Seq.empty)
          traverse(reached.toList ++ rest, visited + current)
        }
  }

  protected def computeFilter[T](infimum: T, latticeEdges: Seq[(T, T)]): Set[T] = {
    // Map each target node to its direct successors: (u, v) implies u => v
    given successors: Map[T, Seq[T]] = latticeEdges.groupMap(_._1)(_._2)
    traverse(List(infimum), Set.empty)
  }

  protected def computeIdeal[T](supremum: T, latticeEdges: Seq[(T, T)]): Set[T] = {
    // Map each target node to its direct predecessors: (u, v) implies u <= v
    given predecessors: Map[T, Seq[T]] = latticeEdges.groupMap(_._2)(_._1)
    traverse(List(supremum), Set.empty)
  }
}
