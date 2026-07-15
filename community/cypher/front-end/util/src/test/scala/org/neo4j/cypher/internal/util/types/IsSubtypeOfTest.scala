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

import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTAnyNotNull
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTFloat32
import org.neo4j.cypher.internal.util.symbols.CTGeometry
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
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CTUUID
import org.neo4j.cypher.internal.util.symbols.CTVector
import org.neo4j.cypher.internal.util.symbols.CTZonedDateTime
import org.neo4j.cypher.internal.util.symbols.CTZonedTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.IsSubtypeOf
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

class IsSubtypeOfTest extends CypherTypeTestSuite {

  test("basics") {
    /*
     * Subtype lattice — arcs point upward (subtype -> supertype).
     *
     *   CTAny
     *     |
     *   CTNull
     *     |
     *   CTNothing
     */
    shouldBeSubtypeLattice(
      CTNothing -> CTNull,
      CTNull -> CTAny
    )
  }

  test("numeric types") {
    /*
     * Subtype lattice — arcs point upward (subtype -> supertype).
     *
     *           CTNumber
     *          /        \
     *   CTInteger     CTFloat
     *         |          |
     *   CTInteger32   CTFloat32
     *         |
     *   CTInteger16
     *         |
     *   CTInteger8
     */
    shouldBeSubtypeLattice(
      CTInteger8 -> CTInteger16,
      CTInteger16 -> CTInteger32,
      CTInteger32 -> CTInteger,
      CTFloat32 -> CTFloat,
      CTInteger -> CTNumber,
      CTFloat -> CTNumber
    )
  }

  test(s"vector types") {
    val v = VectorType(None, None, isNullable = true)(pos)
    val vF32 = VectorType(Some(CTFloat32), None, isNullable = true)(pos)
    val vF = VectorType(Some(CTFloat), None, isNullable = true)(pos)
    val vF32D3 = VectorType(Some(CTFloat32), Some(3), isNullable = true)(pos)
    val vFD3 = VectorType(Some(CTFloat), Some(3), isNullable = true)(pos)
    val vF32D100 = VectorType(Some(CTFloat32), Some(100), isNullable = true)(pos)
    val vFD100 = VectorType(Some(CTFloat), Some(100), isNullable = true)(pos)
    shouldBeSubtypeLattice(
      vF32D3 -> vF32,
      vF32D100 -> vF32,
      vFD3 -> vF,
      vFD100 -> vF,
      vF32 -> v,
      vF -> v
    )
  }

  val baseTypeRepresentatives = Set(
    CTBoolean,
    CTInteger,
    CTFloat,
    CTVector,
    CTString,
    CTList(nullableAnyType),
    rt("z" :: CTBoolean),
    CTPath,
    CTPoint,
    CTGeometry,
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

  test("not subtypes of each other") {
    shouldNotBeSubtypesOfEachOther(baseTypeRepresentatives)
  }

  test("record types") {
    val mt = CTMap
    val aIbS_fo = rt("a" :: CTInteger, "b" :: CTString)
    val aIbS_S = aIbS_fo.default(CTString)
    val aIbS_fc = aIbS_fo.fieldClosed
    val aI_fo = rt("a" :: CTInteger)
    val aI_S = aI_fo.default(CTString)
    val aI_fc = aI_fo.fieldClosed
    val bS_fo = rt("b" :: CTString)
    val bS_S = bS_fo.default(CTString)
    val bS_fc = bS_fo.fieldClosed
    val empty_fo = rt()
    val empty_fc = empty_fo.fieldClosed
    /*
     * Subtype lattice — arcs point upward (subtype -> supertype).
     *
     *           empty_fo == mt
     *            /    |     \
     *           /     |      \
     *       aI_fo  empty_fc  bS_fo
     *       /    \          /   \
     *      /      \       /      \
     *    aI_S      aIbS_fo       bS_S
     *     |       /   |            |
     *     |     /     |            |
     *     |   /       |            |
     *    aI_fc     aIbS_S       bS_fc
     *                 |
     *              aIbS_fc
     */
    shouldBeSubtypeLattice(
      aIbS_fc -> aIbS_S,
      aIbS_S -> aIbS_fo,
      aIbS_S -> aI_S,
      // aIbS_S -> bS_S, // not because bS_S mandates all other fields to be STRING, i.e. field a cannot be INTEGER as in aIbS_S
      aIbS_fo -> aI_fo,
      aIbS_fo -> bS_fo,
      aI_fc -> aI_S,
      aI_S -> aI_fo,
      bS_fc -> bS_S,
      bS_S -> bS_fo,
      aI_fo -> empty_fo,
      bS_fo -> empty_fo,
      empty_fc -> empty_fo,
      empty_fo -> mt,
      mt -> empty_fo
    )
  }

  val recordTypesRandomizedSeed = Random.nextInt(1024)

  test(s"record types randomized (seed: $recordTypesRandomizedSeed)") {
    given Random = new Random(dynamicUnionsRandomizedSeed)

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
     * Subtype lattice — arcs point upward (subtype -> supertype).
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
    shouldBeSubtypeLattice(
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

  test("node reference value types — labels only") {
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
     * Subtype lattice — arcs point upward (subtype -> supertype).
     *
     *          empty_o == nt
     *            /   |   \
     *           /    |    \
     *       lA_o  empty_c  lB_o
     *        |  \         /  |
     *        |   \       /   |
     *       lA_c   lAB_o   lB_c
     *                |
     *                |
     *              lAB_c
     */
    shouldBeSubtypeLattice(
      lAB_c -> lAB_o,
      lAB_o -> lA_o,
      lAB_o -> lB_o,
      lA_c -> lA_o,
      lA_o -> empty_o,
      lB_c -> lB_o,
      lB_o -> empty_o,
      empty_c -> empty_o,
      empty_o -> nt,
      nt -> empty_o
    )
  }

  test("node reference value types — properties only, open and closed") {
    val nt = CTNode
    val aIbS_o = nrt(Set.empty, "a" :: CTInteger, "b" :: CTString)
    val aIbS_S = aIbS_o.default(CTString)
    val aIbS_c = aIbS_o.closed
    val aI_o = nrt(Set.empty, "a" :: CTInteger)
    val aI_S = aI_o.default(CTString)
    val aI_c = aI_o.closed
    val bS_o = nrt(Set.empty, "b" :: CTString)
    val bS_S = bS_o.default(CTString)
    val bS_c = bS_o.closed
    val empty_o = nrt(Set.empty)
    val empty_c = empty_o.closed
    /*
     * Subtype lattice — arcs point upward (subtype -> supertype).
     *
     *          empty_o == nt
     *           /    |    \
     *       aI_o  empty_c  bS_o
     *       /   \         /   \
     *      /     \       /     \
     *    aI_S     aIbS_o      bS_S
     *     |      /   |          |
     *     |    /     |          |
     *     |  /       |          |
     *    aI_c     aIbS_S      bS_c
     *                |
     *             aIbS_c
     */
    shouldBeSubtypeLattice(
      aIbS_c -> aIbS_S,
      aIbS_S -> aIbS_o,
      aIbS_S -> aI_S,
      // aIbS_S -> bS_S, // not because bS_S mandates all other fields to be STRING, i.e. field a cannot be INTEGER as in aIbS_S
      aIbS_o -> aI_o,
      aIbS_o -> bS_o,
      aI_c -> aI_S,
      aI_S -> aI_o,
      bS_c -> bS_S,
      bS_S -> bS_o,
      aI_o -> empty_o,
      bS_o -> empty_o,
      empty_c -> empty_o,
      empty_o -> nt,
      nt -> empty_o
    )
  }

  test("node reference value types — mixed") {
    val nt = CTNode
    val lA_a = nrt("A".label, "a" :: CTInteger)
    val lA = nrt("A".label)
    val empty_a = nrt(Set.empty, "a" :: CTInteger)
    val empty = nrt(Set.empty)
    /*
     * Subtype lattice — arcs point upward (subtype below -> supertype above).
     *
     *    empty = nt
     *      /    \
     *     lA   empty_a
     *      \    /
     *       lA_a
     */
    shouldBeSubtypeLattice(
      lA_a -> lA,
      lA_a -> empty_a,
      lA -> empty,
      empty_a -> empty,
      empty -> nt,
      nt -> empty
    )
  }

  test("node reference value types and record types") {
    val n_lA_a = nrt("A".label, "a" :: CTInteger)
    val n_lA = nrt("A".label)
    val n_empty_a = nrt(Set.empty, "a" :: CTInteger)
    val n_empty = nrt(Set.empty)

    val nt = CTNode

    val r_a_bto = rt("a" :: CTInteger)
    val r_a_btc = r_a_bto.baseTypeClosed
    val r_bto = rt()
    val r_btc = r_bto.baseTypeClosed
    /*
     * Subtype lattice — arcs point upward (subtype below -> supertype above).
     *
     *                  r_bto
     *                /  |    \
     *               /   |     \
     *              /    |      \
     * n_empty == nt   r_a_bto   r_btc
     *      |   \      /   \      /
     *      |    \    /     \    /
     *      |     \  /       \  /
     *    n_lA  n_empty_a   r_a_btc
     *       \    /
     *        \  /
     *       n_lA_a
     */
    shouldBeSubtypeLattice(
      n_lA_a -> n_lA,
      n_lA_a -> n_empty_a,
      n_lA -> n_empty,
      n_empty_a -> n_empty,
      n_empty_a -> r_a_bto,
      n_empty -> nt,
      n_empty -> r_bto,
      nt -> n_empty,
      nt -> r_bto,
      r_a_btc -> r_a_bto,
      r_a_btc -> r_btc,
      r_a_bto -> r_bto,
      r_btc -> r_bto
    )
  }

  test("relationship reference value types — endpoints") {
    val lA_a = nrt("A".label, "a" :: CTInteger)
    val lA = nrt("A".label)
    val lB_a = nrt("B".label, "a" :: CTInteger)
    val lB = nrt("B".label)
    val empty_a = nrt(Set.empty, "a" :: CTInteger)
    val empty = nrt(Set.empty)

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
    shouldBeSubtypeLattice(
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

  test("relationship reference value types and record types") {
    val n_lA = nrt("A".label)
    val n_lB = nrt("B".label)

    val rel_lA_a = rrt(Some("A"), n_lA, n_lB, "a" :: CTInteger)
    val rel_lA = rrt(Some("A"), n_lA, n_lB)
    val rel_empty_a = rrt(Option.empty, n_lA, n_lB, "a" :: CTInteger)
    val rel_empty = rrt(Option.empty, n_lA, n_lB)

    val relt = CTRelationship

    val r_a_bto = rt("a" :: CTInteger)
    val r_a_btc = r_a_bto.baseTypeClosed
    val r_bto = rt()
    val r_btc = r_bto.baseTypeClosed
    /*
     * Subtype lattice — arcs point upward (subtype below -> supertype above).
     *
     *                r_bto
     *              /   |   \
     *          relt    |     \
     *          /       |       \
     *   rel_empty    r_a_bto    r_btc
     *     |    \      /   \      /
     *     |     \    /     \    /
     *     |      \  /       \  /
     *  rel_lA  rel_empty_a  r_a_btc
     *       \    /
     *        \  /
     *      rel_lA_a
     */
    shouldBeSubtypeLattice(
      rel_lA_a -> rel_lA,
      rel_lA_a -> rel_empty_a,
      rel_lA -> rel_empty,
      rel_empty_a -> rel_empty,
      rel_empty_a -> r_a_bto,
      rel_empty -> relt,
      relt -> r_bto,
      r_a_btc -> r_a_bto,
      r_a_btc -> r_btc,
      r_a_bto -> r_bto,
      r_btc -> r_bto
    )
  }

  test("dynamic unions") {
    val uIS = CTInteger | CTString
    val uID = CTInteger | CTDate
    val uSD = CTString | CTDate
    val uISD = CTInteger | CTString | CTDate
    /*
     * Subtype lattice — arcs point upward (subtype -> supertype).
     * X = two edges crossing (not a node).
     *
     *               uISD
     *             /   |  \
     *           /     |    \
     *         /       |      \
     *      uIS       uID      uSD
     *      |   \   /     \   /  |
     *      |     X         X    |
     *      |   /   \     /   \  |
     *  CTInteger   CTString   CTDate
     */
    shouldBeSubtypeLattice(
      CTInteger -> uIS,
      CTInteger -> uID,
      CTString -> uIS,
      CTString -> uSD,
      CTDate -> uID,
      CTDate -> uSD,
      uIS -> uISD,
      uID -> uISD,
      uSD -> uISD
    )
  }

  val dynamicUnionsRandomizedSeed = Random.nextInt(1024)

  test(s"dynamic unions randomized (seed: $dynamicUnionsRandomizedSeed)") {
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
     * Subtype lattice — arcs point upward (subtype -> supertype).
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
    shouldBeSubtypeLattice(
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

  val listTypesAndDynamicUnionsRandomizedSeed = Random.nextInt(1024)

  test(s"list types and dynamic unions randomized (seed: $listTypesAndDynamicUnionsRandomizedSeed)") {
    given Random = new Random(listTypesAndDynamicUnionsRandomizedSeed)

    val elementTypes = pickNFrom(3, baseTypeRepresentatives).toSet
    val Seq(a, b, c) = elementTypes.toSeq

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
     * Subtype lattice — arcs point upward (subtype -> supertype).
     * X = two edges crossing (not a node).
     *
     *                l_abc
     *              / /  | \
     *           /   /   |    \
     *        /     /    |       \
     *    l_ab  l_ac  u_lalblcl  l_bc
     *       |    \   /  |    \     |
     *       |      X    |     \    |
     *       |    /   \  |      \   |
     *      u_lalb    u_lalc    u_lblc
     *       |   \   /      \   /   |
     *       |     X          X     |
     *       |   /   \      /   \   |
     *       l_a       l_b        l_c
     */
    shouldBeSubtypeLattice(
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

  /*
   * checks and assertions
   */

  private def shouldNotBeSubtypesOfEachOther(ts: Set[CypherType]): Unit = {
    for (sub <- ts) {
      for (sup <- ts) {
        if (sub != sup) {
          assertIsNotSubtypeOf(sub, sup)
        }
      }
      checkInvariants(sub)
    }
  }

  private def shouldBeSubtypeLattice(latticeEdges: (CypherType, CypherType)*): Unit = {
    def transitiveClosure[A](edges: (A, A)*): Set[(A, A)] = {
      @annotation.tailrec
      def loop(closure: Set[(A, A)]): Set[(A, A)] = {
        val next =
          closure ++
            (for
              (a, b) <- closure
              (c, d) <- closure
              if b == c
            yield (a, d))
        if next == closure then {
          closure
        } else {
          loop(next)
        }
      }
      loop(edges.toSet)
    }
    val allSubtypeRelationships = transitiveClosure(latticeEdges*)
    for ((sub, sup) <- allSubtypeRelationships) {
      checkIsSubtypeOf(sub, sup)
    }
    val allTypes = latticeEdges.flatMap {
      case (sub, sup) => Seq(sub, sup)
    }
    for (t <- allTypes) {
      checkInvariants(t)
    }
    // for the negative tests we sample 10 from each side to keep the test runtime under control
    for {
      a <- Random.shuffle(allTypes).take(10)
      b <- Random.shuffle(allTypes).take(10) if a != b && !allSubtypeRelationships.contains((a, b))
    } checkIsNotSubtypeOf(a, b)
  }

  private def checkInvariants(t: CypherType, nestingLevel: Int = 0): Unit = {
    checkAgainstAny(t)
    checkAgainstNothingAndNull(t)
    checkAgainstItselfAndNullability(t)
    // check for lists
    if (nestingLevel < 3) checkInvariants(l(t), nestingLevel + 1)
    // check for records
    if (nestingLevel < 3) checkInvariants(rt("x" :: t), nestingLevel + 1)
  }

  private def checkAgainstAny(t: CypherType): Unit = {
    assertIsSubtypeOf(t, CTAny)
    if (!t.isNullable) assertIsSubtypeOf(t, CTAnyNotNull) else assertIsNotSubtypeOf(t, CTAnyNotNull)
    if (t != CTAny) assertIsNotSubtypeOf(CTAny, t)
  }

  private def checkAgainstNothingAndNull(t: CypherType): Unit = {
    assertIsSubtypeOf(CTNothing, t)
    if (t.isNullable) assertIsSubtypeOf(CTNull, t) else assertIsNotSubtypeOf(CTNull, t)
    if (t != CTNothing) assertIsNotSubtypeOf(t, CTNothing)
  }

  private def checkAgainstItselfAndNullability(t: CypherType): Unit = {
    assertIsSubtypeOf(t, t)
    assertIsSubtypeOf(t.notNull, t.nullable)
    assertIsNotSubtypeOf(t.nullable, t.notNull)
  }

  private def checkIsSubtypeOf(sub: CypherType, sup: CypherType, nestingLevel: Int = 0): Unit = {
    assertIsSubtypeOf(sub, sup)
    checkNullabilityInvariantForSubtypes(sub, sup)
    // given SUB <: SUP
    // check for list covariance
    {
      if (nestingLevel < 3) {
        // LIST<SUB> <: LIST<SUP>
        checkIsSubtypeOf(l(sub), l(sup), nestingLevel + 1)
        // LIST<SUB - NULL> <: LIST<SUP + NULL>
        checkIsSubtypeOf(l(sub.notNull), l(sup.nullable), nestingLevel + 1)
        // but not LIST<SUB + NULL> <: LIST<SUP - NULL>
        checkIsNotSubtypeOf(l(sub.nullable), l(sup.notNull), nestingLevel + 1)
      }
      // if SUB is not a LIST or the nothing type, then not SUB <: LIST<SUP>
      if (noListType(sub) && sub != CTNothing && sub != CTNull) checkIsNotSubtypeOf(sub, l(sup), nestingLevel + 1)
      // if SUP is not a RECORD or the nothing type, then not LIST<SUB> <: SUP
      if (noListType(sup) && sup != CTAny) checkIsNotSubtypeOf(l(sub), sup, nestingLevel + 1)
    }
    // check for record covariance
    {
      if (nestingLevel < 3) {
        // { x :: SUB } <: { x :: SUP }
        checkIsSubtypeOf(rt("x" :: sub), rt("x" :: sup), nestingLevel + 1)
        // { x :: SUB, y :: BOOLEAN, z :: SUB } <: { x :: SUP, y :: BOOLEAN, z :: SUP }
        checkIsSubtypeOf(
          rt("x" :: sub, "y" :: CTBoolean, "z" :: sub),
          rt("x" :: sup, "y" :: CTBoolean, "z" :: sup),
          nestingLevel + 1
        )
        // { x :: SUB - NULL } <: { x :: SUP + NULL }
        checkIsSubtypeOf(rt("x" :: sub.notNull), rt("x" :: sup.nullable), nestingLevel + 1)
        // but not { x :: SUB + NULL } <: { x :: SUP - NULL }
        checkIsNotSubtypeOf(rt("x" :: sub.nullable), rt("x" :: sup.notNull), nestingLevel + 1)
      }
      // if SUB is not a RECORD or the nothing type, then not SUB <: { x :: SUP }
      if (noRecordType(sub) && sub != CTNothing && sub != CTNull)
        checkIsNotSubtypeOf(sub, rt("x" :: sup), nestingLevel + 1)
      // if SUP is not a RECORD or the nothing type, then not { x :: SUB } <: SUP
      if (noRecordType(sup) && sup != CTAny) checkIsNotSubtypeOf(rt("x" :: sub), sup, nestingLevel + 1)
    }
  }

  private def checkNullabilityInvariantForSubtypes(sub: CypherType, sup: CypherType): Unit = {
    assertIsSubtypeOf(sub.notNull, sup.nullable)
    assertIsSubtypeOf(sub.notNull, sup.notNull)
    assertIsSubtypeOf(sub.nullable, sup.nullable)
    assertIsNotSubtypeOf(sub.nullable, sup.notNull)
  }

  private def checkIsNotSubtypeOf(sub: CypherType, sup: CypherType, nestingLevel: Int = 0): Unit = {
    assertIsNotSubtypeOf(sub, sup)
    checkNullabilityInvariantForNonSubtypes(sub, sup)
    // check for list covariance
    if (nestingLevel < 3) checkIsNotSubtypeOf(l(sub), l(sup), nestingLevel + 1)
    // check for record covariance
    if (nestingLevel < 3) {
      checkIsNotSubtypeOf(rt("x" :: sub), rt("x" :: sup), nestingLevel + 1)
      checkIsNotSubtypeOf(rt("x" :: sub, "y" :: sup), rt("x" :: sup, "y" :: sub), nestingLevel + 1)
      checkIsNotSubtypeOf(
        rt("x" :: sub, "y" :: CTBoolean, "z" :: sub),
        rt("x" :: sup, "y" :: CTBoolean, "z" :: sup),
        nestingLevel + 1
      )
    }
  }

  private def checkNullabilityInvariantForNonSubtypes(sub: CypherType, sup: CypherType): Unit = {
    if (sub.notNull != CTNothing) {
      if (sup.nullable != CTAny) assertIsNotSubtypeOf(sub.notNull, sup.nullable)
      if (sup.notNull != CTAnyNotNull) assertIsNotSubtypeOf(sub.notNull, sup.notNull)
    }
    if (sub.nullable != CTNull) {
      if (sup.nullable != CTAny) assertIsNotSubtypeOf(sub.nullable, sup.nullable)
      if (sup.notNull != CTAnyNotNull) assertIsNotSubtypeOf(sub.nullable, sup.notNull)
    }
  }

  private def assertIsSubtypeOf(sub: CypherType, sup: CypherType): Unit = {
    if (!IsSubtypeOf(sub, sup)) {
      fail(
        s"""${sub.description} should be subtype of ${sup.description}, but is not
           |sub: ${pprint.apply(sub)}
           |sup: ${pprint.apply(sup)}""".stripMargin
      )
    }
  }

  private def assertIsNotSubtypeOf(sub: CypherType, sup: CypherType): Unit = {
    if (IsSubtypeOf(sub, sup)) {
      fail(s"""${sub.description} should not be subtype of ${sup.description}, but is
              |sub: ${pprint.apply(sub)}
              |sup: ${pprint.apply(sup)}""".stripMargin)
    }
  }
}
