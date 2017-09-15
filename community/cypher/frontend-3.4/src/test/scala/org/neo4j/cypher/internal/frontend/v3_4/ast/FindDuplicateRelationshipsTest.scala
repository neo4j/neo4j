/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.DummyPosition
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticDirection

class FindDuplicateRelationshipsTest extends CypherFunSuite {

  val pos = DummyPosition(0)
  val node = NodePattern(None, Seq.empty, None)(pos)
  val relR = Variable("r")(pos)
  val relS = Variable("s")(pos)

  test("find duplicate relationships across pattern parts") {
    val relPath = EveryPath(RelationshipChain(node, relPattern(relR), node)(pos))
    val pattern = Pattern(Seq(relPath, relPath))(pos)

    Pattern.findDuplicateRelationships(pattern) should equal(Set(Seq(relR, relR)))
  }

  test("find duplicate relationships in a long rel chain") {
    val relPath = EveryPath(relChain(relR, relS, relR))
    val pattern = Pattern(Seq(relPath))(pos)

    Pattern.findDuplicateRelationships(pattern) should equal(Set(Seq(relR, relR)))
  }

  test("does not find duplicate relationships across pattern parts if there is none") {
    val relPath = EveryPath(RelationshipChain(node, relPattern(relR), node)(pos))
    val otherRelPath = EveryPath(RelationshipChain(node, relPattern(relS), node)(pos))
    val pattern = Pattern(Seq(relPath, otherRelPath))(pos)

    Pattern.findDuplicateRelationships(pattern) should equal(Set.empty)
  }

  test("does not find duplicate relationships in a long rel chain if there is none") {
    val relPath = EveryPath(relChain(relS, relR))
    val pattern = Pattern(Seq(relPath))(pos)

    Pattern.findDuplicateRelationships(pattern) should equal(Set.empty)
  }

  private def relChain(ids: Variable*) =
    ids.foldRight(node.asInstanceOf[PatternElement]) {
      (id, n) => RelationshipChain(n, relPattern(id), node)(pos)
    }

  private def relPattern(id: Variable) =
    RelationshipPattern(Some(id), Seq(), None, None, SemanticDirection.OUTGOING)(pos)
}
