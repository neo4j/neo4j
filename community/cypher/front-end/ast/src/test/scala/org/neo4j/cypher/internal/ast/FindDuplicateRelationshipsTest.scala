/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

//noinspection ZeroIndexToHead
class FindDuplicateRelationshipsTest extends CypherFunSuite {

  private val pos = DummyPosition(0)
  private val node = NodePattern(None, Seq.empty, None)(pos)
  private val relR = Variable("r")(pos)
  private val relRBumped = Variable("r")(pos.bumped())
  private val relS = Variable("s")(pos)

  test("find duplicate relationships across pattern parts") {
    val relPath0 = EveryPath(RelationshipChain(node, relPattern(relR), node)(pos))
    val relPath1 = EveryPath(RelationshipChain(node, relPattern(relRBumped), node)(pos))
    val pattern = Pattern(Seq(relPath0, relPath1))(pos)

    RelationshipChain.findDuplicateRelationships(pattern) should equal(Seq(relR))
    RelationshipChain.findDuplicateRelationships(pattern)(0).position should equal(relR.position)
  }

  test("find duplicate relationships in a long rel chain") {
    val relPath = expressions.EveryPath(relChain(relR, relS, relRBumped))
    val pattern = Pattern(Seq(relPath))(pos)

    RelationshipChain.findDuplicateRelationships(pattern) should equal(Seq(relR))
    RelationshipChain.findDuplicateRelationships(pattern)(0).position should equal(relR.position)
  }

  test("does not find duplicate relationships across pattern parts if there is none") {
    val relPath = EveryPath(expressions.RelationshipChain(node, relPattern(relR), node)(pos))
    val otherRelPath = EveryPath(expressions.RelationshipChain(node, relPattern(relS), node)(pos))
    val pattern = Pattern(Seq(relPath, otherRelPath))(pos)

    RelationshipChain.findDuplicateRelationships(pattern) should equal(Seq.empty)
  }

  test("does not find duplicate relationships in a long rel chain if there is none") {
    val relPath = expressions.EveryPath(relChain(relS, relR))
    val pattern = Pattern(Seq(relPath))(pos)

    RelationshipChain.findDuplicateRelationships(pattern) should equal(Seq.empty)
  }

  private def relChain(ids: Variable*) =
    ids.foldLeft(node.asInstanceOf[PatternElement]) {
      (n, id) => expressions.RelationshipChain(n, relPattern(id), node)(pos)
    }

  private def relPattern(id: Variable) =
    RelationshipPattern(Some(id), Seq(), None, None, SemanticDirection.OUTGOING)(pos)
}
