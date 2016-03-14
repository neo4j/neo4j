/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_1.{DummyPosition, SemanticDirection}

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
    RelationshipPattern(Some(id), optional = false, Seq(), None, None, SemanticDirection.OUTGOING)(pos)
}
