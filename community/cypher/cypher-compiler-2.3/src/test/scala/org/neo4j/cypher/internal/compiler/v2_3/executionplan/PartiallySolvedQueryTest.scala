/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.Unsolved
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{CreateNode, DeleteEntityAction, MergeNodeAction, MergePatternAction}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class PartiallySolvedQueryTest extends CypherFunSuite {

  test("should_compact_query") {
    // Given CREATE a1 WITH * CREATE a2 WITH * CREATE a3
    val q1 = Query.start(createNode("a1")).returns()
    val q2 = Query.start(createNode("a2")).tail(q1).returns(AllIdentifiers())
    val q3 = Query.start(createNode("a3")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q3)

    // Then CREATE a1,a2,a3
    psq.start.toSet should equal(Set(Unsolved(createNode("a1")), Unsolved(createNode("a2")), Unsolved(createNode("a3"))))
  }

  test("deletes and creates should not be compacted together") {
    // Given MATCH (a) WITH a DELETE a WITH a CREATE (:Person)
    val deleteAction = DeleteEntityAction(Identifier("a"), forced = false)
    val q3 = Query.start(createNode("a3")).returns()
    val q2 = Query.updates(deleteAction).tail(q3).returns(AllIdentifiers())
    val q1 = Query.matches(SingleNode("a")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q1)

    // Then First query part doesn't contain updates
    psq.updates shouldBe empty

    // Second query part contains no create node
    psq.tail.get.updates.toList should equal(List(Unsolved(deleteAction)))
    psq.tail.get.start shouldBe empty

    // Third part contains the create node
    psq.tail.get.tail.get.start.toList should equal(List(Unsolved(createNode("a3"))))

  }

  test("delete followed by merge node should not be compacted together") {
    // MATCH (a) DELETE a MERGE (b:B)
    val q3 = Query.updates(mergeNode("b")).returns()
    val q2 = Query.updates(DeleteEntityAction(Identifier("a"), forced = false)).tail(q3).returns(AllIdentifiers())
    val q1 = Query.matches(SingleNode("a")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q1)

    // Then First query part doesn't contain updates
    psq.updates shouldBe empty

    // Second query part contains no merge node
    psq.tail.get.updates.toList should equal(List(Unsolved(DeleteEntityAction(Identifier("a"), forced = false))))

    // Third part contains the merge node
    psq.tail.get.tail.get.updates.toList should equal(List(Unsolved(mergeNode("b"))))
  }

  test("merge node followed by merge node should not be compacted together") {
    // MATCH () MERGE (a:A) MERGE (b:B)
    val q3 = Query.updates(mergeNode("b")).returns()
    val q2 = Query.updates(mergeNode("a")).tail(q3).returns(AllIdentifiers())
    val q1 = Query.matches(SingleNode("a")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q1)

    // Then First query part doesn't contain updates
    psq.updates shouldBe empty

    // Second query part contains no merge node
    psq.tail.get.updates.toList should equal(List(Unsolved(mergeNode("a"))))

    // Third part contains the merge node
    psq.tail.get.tail.get.updates.toList should equal(List(Unsolved(mergeNode("b"))))
  }

  test("merge pattern followed by merge node should not be compacted together") {
    // MATCH (n) MERGE (n)-[t:T]->(n) MERGE (a:A)
    val pattern = mergePattern(RelatedTo("n", "n", "t", "T", SemanticDirection.OUTGOING))
    val q3 = Query.updates(mergeNode("a")).returns()
    val q2 = Query.updates(pattern).tail(q3).returns(AllIdentifiers())
    val q1 = Query.matches(SingleNode("a")).tail(q2).returns(AllIdentifiers())

    // When
    val psq = PartiallySolvedQuery(q1)

    // Then First query part doesn't contain updates
    psq.updates shouldBe empty

    // Second query part contains no merge node
    psq.tail.get.updates.toList should equal(List(Unsolved(pattern)))

    // Third part contains the merge node
    psq.tail.get.tail.get.updates.toList should equal(List(Unsolved(mergeNode("a"))))
  }

  private def createNode(name: String) = CreateNodeStartItem(CreateNode(name, Map.empty, Seq.empty))

  private def mergeNode(name: String) = MergeNodeAction(name, Map.empty, Seq.empty, Seq.empty, Seq.empty, Seq.empty, None)

  private def mergePattern(pattern: Pattern) = MergePatternAction(Seq(pattern), Seq.empty, Seq.empty, Seq.empty)
}
