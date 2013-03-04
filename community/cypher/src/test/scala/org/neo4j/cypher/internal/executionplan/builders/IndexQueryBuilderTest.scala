/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Assert._
import org.neo4j.cypher.internal.executionplan.PartiallySolvedQuery
import org.junit.Test
import org.neo4j.cypher.internal.commands._
import expressions.{Identifier, Property, Literal}
import org.neo4j.graphdb.event.{KernelEventHandler, TransactionEventHandler}
import java.lang.Iterable
import org.neo4j.graphdb._
import index._
import java.util.Map
import org.neo4j.cypher.internal.pipes.NodeStartPipe

class IndexQueryBuilderTest extends BuilderTest {

  val builder = new IndexQueryBuilder(new EntityProducerFactory(new Fake_Database_That_Has_All_Indexes))

  @Test
  def says_yes_to_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertAccepts(q)
  }

  @Test
  def only_takes_one_start_item_at_the_time() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(
        Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))),
        Unsolved(NodeByIndexQuery("x", "idx", Literal("foo")))))

    val remaining = assertAccepts(q).query

    assertEquals("No more than 1 startitem should be solved", 1, remaining.start.filter(_.solved).length)
    assertEquals("Stuff should remain", 1, remaining.start.filterNot(_.solved).length)
  }

  @Test
  def fixes_node_by_id_and_keeps_the_rest_around() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1))))


    val result = assertAccepts(q).query

    val expected = Set(Solved(NodeByIndexQuery("s", "idx", Literal("foo"))), Unsolved(RelationshipById("x", 1)))

    assert(result.start.toSet === expected)
  }

  @Test
  def says_no_to_already_solved_node_by_id_queries() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    assertRejects(q)
  }

  @Test
  def builds_a_nice_start_pipe() {
    val q = PartiallySolvedQuery().
      copy(start = Seq(Unsolved(NodeByIndexQuery("s", "idx", Literal("foo")))))

    val remainingQ = assertAccepts(q).query

    assert(remainingQ.start === Seq(Solved(NodeByIndexQuery("s", "idx", Literal("foo")))))
  }

  @Test
  def does_not_offer_to_solve_empty_queries() {
    //GIVEN WHEN THEN
    assertRejects(PartiallySolvedQuery())
  }


  @Test
  def offers_to_solve_query_with_index_hints() {
    //GIVEN
    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(Equals(Property(Identifier("n"), "name"), Literal("Stefan")))),
      start = Seq(Unsolved(IndexHint("n", "Person", "name"))))

    //THEN
    val producedPlan = assertAccepts(q)

    assert(producedPlan.pipe.isInstanceOf[NodeStartPipe])
  }

  /*



  @Test
  def throws_when_finding_hints_without_matching_predicate() {
    //GIVEN
    val q = PartiallySolvedQuery().copy(
      hints = Seq(Unsolved(IndexHint("n", "Person", "name"))))

    //THEN
    intercept[IndexHintException](assertAccepts(q))
  }

  @Test
  def throws_when_the_predicate_is_not_usable_for_index_seek() {
    //GIVEN
    val q = PartiallySolvedQuery().copy(
      where = Seq(Unsolved(Or(
        Equals(Property(Identifier("n"), "name"), Literal("Stefan")),
        Equals(Property(Identifier("n"), "age"), Literal(35))))),
      hints = Seq(Unsolved(IndexHint("n", "Person", "name"))))

    //THEN
    intercept[IndexHintException](assertAccepts(q))
  }
   */
}

class Fake_Database_That_Has_All_Indexes extends GraphDatabaseService with IndexManager {
  def createNode(): Node = null

  def createNode(labels: Label*): Node = null

  def existsForNodes(indexName: String): Boolean = true

  def existsForRelationships(indexName: String): Boolean = true

  def forNodes(indexName: String): Index[Node] = null

  def forNodes(indexName: String, customConfiguration: Map[String, String]): Index[Node] = null

  def forRelationships(indexName: String): RelationshipIndex = null

  def forRelationships(indexName: String, customConfiguration: Map[String, String]): RelationshipIndex = null

  def getConfiguration(index: Index[_ <: PropertyContainer]): Map[String, String] = null

  def getNodeAutoIndexer: AutoIndexer[Node] = null

  def getRelationshipAutoIndexer: RelationshipAutoIndexer = null

  def nodeIndexNames(): Array[String] = null

  def relationshipIndexNames(): Array[String] = null

  def removeConfiguration(index: Index[_ <: PropertyContainer], key: String): String = ""

  def setConfiguration(index: Index[_ <: PropertyContainer], key: String, value: String): String = ""

  def beginTx(): Transaction = null

  def getAllNodes: Iterable[Node] = null

  def getNodeById(id: Long): Node = null

  def getReferenceNode: Node = null

  def getRelationshipById(id: Long): Relationship = null

  def getRelationshipTypes: Iterable[RelationshipType] = null

  def index(): IndexManager = this

  def registerKernelEventHandler(handler: KernelEventHandler): KernelEventHandler = null

  def registerTransactionEventHandler[T](handler: TransactionEventHandler[T]): TransactionEventHandler[T] = null

  def shutdown() {}

  def unregisterKernelEventHandler(handler: KernelEventHandler): KernelEventHandler = null

  def unregisterTransactionEventHandler[T](handler: TransactionEventHandler[T]): TransactionEventHandler[T] = null

  def schema() = ???
}