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
package org.neo4j.cypher.internal.pipes.matching

import org.junit.Test
import org.neo4j.graphdb.{RelationshipType, DynamicRelationshipType, Direction}
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.cypher.internal.pipes.{QueryStateHelper, QueryState}
import org.neo4j.cypher.internal.ExecutionContext

class VariableLengthExpanderStepExpandTest extends GraphDatabaseTestBase {

  private def step(id: Int,
                   typ: Seq[String],
                   direction: Direction,
                   next: Option[ExpanderStep]) = SingleStep(id, typ, direction, next, True(), True())

  private def varStep(id: Int,
                      typ: Seq[String],
                      direction: Direction,
                      min: Int,
                      max: Option[Int],
                      next: Option[ExpanderStep]) = VarLengthStep(id, typ, direction, min, max, next, True(), True())

  private def context = ExecutionContext()
  private def state = QueryStateHelper.queryStateFrom(graph)
  val A = "A"
  val B = "B"
  val C = "C"

  @Test def not_reached_min_zero_with_matching_rels() {
    /*
    Given the pattern:
     ()-[:REL*1..2]->()
     */
    val step = varStep(0, Seq("REL"), Direction.OUTGOING, 1, Some(2), None)

    /*
    And the graph:
     (a)-[r1:REL]->(b)
    */

    val a = createNode()
    val b = createNode()
    val r1 = relate(a, b)

    /*
   When given the start node a
   */
    val (relationships, next) = step.expand(a, context, state)

    /*should return r1 and next step: ()-[:A*0..1]->()*/
    val expectedNext = Some(varStep(0, Seq("REL"), Direction.OUTGOING, 0, Some(1), None))

    assert(relationships.toSeq === Seq(r1))
    assert(next === expectedNext)
  }

  @Test def reached_min_zero_with_matching_rels() {
    /*
    Given the pattern:
     ()-[:A*0..]->()-[:B]->()
     */
    val step2 = step(1, Seq(B), Direction.OUTGOING, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, 0, None, Some(step2))

    /*
    And the graph:
     (a)-[r1:A]->(b)
      ^
      |
    [r2:B]
      |
     (c)
    */

    val a = createNode()
    val b = createNode()
    val c = createNode()

    val r1 = relate(a, b, "A")
    val r2 = relate(a, c, "B")

    /*
   When given the start node a
   */
    val (relationships, next) = step1.expand(a, context, state)

    /*should return both relationships and the same step again */

    assert(relationships.toSeq === Seq(r1, r2))
    assert(next === Some(step1))
  }

  @Test def reached_min_zero_and_not_finding_matching_rels() {
    /*
    Given the pattern:
     ()-[:A*0..]->()-[:B]->()
     */
    val step2 = step(1, Seq(B), Direction.OUTGOING, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, 0, None, Some(step2))

    /*
    And the graph:
     (a)-[r1:B]->(b)
    */

    val a = createNode()
    val b = createNode()

    val r1 = relate(a, b, "B")

    /*When given the start node a*/
    val (relationships, next) = step1.expand(a, context, state)

    /*should return the single relationship and None as the next step*/
    assert(relationships.toSeq === Seq(r1))
    assert(next === None)
  }

  @Test def reached_max_0_should_return_next_step() {
    /*
    Given the pattern:
     ()-[:A*1..1]->()-[:B]->()
     */
    val step2 = step(1, Seq(B), Direction.OUTGOING, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, 1, Some(1), Some(step2))

    /*
    And the graph:
     (a)-[r1:A]->(b)
    */
    val a = createNode()
    val b = createNode()

    val r1 = relate(a, b, "A")

    /*When given the start node a*/
    val (relationships, next) = step1.expand(a, context, state)

    /*should return the single relationship and step2 as the next step*/
    assert(relationships.toSeq === Seq(r1))
    assert(next === Some(step2))
  }

  @Test def not_reached_min_0_and_no_matching_rels() {
    /*
    Given the pattern:
     ()-[:A*1..2]->()-[:B]->()
     */
    val step2 = step(1, Seq(B), Direction.OUTGOING, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, 1, Some(2), Some(step2))

    /*
    And the graph:
     (a)-[r1:B]->(b)
    */
    val a = createNode()
    val b = createNode()

    relate(a, b, "B")

    /*When given the start node a*/
    val (relationships, next) = step1.expand(a, context, state)

    /*should return no relationships, and step 0, but with one less min step as the next step*/
    val expectedNext = varStep(0, Seq(A), Direction.OUTGOING, 0, Some(1), Some(step2))

    assert(relationships.toSeq === Seq())
    assert(next === Some(expectedNext))
  }

  @Test def zero_step_should_return_the_start_node() {
    /*
    Given the pattern:
     ()-[:A*0..1]->()
     */
    val step2 = step(1, Seq(B), Direction.OUTGOING, None)
    val step1 = varStep(0, Seq(A), Direction.OUTGOING, 1, Some(2), Some(step2))

    /*
    And the graph:
     (a)-[r1:B]->(b)
    */
    val a = createNode()
    val b = createNode()

    relate(a, b, "B")

    /*When given the start node a*/
    val (relationships, next) = step1.expand(a, context, state)

    /*should return no relationships, and step 0, but with one less min step as the next step*/
    val expectedNext = varStep(0, Seq(A), Direction.OUTGOING, 0, Some(1), Some(step2))

    assert(relationships.toSeq === Seq())
    assert(next === Some(expectedNext))
  }
}