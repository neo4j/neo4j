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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.True
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.{ExpanderStep, SingleStep, VarLengthStep}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.Direction

class VariableLengthExpanderStepExpandTest extends GraphDatabaseFunSuite with QueryStateTestSupport {
  test("not reached min zero with matching rels") {
    /*
    Given the pattern:
     ()-[:REL*1..2]->()
     */
    val step = varStep(0, Seq("REL"), SemanticDirection.OUTGOING, 1, Some(2), None)

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
    val (relationships, next) = withQueryState { queryState =>
      val (rels, n) = step.expand(a, ExecutionContext.empty, queryState)
      (rels.toSeq, n)
    }

    /*should return r1 and next step: ()-[:A*0..1]->()*/
    val expectedNext = Some(varStep(0, Seq("REL"), SemanticDirection.OUTGOING, 0, Some(1), None))

    relationships should equal(Seq(r1))
    next should equal(expectedNext)
  }

  test("reached min zero with matching rels") {
    /*
    Given the pattern:
     ()-[:A*0..]->()-[:B]->()
     */
    val step2 = step(1, Seq("B"), SemanticDirection.OUTGOING, None)
    val step1 = varStep(0, Seq("A"), SemanticDirection.OUTGOING, 0, None, Some(step2))

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
    val (relationships, next) = withQueryState { queryState =>
      val (rels, n) = step1.expand(a, ExecutionContext.empty, queryState)
      (rels.toSeq, n)    }

    /*should return both relationships and the same step again */

    relationships should equal(Seq(r1, r2))
    next should equal(Some(step1))
  }

  test("reached min zero and not finding matching rels") {
    /*
    Given the pattern:
     ()-[:A*0..]->()-[:B]->()
     */
    val step2 = step(1, Seq("B"), SemanticDirection.OUTGOING, None)
    val step1 = varStep(0, Seq("A"), SemanticDirection.OUTGOING, 0, None, Some(step2))

    /*
    And the graph:
     (a)-[r1:B]->(b)
    */

    val a = createNode()
    val b = createNode()

    val r1 = relate(a, b, "B")

    /*When given the start node a*/
    val (relationships, next) = withQueryState { queryState =>
      val (rels, n) = step1.expand(a, ExecutionContext.empty, queryState)
      (rels.toSeq, n)
    }

    /*should return the single relationship and None as the next step*/
    relationships should equal(Seq(r1))
    next should equal(None)
  }

  test("reached max 0 should return next step") {
    /*
    Given the pattern:
     ()-[:A*1..1]->()-[:B]->()
     */
    val step2 = step(1, Seq("B"), SemanticDirection.OUTGOING, None)
    val step1 = varStep(0, Seq("A"), SemanticDirection.OUTGOING, 1, Some(1), Some(step2))

    /*
    And the graph:
     (a)-[r1:A]->(b)
    */
    val a = createNode()
    val b = createNode()

    val r1 = relate(a, b, "A")

    /*When given the start node a*/
    val (relationships, next) = withQueryState { queryState =>
      val (rels, n) = step1.expand(a, ExecutionContext.empty, queryState)
      (rels.toSeq, n)    }

    /*should return the single relationship and step2 as the next step*/
    relationships should equal(Seq(r1))
    next should equal(Some(step2))
  }

  test("not reached min 0 and no matching rels") {
    /*
    Given the pattern:
     ()-[:A*1..2]->()-[:B]->()
     */
    val step2 = step(1, Seq("B"), SemanticDirection.OUTGOING, None)
    val step1 = varStep(0, Seq("A"), SemanticDirection.OUTGOING, 1, Some(2), Some(step2))

    /*
    And the graph:
     (a)-[r1:B]->(b)
    */
    val a = createNode()
    val b = createNode()

    relate(a, b, "B")

    /*When given the start node a*/
    val (relationships, next) = withQueryState { queryState =>
      val (rels, n) = step1.expand(a, ExecutionContext.empty, queryState)
      (rels.toSeq, n)    }

    /*should return no relationships, and step 0, but with one less min step as the next step*/
    val expectedNext = varStep(0, Seq("A"), SemanticDirection.OUTGOING, 0, Some(1), Some(step2))

    relationships should equal(Seq())
    next should equal(Some(expectedNext))
  }

  test("zero step should return the start node") {
    /*
    Given the pattern:
     ()-[:A*0..1]->()
     */
    val step2 = step(1, Seq("B"), SemanticDirection.OUTGOING, None)
    val step1 = varStep(0, Seq("A"), SemanticDirection.OUTGOING, 1, Some(2), Some(step2))

    /*
    And the graph:
     (a)-[r1:B]->(b)
    */
    val a = createNode()
    val b = createNode()

    relate(a, b, "B")

    /*When given the start node a*/
    val (relationships, next) = withQueryState { queryState =>
      val (rels, n) = step1.expand(a, ExecutionContext.empty, queryState)
      (rels.toSeq, n)
    }

    /*should return no relationships, and step 0, but with one less min step as the next step*/
    val expectedNext = varStep(0, Seq("A"), SemanticDirection.OUTGOING, 0, Some(1), Some(step2))

    relationships should equal(Seq())
    next should equal(Some(expectedNext))
  }

  private def step(id: Int,
                   typ: Seq[String],
                   direction: SemanticDirection,
                   next: Option[ExpanderStep]) = SingleStep(id, typ, direction, next, True(), True())

  private def varStep(id: Int,
                      typ: Seq[String],
                      direction: SemanticDirection,
                      min: Int,
                      max: Option[Int],
                      next: Option[ExpanderStep]) = VarLengthStep(id, typ, direction, min, max, next, True(), True())
}
