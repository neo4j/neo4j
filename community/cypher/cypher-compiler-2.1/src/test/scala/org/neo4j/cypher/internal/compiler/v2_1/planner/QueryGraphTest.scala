/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{PatternRelationship, IdName}
import org.neo4j.graphdb.Direction

class QueryGraphTest extends CypherFunSuite {
  test("returns no pattern relationships when the query graph doesn't contain any") {
    val rels: Set[PatternRelationship] = Set.empty
    val qg = QueryGraph(Map.empty, Selections(), Set.empty, rels)

    qg.findRelationshipsEndingOn(IdName("x")) shouldBe empty
  }

  test("finds single pattern relationship") {
    val r = PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq.empty)
    val qg = QueryGraph(Map.empty, Selections(), Set.empty, Set(r))

    qg.findRelationshipsEndingOn(IdName("x")) shouldBe empty
    qg.findRelationshipsEndingOn(IdName("a")) should equal(Set(r))
    qg.findRelationshipsEndingOn(IdName("b")) should equal(Set(r))
  }

  test("finds multiple pattern relationship") {
    val r = PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.BOTH, Seq.empty)
    val r2 = PatternRelationship(IdName("r2"), (IdName("b"), IdName("c")), Direction.BOTH, Seq.empty)
    val qg = QueryGraph(Map.empty, Selections(), Set.empty, Set(r, r2))

    qg.findRelationshipsEndingOn(IdName("x")) shouldBe empty
    qg.findRelationshipsEndingOn(IdName("a")) should equal(Set(r))
    qg.findRelationshipsEndingOn(IdName("b")) should equal(Set(r, r2))
    qg.findRelationshipsEndingOn(IdName("c")) should equal(Set(r2))
  }
}
