/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.Rewriter

class AddUniquenessPredicatesTest extends CypherFunSuite with RewriteTest {

  test("does not introduce predicate not needed") {
    assertIsNotRewritten("RETURN 42")
    assertIsNotRewritten("MATCH n RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) RETURN n")
    assertIsNotRewritten("MATCH (n)-[r1]->(m) MATCH (m)-[r2]->(x) RETURN x")
  }

  test("uniqueness check is done between relationships") {
    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE r1 <> r2 RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c)-[r3]->(d) WHERE r2 <> r3 AND r1 <> r3 AND r1 <> r2 RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) RETURN *",
      "MATCH (a)-[r1]->(b), (b)-[r2]->(c), (c)-[r3]->(d) WHERE r1 <> r2 AND r1 <> r3 AND r2 <> r3 RETURN *")
  }

  test("no uniqueness check between relationships of different type") {
    assertRewrite(
      "MATCH (a)-[r1:X]->(b)-[r2:Y]->(c) RETURN *",
      "MATCH (a)-[r1:X]->(b)-[r2:Y]->(c) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) RETURN *",
      "MATCH (a)-[r1:X]->(b)-[r2:X|Y]->(c) WHERE r1 <> r2 RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2:X]->(c) WHERE r1 <> r2 RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->(c) WHERE r1 <> r2 RETURN *")
  }

  test("ignores shortestPath relationships for uniqueness") {
    assertRewrite(
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), shortestPath((a)-[r]->(b)) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->c, shortestPath((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->c, shortestPath((a)-[r]->(b)) WHERE r1 <> r2 RETURN *")
  }

  test("ignores allShortestPaths relationships for uniqueness") {
    assertRewrite(
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b), allShortestPaths((a)-[r]->(b)) RETURN *")

    assertRewrite(
      "MATCH (a)-[r1]->(b)-[r2]->c, allShortestPaths((a)-[r]->(b)) RETURN *",
      "MATCH (a)-[r1]->(b)-[r2]->c, allShortestPaths((a)-[r]->(b)) WHERE r1 <> r2 RETURN *")
  }

  val rewriterUnderTest: Rewriter = addUniquenessPredicates
}
