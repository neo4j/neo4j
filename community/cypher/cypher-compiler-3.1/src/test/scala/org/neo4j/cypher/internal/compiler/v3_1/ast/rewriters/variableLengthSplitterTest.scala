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
package org.neo4j.cypher.internal.compiler.v3_1.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite

class variableLengthSplitterTest extends CypherFunSuite with RewriteTest {
  override def rewriterUnderTest = variableLengthSplitter

  shouldNotBeRewritten("return 1")
  shouldNotBeRewritten("match (n)-[r]->(m) return *")
  shouldNotBeRewritten("match (a)-[*0..2]->(b) return *")
  shouldNotBeRewritten("match (a)-[*..1]->()-[*0..1]->(b) return *")

  shouldBeRewritten(
    "match (a)-[*..2]->(b) return *",
    "match (a)-[*1..1]->()-[*0..1]->(b) return *")
  shouldBeRewritten(
    "match (a)-[:T*..2]->(b) return *",
    "match (a)-[:T*1..1]->()-[:T*0..1]->(b) return *")
  shouldBeRewritten(
    "match (x)-->(a)-[*..2]->(b) return *",
    "match (x)-->(a)-[*1..1]->()-[*0..1]->(b) return *")
  shouldBeRewritten(
    "match (a)-[*..2]->(b)-->(x) return *",
    "match (a)-[*1..1]->()-[*0..1]->(b)-->(x) return *")
  shouldBeRewritten(
    "match (a)-[*..2 {prop: 42}]->(b) return *",
    "match (a)-[*1..1 {prop: 42}]->()-[*0..1 {prop: 42}]->(b) return *")
  shouldBeRewritten(
    "match (a)-[:T*..2 {prop: 42}]->(b) return *",
    "match (a)-[:T*1..1 {prop: 42}]->()-[:T*0..1 {prop: 42}]->(b) return *")
  shouldBeRewritten(
    "match (a)-[*..3]->(b) return *",
    "match (a)-[*1..1]->()-[*0..2]->(b) return *")
  shouldBeRewritten(
    "match (a)-[*..4]->(b) return *",
    "match (a)-[*1..2]->()-[*0..2]->(b) return *")

  private def shouldBeRewritten(from: String, to: String): Unit = test("rewrites: " + from) {
    assertRewrite(from, to)
  }

  private def shouldNotBeRewritten(q: String): Unit = test("not rewritten: " + q) {
    assertIsNotRewritten(q)
  }
}
