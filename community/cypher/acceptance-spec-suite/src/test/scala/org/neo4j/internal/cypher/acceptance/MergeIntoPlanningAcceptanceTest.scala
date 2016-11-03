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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.InternalExecutionResult

class MergeIntoPlanningAcceptanceTest extends ExecutionEngineFunSuite{

  test("ON CREATE with update one property") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
      |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = 'foo'""".stripMargin)

    //then
    update should use("Merge(Into)")
  }

  test("ON CREATE with deleting one property") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
                           |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = null""".stripMargin)

    //then
    update should use("Merge(Into)")
  }

  test("ON CREATE with update all properties from node") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r = a")

    //then
    update should use("Merge(Into)")
  }

  test("ON MATCH with update all properties from node") {
    //note the props here should be overwritten with ON MATCH
    relate(createNode("A"), createNode("B"), "TYPE", Map("foo" -> "bar"))

    //when
    val update = execute("MATCH (a {name:'A'}), (b {name:'B'}) MERGE (a)-[r:TYPE]->(b) ON MATCH SET r = a")

    //then
    update should use("Merge(Into)")
  }

  test("ON CREATE with update properties from literal map") {
    //given
    createNode("A")
    createNode("B")

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
      |MERGE (a)-[r:TYPE]->(b) ON CREATE SET r += {foo: 'bar', bar: 'baz'}""".stripMargin)

    //then
    update should use("Merge(Into)")
  }

  test("ON MATCH with update properties from literal map") {
    //given
    relate(createNode("A"), createNode("B"), "TYPE", Map("foo" -> "bar"))

    //when
    val update = execute("""MATCH (a {name:'A'}), (b {name:'B'})
                           |MERGE (a)-[r:TYPE]->(b) ON MATCH SET r += {foo: 'baz', bar: 'baz'}""".stripMargin)

    //then
    update should use("Merge(Into)")
  }

  //MERGE INTO is only used by the rule planner
  override def execute(q: String, params: (String, Any)*): InternalExecutionResult= super.execute(s"CYPHER planner=rule $q", params:_*)
}
