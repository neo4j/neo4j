/**
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
package org.neo4j.cypher



class IndexUsageAcceptanceTest extends ExecutionEngineFunSuite {
  test("should be able to use indexes") {
    // Given
    execute("CREATE (_0:Matrix { name:'The Architect' }),(_1:Matrix { name:'Agent Smith' }),(_2:Matrix:Crew { name:'Cypher' }),(_3:Crew { name:'Trinity' }),(_4:Crew { name:'Morpheus' }),(_5:Crew { name:'Neo' }), _1-[:CODED_BY]->_0, _2-[:KNOWS]->_1, _4-[:KNOWS]->_3, _4-[:KNOWS]->_2, _5-[:KNOWS]->_4, _5-[:LOVES]->_3")
    graph.createIndex("Crew", "name")

    // When
    val result = execute("MATCH (n:Crew) WHERE n.name = 'Neo' RETURN n")

    // Then
    result.executionPlanDescription.toString should include("SchemaIndex")
  }

  test("should not forget predicates") {
    // Given
    execute("CREATE (_0:Matrix { name:'The Architect' }),(_1:Matrix { name:'Agent Smith' }),(_2:Matrix:Crew { name:'Cypher' }),(_3:Crew { name:'Trinity' }),(_4:Crew { name:'Morpheus' }),(_5:Crew { name:'Neo' }), _1-[:CODED_BY]->_0, _2-[:KNOWS]->_1, _4-[:KNOWS]->_3, _4-[:KNOWS]->_2, _5-[:KNOWS]->_4, _5-[:LOVES]->_3")
    graph.createIndex("Crew", "name")


    // When
    val result = execute("cypher 2.1 MATCH (n:Crew) WHERE n.name = 'Neo' AND n.name = 'Morpheus' RETURN n")

    // Then
    result shouldBe empty
    result.executionPlanDescription.toString should include("SchemaIndex")
  }

  test("should use index when there are multiple labels on the node") {
    // Given
    execute("CREATE (_0:Matrix { name:'The Architect' }),(_1:Matrix { name:'Agent Smith' }),(_2:Matrix:Crew { name:'Cypher' }),(_3:Crew { name:'Trinity' }),(_4:Crew { name:'Morpheus' }),(_5:Crew { name:'Neo' }), _1-[:CODED_BY]->_0, _2-[:KNOWS]->_1, _4-[:KNOWS]->_3, _4-[:KNOWS]->_2, _5-[:KNOWS]->_4, _5-[:LOVES]->_3")
    graph.createIndex("Crew", "name")

    // When
    val result = execute("MATCH (n:Matrix:Crew) WHERE n.name = 'Neo' RETURN n")

    // Then
    result.executionPlanDescription.toString should include("SchemaIndex")
  }

  test("should be able to use value coming from UNWIND for index seek") {
    // Given
    graph.createIndex("Prop", "id")

    // When
    val result = execute("unwind [1,2,3] as x match (n:Prop) where n.id = x return *;")

    // Then
    result shouldBe empty
    result.executionPlanDescription.toString should include("SchemaIndex")
  }
}
