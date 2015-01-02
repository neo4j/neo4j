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
package org.neo4j.cypher

import org.junit.Test

class IndexUsageAcceptanceTest extends ExecutionEngineJUnitSuite{
  @Test
  def should_not_forget_predicates() {
    // Given
    execute("CREATE (_0:Matrix { name:'The Architect' }),(_1:Matrix { name:'Agent Smith' }),(_2:Matrix:Crew { name:'Cypher' }),(_3:Crew { name:'Trinity' }),(_4:Crew { name:'Morpheus' }),(_5:Crew { name:'Neo' }), _1-[:CODED_BY]->_0, _2-[:KNOWS]->_1, _4-[:KNOWS]->_3, _4-[:KNOWS]->_2, _5-[:KNOWS]->_4, _5-[:LOVES]->_3")
    execute("CREATE INDEX ON :Crew(name)")

    // When
    val result = execute("MATCH (n:Crew) WHERE n.name = 'Neo' AND n.name= 'Morpheus' RETURN n")

    // Then
    assert(result.isEmpty, "Should not find anything here")
  }
}
