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
package org.neo4j.cypher

import org.junit.Test

class OptionalBehaviourAcceptanceTest extends ExecutionEngineHelper {
  @Test def optional_nodes_with_labels_in_match_clause_should_return_null_when_where_is_no_match() {
    createNode()
    val result = execute("start n=node(1) optional match n-[r]-(m:Person) return r")
    assert(result.toList === List(Map("r" -> null)))
  }

  @Test def optional_nodes_with_labels_in_match_clause_should_not_return_if_where_is_no_match() {
    createNode()
    val result = execute("start n=node(1) optional match (n)-[r]-(m) where m:Person return r")
    assert(result.toList === List(Map("r" -> null)))
  }
}
