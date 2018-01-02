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

import java.util

import collection.JavaConverters._
import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Identifier, ParameterExpression}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{CreateRelationship, RelationshipEndpoint}

class CreateRelationshipTest extends GraphDatabaseFunSuite {

  test("should accept Java arrays as properties for relationships") {
    //given
    val a = createNode()
    val b = createNode()
    val javaArray: util.List[Int] = util.Arrays.asList(1, 2, 3)
    val props = Map("props" -> Map("array" -> javaArray))
    val aEndNode = RelationshipEndpoint(Identifier("a"), Map(), Seq.empty)
    val bEndNode = RelationshipEndpoint(Identifier("b"), Map(), Seq.empty)
    val relCreator = new CreateRelationship("r", aEndNode, bEndNode, "RELTYPE", Map("*" -> ParameterExpression("props")))

    val tx = graph.beginTx()
    try {
      val state = QueryStateHelper.queryStateFrom(graph, tx, props)
      val ctx = ExecutionContext.from("a" -> a, "b" -> b)

      //when
      relCreator.exec(ctx, state)

      //then
      val relationships = a.getRelationships.asScala.toList
      relationships should have size 1
      relationships.head.getProperty("array") should equal(Array(1, 2, 3))
      tx.success()
    } finally {
      tx.close()
    }
  }
}
