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
package org.neo4j.cypher.docgen.refcard

import org.neo4j.cypher.QueryStatisticsTestSupport
import org.neo4j.cypher.docgen.RefcardTest
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

class ConstraintTest extends RefcardTest with QueryStatisticsTestSupport {
  val graphDescription = List("A:Person KNOWS B:Person")
  val title = "CONSTRAINT"
  val css = "write c2-2 c4-4 c5-5 c6-3"
  override val linkId = "query-constraints"

  override protected def newTestGraphDatabaseFactory() = new TestEnterpriseGraphDatabaseFactory()

  override def assert(name: String, result: InternalExecutionResult) {
    name match {
      case "create-unique-property-constraint" =>
        assertStats(result, constraintsAdded = 1)
        assert(result.toList.size === 0)
      case "drop-unique-property-constraint" =>
        assertStats(result, constraintsRemoved = 1)
        assert(result.toList.size === 0)
      case "create-node-property-existence-constraint" =>
        assertStats(result, constraintsAdded = 1)
        assert(result.toList.size === 0)
      case "drop-node-property-existence-constraint" =>
        assertStats(result, constraintsRemoved = 1)
        assert(result.toList.size === 0)
      case "create-relationship-property-existence-constraint" =>
        assertStats(result, constraintsAdded = 1)
        assert(result.toList.size === 0)
      case "drop-relationship-property-existence-constraint" =>
        assertStats(result, constraintsRemoved = 1)
        assert(result.toList.size === 0)
      case "match" =>
        assertStats(result, nodesCreated = 0)
        assert(result.toList.size === 1)
    }
  }

  override val properties: Map[String, Map[String, Any]] = Map(
    "A" -> Map("name" -> "Alice"),
    "B" -> Map("name" -> "Bobo"))

  override def parameters(name: String): Map[String, Any] =
    name match {
      case "parameters=aname" =>
        Map("value" -> "Alice")
      case _ =>
        Map()
    }

  def text = """
###assertion=create-unique-property-constraint
//

CREATE CONSTRAINT ON (p:Person)
       ASSERT p.name IS UNIQUE
###

Create a unique property constraint on the label `Person` and property `name`.
If any other node with that label is updated or created with a `name` that
already exists, the write operation will fail.
This constraint will create an accompanying index.

###assertion=drop-unique-property-constraint
//

DROP CONSTRAINT ON (p:Person)
     ASSERT p.name IS UNIQUE
###

Drop the unique constraint and index on the label `Person` and property `name`.

###assertion=create-node-property-existence-constraint
//

CREATE CONSTRAINT ON (p:Person)
       ASSERT exists(p.name)
###

Create a node property existence constraint on the label `Person` and property `name`.
If a node with that label is created without a `name`, or if the `name` property is
removed from an existing node with the `Person` label, the write operation will fail.

###assertion=drop-node-property-existence-constraint
//

DROP CONSTRAINT ON (p:Person)
     ASSERT exists(p.name)
###

Drop the node property existence constraint on the label `Person` and property `name`.

###assertion=create-relationship-property-existence-constraint
//

CREATE CONSTRAINT ON ()-[l:LIKED]-()
       ASSERT exists(l.when)
###

Create a relationship property existence constraint on the type `LIKED` and property `when`.
If a relationship with that type is created without a `when`, or if the `when` property is
removed from an existing relationship with the `LIKED` type, the write operation will fail.

###assertion=drop-relationship-property-existence-constraint
//

DROP CONSTRAINT ON ()-[l:LIKED]-()
     ASSERT exists(l.when)
###

Drop the relationship property existence constraint on the type `LIKED` and property `when`.
"""
}
