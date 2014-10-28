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
package org.neo4j.cypher

import org.neo4j.graphdb.{DynamicLabel, DynamicRelationshipType}

class ReplanTest extends ExecutionEngineFunSuite {

  private val lLabel = DynamicLabel.label("L")
  private val tLabel = DynamicLabel.label("T")
  private val relType = DynamicRelationshipType.withName("X")

  test("make sure we replan when counts have changed") {
    val ts = graph.inTx {
      (1 to 1500).map { _ =>
        val n1 = graph.createNode(lLabel)
        val n2 = graph.createNode(tLabel)
        val n3 = graph.createNode(tLabel)
        n1.createRelationshipTo(n2, relType)
        n1.createRelationshipTo(n3, relType)
        n3
      }
    }

    val oldPlans = {
      val result = profile("MATCH (x:L)-[r:X]->(y:T) WHERE r.prop = 42 RETURN x, y")
      result.executionPlanDescription().pipe.planDescription.flatten.map(plan => plan.name -> plan.arguments)
    }

    graph.inTx {
      ts.foreach { t =>
        val n4 = graph.createNode(lLabel)
        val n5 = graph.createNode(lLabel)
        n4.createRelationshipTo(t, relType)
        n5.createRelationshipTo(t, relType)
      }
    }

    val newPlans = {
      val result = profile("MATCH (x:L)-[r:X]->(y:T) WHERE r.prop = 42 RETURN x, y")
      result.executionPlanDescription().pipe.planDescription.flatten.map(plan => plan.name -> plan.arguments)
    }

    newPlans should not equal oldPlans
  }
}
