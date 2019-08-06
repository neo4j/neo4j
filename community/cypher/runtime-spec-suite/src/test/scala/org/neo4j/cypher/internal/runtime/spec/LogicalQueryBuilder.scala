/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.logical.builder.{AbstractLogicalPlanBuilder, TokenResolver}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.Variable
import org.neo4j.cypher.internal.v4_0.util.Cardinality

/**
  * Test help utility for hand-writing logical queries.
  */
class LogicalQueryBuilder(tokenResolver: TokenResolver)
  extends AbstractLogicalPlanBuilder[LogicalQuery, LogicalQueryBuilder](tokenResolver) {

  private var semanticTable = new SemanticTable()

  override def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  override def newRelationship(relationship: Variable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship)
  }

  def build(readOnly: Boolean = true): LogicalQuery = {
    val logicalPlan = buildLogicalPlan()
    val cardinalities = new Cardinalities
    logicalPlan.flatten.foreach(plan => cardinalities.set(plan.id, Cardinality(1)))
    LogicalQuery(logicalPlan,
                 "<<queryText>>",
                 readOnly,
                 resultColumns,
                 semanticTable,
                 cardinalities,
                 hasLoadCSV = false,
                 None)
  }
}
