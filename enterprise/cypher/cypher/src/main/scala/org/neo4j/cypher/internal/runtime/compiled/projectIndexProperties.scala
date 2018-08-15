/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.compiled

import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.opencypher.v9_0.util.attribution.Attributes
import org.opencypher.v9_0.util.symbols.CTNode
import org.opencypher.v9_0.util.{Rewriter, topDown}

/**
  * Repalce index plans that have indexed properties with `GetValue` by plans
  * that have `DoNotGetValue` isntead, with a projection to get the values on
  * top of the index plan.
  */
case class projectIndexProperties(attributes: Attributes) {

  def apply(plan: LogicalPlan, semanticTable: SemanticTable): (LogicalPlan, SemanticTable) = {
    var currentTypes = semanticTable.types

    val rewriter = topDown(Rewriter.lift {
      case plan: IndexLeafPlan if plan.propertyNamesWithValues.nonEmpty =>
        val projections = plan.availablePropertiesFromIndexes.map(_.swap)
        // Register all variables in the property lookups as nodes
        projections.values.foreach { prop =>
          currentTypes = currentTypes.updated(prop.map, ExpressionTypeInfo(CTNode.invariant, None))
        }

        val newIndexPlan = plan.copyWithoutGettingValues
        // TODO we should give the projections the SameId as the newIndexPlan after we have runtime independent execution plan descriptions
        // Then we also don't need attributes any more and can revert putting the logicalPlanIdGen in the EnterpriseRuntimeContext
        Projection(newIndexPlan, projections)(attributes.copy(plan.id))
    })

    val rewrittenPlan = rewriter(plan).asInstanceOf[LogicalPlan]
    val newSemanticTable = if(currentTypes == semanticTable.types) semanticTable else semanticTable.copy(types = currentTypes)
    (rewrittenPlan, newSemanticTable)
  }
}
