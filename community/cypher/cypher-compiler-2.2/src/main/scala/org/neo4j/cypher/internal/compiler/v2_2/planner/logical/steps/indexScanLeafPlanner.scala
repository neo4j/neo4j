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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.functions
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{LeafPlanner, LogicalPlanningContext}

object indexScanLeafPlanner extends LeafPlanner {
  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicates: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates

    predicates.collect {
      // MATCH (n:User) WHERE has(n.prop) RETURN n
      case predicate@FunctionInvocation(_, _, IndexedSeq(Property(Identifier(name), propertyKeyName))) if predicate.function == Some(functions.Has) =>

        val idName = IdName(name)
        for (labelPredicate <- labelPredicates.getOrElse(idName, Set.empty);
             labelName <- labelPredicate.labels;
             indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName.name);
             labelId <- labelName.id)
          yield {
            val propertyName = propertyKeyName.name
            val hint = qg.hints.collectFirst {
              case hint@UsingIndexHint(Identifier(`name`), `labelName`, Identifier(`propertyName`)) => hint
            }
            context.logicalPlanProducer.planNodeIndexScan(idName, LabelToken(labelName, labelId),
              PropertyKeyToken(propertyKeyName, propertyKeyName.id.head), Seq(predicate, labelPredicate),
              hint, qg.argumentIds)
          }

    }.flatten
  }

  private def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext) =
    context.planContext.getIndexRule(label, property) orElse context.planContext.getUniqueIndexRule(label, property)
}
