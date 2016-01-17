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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LeafPlanner, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_0.ast._

object indexContainsScanLeafPlanner extends LeafPlanner {
  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicates: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    val lpp = context.logicalPlanProducer

    val resultPlans = predicates.collect {
      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      case predicate@Contains(Property(Variable(name), propertyKey), expr) =>
        val propertyKeyName = propertyKey.name

        val idName = IdName(name)
        for (labelPredicate <- labelPredicates.getOrElse(idName, Set.empty);
             labelName <- labelPredicate.labels;
             indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName);
             labelId <- labelName.id)
          yield {
            val hint = qg.hints.collectFirst {
              case hint@UsingIndexHint(Variable(`name`), `labelName`, PropertyKeyName(`propertyKeyName`)) => hint
            }
            val keyToken = PropertyKeyToken(propertyKey, propertyKey.id.head)
            val labelToken = LabelToken(labelName, labelId)
            val predicates = Seq(predicate, labelPredicate)
            lpp.planNodeIndexContainsScan(idName, labelToken, keyToken, predicates, hint, expr, qg.argumentIds)
          }

    }.flatten

    resultPlans
  }

  private def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext) =
    context.planContext.getIndexRule(label, property) orElse context.planContext.getUniqueIndexRule(label, property)
}
