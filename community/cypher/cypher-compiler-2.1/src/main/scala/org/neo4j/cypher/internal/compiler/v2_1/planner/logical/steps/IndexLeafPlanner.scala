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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._


abstract class IndexLeafPlanner extends LeafPlanner {
  def apply(qg: QueryGraph)(implicit context: QueryGraphSolvingContext) = {
    implicit val semanticTable = context.semanticTable
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap = qg.selections.labelPredicates

    CandidateList(
      predicates.collect {
        // n.prop = value
        case propertyPredicate@Equals(Property(identifier@Identifier(name), propertyKeyName), ConstantExpression(valueExpr)) =>
          val idName = IdName(name)

          for (labelPredicate <- labelPredicateMap.getOrElse(idName, Set.empty);
               labelName <- labelPredicate.labels;
               indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName.name);
               labelId <- labelName.id)
          yield {
            val entryConstructor =
              constructPlan(idName, LabelToken(labelName, labelId), PropertyKeyToken(propertyKeyName, propertyKeyName.id.head), valueExpr)
            entryConstructor(Seq(propertyPredicate, labelPredicate))
          }
      }.flatten
    )
  }

  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: Expression)(implicit context: QueryGraphSolvingContext): (Seq[Expression]) => QueryPlan



  protected def findIndexesFor(label: String, property: String)(implicit context: QueryGraphSolvingContext): Option[IndexDescriptor]
}

object uniqueIndexSeekLeafPlanner extends IndexLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: Expression)
                             (implicit context: QueryGraphSolvingContext): (Seq[Expression]) => QueryPlan =
    (predicates: Seq[Expression]) =>
      planNodeIndexUniqueSeek(idName, label, propertyKey, valueExpr, predicates)


  protected def findIndexesFor(label: String, property: String)(implicit context: QueryGraphSolvingContext): Option[IndexDescriptor] =
    context.planContext.getUniqueIndexRule(label, property)
}

object indexSeekLeafPlanner extends IndexLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKey: PropertyKeyToken,
                              valueExpr: Expression)
                             (implicit context: QueryGraphSolvingContext): (Seq[Expression]) => QueryPlan =
    (predicates: Seq[Expression]) =>
      planNodeIndexSeek(idName, label, propertyKey, valueExpr, predicates)

  protected def findIndexesFor(label: String, property: String)(implicit context: QueryGraphSolvingContext): Option[IndexDescriptor] =
    context.planContext.getIndexRule(label, property)

}
