/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_2.commands.{ManyQueryExpression, QueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.notification.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.ir.v3_2.{IdName, QueryGraph}
import org.neo4j.cypher.internal.compiler.v3_2.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v3_2.{LabelId, PropertyKeyId, SemanticTable}

abstract class AbstractIndexSeekLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph)(implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    producePlanFor(e: Expression, qg.selections.labelPredicates, qg)
  }

  private def producePlanFor(e: Expression, labelPredicateMap: Map[IdName, Set[HasLabels]], qg: QueryGraph)
                            (implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    val arguments: Set[Variable] = qg.argumentIds.map(n => Variable(n.name)(null))
    indexPlannableExpression(qg.argumentIds, arguments, labelPredicateMap, qg.hints)
      .lift(e).flatMap(e => maybeLeafPlans(e.name, e.producePlans()))
  }

  private def indexPlannableExpression(argumentIds: Set[IdName], arguments: Set[Variable],
                                       labelPredicateMap: Map[IdName, Set[HasLabels]], hints: Set[Hint]):
  PartialFunction[Expression, IndexPlannableExpression] = {
    // n.prop IN [ ... ]
    case predicate@AsPropertySeekable(seekable: PropertySeekable)
      if seekable.args.dependencies.forall(arguments) && !arguments(seekable.ident) =>
      IndexPlannableExpression(seekable.name, seekable.propertyKey, predicate,
        seekable.args.asQueryExpression, labelPredicateMap, hints, argumentIds)

    // ... = n.prop
    // In some rare cases, we can't rewrite these predicates cleanly,
    // and so planning needs to search for these cases explicitly
    case predicate@Equals(a, Property(seekable@Variable(name), propKeyName))
      if a.dependencies.forall(arguments) && !arguments(seekable) =>
      val expr = SingleQueryExpression(a)
      IndexPlannableExpression(seekable.name, propKeyName, predicate,
        expr, labelPredicateMap, hints, argumentIds)

    // n.prop STARTS WITH "prefix%..."
    case predicate@AsStringRangeSeekable(seekable) =>
      IndexPlannableExpression(seekable.name, seekable.propertyKey, PartialPredicate(seekable.expr, predicate),
        seekable.asQueryExpression, labelPredicateMap, hints, argumentIds)

    // n.prop <|<=|>|>= value
    case predicate@AsValueRangeSeekable(seekable) =>
      IndexPlannableExpression(seekable.name, seekable.propertyKeyName, predicate,
        seekable.asQueryExpression, labelPredicateMap, hints, argumentIds)
  }

  def producePlanFor(expressions: Seq[Expression], labelPredicateMap: Map[IdName, Set[HasLabels]], qg: QueryGraph)
                    (implicit context: LogicalPlanningContext): Seq[LeafPlansForVariable] = {
    val arguments: Set[Variable] = qg.argumentIds.map(n => Variable(n.name)(null))
    val plannables: Seq[IndexPlannableExpression] = expressions.collect(
      indexPlannableExpression(qg.argumentIds, arguments, labelPredicateMap, qg.hints))
    plannables.map(_.name).distinct.flatMap { name =>
      val idName = IdName(name)
      val labelPredicates = labelPredicateMap.getOrElse(idName, Set.empty);
      val nodePlannables = plannables.filter(p => p.name == name)
      maybeLeafPlans(name, producePlansFor(idName, nodePlannables, labelPredicates, qg.hints, qg.argumentIds))
    }
  }

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val labelPredicateMap: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    if (labelPredicateMap.isEmpty)
      Seq.empty
    else {
      val predicates: Seq[Expression] = qg.selections.flatPredicates
      val compositePlans: Seq[LogicalPlan] = producePlanFor(predicates, labelPredicateMap, qg).flatMap(p => p.plans)

      if (compositePlans.isEmpty) {
        DynamicPropertyNotifier.process(findNonSeekableIdentifiers(qg.selections.flatPredicates), IndexLookupUnfulfillableNotification, qg)
      }

      (compositePlans).distinct
    }
  }

  protected def findNonSeekableIdentifiers(predicates: Seq[Expression])(implicit context: LogicalPlanningContext) =
    predicates.flatMap {
      // n['some' + n.prop] IN [ ... ]
      case predicate@AsDynamicPropertyNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] STARTS WITH "prefix%..."
      case predicate@AsStringRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      // n['some' + n.prop] <|<=|>|>= value
      case predicate@AsValueRangeNonSeekable(nonSeekableId)
        if context.semanticTable.isNode(nonSeekableId) => Some(nonSeekableId)

      case _ => None
    }.toSet

  protected def collectPlans(predicates: Seq[Expression], argumentIds: Set[IdName],
                             labelPredicateMap: Map[IdName, Set[HasLabels]],
                             hints: Set[Hint])
                            (implicit context: LogicalPlanningContext): Seq[(String, Set[LogicalPlan])] = {
    val arguments: Set[Variable] = argumentIds.map(n => Variable(n.name)(null))
    val results = predicates.collect(indexPlannableExpression(argumentIds, arguments, labelPredicateMap, hints))
    results.map(e => (e.name, e.producePlans()))
  }

  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanningContext): Iterator[IndexDescriptor]

  protected def findIndexesFor(label: String, properties: Seq[String])(implicit context: LogicalPlanningContext): Option[IndexDescriptor]

  protected def producePlansFor(idName: IdName, nodePlannables: Seq[IndexPlannableExpression],
                                labelPredicates: Set[HasLabels],
                                hints: Set[Hint], argumentIds: Set[IdName])
                               (implicit context: LogicalPlanningContext): Set[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    for (labelPredicate <- labelPredicates;
         labelName <- labelPredicate.labels;
         labelId: LabelId <- labelName.id.toSeq;
         indexDescriptor <- findIndexesForLabel(labelId);
         plannables <- plannablesForIndex(indexDescriptor, nodePlannables))
      yield {
        val propertyKeyTokens = plannables.map(p => p.propertyKeyName).map(n => PropertyKeyToken(n, n.id.head))
        // TODO: support for composite hints
        val hint = if (plannables.length == 1) {
          val name = idName.name
          val propertyName = plannables.head.propertyKeyName.name
          hints.collectFirst {
            case hint@UsingIndexHint(Variable(`name`), `labelName`, PropertyKeyName(`propertyName`)) => hint
          }
        } else None
        // TODO: Decide on the best composite expression, currently mimicking a literal array of values
        val queryExpression = if (plannables.length == 1)
          plannables.head.queryExpression
        else {
          val pos = plannables.head.queryExpression.expression.position
          ManyQueryExpression(ListLiteral(plannables.map(_.queryExpression.expression))(pos))
        }
        val entryConstructor: (Seq[Expression]) => LogicalPlan =
          constructPlan(idName, LabelToken(labelName, labelId), propertyKeyTokens,
            queryExpression, hint, argumentIds)
        entryConstructor(plannables.map(p => p.propertyPredicate) :+ labelPredicate)
      }
  }

  private def plannablesForIndex(indexDescriptor: IndexDescriptor, plannables: Seq[IndexPlannableExpression])
                                (implicit semanticTable: SemanticTable): Option[Seq[IndexPlannableExpression]] = {
    // TODO: Make sure plannable and expression order matches IndexDescriptor propery order
    val foundPredicates = indexDescriptor.properties.flatMap { propertyKeyId =>
      plannables find (p => propertyKeyId == p.propertyKeyName.id.getOrElse(-1))
    }
    // Currently we only support using the composite index if ALL properties are specified, but this could be generalized
    if (foundPredicates.length == indexDescriptor.properties.length)
      Some(foundPredicates)
    else
      None
  }

  case class IndexPlannableExpression(name: String, propertyKeyName: PropertyKeyName,
                                      propertyPredicate: Expression, queryExpression: QueryExpression[Expression],
                                      labelPredicateMap: Map[IdName, Set[HasLabels]],
                                      hints: Set[Hint], argumentIds: Set[IdName]) {
    def producePlans()(implicit context: LogicalPlanningContext): Set[LogicalPlan] = {
      implicit val semanticTable = context.semanticTable
      val idName = IdName(name)
      for (labelPredicate <- labelPredicateMap.getOrElse(idName, Set.empty);
           labelName <- labelPredicate.labels;
           indexDescriptor <- findIndexesFor(labelName.name, Seq(propertyKeyName.name));
           labelId <- labelName.id)
        yield {
          val propertyName = propertyKeyName.name
          val hint = hints.collectFirst {
            case hint@UsingIndexHint(Variable(`name`), `labelName`, PropertyKeyName(`propertyName`)) => hint
          }
          val entryConstructor: (Seq[Expression]) => LogicalPlan =
            constructPlan(idName, LabelToken(labelName, labelId), Seq(PropertyKeyToken(propertyKeyName, propertyKeyName.id.head)),
              queryExpression, hint, argumentIds)
          entryConstructor(Seq(propertyPredicate, labelPredicate))
        }
    }
  }

}

object uniqueIndexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan =
    (predicates: Seq[Expression]) =>
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName, label, propertyKeys, valueExpr, predicates, hint, argumentIds)

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.uniqueIndexesGetForLabel(labelId)

  protected def findIndexesFor(label: String, properties: Seq[String])(implicit context: LogicalPlanningContext): Option[IndexDescriptor] =
    context.planContext.getUniqueIndexRule(label, properties)
}

object indexSeekLeafPlanner extends AbstractIndexSeekLeafPlanner {
  protected def constructPlan(idName: IdName,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              hint: Option[UsingIndexHint],
                              argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): (Seq[Expression]) => LogicalPlan =
    (predicates: Seq[Expression]) =>
      context.logicalPlanProducer.planNodeIndexSeek(idName, label, propertyKeys, valueExpr, predicates, hint, argumentIds)

  protected def findIndexesForLabel(labelId: Int)(implicit context: LogicalPlanningContext): Iterator[IndexDescriptor] =
    context.planContext.indexesGetForLabel(labelId)

  protected def findIndexesFor(label: String, properties: Seq[String])(implicit context: LogicalPlanningContext): Option[IndexDescriptor] = {
    if (uniqueIndex(label, properties).isDefined) None else anyIndex(label, properties)
  }

  private def anyIndex(label: String, properties: Seq[String])(implicit context: LogicalPlanningContext) =
    context.planContext.getIndexRule(label, properties)

  private def uniqueIndex(label: String, properties: Seq[String])(implicit context: LogicalPlanningContext) =
    context.planContext.getUniqueIndexRule(label, properties)
}

