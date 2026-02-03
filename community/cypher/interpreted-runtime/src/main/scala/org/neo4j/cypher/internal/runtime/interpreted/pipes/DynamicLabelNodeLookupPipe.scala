/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.DynamicElement.All
import org.neo4j.cypher.internal.logical.plans.DynamicElement.Any
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingLongIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.ReferenceCursorIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.IntersectionNodeByLabelsScanPipe.intersectionIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.UnionNodeByLabelsScanPipe.unionIterator
import org.neo4j.cypher.internal.runtime.iterators.LabelFilteringNodeIterator
import org.neo4j.cypher.internal.runtime.iterators.PropertyFilteringNodeIterator
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.util.SeqSupport.RichSeq
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.token.api.TokenConstants.NO_TOKEN
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

import java.util.Comparator

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.math.Ordering.comparatorToOrdering

case class DynamicLabelNodeLookupPipe(
  ident: String,
  labelExpr: Expression,
  operator: DynamicElement.SetOperator,
  propertyExpressions: Map[PropertyKeyToken, Expression]
)(val id: Id = Id.INVALID_ID) extends Pipe {

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val context = state.newRowWithArgument(rowFactory)
    val propertyLookups = DynamicLabelNodeLookupBase.mapPropertyLookups(propertyExpressions, _.apply(context, state))
    DynamicLabelNodeLookupIterator(state, labelExpr.apply(context, state), propertyLookups, operator)
      .toIterator(n => rowFactory.copyWith(context, ident, VirtualValues.node(n)))
  }
}

abstract class DynamicLabelNodeLookupBase[A](state: QueryState) {
  protected def propertyFilter(iterator: A, propertyExprs: Array[PropertyIndexQuery]): A
  protected def labelFilter(iterator: A, labels: Array[Int]): A
  protected def seek(index: IndexDescriptor, properties: Seq[PropertyIndexQuery.ExactPredicate]): A
  protected def allNodes: A
  protected def empty: A
  protected def allLabels(labels: Array[Int]): A
  protected def anyLabel(labels: Array[Int]): A
  protected def label(label: Int): A

  private def orderedSeek(
    index: IndexDescriptor,
    properties: Seq[PropertyIndexQuery.ExactPredicate]
  ): A =
    seek(
      index,
      index.schema().getPropertyIds
        .map(id => properties.find(id == _.propertyKeyId()).get)
    )

  private def getIndexComparator: Comparator[IndexDescriptor] =
    state.indexComparatorFactory.createComparator(state.query.dataRead, state.query.transactionalContext.schemaRead)

  private def findIndicesForLabel(
    labelId: Int,
    predicates: Seq[PropertyIndexQuery]
  ): Iterator[IndexDescriptor] = {
    predicates.orderedSubsets.flatMap { predicateSubset =>
      val indices = state.query.indexReferences(labelId, EntityType.NODE, predicateSubset.map(_.propertyKeyId): _*)
      indices.asScala.filter {
        index => predicateSubset.forall(p => index.getCapability.isQuerySupported(p.`type`(), p.valueCategory()))
      }
    }
  }

  private def findIndicesForLabels(
    labelIds: Seq[Int],
    predicates: Seq[PropertyIndexQuery]
  ): Iterator[IndexDescriptor] = {
    labelIds.iterator.flatMap(findIndicesForLabel(_, predicates))
  }

  private def labelScan(labels: Array[Int], operator: DynamicElement.SetOperator): A = {
    operator match {
      case All if labels.isEmpty   => allNodes
      case Any if labels.isEmpty   => empty
      case _ if labels.length == 1 => label(labels.head)
      case All                     => allLabels(labels)
      case Any                     => anyLabel(labels)
    }
  }

  def getNodes(
    labelExpr: AnyValue,
    operator: DynamicElement.SetOperator,
    propertyConstraints: Array[PropertyIndexQuery.ExactPredicate]
  ): A = {
    val labels =
      CypherFunctions.getDynamicLabels(labelExpr)
        .map(state.query.nodeLabel)

    val iter = (labels, operator) match {
      case (Array(), _) =>
        None
      case _ if propertyConstraints.isEmpty =>
        Some(labelScan(labels, operator))
      case (labels, All) if labels.contains(NO_TOKEN) =>
        None
      case (labels, Any) if labels.forall(_ == NO_TOKEN) =>
        None

      case (labels, All) =>
        val propPredicates = propertyConstraints.toSeq
        findIndicesForLabels(labels, propPredicates).maxOption(comparatorToOrdering(getIndexComparator))
          .map { index =>
            val otherLabels = labels.filterNot(_ == index.schema().getLabelId)
            val (indexProps, otherProps) =
              propPredicates.partition(p => index.schema().getPropertyIds.contains(p.propertyKeyId()))

            (otherLabels.nonEmpty, otherProps.nonEmpty) match {
              case (true, true) =>
                labelFilter(
                  propertyFilter(orderedSeek(index, indexProps), otherProps.toArray),
                  otherLabels
                )
              case (true, false) =>
                labelFilter(orderedSeek(index, indexProps), otherLabels)
              case (false, true) =>
                propertyFilter(orderedSeek(index, indexProps), otherProps.toArray)
              case (false, false) =>
                orderedSeek(index, indexProps)
            }
          }

      /**
       * Conspicuously absent: Index seeks for any multiple labels, because we can't (currently) union the
       * seeks from multiple property indexes; the index can only return properties in value order, not ID
       * order, which makes deduplication of results much more difficult. So we uh, don't do it.
       */
      case (Array(label), Any) =>
        val propPredicates = propertyConstraints.toSeq
        findIndicesForLabel(label, propPredicates).maxOption(comparatorToOrdering(getIndexComparator))
          .map { index =>
            val (indexProps, otherProps) =
              propPredicates.partition(p => index.schema().getPropertyIds.contains(p.propertyKeyId()))

            if (otherProps.nonEmpty) {
              propertyFilter(seek(index, indexProps), otherProps.toArray)
            } else {
              seek(index, indexProps)
            }
          }

      case _ =>
        None
    }

    iter.getOrElse {
      if (propertyConstraints.nonEmpty) {
        propertyFilter(labelScan(labels, operator), propertyConstraints.asInstanceOf[Array[PropertyIndexQuery]])
      } else {
        labelScan(labels, operator)
      }
    }
  }
}

object DynamicLabelNodeLookupBase {

  def mapPropertyLookups(
    propExpressions: Map[PropertyKeyToken, Expression],
    expressionMapper: Expression => AnyValue
  ): Array[PropertyIndexQuery.ExactPredicate] = {
    propExpressions.view
      .map { case (prop, expr) =>
        PropertyIndexQuery.exact(prop.nameId.id, makeValueNeoSafe(expressionMapper(expr)))
      }
      .toArray
  }
}

case class DynamicLabelNodeLookupIterator(
  state: QueryState,
  labelExpression: AnyValue,
  propertyConstraints: Array[PropertyIndexQuery.ExactPredicate],
  operator: DynamicElement.SetOperator
) extends DynamicLabelNodeLookupBase[ClosingLongIterator](state) {

  private lazy val nodeIterator = getNodes(labelExpression, operator, propertyConstraints)

  override protected def propertyFilter(
    iterator: ClosingLongIterator,
    propertyExprs: Array[PropertyIndexQuery]
  ): ClosingLongIterator = {
    new PropertyFilteringNodeIterator(
      iterator,
      state.query.dataRead,
      state.cursors.nodeCursor,
      state.cursors.propertyCursor,
      propertyExprs
    )
  }

  override protected def labelFilter(iterator: ClosingLongIterator, labels: Array[Int]): ClosingLongIterator =
    new LabelFilteringNodeIterator(
      iterator,
      state.query.dataRead,
      state.cursors.nodeCursor,
      labels
    )

  override protected def seek(
    index: IndexDescriptor,
    properties: Seq[PropertyIndexQuery.ExactPredicate]
  ): ReferenceCursorIterator = {
    val cursor = state.query.nodeIndexSeek(
      state.query.dataRead.indexReadSession(index),
      needsValues = false, // we already know the values because we only support ExactPredicate
      IndexOrderNone,
      properties
    )
    new ReferenceCursorIterator(cursor)
  }

  override protected def allNodes: ClosingLongIterator = state.query.nodeReadOps.all

  override protected def allLabels(labels: Array[Int]): ClosingLongIterator =
    intersectionIterator(state.query, labels, IndexOrderNone, state.nodeLabelTokenReadSession.get)

  override protected def anyLabel(labels: Array[Int]): ClosingLongIterator =
    unionIterator(state.query, labels, IndexOrderNone, state.nodeLabelTokenReadSession.get)

  override protected def label(label: Int): ClosingLongIterator =
    state.query.getNodesByLabel(state.nodeLabelTokenReadSession.get, label, IndexOrderNone)

  override protected def empty: ClosingLongIterator = ClosingLongIterator.empty

  def toIterator(rowMapper: Long => CypherRow): ClosingIterator[CypherRow] = nodeIterator.mapToObj(rowMapper)
}
