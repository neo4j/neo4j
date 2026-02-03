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
import org.neo4j.cypher.internal.logical.plans.DynamicElement.SetOperator
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingRelationshipIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DirectedAllRelationshipsScanPipe.allRelationshipsIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DynamicLabelNodeLookupBase.mapPropertyLookups
import org.neo4j.cypher.internal.runtime.iterators.BaseRelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.iterators.PropertyFilteringRelationshipIterator
import org.neo4j.cypher.internal.runtime.iterators.RelationshipIndexCursorIterator
import org.neo4j.cypher.internal.util.SeqSupport.RichSeq
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.cypher.operations.CypherFunctions.GetSingleDynamicTypeResult.ConflictingDynamicTypes
import org.neo4j.cypher.operations.CypherFunctions.GetSingleDynamicTypeResult.EmptyDynamicTypeList
import org.neo4j.cypher.operations.CypherFunctions.GetSingleDynamicTypeResult.SingleDynamicType
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

import java.util.Comparator

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.math.Ordering.comparatorToOrdering

case class DynamicDirectedRelationshipTypeLookupPipe(
  ident: Option[String],
  fromNode: Option[String],
  typeExpr: Expression,
  toNode: Option[String],
  operator: DynamicElement.SetOperator,
  propertyExpressions: Map[PropertyKeyToken, Expression]
)(val id: Id = Id.INVALID_ID) extends Pipe {

  private val relationshipWriter =
    Relationships.compileRelationshipWriter(ident, fromNode, toNode)

  protected def internalCreateResults(state: QueryState): ClosingIterator[CypherRow] = {
    val ctx = state.newRowWithArgument(rowFactory)
    val propertyQueries = mapPropertyLookups(propertyExpressions, _(ctx, state))
    val relIterator =
      new DynamicRelationshipTypeLookupIterator(state).getRows(typeExpr(ctx, state), propertyQueries, operator)
    PrimitiveLongHelper.map(
      relIterator,
      relationshipId => {
        relationshipWriter.writeRow(
          rowFactory,
          ctx,
          VirtualValues.relationship(relationshipId),
          VirtualValues.node(relIterator.startNodeId()),
          VirtualValues.node(relIterator.endNodeId())
        )
      }
    )
  }
}

class DynamicRelationshipTypeLookupIterator(
  state: QueryState
) extends DynamicRelationshipTypeLookupBase[ClosingRelationshipIterator](state) {
  override protected def empty: ClosingRelationshipIterator = BaseRelationshipCursorIterator.EMPTY
  override protected def allRels: ClosingRelationshipIterator = allRelationshipsIterator(state.query)

  override protected def getSingleType(relType: Int): ClosingRelationshipIterator = {
    state.query.getRelationshipsByType(state.relTypeTokenReadSession.get, relType, IndexOrderNone)
  }

  override protected def anyTypes(relTypes: Array[Int]): ClosingRelationshipIterator = {
    DirectedUnionRelationshipTypesScanPipe.unionTypeIterator(
      state,
      relTypes,
      IndexOrderNone,
      state.relTypeTokenReadSession.get,
      callReadFromStore = true
    )
  }

  override def seek(
    index: IndexDescriptor,
    predicates: Array[PropertyIndexQuery.ExactPredicate]
  ): ClosingRelationshipIterator = {
    val cursor = state.query.relationshipIndexSeek(
      state.query.dataRead.indexReadSession(index),
      needsValues = false,
      IndexOrderNone,
      predicates
    )
    new RelationshipIndexCursorIterator(cursor)
  }

  override def propertyFilter(
    scan: ClosingRelationshipIterator,
    predicates: Array[PropertyIndexQuery.ExactPredicate]
  ): ClosingRelationshipIterator = {
    new PropertyFilteringRelationshipIterator(
      scan,
      state.query.dataRead,
      state.cursors.relationshipScanCursor,
      state.cursors.propertyCursor,
      predicates.asInstanceOf[Array[PropertyIndexQuery]]
    )
  }
}

abstract class DynamicRelationshipTypeLookupBase[A](state: QueryState) {
  protected def empty: A
  protected def allRels: A
  protected def getSingleType(relType: Int): A
  protected def anyTypes(relTypes: Array[Int]): A
  protected def seek(descriptor: IndexDescriptor, predicates: Array[PropertyIndexQuery.ExactPredicate]): A
  protected def propertyFilter(scan: A, predicates: Array[PropertyIndexQuery.ExactPredicate]): A

  private def orderedSeek(descriptor: IndexDescriptor, predicates: Array[PropertyIndexQuery.ExactPredicate]): A =
    seek(
      descriptor,
      descriptor.schema().getPropertyIds
        .map(id => predicates.find(id == _.propertyKeyId()).get)
    )

  private def typeScan(relTypes: Option[Array[Option[Int]]], operator: SetOperator): A = {
    (relTypes, operator) match {
      case (None, _) =>
        /* no types == no rows */
        empty
      case (Some(Array()), All) =>
        /* all(empty) is always true */
        allRels
      case (Some(Array()), Any) =>
        /* any(empty) is always false */
        empty
      case (Some(values), _) if values.forall(_.isEmpty) =>
        /* just a bunch of unknowns */
        empty

      case (Some(Array(Some(singleType))), All) =>
        getSingleType(singleType)

      case (Some(relTypes), Any) =>
        relTypes.flatten match {
          case Array() =>
            empty
          case something =>
            anyTypes(something)
        }

      case unknown =>
        assert(assertion = false, s"Failed to handle ${String.valueOf(unknown)} during dynamic type scan")
        empty
    }
  }

  private def maybeIndexSeek(relId: Int, predicates: Array[PropertyIndexQuery.ExactPredicate]): Option[A] = {
    findIndicesForType(relId, predicates).maxOption(comparatorToOrdering(getIndexComparator))
      .map { index =>
        // if we have multiple properties, filter here
        val (indexProps, otherProps) =
          predicates.partition(p => index.schema().getPropertyIds.contains(p.propertyKeyId()))

        if (otherProps.nonEmpty) {
          propertyFilter(orderedSeek(index, indexProps), otherProps)
        } else {
          orderedSeek(index, indexProps)
        }
      }
  }

  private def getIndexComparator: Comparator[IndexDescriptor] =
    state.indexComparatorFactory.createComparator(state.query.dataRead, state.query.transactionalContext.schemaRead)

  private def findIndicesForType(
    typeId: Int,
    predicates: Seq[PropertyIndexQuery.ExactPredicate]
  ): Iterator[IndexDescriptor] = {
    predicates.orderedSubsets.flatMap { predicateSubset =>
      val indices =
        state.query.indexReferences(typeId, EntityType.RELATIONSHIP, predicateSubset.map(_.propertyKeyId): _*)
      indices.asScala.filter {
        index => predicateSubset.forall(p => index.getCapability.isQuerySupported(p.`type`(), p.valueCategory()))
      }
    }
  }

  def getRows(
    typeValue: AnyValue,
    propertyPredicates: Array[PropertyIndexQuery.ExactPredicate],
    operator: SetOperator
  ): A = {
    val relTypes: Option[Array[Option[Int]]] = operator match {
      case Any =>
        Some(CypherFunctions.getDynamicTypes(typeValue)
          .map(state.query.getOptRelTypeId))
      case All =>
        CypherFunctions.getSingleDynamicType(typeValue, state) match {
          case singleType: SingleDynamicType =>
            Some(Array(singleType.value()).map(state.query.getOptRelTypeId))
          case _: EmptyDynamicTypeList =>
            Some(Array())
          case _: ConflictingDynamicTypes =>
            None
        }
    }

    val indexScan = (relTypes) match {
      case Some(Array(Some(singleType))) if propertyPredicates.nonEmpty =>
        maybeIndexSeek(singleType, propertyPredicates)

      /**
       * Conspicuously absent: Index seeks for any multiple types, because we can't (currently) union the
       * seeks from multiple property indexes; the index can only return properties in value order, not ID
       * order, which makes deduplication of results much more difficult. So we uh, don't do it.
       */
      case _ =>
        None
    }

    indexScan.getOrElse {
      if (propertyPredicates.nonEmpty) {
        propertyFilter(typeScan(relTypes, operator), propertyPredicates)
      } else {
        typeScan(relTypes, operator)
      }
    }
  }
}
