/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe.EntityTransformer
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe.evaluateBatchSize
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe.inNewTransaction
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.impl.core.TransactionalEntityFactory
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue
import org.neo4j.kernel.impl.util.PathWrappingPathValue
import org.neo4j.kernel.impl.util.RelationshipEntityWrappingValue
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.control.NonFatal

case class TransactionForeachPipe(source: Pipe,
                                  inner: Pipe,
                                  batchSize: Expression
                                 )(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  override protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    val batchSizeLong = evaluateBatchSize(batchSize, state)

    input.grouped(batchSizeLong).flatMap { batch =>
      inNewTransaction(state) { stateWithNewTransaction =>

        batch.foreach { outerRow =>
          // Row based caching relies on the transaction state to avoid stale reads (see AbstractCachedProperty.apply).
          // Since we do not share the transaction state we must clear the cached properties.
          outerRow.invalidateCachedProperties()

          val entityTransformer = new EntityTransformer(stateWithNewTransaction.query.entityAccessor)
          val reboundRow = entityTransformer.copyWithEntityWrappingValuesRebound(outerRow)
          val innerState = stateWithNewTransaction.withInitialContext(reboundRow)
          val ignoredResult = inner.createResults(innerState)
          while (ignoredResult.hasNext) {
            ignoredResult.next()
          }

          outerRow
        }

        val subqueryStatistics = stateWithNewTransaction.getStatistics
        state.query.addStatistics(subqueryStatistics)
      }
      batch
    }
  }
}

object TransactionForeachPipe {
  /**
   * Recursively finds entity wrappers and rebinds the entities to the current transaction
   */
  // TODO: Remove rebinding here, and transform wrappers to Reference:s
  // Currently, replacing e.g. NodeEntityWrappingNodeValue with NodeReference causes failures downstream.
  // We can for example end up in PathValueBuilder, which assumes that we have NodeValue and not NodeReference.
  class EntityTransformer(entityFactory: TransactionalEntityFactory) {

    def copyWithEntityWrappingValuesRebound(row: CypherRow): CypherRow =
      row.copyMapped(rebindEntityWrappingValues)

    private def rebindEntityWrappingValues(value: AnyValue): AnyValue = value match {

      case n: NodeEntityWrappingNodeValue =>
        rebindNode(n.getEntity)

      case r: RelationshipEntityWrappingValue =>
      rebindRelationship(r.getEntity)

      case p: PathWrappingPathValue =>
        val nodeValues = p.path().nodes().asScala.map(rebindNode).toArray
        val relValues = p.path().relationships().asScala.map(rebindRelationship).toArray
        VirtualValues.path(nodeValues, relValues)

      case m: MapValue =>
        val builder = new MapValueBuilder(m.size())
        m.foreach((k, v) => builder.add(k, rebindEntityWrappingValues(v)))
        builder.build()

      case l: ListValue =>
        val builder = ListValueBuilder.newListBuilder(l.size())
        l.forEach(v => builder.add(rebindEntityWrappingValues(v)))
        builder.build()

      case other =>
        other
    }

    private def rebindNode(node: Node): NodeValue =
      ValueUtils.fromNodeEntity(entityFactory.newNodeEntity(node.getId))

    private def rebindRelationship(relationship:Relationship): RelationshipValue =
      ValueUtils.fromRelationshipEntityLazyLoad(entityFactory.newRelationshipEntity(relationship.getId))
  }



  private def inNewTransaction(state: QueryState)(f: QueryState => Unit): Unit = {

    // Ensure that no write happens before a 'CALL { ... } IN TRANSACTIONS'
    if (state.query.transactionalContext.dataRead.transactionStateHasChanges) throw new InternalException("Expected transaction state to be empty when calling transactional subquery.")

    // beginTx()
    val stateWithNewTransaction = state.withNewTransaction()
    val innerTxContext = stateWithNewTransaction.query.transactionalContext

    try {
      f(stateWithNewTransaction)

      // commitTx()
      innerTxContext.close()
      innerTxContext.commitTransaction()
    } catch {
      case NonFatal(e) =>
        try {
          innerTxContext.rollback()
        } catch {
          case NonFatal(rollbackException) =>
            e.addSuppressed(rollbackException)
        }
        throw e
    } finally {
      innerTxContext.close()
      stateWithNewTransaction.close()
    }
  }

  def evaluateBatchSize(batchSize: Expression, state: QueryState): Long = {
    PipeHelper.evaluateStaticLongOrThrow(batchSize, _ > 0, state, "OF ... ROWS", " Must be a positive integer.")
  }
}
