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
package org.neo4j.cypher

import java.util.concurrent.TimeUnit

import org.neo4j.cypher.ExecutionEngineHelper.asJavaMapDeep
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.graphdb.schema.IndexDefinition
import org.neo4j.graphdb.schema.IndexSetting
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.kernel.impl.util.ValueUtils

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

trait GraphIcing {

  implicit class RichNode(n: Node) {
    def labels: List[String] = n.getLabels.asScala.map(_.name()).toList

    def addLabels(input: String*) {
      input.foreach(l => n.addLabel(label(l)))
    }
  }

  implicit class RichGraphDatabaseQueryService(graphService: GraphDatabaseQueryService) {

    private val graph: GraphDatabaseFacade = graphService.asInstanceOf[GraphDatabaseCypherService].getGraphDatabaseService

    def createUniqueConstraint(label: String, property: String): ConstraintDefinition = {
      withTx( tx =>  {
        tx.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(property).create()
      } )
    }

    def createUniqueConstraintWithName(name: String, label: String, property: String): ConstraintDefinition = {
      withTx( tx =>  {
        tx.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(property).withName(name).create()
      } )
    }

    def createNodeExistenceConstraint(label: String, property: String): Result = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.$property) IS NOT NULL")
      })
    }

    def createNodeExistenceConstraintWithName(name: String, label: String, property: String): Result = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT `$name` ON (n:$label) ASSERT (n.$property) IS NOT NULL")
      })
    }

    def createRelationshipExistenceConstraint(relType: String, property: String, direction: Direction = Direction.BOTH): Result = {
      val relSyntax = direction match {
        case Direction.OUTGOING => s"()-[r:$relType]->()"
        case Direction.INCOMING => s"()<-[r:$relType]-()"
        case _ => s"()-[r:$relType]-()"
      }
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT ON $relSyntax ASSERT (r.$property) IS NOT NULL")
      })
    }

    def createRelationshipExistenceConstraintWithName(name: String, relType: String, property: String, direction: Direction = Direction.BOTH): Result = {
      val relSyntax = direction match {
        case Direction.OUTGOING => s"()-[r:$relType]->()"
        case Direction.INCOMING => s"()<-[r:$relType]-()"
        case _ => s"()-[r:$relType]-()"
      }
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT `$name` ON $relSyntax ASSERT (r.$property) IS NOT NULL")
      })
    }

    def createNodeKeyConstraint(label: String, properties: String*): Result = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT ON (n:$label) ASSERT (n.${properties.mkString(", n.")}) IS NODE KEY")
      })
    }

    def createNodeKeyConstraintWithName(name: String, label: String, properties: String*): Result = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT `$name` ON (n:$label) ASSERT (n.${properties.mkString(", n.")}) IS NODE KEY")
      })
    }

    def createNodeIndex(label: String, properties: String*): IndexDefinition = {
      createNodeIndex(None, label, properties)
    }

    def createRelationshipIndex(relType: String, properties: String*): IndexDefinition = {
      createRelIndex(None, relType, properties)
    }

    def createNodeIndexWithName(name: String, label: String, properties: String*): IndexDefinition = {
      createNodeIndex(Some(name), label, properties)
    }

    def createRelationshipIndexWithName(name: String, relType: String, properties: String*): IndexDefinition = {
      createRelIndex(Some(name), relType, properties)
    }

    private def createNodeIndex(maybeName: Option[String], label: String, properties: Seq[String]): IndexDefinition = {
      createIndex(maybeName, s"(e:$label)", properties, () => getNodeIndex(label, properties))
    }

    private def createRelIndex(maybeName: Option[String], relType: String, properties: Seq[String]): IndexDefinition = {
      createIndex(maybeName, s"()-[e:$relType]-()", properties, () => getRelIndex(relType, properties))
    }

    private def createIndex(maybeName: Option[String], pattern: String, properties: Seq[String], getIndex: () => IndexDefinition): IndexDefinition = {
      val nameString = maybeName.map(n => s" `$n`").getOrElse("")
      withTx( tx => {
        tx.execute(s"CREATE INDEX$nameString FOR $pattern ON (${properties.map(p => s"e.`$p`").mkString(",")})")
      })

      withTx( tx =>  {
        tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
      } )

      getIndex()
    }

    def awaitIndexesOnline(): Unit = {
      withTx( tx =>  {
        tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
      } )
    }

    def getNodeIndex(label: String, properties: Seq[String]): IndexDefinition = {
      withTx( tx => {
        tx.schema().getIndexes(Label.label(label)).asScala.find(index => index.getPropertyKeys.asScala.toList == properties.toList).get
      } )
    }

    def getRelIndex(relType: String, properties: Seq[String]): IndexDefinition = {
      withTx( tx => {
        tx.schema().getIndexes(RelationshipType.withName(relType)).asScala.find(index => index.getPropertyKeys.asScala.toList == properties.toList).get
      } )
    }

    def getMaybeNodeIndex(label: String, properties: Seq[String]): Option[IndexDefinition] = {
      withTx( tx =>  {
        tx.schema().getIndexes(Label.label(label)).asScala.find(index => index.getPropertyKeys.asScala.toList == properties.toList)
      } )
    }

    def getMaybeRelIndex(relType: String, properties: Seq[String]): Option[IndexDefinition] = {
      withTx( tx =>  {
        tx.schema().getIndexes(RelationshipType.withName(relType)).asScala.find(index => index.getPropertyKeys.asScala.toList == properties.toList)
      } )
    }

    def getIndexSchemaByName(name: String): (String, Seq[String]) = {
      withTx( tx =>  {
        val index = tx.schema().getIndexByName(name)
        val labelOrRelType = if (index.isNodeIndex) Iterables.single(index.getLabels).name() else Iterables.single(index.getRelationshipTypes).name()
        val properties = index.getPropertyKeys.asScala.toList
        (labelOrRelType, properties)
      } )
    }

    def getIndexConfig(name: String): Map[IndexSetting, AnyRef] = {
      withTx( tx =>  {
        val index = tx.schema().getIndexByName(name)
        index.getIndexConfiguration.asScala.toMap
      } )
    }

    def getIndexProvider(name: String): IndexProviderDescriptor = {
      withTx( tx =>  {
        val index: IndexDefinitionImpl = tx.schema().getIndexByName(name).asInstanceOf[IndexDefinitionImpl] // only implementation of interface
        index.getIndexReference.getIndexProvider
      } )
    }

    def getNodeConstraint(label: String, properties: Seq[String]): ConstraintDefinition = {
      withTx( tx =>  {
        tx.schema().getConstraints(Label.label(label)).asScala.find(constraint => constraint.getPropertyKeys.asScala.toList == properties.toList).get
      } )
    }

    def getMaybeNodeConstraint(label: String, properties: Seq[String]): Option[ConstraintDefinition] = {
      withTx( tx =>  {
        tx.schema().getConstraints(Label.label(label)).asScala.find(constraint => constraint.getPropertyKeys.asScala.toList == properties.toList)
      } )
    }

    def getRelationshipConstraint(relType: String, property: String): ConstraintDefinition = {
      withTx( tx =>  {
        tx.schema().getConstraints(RelationshipType.withName(relType)).asScala.find(constraint => constraint.getPropertyKeys.asScala.toList == List(property)).get
      } )
    }

    def getMaybeRelationshipConstraint(relType: String, property: String): Option[ConstraintDefinition] = {
      withTx( tx =>  {
        tx.schema().getConstraints(RelationshipType.withName(relType)).asScala.find(constraint => constraint.getPropertyKeys.asScala.toList == List(property))
      } )
    }

    def getConstraintSchemaByName(name: String): (String, Seq[String])  = withTx( tx =>  {
      val constraint = tx.schema().getConstraintByName(name)
      val properties = constraint.getPropertyKeys.asScala.toList
      val labelOrRelType = constraint.getConstraintType match {
        case ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE => constraint.getRelationshipType.name()
        case _ => constraint.getLabel.name()
      }
      (labelOrRelType, properties)
    } )

    def createUniqueIndex(label: String, property: String): Unit = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT ON (p:$label) ASSERT p.$property IS UNIQUE")
      } )

      withTx( tx => {
        tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
      } )
    }

    // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
    def inTx[T](f: InternalTransaction => T, txType: Type = Type.IMPLICIT): T = withTx(f, txType)

    def inTx[T](f: => T): T = inTx(_ => f, Type.IMPLICIT)

    private def createTransactionalContext(tx: InternalTransaction, queryText: String, params: Map[String, Any]): TransactionalContext = {
      val contextFactory = Neo4jTransactionalContextFactory.create(graphService)
      contextFactory.newContext(tx, queryText, ValueUtils.asParameterMapValue(asJavaMapDeep(params)))
    }

    def transactionalContext(tx: InternalTransaction, query: (String, Map[String, Any])): TransactionalContext = {
      val (queryText, params) = query
      createTransactionalContext(tx, queryText, params)
    }

    // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
    def withTx[T](f: InternalTransaction => T, txType: Type = Type.IMPLICIT): T = {
      val tx = graph.beginTransaction(txType, AUTH_DISABLED)
      try {
        val result = f(tx)
        if (tx.isOpen) {
          tx.commit()
        }
        result
      } finally {
        tx.close()
      }

    }

    def rollback[T](f: InternalTransaction => T): T = {
      val tx = graph.beginTransaction(Type.IMPLICIT, AUTH_DISABLED)
      try {
        val result = f(tx)
        tx.rollback()
        result
      } finally {
        tx.close()
      }
    }

    def txCounts = TxCounts(txMonitor.getNumberOfCommittedTransactions, txMonitor.getNumberOfRolledBackTransactions, txMonitor.getNumberOfActiveTransactions)

    private def txMonitor: DatabaseTransactionStats = graph.getDependencyResolver.resolveDependency(classOf[DatabaseTransactionStats])
  }
}

final case class TxCounts(commits: Long = 0, rollbacks: Long = 0, active: Long = 0) {
  def +(other: TxCounts) = TxCounts(commits + other.commits, rollbacks + other.rollbacks, active + other.active)
  def -(other: TxCounts) = TxCounts(commits - other.commits, rollbacks - other.rollbacks, active - other.active)
}
