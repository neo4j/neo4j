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

import org.neo4j.cypher.ExecutionEngineHelper.asJavaMapDeep
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.ConstraintDefinition
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.graphdb.schema.IndexDefinition
import org.neo4j.graphdb.schema.IndexSetting
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats
import org.neo4j.kernel.impl.util.ValueUtils

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

trait GraphIcing {

  implicit class RichNode(n: Node) {
    def labels: List[String] = n.getLabels.asScala.map(_.name()).toList

    def addLabels(input: String*): Unit = {
      input.foreach(l => n.addLabel(label(l)))
    }
  }

  implicit class RichGraphDatabaseQueryService(graphService: GraphDatabaseQueryService) {

    // Create uniqueness constraint

    def createUniqueConstraint(label: String, property: String): ConstraintDefinition = {
      val constraint = withTx( tx =>  {
        tx.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(property).create()
      } )
      awaitIndexesOnline()
      constraint
    }

    def createUniqueConstraint(label: String, properties: String*): ConstraintDefinition = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT FOR (n:$label) REQUIRE (n.${properties.mkString(", n.")}) IS UNIQUE")
      })
      awaitIndexesOnline()
      getNodeConstraint(label, properties)
    }

    def createUniqueConstraintWithName(name: String, label: String, property: String): ConstraintDefinition = {
      val constraint = withTx( tx =>  {
        tx.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(property).withName(name).create()
      } )
      awaitIndexesOnline()
      constraint
    }

    def createUniqueConstraintWithName(name: String, label: String, properties: String*): ConstraintDefinition = {
      withTx( tx =>  {
        tx.execute(s"CREATE CONSTRAINT `$name` FOR (n:$label) REQUIRE (n.${properties.mkString(", n.")}) IS UNIQUE")
      } )
      awaitIndexesOnline()
      getNodeConstraint(label, properties)
    }

    // Create node existence constraint

    def createNodeExistenceConstraint(label: String, property: String): ConstraintDefinition = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT FOR (n:$label) REQUIRE (n.$property) IS NOT NULL")
      })
      getNodeConstraint(label, Seq(property))
    }

    def createNodeExistenceConstraintWithName(name: String, label: String, property: String): ConstraintDefinition = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT `$name` FOR (n:$label) REQUIRE (n.$property) IS NOT NULL")
      })
      getNodeConstraint(label, Seq(property))
    }

    // Create relationship existence constraint

    def createRelationshipExistenceConstraint(relType: String, property: String, direction: Direction = Direction.BOTH): ConstraintDefinition = {
      val relSyntax = direction match {
        case Direction.OUTGOING => s"()-[r:$relType]->()"
        case Direction.INCOMING => s"()<-[r:$relType]-()"
        case _ => s"()-[r:$relType]-()"
      }
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT FOR $relSyntax REQUIRE (r.$property) IS NOT NULL")
      })
      getRelationshipConstraint(relType, property)
    }

    def createRelationshipExistenceConstraintWithName(name: String, relType: String, property: String, direction: Direction = Direction.BOTH): ConstraintDefinition = {
      val relSyntax = direction match {
        case Direction.OUTGOING => s"()-[r:$relType]->()"
        case Direction.INCOMING => s"()<-[r:$relType]-()"
        case _ => s"()-[r:$relType]-()"
      }
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT `$name` FOR $relSyntax REQUIRE (r.$property) IS NOT NULL")
      })
      getRelationshipConstraint(relType, property)
    }

    // Create node key constraint

    def createNodeKeyConstraint(label: String, properties: String*): ConstraintDefinition = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT FOR (n:$label) REQUIRE (n.${properties.mkString(", n.")}) IS NODE KEY")
      })
      awaitIndexesOnline()
      getNodeConstraint(label, properties)
    }

    def createNodeKeyConstraintWithName(name: String, label: String, properties: String*): ConstraintDefinition = {
      withTx( tx => {
        tx.execute(s"CREATE CONSTRAINT `$name` FOR (n:$label) REQUIRE (n.${properties.mkString(", n.")}) IS NODE KEY")
      })
      awaitIndexesOnline()
      getNodeConstraint(label, properties)
    }

    // Create index with given type

    def createNodeIndex(indexType: IndexType, label: String, properties: String*): IndexDefinition = {
      createNodeIndex(None, label, properties, indexType)
    }

    def createNodeIndexWithName(indexType: IndexType, name: String, label: String, properties: String*): IndexDefinition = {
      createNodeIndex(Some(name), label, properties, indexType)
    }

    def createRelationshipIndex(indexType: IndexType, relType: String, properties: String*): IndexDefinition = {
      createRelationshipIndex(None, relType, properties, indexType)
    }

    def createRelationshipIndexWithName(indexType: IndexType, name: String, relType: String, properties: String*): IndexDefinition = {
      createRelationshipIndex(Some(name), relType, properties, indexType)
    }

    // Create default (range) index

    def createNodeIndex(label: String, properties: String*): IndexDefinition = {
      createNodeIndex(None, label, properties)
    }

    def createNodeIndexWithName(name: String, label: String, properties: String*): IndexDefinition = {
      createNodeIndex(Some(name), label, properties)
    }

    def createRelationshipIndex(relType: String, properties: String*): IndexDefinition = {
      createRelationshipIndex(None, relType, properties)
    }

    def createRelationshipIndexWithName(name: String, relType: String, properties: String*): IndexDefinition = {
      createRelationshipIndex(Some(name), relType, properties)
    }

    // Create text index

    def createTextNodeIndex(label: String, property: String): IndexDefinition = {
      createNodeIndex(None, label, Seq(property), IndexType.TEXT)
    }

    def createTextNodeIndexWithName(name: String, label: String, property: String): IndexDefinition = {
      createNodeIndex(Some(name), label, Seq(property), IndexType.TEXT)
    }

    def createTextRelationshipIndex(relType: String, property: String): IndexDefinition = {
      createRelationshipIndex(None, relType, Seq(property), IndexType.TEXT)
    }

    def createTextRelationshipIndexWithName(name: String, relType: String, property: String): IndexDefinition = {
      createRelationshipIndex(Some(name), relType, Seq(property), IndexType.TEXT)
    }

    // Create point index

    def createPointNodeIndex(label: String, property: String): IndexDefinition = {
      createNodeIndex(None, label, Seq(property), IndexType.POINT)
    }

    def createPointNodeIndexWithName(name: String, label: String, property: String): IndexDefinition = {
      createNodeIndex(Some(name), label, Seq(property), IndexType.POINT)
    }

    def createPointRelationshipIndex(relType: String, property: String): IndexDefinition = {
      createRelationshipIndex(None, relType, Seq(property), IndexType.POINT)
    }

    def createPointRelationshipIndexWithName(name: String, relType: String, property: String): IndexDefinition = {
      createRelationshipIndex(Some(name), relType, Seq(property), IndexType.POINT)
    }

    // Create label/prop index help methods

    private def createNodeIndex(maybeName: Option[String], label: String, properties: Seq[String], indexType: IndexType = IndexType.RANGE, options: Map[String, String] = Map.empty): IndexDefinition = {
      createIndex(maybeName, s"(e:$label)", properties, indexType, () => getNodeIndex(label, properties, indexType), options)
    }

    private def createRelationshipIndex(maybeName: Option[String], relType: String, properties: Seq[String], indexType: IndexType = IndexType.RANGE, options: Map[String, String] = Map.empty): IndexDefinition = {
      createIndex(maybeName, s"()-[e:$relType]-()", properties, indexType, () => getRelIndex(relType, properties, indexType), options)
    }

    private def createIndex(maybeName: Option[String], pattern: String, properties: Seq[String], indexType: IndexType, getIndex: () => IndexDefinition, options: Map[String, String]): IndexDefinition = {
      val nameString = maybeName.map(n => s" `$n`").getOrElse("")
      withTx( tx => {
        tx.execute(s"CREATE $indexType INDEX$nameString FOR $pattern ON (${properties.map(p => s"e.`$p`").mkString(",")})${optionsString(options)}")
      })
      awaitIndexesOnline()
      getIndex()
    }

    private def optionsString(options: Map[String, String]): String = {
      if (options.nonEmpty) {
        val keyValueString = options
          .map { case (key, value) => s"$key: $value" }
          .mkString("{ ", ", ", "}")
        " OPTIONS " + keyValueString
      } else {
        ""
      }
    }

    // Create fulltext index

    def createNodeFulltextIndex(labels:List[String], properties: List[String], maybeName: Option[String] = None): IndexDefinition = {
      val pattern = s"(e:${labels.map(l => s"`$l`").mkString("|")})"
      createFulltextIndex(pattern, properties, () => getFulltextIndex(labels, properties, isNodeIndex = true), maybeName)
    }

    def createRelationshipFulltextIndex(relTypes: List[String], properties: List[String], maybeName: Option[String] = None): IndexDefinition = {
      val pattern = s"()-[e:${relTypes.map(r => s"`$r`").mkString("|")}]-()"
      createFulltextIndex(pattern, properties, () => getFulltextIndex(relTypes, properties, isNodeIndex = false), maybeName)
    }

    private def createFulltextIndex(pattern: String, properties: Seq[String], getIndex: () => IndexDefinition, maybeName: Option[String]): IndexDefinition = {
      val nameString = maybeName.map(n => s" `$n`").getOrElse("")
      withTx( tx => {
        tx.execute(s"CREATE FULLTEXT INDEX$nameString FOR $pattern ON EACH [${properties.map(p => s"e.`$p`").mkString(",")}]")
      })
      awaitIndexesOnline()
      getIndex()
    }

    // Create and drop lookup index

    def createLookupIndex(isNodeIndex: Boolean, maybeName: Option[String] = None): IndexDefinition = {
      val nameString = maybeName.map(n => s" `$n`").getOrElse("")
      val (pattern, function) = if (isNodeIndex) ("(n)", "labels(n)") else ("()-[r]-()", "type(r)")
      withTx( tx => {
        tx.execute(s"CREATE LOOKUP INDEX$nameString FOR $pattern ON EACH $function")
      })
      awaitIndexesOnline()
      getLookupIndex(isNodeIndex)
    }

    def dropLookupIndex(isNodeIndex: Boolean): Unit = withTx(tx => {
      tx.schema().getIndexes().asScala.find(id => id.getIndexType.equals(IndexType.LOOKUP) && id.isNodeIndex == isNodeIndex).foreach(idx => idx.drop())
    })

    // Wait for indexes

    def awaitIndexesOnline(): Unit = {
      withTx( tx =>  {
        tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
      } )
    }

    // Find index

    def getNodeIndex(label: String, properties: Seq[String], indexType: IndexType = IndexType.RANGE): IndexDefinition =
      getMaybeNodeIndex(label, properties, indexType).get

    def getRelIndex(relType: String, properties: Seq[String], indexType: IndexType = IndexType.RANGE): IndexDefinition =
      getMaybeRelIndex(relType, properties, indexType).get

    def getLookupIndex(isNodeIndex: Boolean): IndexDefinition =
      getMaybeLookupIndex(isNodeIndex).get

    def getFulltextIndex(entities: List[String], props: List[String], isNodeIndex: Boolean): IndexDefinition =
      getMaybeFulltextIndex(entities, props, isNodeIndex).get

    def getMaybeNodeIndex(label: String, properties: Seq[String], indexType: IndexType = IndexType.RANGE): Option[IndexDefinition] = withTx(tx =>  {
      tx.schema().getIndexes(Label.label(label)).asScala
        .find(index => index.getIndexType.equals(indexType) && index.getPropertyKeys.asScala.toList == properties.toList)
    } )

    def getMaybeRelIndex(relType: String, properties: Seq[String], indexType: IndexType = IndexType.RANGE): Option[IndexDefinition] = withTx(tx =>  {
      tx.schema().getIndexes(RelationshipType.withName(relType)).asScala
        .find(index => index.getIndexType.equals(indexType) && index.getPropertyKeys.asScala.toList == properties.toList)
    } )

    def getMaybeLookupIndex(isNodeIndex: Boolean): Option[IndexDefinition] = withTx(tx => {
      tx.schema().getIndexes().asScala.find(id => id.getIndexType.equals(IndexType.LOOKUP) && id.isNodeIndex == isNodeIndex)
    })

    def getMaybeFulltextIndex(entities: List[String], props: List[String], isNodeIndex: Boolean): Option[IndexDefinition] = withTx(tx => {
      tx.schema().getIndexes().asScala.find(id =>
        id.getIndexType.equals(IndexType.FULLTEXT) &&
        id.isNodeIndex == isNodeIndex &&
        getEntities(id).equals(entities) &&
        id.getPropertyKeys.asScala.toList.equals(props)
      )
    })

    def getIndexSchemaByName(name: String): (String, Seq[String]) = withTx(tx =>  {
      val index = tx.schema().getIndexByName(name)
      val labelOrRelType = if (index.isNodeIndex) Iterables.single(index.getLabels).name() else Iterables.single(index.getRelationshipTypes).name()
      val properties = index.getPropertyKeys.asScala.toList
      (labelOrRelType, properties)
    } )

    def getLookupIndexByName(name: String): (IndexType, Boolean) = withTx(tx =>  {
      val index = tx.schema().getIndexByName(name)
      (index.getIndexType, index.isNodeIndex)
    } )

    def getFulltextIndexSchemaByName(name: String): (List[String], List[String], Boolean) = withTx(tx =>  {
      val index = tx.schema().getIndexByName(name)
      val labelOrRelTypes = getEntities(index)
      val properties = index.getPropertyKeys.asScala.toList
      (labelOrRelTypes, properties, index.isNodeIndex)
    } )

    // returns the list of labels/types for the given index
    private def getEntities(index: IndexDefinition): List[String] =
      if (index.isNodeIndex) index.getLabels.asScala.toList.map(_.name) else index.getRelationshipTypes.asScala.toList.map(_.name)

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

    // Find constraint

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

    // Transaction methods

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
      val tx = graphService.beginTransaction(txType, AUTH_DISABLED)
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
      val tx = graphService.beginTransaction(Type.IMPLICIT, AUTH_DISABLED)
      try {
        val result = f(tx)
        tx.rollback()
        result
      } finally {
        tx.close()
      }
    }

    def txCounts: TxCounts = TxCounts(txMonitor.getNumberOfCommittedTransactions, txMonitor.getNumberOfRolledBackTransactions, txMonitor.getNumberOfActiveTransactions)

    private def txMonitor: DatabaseTransactionStats = graphService.getDependencyResolver.resolveDependency(classOf[DatabaseTransactionStats])
  }
}

final case class TxCounts(commits: Long = 0, rollbacks: Long = 0, active: Long = 0) {
  def +(other: TxCounts): TxCounts = TxCounts(commits + other.commits, rollbacks + other.rollbacks, active + other.active)
  def -(other: TxCounts): TxCounts = TxCounts(commits - other.commits, rollbacks - other.rollbacks, active - other.active)
}
