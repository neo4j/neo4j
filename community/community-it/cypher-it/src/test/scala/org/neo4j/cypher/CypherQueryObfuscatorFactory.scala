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
package org.neo4j.cypher

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CypherQueryObfuscator
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases.CypherVersion
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.frontend.phases.Parse
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherProcedureSignature
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.procedure.impl.GlobalProceduresRegistry

class CypherQueryObfuscatorFactory {

  def obfuscatorForQuery(query: String): QueryObfuscator = {
    val preParsedQuery = preParser.preParseQuery(query, devNullLogger)
    val state = InitialState(
      preParsedQuery.statement,
      null,
      new AnonymousVariableNameGenerator()
    )
    val res = pipeline.transform(state, plannerContext(query))
    CypherQueryObfuscator(res.obfuscationMetadata())
  }

  def registerComponent[T](cls: Class[T]): Unit =
    procedures.registerComponent(cls, _ => cls.cast(null), true)

  def registerProcedure[T](cls: Class[T]): Unit =
    procedures.registerProcedure(cls)

  private val procedures = new GlobalProceduresRegistry()

  private val preParser = new CachingPreParser(
    CypherConfiguration.fromConfig(Config.defaults()),
    new LFUCache[String, PreParsedQuery](new ExecutorBasedCaffeineCacheFactory((r: Runnable) => r.run()), 1)
  )

  private val pipeline =
    Parse(
      useAntlr = GraphDatabaseInternalSettings.cypher_parser_antlr_enabled.defaultValue(),
      CypherVersion.Default
    ) andThen
      RewriteProcedureCalls andThen
      ObfuscationMetadataCollection

  private def plannerContext(query: String) =
    new PlannerContext(
      Neo4jCypherExceptionFactory(query, None),
      CompilationPhaseTracer.NO_TRACING,
      null,
      PlanContextWithProceduresRegistry,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      CancellationChecker.NeverCancelled,
      false,
      CypherEagerAnalyzerOption.default,
      CypherStatefulShortestPlanningModeOption.default,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )

  private object PlanContextWithProceduresRegistry extends PlanContext {

    override def procedureSignature(name: QualifiedName): ProcedureSignature = {
      val neo4jName = new org.neo4j.internal.kernel.api.procs.QualifiedName(name.namespace.toArray, name.name)
      val handle = procedures.getCurrentView().procedure(neo4jName, org.neo4j.kernel.api.CypherScope.CYPHER_5)
      asCypherProcedureSignature(name, handle.id(), handle.signature())
    }

    // unused

    override def textIndexesGetForLabel(labelId: Int): Nothing = fail()
    override def textIndexesGetForRelType(relTypeId: Int): Nothing = fail()
    override def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = fail()
    override def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = fail()
    override def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = fail()
    override def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = fail()
    override def indexExistsForLabel(labelId: Int): Nothing = fail()
    override def indexExistsForRelType(labelId: Int): Nothing = fail()
    override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = fail()
    override def nodeTokenIndex: Nothing = fail()
    override def relationshipTokenIndex: Nothing = fail()
    override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Nothing = fail()
    override def getNodePropertiesWithExistenceConstraint(labelName: String): Nothing = fail()

    override def hasRelationshipPropertyExistenceConstraint(
      relationshipTypeName: String,
      propertyKey: String
    ): Nothing = fail()
    override def getRelationshipPropertiesWithExistenceConstraint(relationshipTypeName: String): Nothing = fail()
    override def getPropertiesWithExistenceConstraint: Nothing = fail()
    override def lastCommittedTxIdProvider: Nothing = fail()
    override def statistics: Nothing = fail()
    override def notificationLogger(): Nothing = fail()
    override def functionSignature(name: QualifiedName): Nothing = fail()
    override def getLabelName(id: Int): Nothing = fail()
    override def getOptLabelId(labelName: String): Nothing = fail()
    override def getLabelId(labelName: String): Nothing = fail()
    override def getPropertyKeyName(id: Int): Nothing = fail()
    override def getOptPropertyKeyId(propertyKeyName: String): Nothing = fail()
    override def getPropertyKeyId(propertyKeyName: String): Nothing = fail()
    override def getRelTypeName(id: Int): Nothing = fail()
    override def getOptRelTypeId(relType: String): Nothing = fail()
    override def getRelTypeId(relType: String): Nothing = fail()
    override def txStateHasChanges(): Nothing = fail()
    override def textIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def textIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def textIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()

    override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Nothing =
      fail()
    override def rangeIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def rangeIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def rangeIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()

    override def rangeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Nothing =
      fail()
    override def pointIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def pointIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def pointIndexExistsForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()

    override def pointIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Nothing =
      fail()

    override def hasNodePropertyTypeConstraint(
      labelName: String,
      propertyKey: String,
      cypherType: SchemaValueType
    ): Nothing = fail()

    override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]] = fail()

    override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[SchemaValueType]] =
      fail()

    override def hasRelationshipPropertyTypeConstraint(
      relTypeName: String,
      propertyKey: String,
      cypherType: SchemaValueType
    ): Boolean = fail()

    private def fail() = throw new IllegalStateException("Should not have been called in this test.")

    override def procedureSignatureVersion: Long = -1

    override def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlanContext = this

    override def databaseMode: DatabaseMode = fail()
  }
}
