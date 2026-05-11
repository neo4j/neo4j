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
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CypherQueryObfuscator
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.compiler.phases.PlannerContextImpl
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.compiler.planner.CypherPlannerVersionWithOptimisations
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExtractSensitiveLiterals
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.options.CypherParallelRepeatHeuristicOption
import org.neo4j.cypher.internal.options.CypherPlanVarExpandInto
import org.neo4j.cypher.internal.options.CypherPlannerVersionOption
import org.neo4j.cypher.internal.options.CypherStatefulShortestPlanningModeOption
import org.neo4j.cypher.internal.options.CypherTransactionBatchStrategyOption
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherProcedureSignature
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.constraints.ConstrainableType
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.procedure.impl.GlobalProceduresRegistry

class CypherQueryObfuscatorFactory {

  def obfuscatorForQuery(query: String, defaultLanguage: CypherVersion): QueryObfuscator = {
    val preParsedQuery = preParser.preParseQuery(query, devNullLogger, defaultLanguage)
    val state = InitialState(
      preParsedQuery.statement,
      null,
      new AnonymousVariableNameGenerator()
    )
    val res = pipeline.transform(
      state,
      plannerContext(preParsedQuery.resolvedLanguage, query)
    )
    CypherQueryObfuscator(res.obfuscationMetadata())
  }

  def registerComponent[T](cls: Class[T]): Unit =
    procedures.registerComponent(cls, _ => cls.cast(null), true)

  def registerProcedure[T](cls: Class[T]): Unit =
    procedures.registerProcedure(cls)

  private val procedures = new GlobalProceduresRegistry()

  private val preParser = new CachingPreParser(
    CypherConfiguration.fromConfig(Config.defaults(
      // Might need to be enabled when the next experimental version appear:
      // GraphDatabaseInternalSettings.enable_experimental_cypher_versions,
      // java.lang.Boolean.TRUE
    )),
    new LFUCache[PreParsedQuery.CacheKey, PreParsedQuery](
      new ExecutorBasedCaffeineCacheFactory((r: Runnable) => r.run()),
      1
    )
  )

  private val pipeline =
    Parse andThen
      RewriteProcedureCalls andThen
      ExtractSensitiveLiterals.andThen(ObfuscationMetadataCollection)

  private def plannerContext(version: CypherVersion, query: String) =
    new PlannerContextImpl(
      version,
      Neo4jCypherExceptionFactory(query, None),
      CompilationPhaseTracer.NO_TRACING,
      null,
      new PlanContextWithProceduresRegistry(version),
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
      CypherStatefulShortestPlanningModeOption.default,
      CypherPlanVarExpandInto.default,
      CypherPlannerVersionWithOptimisations.allSupportedOptimisations(CypherPlannerVersionOption.latest),
      CypherParallelRepeatHeuristicOption.default,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      semanticFeatures = Seq.empty,
      shadowedFunctions = Set.empty,
      transactionBatchStrategy = CypherTransactionBatchStrategyOption.default
    )

  private class PlanContextWithProceduresRegistry(version: CypherVersion) extends PlanContext {

    override def procedureSignature(name: ProcedureName): ProcedureSignature = {
      val neo4jName = new org.neo4j.internal.kernel.api.procs.QualifiedName(name.namespace.parts.toArray, name.name)
      val kernelScope = org.neo4j.cypher.internal.frontend.phases.QueryLanguage.toKernelScope(version)
      val handle = procedures.getCurrentView().procedure(neo4jName, kernelScope)
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

    override def nodeVectorIndexByName(indexName: String): Nothing = fail()
    override def relationshipVectorIndexByName(indexName: String): Nothing = fail()
    override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Nothing = fail()
    override def getNodePropertiesWithExistenceConstraint(labelName: String): Nothing = fail()

    override def hasRelationshipPropertyExistenceConstraint(
      relationshipTypeName: String,
      propertyKey: String
    ): Nothing = fail()
    override def getRelationshipPropertiesWithExistenceConstraint(relationshipTypeName: String): Nothing = fail()
    override def getPropertiesWithExistenceConstraint: Nothing = fail()

    override def hasRelationshipEndpointLabelConstraint(
      relTypeName: String,
      labelName: String,
      endpointType: EndpointType
    ): Boolean = fail()
    override def getRelationshipEndpointLabelConstraints(relTypeName: String): Map[EndpointType, String] = fail()
    override def hasNodeLabelConstraint(constrainedLabel: String, impliedLabel: String): Boolean = fail()
    override def getNodeLabelConstraints(constrainedLabel: String): Set[String] = fail()
    override def lastCommittedTxIdProvider: Nothing = fail()
    override def statistics: Nothing = fail()
    override def notificationLogger(): Nothing = fail()

    override def functionSignature(name: FunctionName): Option[UserFunctionSignature] = {
      val maybeSig = org.neo4j.cypher.internal.expressions.functions.Function
        .scopedFunctionInfo(version)
        .find(_.function.name == name.name)

      maybeSig.map { sig =>
        val inputSig: IndexedSeq[FieldSignature] =
          sig.names.zip(sig.argumentTypes).map { case (n, t) => FieldSignature(n, t) }

        UserFunctionSignature(
          name = name,
          inputSignature = inputSig,
          outputType = sig.outputType,
          deprecationInfo = None,
          description = Option(sig.description),
          isAggregate = sig.isAggregationFunction,
          id = -1,
          builtIn = true
        )
      }
    }
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
      cypherType: ConstrainableType
    ): Nothing = fail()

    override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[ConstrainableType]] = fail()

    override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[ConstrainableType]] =
      fail()

    override def hasRelationshipPropertyTypeConstraint(
      relTypeName: String,
      propertyKey: String,
      cypherType: ConstrainableType
    ): Boolean = fail()

    private def fail() = throw new IllegalStateException("Should not have been called in this test.")

    override def procedureSignatureVersion: Long = -1

    override def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlanContext = this

    override def databaseMode: DatabaseMode = fail()

    override def storageHasPropertyColocation: Boolean = fail()

    override def storageSupportsFastExpandInto: Boolean = fail()

    override def queryLanguage: QueryLanguage = QueryLanguage.from(version)
  }
}
