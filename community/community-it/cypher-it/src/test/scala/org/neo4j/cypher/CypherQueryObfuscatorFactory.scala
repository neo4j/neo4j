/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.ObfuscationMetadataCollection
import org.neo4j.cypher.internal.frontend.phases.Parsing
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherProcedureSignature
import org.neo4j.cypher.internal.CypherQueryObfuscator
import org.neo4j.cypher.internal.PreParser
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.procedure.impl.GlobalProceduresRegistry

class CypherQueryObfuscatorFactory {

  def obfuscatorForQuery(query: String): QueryObfuscator = {
    val preParsedQuery = preParser.preParseQuery(query)
    val state = InitialState(preParsedQuery.statement, Some(preParsedQuery.options.offset), null)
    val res = pipeline.transform(state, plannerContext(query))
    CypherQueryObfuscator(res.obfuscationMetadata())
  }

  def registerComponent[T](cls: Class[T]): Unit =
    procedures.registerComponent(cls, _ => cls.cast(null), true)

  def registerProcedure[T](cls: Class[T]): Unit =
    procedures.registerProcedure(cls, true)

  private val procedures = new GlobalProceduresRegistry()

  private val preParser = new PreParser(CypherVersion.default,
    CypherPlannerOption.default,
    CypherRuntimeOption.default,
    CypherExpressionEngineOption.default,
    CypherOperatorEngineOption.default,
    CypherInterpretedPipesFallbackOption.default,
    1)

  private val pipeline =
    Parsing andThen
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
      null)

  private object PlanContextWithProceduresRegistry extends PlanContext {

    override def procedureSignature(name: QualifiedName): ProcedureSignature = {
      val neo4jName = new org.neo4j.internal.kernel.api.procs.QualifiedName(name.namespace.toArray, name.name)
      val handle = procedures.procedure(neo4jName)
      asCypherProcedureSignature(name, handle.id(), handle.signature())
    }

    // unused

    override def indexesGetForLabel(labelId: Int): Nothing = fail()
    override def indexExistsForLabel(labelId: Int): Nothing = fail()
    override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Nothing = fail()
    override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Nothing = fail()
    override def uniqueIndexesGetForLabel(labelId: Int): Nothing = fail()
    override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Nothing = fail()
    override def getPropertiesWithExistenceConstraint(labelName: String): Nothing = fail()
    override def txIdProvider: Nothing = fail()
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

    private def fail() = throw new IllegalStateException("Should not have been called in this test.")
  }
}
