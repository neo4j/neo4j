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
package org.neo4j.importer

import ParseResult.Failure
import ParseResult.Success
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType
import org.neo4j.cypher.internal.ast.AlterCurrentGraphType.AlterOperation
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.phases.BaseContextImpl
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.notification
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedCatalogEntry
import org.neo4j.kernel.database.NormalizedDatabaseName

import java.util.Optional
import java.util.UUID

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.reflect.ClassTag

class SchemaCommandParser(
  configuration: CypherConfiguration,
  graphTypeSupport: GraphTypeSupport
) {
  final private val preparser: PreParser = new PreParser(configuration)
  final private val parsingConfig = CypherParsingConfig.fromCypherConfiguration(configuration)

  def parse(query: String): ParseResult = {
    val preparsed = preparser.preParse(query, configuration.systemDefaultLanguage)
    val context = BaseContextImpl(
      cypherVersion = preparsed.resolvedLanguage,
      tracer = CompilationPhaseTracer.NO_TRACING,
      notificationLogger = notification.devNullLogger,
      queryText = query,
      offset = Option.empty,
      monitors = SchemaCommandParser.noMonitors,
      cancellationChecker = CancellationChecker.neverCancelled(),
      internalSyntaxUsageStats = InternalUsageStatsNoOp,
      sessionDatabase = SchemaCommandParser.noDatabaseReference,
      semanticFeatures = parsingConfig.semanticFeatures,
      isScopeQuery = false,
      shadowedFunctions = Set.empty,
      isDebugSession = false
    )
    val state = InitialState(query, SchemaCommandParser.noPlannerName, new AnonymousVariableNameGenerator)
    val cypherLanguageAstParser = AstParserFactory(preparsed.resolvedLanguage)
    val statements: Statements = cypherLanguageAstParser(
      query = preparsed.statement,
      cypherExceptionFactory = context.cypherExceptionFactory,
      notificationLogger = Some(notification.devNullLogger),
      semanticFeatures = parsingConfig.semanticFeatures
    ).statements()

    val steps = PreparatoryRewriting.andThen(SemanticAnalysis(Some(true)))

    def syntaxException(pos: InputPosition, message: String): Err = {
      Err(context.cypherExceptionFactory.syntaxException(null, message, pos).getMessage)
    }

    val supportedOperations: Set[AlterOperation] = graphTypeSupport match {
      case Supported(operations) => operations
      case Unsupported(_)        => Set.empty
    }

    def pipeline(statement: Statement): StatementResult = {
      val position = statement.position

      try {
        statement match {
          case candidate: ast.SchemaCommand if candidate.useGraph.isDefined =>
            syntaxException(
              position,
              "Schema commands are only applied to the database to be imported into so graph names are not allowed: " +
                candidate.useGraph.get.graphReference.print
            )

          case ast.AlterCurrentGraphType(_, op, _) if !supportedOperations.contains(op) =>
            // Note: Fail unsupported operations here, rather than in semantic analysis
            val reason = graphTypeSupport match {
              case Supported(_)        => "The GRAPH TYPE operation %s is not supported".format(op.name())
              case Unsupported(reason) => reason
            }

            syntaxException(
              position,
              reason
            )

          case candidate: ast.SchemaCommand =>
            // We need to rewrite the query to ensure that it is correct
            val work = state.withStatement(candidate)
            val rewritten = steps.transform(work, context)
            Ok(rewritten.statement().asInstanceOf[ast.SchemaCommand])

          case residual =>
            syntaxException(
              position,
              "Only schema change clauses are allowed here but found: " +
                residual.getClass.getSimpleName
            )
        }
      } catch {
        case exc: SyntaxException => Err(exc.getMessage)
      }
    }

    val results = statements.statements.map(pipeline)
    results.partitionMap {
      case Ok(command)  => Left(command)
      case Err(message) => Right(message)
    } match {
      case (success, Seq()) => new Success(preparsed.resolvedLanguage, success.asJava)
      case (_, errors)      => new Failure(errors.asJava)
    }
  }
}

sealed trait GraphTypeSupport
case class Supported(operations: Set[AlterOperation]) extends GraphTypeSupport
case class Unsupported(reason: String) extends GraphTypeSupport

sealed private trait StatementResult
private case class Ok(schemaCommand: ast.SchemaCommand) extends StatementResult
private case class Err(message: String) extends StatementResult

object SchemaCommandParser {

  final private val noPlannerName: PlannerName = new PlannerName {
    override def name: String = ???
    override def toTextOutput: String = ???
    override def version: String = ???
  }

  final private val noMonitors: Monitors = new Monitors {
    override def addMonitorListener[T](monitor: T, tags: String*): Unit = ???
    override def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = ???
  }

  final private val noDatabaseReference: DatabaseReference = new DatabaseReferenceImpl {
    override val alias: NormalizedDatabaseName = new NormalizedDatabaseName("")
    override def namespace(): Optional[NormalizedDatabaseName] = ???
    override def isPrimary: Boolean = ???
    override def id(): UUID = ???
    override def namedDatabaseId(): NamedDatabaseId = ???
    override def catalogEntry(): NormalizedCatalogEntry = ???
    override def owningDatabaseName(): String = ???
    override def isShard: Boolean = ???
  }

  def createCommunity(configuration: CypherConfiguration): SchemaCommandParser = {
    new SchemaCommandParser(
      configuration,
      Unsupported("GRAPH TYPE requires Enterprise Edition")
    )
  }

  def createEnterprise(configuration: CypherConfiguration): SchemaCommandParser = {
    // There is no reason to allow operations other than SET on a fresh import.
    new SchemaCommandParser(configuration, Supported(Set(AlterCurrentGraphType.Set)))
  }

  def createEnterpriseIncremental(configuration: CypherConfiguration): SchemaCommandParser = new SchemaCommandParser(
    configuration,
    Supported(Set(AlterCurrentGraphType.Add, AlterCurrentGraphType.Drop, AlterCurrentGraphType.Alter))
  )

}
