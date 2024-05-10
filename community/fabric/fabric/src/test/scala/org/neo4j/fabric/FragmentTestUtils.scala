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
package org.neo4j.fabric

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.pipeline.FabricFrontEnd
import org.neo4j.fabric.planning.FabricFragmenter
import org.neo4j.fabric.planning.Fragment
import org.neo4j.fabric.planning.Fragment.Apply
import org.neo4j.fabric.planning.Fragment.Chain
import org.neo4j.fabric.planning.Fragment.Exec
import org.neo4j.fabric.planning.Fragment.Init
import org.neo4j.fabric.planning.Fragment.Leaf
import org.neo4j.fabric.planning.Fragment.Union
import org.neo4j.fabric.planning.Use
import org.neo4j.fabric.util.Rewritten.RewritingOps
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

import java.util.UUID
import java.util.concurrent.Executors

import scala.reflect.ClassTag

trait FragmentTestUtils {

  def init(use: Use, argumentColumns: Seq[String] = Seq(), importColumns: Seq[String] = Seq()): Init =
    Init(use, argumentColumns, importColumns)

  implicit class FragBuilder(input: Chain) {

    def apply(
      fragmentInheritUse: Use => Fragment,
      inTransactionsParameters: Option[SubqueryCall.InTransactionsParameters] = None,
      pos: InputPosition = InputPosition.NONE
    ): Apply = Apply(input, fragmentInheritUse(input.use), inTransactionsParameters)(pos)

    def leaf(clauses: Seq[ast.Clause], outputColumns: Seq[String], pos: InputPosition = InputPosition.NONE): Leaf =
      Leaf(input, clauses, outputColumns)(pos)

    def exec(query: Query, outputColumns: Seq[String]): Exec =
      Exec(input, query, dummyLocalQuery, dummyRemoteQuery, false, outputColumns)
  }

  val dummyLocalQuery: BaseState = DummyState
  val dummyRemoteQuery: Fragment.RemoteQuery = Fragment.RemoteQuery("", Map.empty)

  case object DummyState extends BaseState {
    override val queryText: String = ""

    override val plannerName: PlannerName = new PlannerName {
      def name: String = ""
      def toTextOutput: String = ""
      def version: String = ""
    }
    override val maybeProcedureSignatureVersion: Option[Long] = Option.empty
    override val maybeStatement: Option[Statement] = Option.empty
    override val maybeReturnColumns: Option[Seq[String]] = Option.empty
    override val maybeSemantics: Option[SemanticState] = Option.empty
    override val maybeExtractedParams: Option[Map[AutoExtractedParameter, Expression]] = Option.empty
    override val maybeSemanticTable: Option[SemanticTable] = Option.empty
    override val maybeObfuscationMetadata: Option[ObfuscationMetadata] = Option.empty
    override val accumulatedConditions: Set[StepSequencer.Condition] = Set.empty
    override val anonymousVariableNameGenerator: AnonymousVariableNameGenerator = new AnonymousVariableNameGenerator()
    override def withStatement(s: Statement): BaseState = this
    override def withReturnColumns(cols: Seq[String]): BaseState = this
    override def withSemanticTable(s: SemanticTable): BaseState = this
    override def withSemanticState(s: SemanticState): BaseState = this
    override def withParams(p: Map[AutoExtractedParameter, Expression]): BaseState = this
    override def withObfuscationMetadata(o: ObfuscationMetadata): BaseState = this
    override def withProcedureSignatureVersion(signatureVersion: Option[Long]): BaseState = this
  }

  implicit class FragBuilderInit(input: Fragment.Init) {

    def union(lhs: Fragment, rhs: Chain, pos: InputPosition = InputPosition.NONE): Union =
      Union(input, true, lhs, rhs)(pos)

    def unionAll(lhs: Fragment, rhs: Chain, pos: InputPosition = InputPosition.NONE): Union =
      Union(input, false, lhs, rhs)(pos)
  }

  object ct {
    val any: AnyType = org.neo4j.cypher.internal.util.symbols.CTAny
    val int: IntegerType = org.neo4j.cypher.internal.util.symbols.CTInteger
  }

  private object AstUtils extends AstConstructionTestSupport

  def use(name: String): UseGraph = AstUtils.use(List(name))

  val defaultGraphName: String = "default"
  val defaultGraph: UseGraph = use(defaultGraphName)
  val defaultUse: Use.Inherited = Use.Inherited(Use.Default(defaultGraph))(InputPosition.NONE)

  val defaultRef = new DatabaseReferenceImpl.Internal(
    new NormalizedDatabaseName(defaultGraphName),
    DatabaseIdFactory.from(defaultGraphName, UUID.randomUUID()),
    true
  )

  val defaultGraphAlias: Catalog.InternalAlias = Catalog.InternalAlias(0, defaultRef)
  val defaultCatalog: Catalog = Catalog.byQualifiedName(Seq(defaultGraphAlias))
  val params: MapValue = MapValue.EMPTY

  def signatures: ProcedureSignatureResolver

  val cypherConfig: CypherConfiguration = CypherConfiguration.fromConfig(Config.defaults())

  val cypherConfigWithQueryObfuscation: CypherConfiguration =
    CypherConfiguration.fromConfig(Config.newBuilder()
      .set(GraphDatabaseSettings.log_queries_obfuscate_literals, java.lang.Boolean.TRUE)
      .build())
  val monitors: Monitors = new Monitors

  val cacheFactory = new ExecutorBasedCaffeineCacheFactory(Executors.newWorkStealingPool)
  val frontend: FabricFrontEnd = FabricFrontEnd(cypherConfig, monitors, cacheFactory)

  def pipeline(query: String): frontend.Pipeline =
    frontend.Pipeline(
      signatures,
      frontend.preParsing.preParse(query, devNullLogger),
      params,
      CancellationChecker.NeverCancelled,
      devNullLogger,
      InternalSyntaxUsageStatsNoOp
    )

  def fragment(query: String): Fragment = {
    val state = pipeline(query).parseAndPrepare.process()
    val fragmenter = new FabricFragmenter(defaultGraphName, query, state.statement(), state.semantics())
    fragmenter.fragment
  }

  def parse(query: String): Statement =
    pipeline(query).parseAndPrepare.process().statement()

  def preParse(query: String): PreParsedQuery =
    frontend.preParsing.preParse(query, devNullLogger)

  implicit class FragmentOps[F <: Fragment](fragment: F) {

    def withoutLocalAndRemote: F =
      fragment
        .rewritten
        .topDown {
          case e: Fragment.Exec => e.copy(localQuery = dummyLocalQuery, remoteQuery = dummyRemoteQuery)
        }
  }

  implicit class Caster[A](a: A) {

    def as[T](implicit ct: ClassTag[T]): T = {
      assert(ct.runtimeClass.isInstance(a), s"expected: ${ct.runtimeClass.getName}, was: ${a.getClass.getName}")
      a.asInstanceOf[T]
    }
  }
}
