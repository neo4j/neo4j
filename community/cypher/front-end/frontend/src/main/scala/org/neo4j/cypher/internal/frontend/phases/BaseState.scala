/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.VariableCheckerDebugLogger
import org.neo4j.cypher.internal.rewriting.SimpleBaseState
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.StepSequencer

trait BaseState extends SimpleBaseState {
  def queryText: String
  def plannerName: PlannerName
  def maybeProcedureSignatureVersion: Option[Long]
  def maybeStatement: Option[Statement]
  def maybeReturnColumns: Option[Seq[String]]
  def maybeScopeState: Option[ScopeState]
  def maybeLocalDefinitions: Option[LocalDefinitionsDirectory]
  def maybeSemantics: Option[SemanticState]
  def maybeExtractedParams: Option[Map[AutoExtractedParameter, Expression]]
  def maybeResolvedParams: Option[Set[String]]
  def maybeSemanticTable: Option[SemanticTable]
  def maybeObfuscationMetadata: Option[ObfuscationMetadata]
  def maybeDebugInfo: Option[PipelineDebugInfo]
  def anonymousVariableNameGenerator: AnonymousVariableNameGenerator

  def accumulatedConditions: Set[StepSequencer.Condition]
  def semanticsUpToDate: Boolean

  def statement(): Statement = maybeStatement getOrElse fail("Statement")
  def localDefinitions(): LocalDefinitionsDirectory = maybeLocalDefinitions getOrElse fail("Local Callable Definitions")
  def returnColumns(): Seq[String] = maybeReturnColumns getOrElse fail("Return columns")
  def semantics(): SemanticState = maybeSemantics getOrElse fail("Semantics")
  def semanticTable(): SemanticTable = maybeSemanticTable getOrElse fail("Semantic table")
  def obfuscationMetadata(): ObfuscationMetadata = maybeObfuscationMetadata getOrElse fail("Obfuscation metadata")
  def scopeState(): ScopeState = maybeScopeState getOrElse fail("Scope state")
  def debugInfo(): PipelineDebugInfo = maybeDebugInfo getOrElse fail("Debug info")

  protected def fail(what: String) = {
    throw new IllegalStateException(s"$what not yet initialised")
  }

  def withStatement(s: Statement): BaseState
  def withProcedureSignatureVersion(signatureVersion: Option[Long]): BaseState
  def withReturnColumns(cols: Seq[String]): BaseState
  def withScopeState(s: ScopeState): BaseState
  def withLocalDefinitions(localDefinitionsDirectory: LocalDefinitionsDirectory): BaseState
  def withSemanticTable(s: SemanticTable): BaseState
  def withSemanticState(s: SemanticState): BaseState
  def withParams(p: Map[AutoExtractedParameter, Expression]): BaseState

  final def withResolvedParamsAdded(p: Set[String]): BaseState =
    withResolvedParams(maybeResolvedParams.fold(p)(_.union(p)))
  protected def withResolvedParams(p: Set[String]): BaseState
  def withObfuscationMetadata(o: ObfuscationMetadata): BaseState
  def withSemanticsUpToDate(b: Boolean): BaseState
  def withDebugInfo(debugInfo: PipelineDebugInfo): BaseState
}

case class InitialState(
  queryText: String,
  plannerName: PlannerName,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  maybeProcedureSignatureVersion: Option[Long] = None,
  maybeStatement: Option[Statement] = None,
  maybeScopeState: Option[ScopeState] = None,
  maybeLocalDefinitions: Option[LocalDefinitionsDirectory] = None,
  maybeSemantics: Option[SemanticState] = None,
  maybeExtractedParams: Option[Map[AutoExtractedParameter, Expression]] = None,
  maybeResolvedParams: Option[Set[String]] = None,
  maybeSemanticTable: Option[SemanticTable] = None,
  accumulatedConditions: Set[StepSequencer.Condition] = Set.empty,
  maybeReturnColumns: Option[Seq[String]] = None,
  maybeObfuscationMetadata: Option[ObfuscationMetadata] = None,
  maybeDebugInfo: Option[PipelineDebugInfo] = None,
  semanticsUpToDate: Boolean = false
) extends BaseState {

  override def withStatement(s: Statement): InitialState = copy(maybeStatement = Some(s))

  override def withReturnColumns(cols: Seq[String]): InitialState = copy(maybeReturnColumns = Some(cols))

  override def withScopeState(s: ScopeState): InitialState = copy(maybeScopeState = Some(s))

  override def withLocalDefinitions(localDefinitionsDirectory: LocalDefinitionsDirectory): InitialState =
    copy(maybeLocalDefinitions = Some(localDefinitionsDirectory))

  override def withSemanticTable(s: SemanticTable): InitialState = copy(maybeSemanticTable = Some(s))

  override def withSemanticState(s: SemanticState): InitialState = copy(maybeSemantics = Some(s))

  override def withParams(p: Map[AutoExtractedParameter, Expression]): InitialState =
    copy(maybeExtractedParams = Some(p))

  override protected def withResolvedParams(p: Set[String]): InitialState = copy(maybeResolvedParams = Some(p))

  override def withObfuscationMetadata(o: ObfuscationMetadata): InitialState = copy(maybeObfuscationMetadata = Some(o))

  override def withDebugInfo(debugInfo: PipelineDebugInfo): InitialState = copy(maybeDebugInfo = Some(debugInfo))

  override def withProcedureSignatureVersion(signatureVersion: Option[Long]): BaseState =
    copy(maybeProcedureSignatureVersion = signatureVersion)

  override def withSemanticsUpToDate(b: Boolean): BaseState = copy(semanticsUpToDate = b)
}

case class LocalDefinitionsDirectory(
  localProcedureDefinitions: Map[ProcedureName, LocalProcedureDefinition],
  localFunctionDefinitions: Map[FunctionName, LocalFunctionDefinition]
)

object LocalDefinitionsDirectory {
  val empty: LocalDefinitionsDirectory = LocalDefinitionsDirectory(Map.empty, Map.empty)
}

case class PipelineDebugInfo(maybeVariableCheckerDebugLogger: Option[VariableCheckerDebugLogger])
