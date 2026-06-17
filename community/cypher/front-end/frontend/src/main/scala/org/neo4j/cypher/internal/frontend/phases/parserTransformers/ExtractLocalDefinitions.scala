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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.LocalDefinitionsDirectory
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition

/**
 * ExtractLocalDefinitions extracts DEFINE statements from the queries in the LocalDefinitionsDirectory of the BaseState.
 */
case object ExtractLocalDefinitions extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    /* Note that
     * 1) The directory distinguishes local definitions by type (e.g. functions vs. procedures), even if they have the same name
     * 2) Naming conflicts are handled in the VariableChecker, cf. 42I77 for local callables
     * 3) Name resolution of local callables is handled in ResolveLocalFunctions, ResolveLocalProceduresStep1 and ResolveLocalProceduresStep2
     */
    val localCallableDefinitions = from.statement().folder.treeCollect {
      case lcd: LocalCallableDefinition => lcd
    }.foldLeft((
      Seq.empty[(ProcedureName, LocalProcedureDefinition)],
      Seq.empty[(FunctionName, LocalFunctionDefinition)]
    )) {
      case ((localProcedureDefinitions, localFunctionDefinitions), lpd: LocalProcedureDefinition) =>
        (localProcedureDefinitions :+ lpd.name -> lpd, localFunctionDefinitions)
      case ((localProcedureDefinitions, localFunctionDefinitions), lpf: LocalFunctionDefinition) =>
        (localProcedureDefinitions, localFunctionDefinitions :+ lpf.name -> lpf)
    }
    from.withLocalDefinitions(LocalDefinitionsDirectory(
      localCallableDefinitions._1.toMap,
      localCallableDefinitions._2.toMap
    ))
  }

  override def preConditions: Set[Condition] =
    Set(BaseContains[Statement](), LocalFunctionsResolved, LocalProceduresFullyResolved)

  override def postConditions: Set[Condition] = Set(LocalCallableDefinitionsExtracted)

  override def invalidatedConditions: Set[Condition] = Set()

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    ExtractLocalDefinitions

  override def phase: CompilationPhaseTracer.CompilationPhase = CompilationPhase.LOCAL_DEFINITION_EXTRACTION
}

case object LocalCallableDefinitionsExtracted extends Condition
