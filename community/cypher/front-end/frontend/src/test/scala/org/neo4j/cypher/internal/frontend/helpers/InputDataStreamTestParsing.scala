/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.helpers

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.parser.Expressions
import org.neo4j.cypher.internal.parser.Statement
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.parboiled.scala.EOI
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1
import org.parboiled.scala.group

/**
 * Parse text into an AST object.
 */
case object InputDataStreamTestParsing extends Phase[BaseContext, BaseState, BaseState] {
  private val parser = new InputDataStreamTestCypherParser

  override def process(in: BaseState, context: BaseContext): BaseState = {
    val idsIn = in.asInstanceOf[InputDataStreamTestInitialState]
    idsIn.withStatement(parser.parse(idsIn.idsQueryText, context.cypherExceptionFactory))
  }

  override val phase = PARSING

  override def postConditions = Set(BaseContains[ast.Statement])

}

case class InputDataStreamTestInitialState(idsQueryText: String,
                                           queryText: String,
                                           startPosition: Option[InputPosition],
                                           plannerName: PlannerName,
                                           anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
                                           initialFields: Map[String, CypherType] = Map.empty,
                                           maybeStatement: Option[ast.Statement] = None,
                                           maybeSemantics: Option[SemanticState] = None,
                                           maybeExtractedParams: Option[Map[String, Any]] = None,
                                           maybeSemanticTable: Option[SemanticTable] = None,
                                           accumulatedConditions: Set[StepSequencer.Condition] = Set.empty,
                                           maybeReturnColumns: Option[Seq[String]] = None,
                                           maybeObfuscationMetadata: Option[ObfuscationMetadata] = None) extends BaseState {


  override def withStatement(s: ast.Statement): InputDataStreamTestInitialState = {
    // the unmodified parser is part of the pipeline and it will try to set the result of parsing 'RETURN 1'
    // we simply ignore statements that do not contain InputDataStream AST node
    if (s.folder.findAllByClass[ast.InputDataStream].isEmpty) {
      copy()
    } else {
      copy(maybeStatement = Some(s))
    }
  }

  override def withSemanticTable(s: SemanticTable): InputDataStreamTestInitialState = copy(maybeSemanticTable = Some(s))

  override def withSemanticState(s: SemanticState): InputDataStreamTestInitialState = copy(maybeSemantics = Some(s))

  override def withParams(p: Map[String, Any]): InputDataStreamTestInitialState = copy(maybeExtractedParams = Some(p))

  override def withReturnColumns(cols: Seq[String]): InputDataStreamTestInitialState = copy(maybeReturnColumns = Some(cols))

  override def withObfuscationMetadata(o: ObfuscationMetadata): InputDataStreamTestInitialState = copy(maybeObfuscationMetadata = Some(o))
}

class InputDataStreamTestCypherParser extends Parser
  with Statement
  with Expressions {


  @throws(classOf[CypherException])
  def parse(queryText: String, cypherExceptionFactory: CypherExceptionFactory, offset: Option[InputPosition] = None): ast.Statement =
    parseOrThrow(queryText, cypherExceptionFactory, InputDataStreamTestCypherParser.Statements)
}

object InputDataStreamTestCypherParser extends Parser with Statement with Expressions {
  val Statements: Rule1[Seq[ast.Statement]] = rule {
    oneOrMore(WS ~ Statement ~ WS, separator = ch(';')) ~~ optional(ch(';')) ~~ EOI.label("end of input")
  }

  override def Clause: Rule1[ast.Clause] = (
    Unwind
      | With
      | Match
      | Call
      | Return
      | InputDataStream
    )

  def InputDataStream: Rule1[ast.InputDataStream] = rule("INPUT DATA STREAM") {
    group(keyword("INPUT DATA STREAM") ~~ oneOrMore(Variable, separator = CommaSep)) ~~>> (ast.InputDataStream(_))
  }
}
