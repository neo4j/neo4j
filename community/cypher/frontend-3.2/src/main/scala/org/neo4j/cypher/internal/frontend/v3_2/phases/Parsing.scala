package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.PARSING

case object Parsing extends Phase[BaseContext, BaseState, BaseState] {
  private val parser = new CypherParser

  override def process(in: BaseState, ignored: BaseContext): BaseState =
    in.withStatement(parser.parse(in.queryText, in.startPosition))

  override val phase = PARSING

  override val description = "parse text into an AST object"

  override def postConditions = Set(BaseContains[Statement])
}
