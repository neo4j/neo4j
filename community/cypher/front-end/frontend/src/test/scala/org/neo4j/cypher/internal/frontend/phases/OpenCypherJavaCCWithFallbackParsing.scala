package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.OpenCypherJavaCCParserWithFallback
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.PARSING
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory

case object OpenCypherJavaCCWithFallbackParsing extends Phase[BaseContext, BaseState, BaseState] {
  private val exceptionFactory = OpenCypherExceptionFactory(None)

  override def process(in: BaseState, context: BaseContext): BaseState = {
    val statement = OpenCypherJavaCCParserWithFallback.parse(in.queryText, exceptionFactory, new AnonymousVariableNameGenerator)
    in.withStatement(statement)
  }

  override val phase = PARSING

  override def postConditions = Set(BaseContains[Statement])
}
