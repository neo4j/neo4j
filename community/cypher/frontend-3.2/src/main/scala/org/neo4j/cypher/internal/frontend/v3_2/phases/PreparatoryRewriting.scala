package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.{expandCallWhere, normalizeReturnClauses, normalizeWithClauses, replaceAliasedFunctionInvocations}
import org.neo4j.cypher.internal.frontend.v3_2.inSequence
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE

case object PreparatoryRewriting extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {

    val rewrittenStatement = from.statement().endoRewrite(inSequence(
      normalizeReturnClauses(context.exceptionCreator),
      normalizeWithClauses(context.exceptionCreator),
      expandCallWhere,
      replaceAliasedFunctionInvocations))

    from.withStatement(rewrittenStatement)
  }

  override val phase = AST_REWRITE

  override val description = "rewrite the AST into a shape that semantic analysis can be performed on"

  override def postConditions = Set.empty
}
