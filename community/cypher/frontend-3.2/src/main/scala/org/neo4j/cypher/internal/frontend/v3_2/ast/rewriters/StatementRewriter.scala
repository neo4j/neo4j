package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.Rewriter
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseContext, BaseState, Phase}

trait StatementRewriter extends Phase[BaseContext, BaseState, BaseState] {
  override def phase: CompilationPhase = AST_REWRITE

  def instance(context: BaseContext): Rewriter

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val rewritten = from.statement().endoRewrite(instance(context))
    from.withStatement(rewritten)
  }
}
