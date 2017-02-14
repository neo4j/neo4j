package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.frontend.v3_2.ast.conditions.{StatementCondition, containsNoNodesOfType}
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.SEMANTIC_CHECK
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticChecker, SemanticState}

case class SemanticAnalysis(warn: Boolean) extends Phase[BaseContext, BaseState, BaseState] {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val semanticState = SemanticChecker.check(from.statement(), context.exceptionCreator)
    if (warn) semanticState.notifications.foreach(context.notificationLogger.log)
    from.withSemanticState(semanticState)
  }

  override def phase = SEMANTIC_CHECK

  override def description = "do variable binding, typing, type checking and other semantic checks"

  override def postConditions = Set(BaseContains[SemanticState], StatementCondition(containsNoNodesOfType[UnaliasedReturnItem]))
}
