package org.neo4j.cypher.internal.frontend.v3_2.phases

import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.replaceAliasedFunctionInvocations.aliases
import org.neo4j.cypher.internal.frontend.v3_2.ast.{FunctionInvocation, FunctionName, Statement}
import org.neo4j.cypher.internal.frontend.v3_2.notification.{DeprecatedFunctionNotification, InternalNotification}
import org.neo4j.cypher.internal.frontend.v3_2.phases.CompilationPhaseTracer.CompilationPhase.DEPRECATION_WARNINGS

object SyntaxDeprecationWarnings extends VisitorPhase[BaseContext, BaseState] {
  override def visit(state: BaseState, context: BaseContext): Unit = {
    val warnings = findDeprecations(state.statement())

    warnings.foreach(context.notificationLogger.log)
  }

  private def findDeprecations(statement: Statement): Set[InternalNotification] =
    statement.treeFold(Set.empty[InternalNotification]) {
      case f@FunctionInvocation(_, FunctionName(name), _, _) if aliases.get(name).nonEmpty =>
        (seq) => (seq + DeprecatedFunctionNotification(f.position, name, aliases(name)), None)
    }

  override def phase = DEPRECATION_WARNINGS

  override def description = "find deprecated Cypher constructs and generate warnings for them"
}
