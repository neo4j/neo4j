package org.neo4j.cypher.internal.frontend.v3_2

import org.neo4j.cypher.internal.frontend.v3_2.ast.Statement

object SemanticChecker {
  def check(statement: Statement, mkException: (String, InputPosition) => CypherException): SemanticState = {

    val SemanticCheckResult(semanticState, semanticErrors) = statement.semanticCheck(SemanticState.clean)

    val scopeTreeIssues = ScopeTreeVerifier.verify(semanticState.scopeTree)
    if (scopeTreeIssues.nonEmpty)
      throw new InternalException(scopeTreeIssues.mkString(s"\n"))

    semanticErrors.map { error => throw mkException(error.msg, error.position) }

    semanticState
  }
}
