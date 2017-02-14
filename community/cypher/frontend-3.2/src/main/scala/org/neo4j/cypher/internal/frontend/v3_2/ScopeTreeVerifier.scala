package org.neo4j.cypher.internal.frontend.v3_2

import scala.compat.Platform.EOL

object ScopeTreeVerifier {
  def verify(root: Scope): Seq[String] = {
    val localSymbolTableIssues = root.allScopes.flatMap {
      scope =>
        scope.symbolTable.collect {
          case (name, symbol) if name != symbol.name =>
            s"'$name' points to symbol with different name '$symbol' in scope #${Ref(scope).toIdString}. Scope tree:$EOL$root"
        }
    }
    localSymbolTableIssues
  }

}
