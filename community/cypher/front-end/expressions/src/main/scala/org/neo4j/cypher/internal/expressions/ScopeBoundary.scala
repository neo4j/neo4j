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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.ASTNode

/**
 * An AST node that opens a scope which only sees the variables it imports.
 *
 * A variable of the enclosing scope whose name is not imported cannot be referenced behind the
 * boundary, so a variable of that name occurring there is a different variable.
 * [[Expression.occurrences]] uses this to end its search at the boundary.
 */
trait ScopeBoundary extends ASTNode {

  /**
   * The nodes reachable from the enclosing scope when searching for occurrences of a variable of
   * this name: the whole of a part that imports it, and otherwise only that part's own uses of the
   * enclosing scope. A node may have several parts deciding this independently, e.g. the UNION
   * branches of a `CALL { ... }` body.
   */
  def nodesInEnclosingScope(name: String): Seq[ASTNode]
}
