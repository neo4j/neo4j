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
package org.neo4j.cypher.internal.cst.factory.neo4j.ast

import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.TerminalNode
import org.neo4j.cypher.internal.parser.AbstractAstBuilder
import org.neo4j.cypher.internal.parser.CypherParser

/**
 * Antlr parser listener that builds Neo4j ASTs during parsing.
 *
 * All exit methods implemented here must:
 * - Set [[org.neo4j.cypher.internal.parser.AstRuleCtx]].ast of their context.
 * - When antlr grammar do not exactly match neo4j ast, create transient ASTNode classes in [[TransientAstNode]].
 */
final class AstBuilder extends AbstractAstBuilder
    with LiteralBuilder
    with LabelExpressionBuilder
    with DdlBuilder
    with DdlCreateBuilder
    with ExpressionBuilder
    with StatementBuilder {
  override def visitTerminal(node: TerminalNode): Unit = {}
  override def visitErrorNode(node: ErrorNode): Unit = {}
  override def enterEveryRule(ctx: ParserRuleContext): Unit = {}
  override def exitEndOfFile(ctx: CypherParser.EndOfFileContext): Unit = {}
}
