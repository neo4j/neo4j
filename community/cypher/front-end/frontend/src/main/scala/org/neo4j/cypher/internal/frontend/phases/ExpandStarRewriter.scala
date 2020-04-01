/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar

object ExpandStarRewriter extends Phase[BaseContext, BaseState, BaseState] {

  def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE

  def description: String = "expand *"

  def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(from.statement().endoRewrite(expandStar(from.semantics())))

  override def postConditions: Set[Condition] =
    Set(StatementCondition(containsNoReturnAll))
}
