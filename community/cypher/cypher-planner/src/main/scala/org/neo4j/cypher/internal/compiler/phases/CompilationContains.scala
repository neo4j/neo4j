/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker

import scala.reflect.ClassTag

case class CompilationContains[T]()(implicit val tag: ClassTag[T]) extends ValidatingCondition {

  override def apply(in: Any)(cancellationChecker: CancellationChecker): Seq[String] = in match {
    case state: LogicalPlanState =>
      tag.runtimeClass match {
        case x if classOf[Statement] == x && state.maybeStatement.isEmpty     => Seq("Statement missing")
        case x if classOf[SemanticState] == x && state.maybeSemantics.isEmpty => Seq("Semantic State missing")
        case x if classOf[PlannerQuery] == x && state.maybeQuery.isEmpty      => Seq("Planner query missing")
        case x if classOf[LogicalPlan] == x && state.maybeLogicalPlan.isEmpty => Seq("Logical plan missing")
        case x if classOf[CypherEagerAnalyzerOption] == x && state.maybeEagerAnalyzerOption.isEmpty =>
          Seq("EagerAnalyzerOption missing")
        case _ => Seq.empty
      }
    case x => throw new IllegalArgumentException(s"Unknown state: $x")
  }

  override def name: String = s"$productPrefix[${tag.runtimeClass.getSimpleName}]"

  override def hashCode(): Int = tag.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case cc: CompilationContains[_] => tag.equals(cc.tag)
    case _                          => false
  }
}
