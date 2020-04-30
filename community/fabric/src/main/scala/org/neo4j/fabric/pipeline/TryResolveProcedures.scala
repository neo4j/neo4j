/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.pipeline

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.fabric.util.Rewritten.RewritingOps

import scala.util.Try

case class TryResolveProcedures(signatures: ProcedureSignatureResolver) extends Rewriter {

  override def apply(input: AnyRef): AnyRef =
    input
      .rewritten
      .bottomUp {
        // Try resolving procedures
        case unresolved: UnresolvedCall =>
          Try(ResolvedCall(signatures.procedureSignature)(unresolved))
            .getOrElse(unresolved)
        // Try resolving functions
        case function: FunctionInvocation if function.needsToBeResolved =>
          val name = QualifiedName(function)
          signatures.functionSignature(name)
            .map(sig => ResolvedFunctionInvocation(name, Some(sig), function.args)(function.position))
            .getOrElse(function)
      }
      .rewritten
      .bottomUp {
        // Expand implicit yields and add return
        case q @ Query(None, part @ SingleQuery(Seq(resolved: ResolvedCall))) =>
          val expanded = resolved.withFakedFullDeclarations
          val aliases = expanded.callResults.map { item =>
            val copy1 = Variable(item.variable.name)(item.variable.position)
            val copy2 = Variable(item.variable.name)(item.variable.position)
            AliasedReturnItem(copy1, copy2)(resolved.position)
          }
          val projection = Return(distinct = false, ReturnItems(includeExisting = false, aliases)(resolved.position),
            None, None, None)(resolved.position)
          q.copy(part = part.copy(clauses = Seq(expanded, projection))(part.position))(q.position)
      }

}
