/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_0

import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.spi.{ProcedureSignature, QualifiedProcedureName}
import org.neo4j.cypher.internal.frontend.v3_0.{Rewriter, bottomUp}

case class rewriteProcedureCalls(signatureLookup: QualifiedProcedureName => ProcedureSignature)
  extends Rewriter {

  import rewriteProcedureCalls.fakeStandaloneCallDeclarations

  def apply(in: AnyRef): AnyRef = in match {
    case statement: Statement =>
      statement.endoRewrite(resolveCalls andThen fakeStandaloneCallDeclarations)
    case other =>
      other
  }

  private def resolveCalls = bottomUp(Rewriter.lift {
    case unresolved: UnresolvedCall =>
      val resolved = unresolved.resolve(signatureLookup)
      // We coerce here to ensure that the semantic check run after this rewriter assigns a type
      // to the coercion expression
      val coerced = resolved.coerceArguments
      coerced
  })
}

object rewriteProcedureCalls {
  val fakeStandaloneCallDeclarations = Rewriter.lift {
    case q@Query(None, part@SingleQuery(Seq(resolved@ResolvedCall(_, _, _, _, _)))) if !resolved.fullyDeclared =>
      val result = q.copy(part = part.copy(clauses = Seq(resolved.fakeDeclarations))(part.position))(q.position)
      result
  }
}
