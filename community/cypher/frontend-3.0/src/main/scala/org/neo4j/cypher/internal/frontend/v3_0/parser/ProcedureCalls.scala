/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_0.parser

import org.neo4j.cypher.internal.frontend.v3_0.ast
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.parboiled.scala._

trait ProcedureCalls {
  self: Parser with Base with Expressions with Literals =>

  def Call: Rule1[UnresolvedCall] = rule("CALL") {
    group(keyword("CALL") ~~ ProcedureNamespace ~ ProcedureName ~ ProcedureArguments ~~ ProcedureResults) ~~>> (ast.UnresolvedCall(_, _, _, _))
  }

  private def ProcedureNamespace: Rule1[ast.ProcedureNamespace] = rule("namespace of a procedure") {
    zeroOrMore(SymbolicNameString ~ ".") ~~>> (ast.ProcedureNamespace(_))
  }

  private def ProcedureArguments: Rule1[Option[Seq[Expression]]] = rule("arguments to a procedure") {
    optional(group("(" ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq))
  }

  private def ProcedureResults: Rule1[Option[Seq[ProcedureResultItem]]] =
    rule("result fields of a procedure") {
      optional(
        keyword("YIELD") ~~ oneOrMore(ProcedureResult, separator = CommaSep) ~~> (_.toIndexedSeq)
      )
    }

  private def ProcedureResult: Rule1[ProcedureResultItem] =
    AliasedProcedureResult | SimpleProcedureResult

  private def AliasedProcedureResult: Rule1[ast.ProcedureResultItem] =
    rule("aliased procedure result field") {
      ProcedureOutput ~~ keyword("AS") ~~ Variable ~~>> (ast.ProcedureResultItem(_, _))
    }

  private def SimpleProcedureResult: Rule1[ast.ProcedureResultItem] =
    rule("simple procedure result field") {
      Variable ~~>> (ast.ProcedureResultItem(_))
    }

  private def ProcedureOutput: Rule1[ast.ProcedureOutput] =
    rule("procedure output") {
      SymbolicNameString ~~>> (ast.ProcedureOutput(_))
    }
}
