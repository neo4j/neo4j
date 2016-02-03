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
package org.neo4j.cypher.internal.frontend.v3_0.parser

import org.neo4j.cypher.internal.frontend.v3_0.ast
import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, Variable}
import org.parboiled.scala._

import scala.collection.immutable.IndexedSeq

trait ProcedureCalls {
  self: Parser with Base with Expressions with Literals =>

  def ProcedureCall = rule("CALL") {
    group(keyword("CALL") ~~ zeroOrMore(SymbolicNameString ~ ".") ~ ProcedureName ~ ProcedureArguments ~~ ProcedureResult) ~~>> (ast.ProcedureCall(_, _, _, _))
  }

  private def ProcedureArguments: Rule1[Option[Seq[Expression]]] = rule("arguments to a procedure") {
    optional(group("(" ~~
      zeroOrMore(Expression, separator = CommaSep) ~~ ")"
    ) ~~> (_.toIndexedSeq))
  }

  private def ProcedureResult: Rule1[Option[Seq[Variable]]] = rule("result fields of a procedure") {
    optional(
      keyword("AS") ~~ oneOrMore(Variable, separator = CommaSep) ~~> (_.toIndexedSeq)
    )
  }
}
