/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0peg.ast

import org.neo4j.cypher.internal.parser.v2_0peg._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.commands

trait Statement extends AstNode with SemanticCheckable {
  def check : Seq[SemanticError] = {
    repeatUntil((SemanticCheckResult.success(SemanticState.clean), 10)) { case (previous, n) =>
      val latest = semanticCheck(previous.state)
      if (!latest.errors.isEmpty || latest.state == previous.state)
        ((latest, n-1), true)
      else if (n == 0)
        throw new ThisShouldNotHappenError("chris", "Too many semantic check passes")
      else
        ((latest, n-1), !latest.errors.isEmpty || latest.state == previous.state)
    }._1.errors
  }

  def toLegacyQuery : commands.AbstractQuery

  private def repeatUntil[A](seed: A)(f: A => (A, Boolean)): A = f(seed) match {
    case (a, false) => repeatUntil(a)(f)
    case (a, true) => a
  }
}
