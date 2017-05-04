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
package org.neo4j.cypher.internal.frontend.v3_3.parser

import java.util.regex.Pattern._

/**
 * Converts [[ParsedLikePattern]] into a regular expression string
 */
case object convertLikePatternToRegex {
  def apply(in: ParsedLikePattern, caseInsensitive: Boolean = false): String =
    in.ops.map(convert).mkString(if (caseInsensitive) "(?i)" else "", "", "")

  private def convert(in: LikePatternOp): String = in match {
    case MatchText(s) => quote(s)
    case MatchMany => ".*"
    case MatchSingle => "."
  }
}
