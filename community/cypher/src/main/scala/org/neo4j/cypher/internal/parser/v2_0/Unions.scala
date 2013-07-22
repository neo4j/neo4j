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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commands.{Union, AbstractQuery}


trait Unions extends Base with QueryParser {
  def mixedUnion = Parser {
    case in =>
      val parser = query ~> UNION ~> opt(ALL)

      parser.apply(in) match {
        case Success(first, rest) => parser.apply(rest) match {
          case Success(second, remaining) if first.isDefined && second.isEmpty => throw new SyntaxException("can't mix UNION and UNION ALL")
          case Success(second, remaining) if first.isEmpty && second.isDefined => throw new SyntaxException("can't mix UNION and UNION ALL")
          case _                                                               => Failure("", rest)
        }
        case _                    => Failure("", in)
      }
  }

  def union: Parser[AbstractQuery] = mixedUnion |
    rep2sep(query, UNION)       ^^ { queries => Union(queries, distinct = true) } |
    rep2sep(query, UNION ~ ALL) ^^ { queries => Union(queries, distinct = false) }
}