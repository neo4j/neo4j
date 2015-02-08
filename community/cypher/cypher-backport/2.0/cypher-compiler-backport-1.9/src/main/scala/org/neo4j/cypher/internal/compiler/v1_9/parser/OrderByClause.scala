/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.parser

import org.neo4j.cypher.internal.compiler.v1_9.commands.SortItem


trait OrderByClause extends Base with Expressions  {
  def desc:Parser[String] = ignoreCases("descending", "desc")

  def asc:Parser[String] = ignoreCases("ascending", "asc")

  def ascOrDesc:Parser[Boolean] = opt(asc | desc) ^^ {
    case None => true
    case Some(txt) => txt.toLowerCase.startsWith("a")
  }

  def sortItem :Parser[SortItem] = expression ~ ascOrDesc ^^ { case expression ~ reverse => SortItem(expression, reverse)  }

  def order: Parser[Seq[SortItem]] =
    (ignoreCase("order by") ~> commaList(sortItem)
      | ignoreCase("order") ~> failure("expected by"))
}





