/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v1_5


import org.neo4j.cypher._
import internal.commands.{Sort, Entity, ReturnItem, SortItem}
import scala.util.parsing.combinator._
trait OrderByClause extends JavaTokenParsers with Tokens with ReturnItems  {
  def desc:Parser[String] = ignoreCase("descending") | ignoreCase("desc")

  def asc:Parser[String] = ignoreCase("ascending") | ignoreCase("asc")

  def ascOrDesc:Parser[Boolean] = opt(asc | desc) ^^ {
    case None => true
    case Some(txt) => txt.toLowerCase.startsWith("a")
  }

  def sortItem :Parser[SortItem] = (aggregate | returnItem) ~ ascOrDesc ^^ {
    case returnItem ~ reverse => {
      returnItem match {
        case ReturnItem(Entity(_), _) => throw new SyntaxException("Cannot ORDER BY on nodes or relationships")
        case _ => SortItem(returnItem.expression, reverse)
      }
    }
  }

  def order: Parser[Sort] = ignoreCase("order by")  ~> rep1sep(sortItem, ",") ^^
    {
      case items => Sort(items:_*)
    }
}





