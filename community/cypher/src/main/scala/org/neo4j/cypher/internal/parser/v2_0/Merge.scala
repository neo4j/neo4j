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

import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.mutation.UpdateAction
import org.neo4j.cypher.internal.parser.{On, OnAction}


trait Merge extends Base with Labels with ParserPattern {

  def merge: Parser[StartAst] = rep1(MERGE ~> patterns) ~ rep(onCreate | onMatch) ^^ {
    case nodes ~ actions => StartAst(merge = Seq(MergeAst(nodes.flatten.toSeq, actions)))
  }

  private def onCreate: Parser[OnAction] = ON ~> CREATE ~> identity ~ set ^^ {
    case id ~ setActions => OnAction(On.Create, id, setActions)
  }

  private def onMatch: Parser[OnAction] = ON ~> MATCH ~> identity ~ set ^^ {
    case id ~ setActions => OnAction(On.Match, id, setActions)
  }

  def set: Parser[Seq[UpdateAction]]
}