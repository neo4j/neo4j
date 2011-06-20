package org.neo4j.cypher.parser

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.neo4j.cypher.commands._
import scala.util.parsing.combinator._
import org.neo4j.graphdb.Direction


trait MatchClause extends JavaTokenParsers with Tokens {
  def matching: Parser[Match] = ignoreCase("match") ~> rep1sep(relatedTo, ",") ^^ { case matching:List[List[Pattern]] => Match(matching.flatten: _*) }

  def relatedTo: Parser[List[Pattern]] = relatedHead ~ rep1(relatedTail) ^^ {
    case head ~ tails => {
      val namer = new NodeNamer
      var last = namer.name(head)
      val list = tails.map((item) => item match { case (back, relName, relType, forward, end) => {
        val endName = namer.name(end)
        val result: Pattern = RelatedTo(last, endName, relName, relType, getDirection(back, forward))

        last = endName

        result
      }})

      list
    }
  }
  private def getDirection(back:Option[String], forward:Option[String]):Direction =
    (back.nonEmpty, forward.nonEmpty) match {
      case (true,false) => Direction.INCOMING
      case (false,true) => Direction.OUTGOING
      case (false,false) => Direction.BOTH
      case (true,true) => Direction.BOTH
    }
  class NodeNamer {
    var lastNodeNumber = 0

    def name(s:Option[String]):String = s match {
      case None => {
        lastNodeNumber += 1
        "___NODE" + lastNodeNumber
      }
      case Some(x) => x
    }
  }

  def relatedHead:Parser[Option[String]] = "(" ~> opt(identity) <~ ")" ^^ { case name => name }

  def relatedTail = opt("<") ~ "-" ~ opt("[" ~> (relationshipInfo1 | relationshipInfo2) <~ "]") ~ "-" ~ opt(">") ~ "(" ~ opt(identity) ~ ")" ^^ {
    case back ~ "-" ~ relInfo ~ "-" ~ forward ~ "(" ~ end ~ ")" => relInfo match {
      case Some(x) => (back, x._1, x._2, forward, end)
      case None => (back, None, None, forward, end)
    }
  }

  def relationshipInfo1 = opt(identity <~ ",") ~ ":" ~ identity ^^ {
    case relName ~ ":" ~ relType => (relName, Some(relType))
  }

  def relationshipInfo2 = identity ^^ {
    case relName => (Some(relName), None)
  }

}







