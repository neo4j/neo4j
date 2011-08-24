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
import org.neo4j.cypher.SyntaxException


trait MatchClause extends JavaTokenParsers with Tokens {

  val namer = new NodeNamer

  def matching: Parser[Match] = ignoreCase("match") ~> rep1sep(path, ",") ^^ { case matching:List[List[Pattern]] => Match(matching.flatten: _*) }

  def path = pathSegment | parenPath | noParenPath

  def parenPath : Parser[List[Pattern]] = identity ~ "=" ~ "(" ~ pathSegment ~ ")" ^^ {
    case p ~ "=" ~ "(" ~ pathSegment ~ ")" => List(PathItem(p, pathSegment: _*))
  }

  def noParenPath : Parser[List[Pattern]] = identity ~ "=" ~ pathSegment ^^ {
    case p ~ "=" ~ pathSegment => List(PathItem(p, pathSegment: _*))
  }

  def pathSegment: Parser[List[Pattern]] = node ~ rep1(relatedTail) ^^ {
    case head ~ tails => {
      var fromNode = namer.name(head)
      val list = tails.map(item => item match { case (back, rel, relType, forward, end) => {
        val toNode = namer.name(end)
        val relName = namer.name(rel)
        val result: Pattern = RelatedTo(fromNode, toNode, relName, relType, getDirection(back, forward))

        fromNode = toNode

        result
      }})

      list
    }
  }
  private def getDirection(back:Option[String], forward:Option[String]):Direction =
    (back.nonEmpty, forward.nonEmpty) match {
      case (true,false) => Direction.INCOMING
      case (false,true) => Direction.OUTGOING
      case _ => Direction.BOTH
    }

  class NodeNamer {
    var lastNodeNumber = 0

    def name(s:Option[String]):String = s match {
      case None => {
        lastNodeNumber += 1
        "  UNNAMED" + lastNodeNumber
      }
      case Some(x) => x
    }
  }

  def node:Parser[Option[String]] =  parensNode | relatedNode

  def parensNode:Parser[Option[String]] = "(" ~> opt(identity) <~ ")"

  def relatedNode:Parser[Option[String]] = opt(identity) ^^ {
    case None => throw new SyntaxException("Matching nodes without identifiers have to have parenthesis: ()")
    case x => x
  }

  def relatedTail = opt("<") ~ "-" ~ opt("[" ~> relationshipInfo  <~ "]") ~ "-" ~ opt(">") ~ node ^^ {
    case back ~ "-" ~ relInfo ~ "-" ~ forward ~ end => relInfo match {
      case Some((relName, relType)) => (back, relName, relType, forward, end)
      case None => (back, None, None, forward, end)
    }
  }

  def relationshipInfo:Parser[(Option[String],Option[String])] = opt(identity) ~ opt(":" ~ identity) ^^ {
    case relName  ~ Some(":" ~ relType) => (relName, Some(relType))
    case relName  ~ None => (relName, None)
  }
}







