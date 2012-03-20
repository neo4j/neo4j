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


import scala.util.parsing.combinator._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commands._

trait MatchClause extends JavaTokenParsers with Tokens {
  val namer = new NodeNamer

  def matching: Parser[(Match, NamedPaths)] = ignoreCase("match") ~> rep1sep(path, ",") ^^ {
    case matching => {
      val unamedPaths: List[Pattern] = matching.filter(_.isInstanceOf[List[Pattern]]).map(_.asInstanceOf[List[Pattern]]).flatten
      val namedPaths: List[NamedPath] = matching.filter(_.isInstanceOf[NamedPath]).map(_.asInstanceOf[NamedPath])

      (Match(unamedPaths: _*), NamedPaths(namedPaths: _*))
    }
  }

  def path: Parser[Any] = pathSegment | parenPath

  def parenPath: Parser[NamedPath] = identity ~ "=" ~ optParens(pathSegment) ^^ {
    case p ~ "=" ~ pathSegment => NamedPath(p, pathSegment: _*)
  }

  def pathSegment: Parser[List[Pattern]] = relatedTos | shortestPath

  def singlePathSegment: Parser[Pattern] = relatedTos ^^ {
    case p => if (p.length > 1)
      throw new SyntaxException("Shortest path does not support having multiple path segments.")
    else
      p.head
  }

  def shortestPath: Parser[List[Pattern]] = ignoreCase("shortestPath") ~> parens(singlePathSegment) ^^ {
    _ match {
      case RelatedTo(left, right, relName, relType, direction, optional, _) => List(ShortestPath(namer.name(None), left, right, relType, direction, Some(1), optional, true, None, True()))
      case VarLengthRelatedTo(pathName, start, end, minHops, maxHops, relType, direction, relIterable, optional, _) => {
        if (minHops.nonEmpty) {
          throw new SyntaxException("Shortest path does not support a minimal length")
        }
        List(ShortestPath(namer.name(None), start, end, relType, direction, maxHops, optional, true, None, True()))
      }
    }
  }

  def relatedTos: Parser[List[Pattern]] = node ~ rep1(relatedTail) ^^ {
    case head ~ tails => {
      var fromNode = namer.name(head)
      val list = tails.map(_ match {
        case (back, rel, relType, forward, end, varLength, optional) => {
          val toNode = namer.name(end)
          val dir = getDirection(back, forward)

          val result: Pattern = varLength match {
            case None => RelatedTo(fromNode, toNode, namer.name(rel), relType, dir, optional, True())
            case Some((minHops, maxHops)) => VarLengthRelatedTo(namer.name(None), fromNode, toNode, minHops, maxHops, relType, dir, rel, optional, True())
          }

          fromNode = toNode

          result
        }
      })

      list
    }
  }

  private def getDirection(back: Option[String], forward: Option[String]): Direction =
    (back.nonEmpty, forward.nonEmpty) match {
      case (true, false) => Direction.INCOMING
      case (false, true) => Direction.OUTGOING
      case _ => Direction.BOTH
    }

  class NodeNamer {
    var lastNodeNumber = 0

    def name(s: Option[String]): String = s match {
      case None => {
        lastNodeNumber += 1
        "  UNNAMED" + lastNodeNumber
      }
      case Some(x) => x
    }
  }

  def node: Parser[Option[String]] = parensNode | relatedNode

  def parensNode: Parser[Option[String]] = parens(opt(identity))

  def relatedNode: Parser[Option[String]] = opt(identity) ^^ {
    case None => throw new SyntaxException("Matching nodes without identifiers have to have parenthesis: ()")
    case x => x
  }

  def relatedTail = opt("<") ~ "-" ~ opt("[" ~> relationshipInfo <~ "]") ~ "-" ~ opt(">") ~ node ^^ {
    case back ~ "-" ~ relInfo ~ "-" ~ forward ~ end => relInfo match {
      case Some((relName, relType, varLength, optional)) => (back, relName, relType, forward, end, varLength, optional)
      case None => (back, None, Seq(), forward, end, None, false)
    }
  }

  private def intOrNone(s: Option[String]): Option[Int] = s match {
    case None => None
    case Some(x) => Some(x.toInt)
  }

  def relationshipInfo: Parser[(Option[String], Seq[String], Option[(Option[Int], Option[Int])], Boolean)] =
    opt(identity) ~ opt("?") ~ opt(":" ~> identity) ~ opt("*" ~ opt(wholeNumber) ~ opt("..") ~ opt(wholeNumber)) ^^ {
      case relName ~ optional ~ relType ~ varLength => {
        val hops = varLength match {
          case Some("*" ~ x ~ None ~ None) => Some((intOrNone(x), intOrNone(x)))
          case Some("*" ~ minHops ~ punktpunkt ~ maxHops) => Some((intOrNone(minHops), intOrNone(maxHops)))
          case None => None
        }
        (relName, relType.toSeq, hops, optional.isDefined)
      }
    }
}