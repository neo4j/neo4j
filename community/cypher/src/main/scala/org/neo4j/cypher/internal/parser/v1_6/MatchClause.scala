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
package org.neo4j.cypher.internal.parser.v1_6

import org.neo4j.cypher.SyntaxException
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands._

trait MatchClause extends Base {
  val namer = new NodeNamer

  def matching: Parser[(Match, NamedPaths)] = 
    correctMatch |
  ignoreCase("match") ~> failure("invalid pattern")

  def correctMatch = ignoreCase("match") ~> comaList(path) ^^ {
    case matching => {
      val unamedPaths: List[Pattern] = matching.filter(_.isInstanceOf[List[Pattern]]).map(_.asInstanceOf[List[Pattern]]).flatten ++ matching.filter(_.isInstanceOf[Pattern]).map(_.asInstanceOf[Pattern])
      val namedPaths: List[NamedPath] = matching.filter(_.isInstanceOf[NamedPath]).map(_.asInstanceOf[NamedPath])

      (Match(unamedPaths: _*), NamedPaths(namedPaths: _*))
    }
  }

  def path: Parser[Any] =
    (pathSegment
      | parenPath
      | failure("expected identifier"))

  def parenPath: Parser[Any] = identity ~ "=" ~ optParens(pathSegment) ^^ {
    case p ~ "=" ~ pathSegment => {
      if (pathSegment.size == 1 && pathSegment.head.isInstanceOf[PathPattern])
        pathSegment.head.asInstanceOf[PathPattern].cloneWithOtherName(p).asInstanceOf[Pattern]
      else
        NamedPath(p, pathSegment: _*)
    }
  }

  def pathSegment: Parser[List[Pattern]] = relatedTos | shortestPath

  def singlePathSegment: Parser[Pattern] = onlyOne("expected single path segment", relatedTos)

  def optionRelName(relName: String): Option[String] =
    if (relName.startsWith("  UNNAMED"))
      None
    else
      Some(relName)

  def shortestPath: Parser[List[Pattern]] = (ignoreCase("shortestPath") | ignoreCase("allShortestPaths")) ~ parens(singlePathSegment) ^^ {
    case algo ~ relInfo => {

      val single = algo match {
        case "shortestpath" => true
        case "allshortestpaths" => false
      }

      relInfo match {
        case RelatedTo(left, right, relName, relType, direction, optional,_) => List(ShortestPath(namer.name(None), left, right, relType, direction, Some(1), optional, single, optionRelName(relName), True()))
        case VarLengthRelatedTo(pathName, start, end, minHops, maxHops, relType, direction, relIterable, optional, _) => {
          if (minHops.nonEmpty) {
            throw new SyntaxException("Shortest path does not support a minimal length", "quert", 666)
          }
          List(ShortestPath(namer.name(None), start, end, relType, direction, maxHops, optional, single, relIterable, True()))
        }
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

  def node: Parser[Option[String]] =
    (parensNode
      | relatedNode
      | failure("expected node identifier"))

  def parensNode: Parser[Option[String]] = parens(opt(identity))

  def relatedNode: Parser[Option[String]] = identity ^^ (x => Some(x)) 

  def relatedTail = workingLink |  
    opt("<") ~> "-" ~> opt("[" ~> relationshipInfo ~> "]") ~> failure("expected -") |
    opt("<") ~ "-" ~ "[" ~> relationshipInfo ~> failure("unclosed bracket") |
    opt("<") ~ "-" ~ "[" ~> failure("expected relationship information") |
    opt("<") ~ "-" ~> failure("expected [ or -") |
    opt("<") ~> failure("w7")

  def workingLink = opt("<") ~ "-" ~ opt("[" ~> relationshipInfo <~ "]") ~ "-" ~ opt(">") ~ node ^^ {
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
