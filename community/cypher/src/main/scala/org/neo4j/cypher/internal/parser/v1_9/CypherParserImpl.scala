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
package org.neo4j.cypher.internal.parser.v1_9

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.parser.ActualParser
import org.neo4j.cypher.internal.commands._
import expressions.AggregationExpression
import org.neo4j.cypher.internal.ReattachAliasedExpressions
import org.neo4j.cypher.internal.mutation.UpdateAction

class CypherParserImpl extends Base
with StartClause
with MatchClause
with WhereClause
with ReturnClause
with SkipLimitClause
with OrderByClause
with Updates
with ActualParser {
  @throws(classOf[SyntaxException])
  def parse(text: String): AbstractQuery = {
    namer = new NodeNamer
    parseAll(query, text) match {
      case Success(r, q) => ReattachAliasedExpressions(r.setQueryText(text))
      case NoSuccess(message, input) => {
        if (message.startsWith("INNER"))
          throw new SyntaxException(message.substring(5), text, input.offset)
        else
          throw new SyntaxException(message + """

Think we should have better error message here? Help us by sending this query to cypher@neo4j.org.

Thank you, the Neo4j Team.
""", text, input.offset)
      }
    }
  }

  def query = start ~ body <~ opt(";") ^^ {
    case start ~ body => {
      val q: Query = expandQuery(start._1, start._2, Seq(), body)

      if (q.returns == Return(List.empty) &&
        !q.start.forall(_.mutating)) {
        throw new SyntaxException("Non-mutating queries must return data")
      }

      q
    }
  }

  def body = bodyWith | simpleUpdate | bodyReturn | noBody

  def simpleUpdate: Parser[Body] = opt(matching) ~ opt(where) ~ atLeastOneUpdateCommand ~ body ^^ {
    case matching ~ where ~ updateCmds ~ nextQ => {
      val (updates, startItems, paths) = updateCmds
      val (pattern, namedPaths) = extractMatches(matching)

      val returns = Return(List("*"), AllIdentifiers())
      BodyWith(updates, pattern, namedPaths, None, where, Seq(), returns, None, startItems, paths, nextQ)
    }
  }

  def bodyWith: Parser[Body] = opt(matching) ~ opt(where) ~ WITH ~ opt(order) ~ opt(skip) ~ opt(limit) ~ opt(start) ~ updates ~ body ^^ {
    case matching ~ where ~ returns ~ order ~ skip ~ limit ~ start ~ updates ~ nextQ => {
      val (pattern, matchPaths) = extractMatches(matching)
      val startItems = start.toSeq.flatMap(_._1)
      val startPaths = start.toSeq.flatMap(_._2)
      val slice = (skip, limit) match {
        case (None, None) => None
        case (s, l) => Some(Slice(s, l))
      }

      BodyWith(updates._1, pattern, matchPaths ++ updates._2, slice, where, order.toSeq.flatten, returns._1, returns._2, startItems, startPaths, nextQ)
    }
  }

  def bodyReturn: Parser[Body] = opt(matching) ~ opt(where) ~ returns ~ opt(order) ~ opt(skip) ~ opt(limit) ^^ {
    case matching ~ where ~ returns ~ order ~ skip ~ limit => {
      val slice = (skip, limit) match {
        case (None, None) => None
        case (s, l) => Some(Slice(s, l))
      }

      val (pattern, namedPaths) = extractMatches(matching)
      BodyReturn(pattern, namedPaths, slice, where, order.toSeq.flatten, returns._1, returns._2)
    }
  }

  def noBody: Parser[Body] = opt(";") ~> "$".r ^^ (x => NoBody())

  def checkForAggregates(where: Option[Predicate]) {
    where match {
      case Some(w) => if (w.exists(_.isInstanceOf[AggregationExpression])) throw new SyntaxException("Can't use aggregate functions in the WHERE clause.")
      case _ =>
    }
  }

  private def expandQuery(start: Seq[StartItem], namedPaths: Seq[NamedPath], updates: Seq[UpdateAction], body: Body): Query = body match {
    case b: BodyWith => {
      checkForAggregates(b.where)
      Query(b.returns, start, updates, b.matching, Seq(), b.where.getOrElse(True()), b.aggregate, b.order, b.slice, b.namedPath ++ namedPaths, Some(expandQuery(b.start, b.startPaths, b.updates, b.next)))
    }
    case b: BodyReturn => {
      checkForAggregates(b.where)
      Query(b.returns, start, updates, b.matching, Seq(), b.where.getOrElse(True()), b.aggregate, b.order, b.slice, b.namedPath ++ namedPaths, None)
    }
    case NoBody() => {
      Query(Return(Nil), start, updates, Seq(), Seq(), True(), None, Seq(), None, namedPaths, None)
    }
  }

  override def handleWhiteSpace(source: CharSequence, offset: Int): Int = {
    if (offset >= source.length())
      return offset

    val a = source.charAt(offset)

    if ((a == ' ') || (a == '\r') || (a == '\t') || (a == '\n'))
      handleWhiteSpace(source, offset + 1)
    else if ((offset + 1) >= source.length())
      offset
    else {
      val b = source.charAt(offset + 1)

      if ((a == '/') && (b == '/')) {

        var loop = 0
        while ((offset + loop) < source.length() && !(source.charAt(offset + loop) == '\n')) {
          loop = loop + 1
        }

        handleWhiteSpace(source, loop + offset)
      } else {
        offset
      }
    }
  }

  private def extractMatches(matching: Option[(Seq[Pattern], Seq[NamedPath])]): (Seq[Pattern], Seq[NamedPath]) = matching match {
    case Some((a,b)) => (a,b)
    case None => (Seq(), Seq())
  }

  private def updateCommands: Parser[(Seq[UpdateAction], Seq[StartItem], Seq[NamedPath])] = opt(createStart) ~ updates ^^ {
    case starts ~ updates =>
      val createCommands = starts.toSeq.flatMap(_._1)
      val paths = starts.toSeq.flatMap(_._2) ++ updates._2
      val updateActions = updates._1

      (updateActions , createCommands, paths)
  }

  private def atLeastOneUpdateCommand: Parser[(Seq[UpdateAction], Seq[StartItem], Seq[NamedPath])] = Parser {
    case in => updateCommands(in) match {
      case Success((changes, starts, paths), rest) if (starts.size + changes.size) == 0 => Failure("", rest)
      case x => x
    }
  }
}

/*
A query is split up into a start-part, and one or more Body parts, like a linked list. The start part is either a
START clause or a CREATE clause, and the body can be one of three: BodyReturn, BodyWith, NoBody
 */

abstract sealed class Body

/*
This Body is used when a query ends in a RETURN clause. Once you RETURN, no more query parts are allowed, so this structure
is one of two possible query tails
 */
case class BodyReturn(matching: Seq[Pattern], namedPath: Seq[NamedPath], slice: Option[Slice], where: Option[Predicate], order: Seq[SortItem], returns: Return, aggregate: Option[Seq[AggregationExpression]]) extends Body

/*
If a Body is an intermediate part, either explicitly with WITH, or implicitly when first MATCHing and then updating the graph, this structure will be used.

This structure has three parts
 */
case class BodyWith(updates:Seq[UpdateAction], matching: Seq[Pattern], namedPath: Seq[NamedPath], slice: Option[Slice], where: Option[Predicate], order:Seq[SortItem], returns: Return, aggregate: Option[Seq[AggregationExpression]],// These items belong to the query part before the WITH delimiter
                    start:Seq[StartItem], startPaths:Seq[NamedPath],                                                                                                                       // These are START or CREATE clauses directly following WITH
                    next: Body) extends Body                                                                                                                                               // This is the pointer to the next query part

/*
This is the plug used when a query doesn't end in RETURN.
 */
case class NoBody() extends Body
