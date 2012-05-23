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
package org.neo4j.cypher.internal.parser.v1_8

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.parser.ActualParser
import org.neo4j.cypher.internal.commands._
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
  def parse(text: String): Query = {
    namer = new NodeNamer
    parseAll(query, text) match {
      case Success(r, q) => ReattachAliasedExpressions(r.copy(queryString = text))
      case NoSuccess(message, input) => {
        if (message.startsWith("INNER"))
          throw new SyntaxException(message.substring(5), text, input.offset)
        else
          throw new SyntaxException(message + """
Unfortunately, you have run into a syntax error that we don't have a nice message for.
By sending the query that produced this error to cypher@neo4j.org, you'll save the
puppies and get better error messages in our next release.

Thank you, the Neo4j Team.
""", text, input.offset)
      }
    }
  }

  def query = start ~ body <~ opt(";") ^^ {
    case start ~ body => {
      val q: Query = expandQuery(start, Seq(), body)

      if (q.returns == Return(List()) &&
        !q.start.startItems.forall(_.mutating)) {
        throw new SyntaxException("Non-mutating queries must return data")
      }

      q
    }
  }

  def body = bodyWith | simpleUpdate | bodyReturn | noBody

  def simpleUpdate: Parser[Body] = opt(matching) ~ opt(where) ~ atLeastOneUpdateCommand ~ body ^^ {
    case matching ~ where ~ updates ~ nextQ => {
      val (pattern, namedPaths) = extractMatches(matching)

      val returns = Return(List("*"), AllIdentifiers())
      BodyWith(updates._1, pattern, namedPaths, where, returns, None, updates._2, nextQ)
    }
  }

  def bodyWith: Parser[Body] = opt(matching) ~ opt(where) ~ WITH ~ opt(start) ~ updates ~ body ^^ {
    case matching ~ where ~ returns ~ start ~ updates ~ nextQ => {
      val (pattern, namedPaths) = extractMatches(matching)
      val startItems = start match {
        case None => Seq()
        case Some(s) => s.startItems
      }
      BodyWith(updates, pattern, namedPaths, where, returns._1, returns._2, startItems, nextQ)
    }
  }

  def bodyReturn: Parser[Body] = opt(matching) ~ opt(where) ~ returns ~ opt(order) ~ opt(skip) ~ opt(limit) ^^ {
    case matching ~ where ~ returns ~ order ~ skip ~ limit => {
      val slice = (skip, limit) match {
        case (None, None) => None
        case (s, l) => Some(Slice(s, l))
      }

      val (pattern, namedPaths) = extractMatches(matching)
      BodyReturn(pattern, namedPaths, slice, where, order, returns._1, returns._2)
    }
  }

  def noBody: Parser[Body] = opt(";") ~> "$".r ^^ (x => NoBody())

  def checkForAggregates(where: Option[Predicate]) {
    where match {
      case Some(w) => if (w.exists(_.isInstanceOf[AggregationExpression])) throw new SyntaxException("Can't use aggregate functions in the WHERE clause.")
      case _ =>
    }
  }

  private def expandQuery(start: Start, updates: Seq[UpdateAction], body: Body): Query = body match {
    case b: BodyWith => {
      checkForAggregates(b.where)
      Query(b.returns, start, updates, b.matching, b.where, b.aggregate, None, None, b.namedPath, Some(expandQuery(Start(b.start:_*), b.updates, b.next)))
    }
    case b: BodyReturn => {
      checkForAggregates(b.where)
      Query(b.returns, start, updates, b.matching, b.where, b.aggregate, b.order, b.slice, b.namedPath, None)
    }
    case NoBody() => {
      Query(Return(List()), start, updates, None, None, None, None, None, None, None)
    }
  }

  def createProperty(entity: String, propName: String): Expression = Property(entity, propName)

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

  private def extractMatches(matching: Option[(Match, NamedPaths)]): (Option[Match], Option[NamedPaths]) = matching match {
    case Some((p, NamedPaths())) => (Some(p), None)
    case Some((Match(), nP)) => (None, Some(nP))
    case Some((p, nP)) => (Some(p), Some(nP))
    case None => (None, None)
  }

  private def updateCommands:Parser[(Seq[UpdateAction], Seq[StartItem])] = opt(createStart) ~ updates ^^ {
    case starts ~ updates => {

      val createCommands = starts.map( _.startItems).flatten.toSeq
      (updates,  createCommands)
    }
  }

  private def atLeastOneUpdateCommand:Parser[(Seq[UpdateAction], Seq[StartItem])] = Parser {
    case in => {
      updateCommands(in) match {
        case Success((changes, starts), rest) if (starts.size + changes.size) == 0 => Failure("", rest)
        case x => x
      }
    }
  }
}

abstract sealed class Body

case class BodyReturn(matching: Option[Match], namedPath: Option[NamedPaths], slice: Option[Slice], where: Option[Predicate], order: Option[Sort], returns: Return, aggregate: Option[Aggregation]) extends Body

case class BodyWith(updates:Seq[UpdateAction], matching: Option[Match], namedPath: Option[NamedPaths], where: Option[Predicate], returns: Return, aggregate: Option[Aggregation], start:Seq[StartItem], next: Body) extends Body

case class NoBody() extends Body