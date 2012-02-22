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
package org.neo4j.cypher.internal.parser.v1_7

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.parser.ActualParser
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.ReattachAliasedExpressions

class CypherParserImpl extends Base
with StartClause
with MatchClause
with WhereClause
with ReturnClause
with SkipLimitClause
with OrderByClause
with ActualParser {

  def query: Parser[String => Query] = start ~ opt(matching) ~ opt(where) ~ returns ~ opt(order) ~ opt(skip) ~ opt(limit) ^^ {

    case start ~ matching ~ where ~ returns ~ order ~ skip ~ limit => {
      val slice = (skip, limit) match {
        case (None, None) => None
        case (s, l) => Some(Slice(s, l))
      }

      val (pattern: Option[Match], namedPaths: Option[NamedPaths]) = matching match {
        case Some((p, NamedPaths())) => (Some(p), None)
        case Some((Match(), nP)) => (None, Some(nP))
        case Some((p, nP)) => (Some(p), Some(nP))
        case None => (None, None)
      }
      
      where match {
        case Some(w) => if(w.exists(_.isInstanceOf[AggregationExpression])) throw new SyntaxException("Can't use aggregate functions in the WHERE clause. Move it to the HAVING clause.")
        case _ =>
      }
      (queryText: String) => Query(returns._1, start, pattern, where, returns._2, order, slice, namedPaths, queryText)
    }
  }

  def createProperty(entity: String, propName: String): Expression = Property(entity, propName)

  @throws(classOf[SyntaxException])
  def parse(queryText: String): Query = parseAll(query, queryText) match {
    case Success(r, q) => ReattachAliasedExpressions(r(queryText))
    case NoSuccess(message, input) => {
      if (message.startsWith("INNER"))
        throw new SyntaxException(message.substring(5), queryText, input.offset)
      else
        throw new SyntaxException(message + """
Unfortunately, you have run into a syntax error that we don't have a nice message for.
By sending the query that produced this error to cypher@neo4j.org, you'll save the
puppies and get better error messages in our next release.

Thank you, the Neo4j Team.""")
    }
  }

  override def handleWhiteSpace(source: CharSequence, offset: Int): Int = {
    if (offset >= source.length())
      return offset

    val a = source.charAt(offset)

    if ((a == ' ') || (a == '\r') || (a == '\t') || (a == '\n'))
      handleWhiteSpace(source, offset + 1)
    else if ((offset+1) >= source.length())
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
}