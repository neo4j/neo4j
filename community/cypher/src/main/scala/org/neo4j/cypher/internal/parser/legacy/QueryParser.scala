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
package org.neo4j.cypher.internal.parser.legacy

import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions.AggregationExpression
import org.neo4j.cypher.internal.mutation.UpdateAction
import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.commands.SortItem
import org.neo4j.cypher.internal.commands.Slice
import org.neo4j.cypher.internal.commands.Return
import org.neo4j.cypher.SyntaxException
import org.neo4j.helpers.ThisShouldNotHappenError


trait QueryParser
  extends Base
  with StartAndCreateClause
  with MatchClause
  with Using
  with WhereClause
  with OrderByClause
  with ReturnClause
  with Updates
  with SkipLimitClause
{
  def query: Parser[Query] = Parser {
    in =>
      val result = (opt(queryStart) ~ body).apply(in)
      result match {
        case Success(Some(start) ~ body, rest) =>

          val q = expandQuery(start, body)
          if (isNonMutatingQueryWithoutReturn(q))
            failure("expected return clause", rest)
          else
            Success(q, rest)

        case Success(None ~ body, rest)
          if body.isInstanceOf[BodyReturn] =>

          Success(expandQuery(body), rest)

        case ns:NoSuccess =>
          ns
      }
  }

  def body: Parser[Body] = implicitWith | bodyWith | bodyReturn | noBody | failure("expected valid query body")

  def implicitWith: Parser[Body] = atLeastOneUpdateCommand ~ body ^^ {
    case updateCmds ~ nextQ => {

      val (updates, startAst) = updateCmds
      val returns = Return(List("*"), AllIdentifiers())

      val start = QueryStart(startAst, Seq.empty, Seq.empty, updates, True())

      BodyWith("implicit_with", None, Seq.empty, returns, None, start, nextQ)
    }
  }

  def bodyWith: Parser[Body] = withClause ~ opt(order) ~ slice ~ opt(afterWith) ~ body ^^ {
    case returns ~ order ~ slice ~ maybeStart ~ nextQ => {
      val start = maybeStart.getOrElse(QueryStart.empty)

      BodyWith("with", slice, order.toSeq.flatten, returns._1, returns._2, start, nextQ)
    }
  }

  def bodyReturn: Parser[Body] = returns ~ opt(order) ~ slice ^^ {
    case returns ~ order ~ slice => {

      BodyReturn(slice, order.toSeq.flatten, returns._1, returns._2)
    }
  }

  private def noBody: Parser[Body] = opt(";") ~> "$".r ^^^ NoBody()

  protected def queryStart = optManQuery(mandatory = true, "invalid start of query")

  protected def afterWith = optManQuery(mandatory = false, "invalid query part")

  protected def optManQuery(mandatory:Boolean, failureText: String):Parser[QueryStart] =
    // almost everything is optional so we take the one that parses the most
    longestOf(failureText, updatingStart(mandatory), explicitStart(mandatory), matchStart(mandatory))

  private def optMan[T](mandatory: Boolean, parser: Parser[T]): Parser[Option[T]] =
    if (mandatory)
        parser ^^ Some.apply
    else
        opt(parser)

  private def explicitStart(mandatory: Boolean) = optMan(mandatory, readStart) ~ opt(matching) ~ hints ~ opt(where) ^^  {
    case start ~ matching ~ hints ~ where =>
      val (pattern, matchPaths, matchPredicate) = extractMatches(matching)
      val startAst = start.getOrElse(StartAst())
      val predicate = where.getOrElse(True()).andWith(matchPredicate)

      QueryStart(startAst.copy(namedPaths = startAst.namedPaths ++ matchPaths), pattern, hints, Seq.empty, predicate)
  }

  def isNonMutatingQueryWithoutReturn(q: Query) = q.returns == Return(List()) && !q.start.forall(_.mutating)

  private def matchStart(mandatory: Boolean) = optMan(mandatory, matching) ~ hints ~ opt(where) ^^ {
    case matching ~ hints ~ where =>
      val (pattern, matchPaths, matchPredicate) = extractMatches(matching)
      val predicate = where.getOrElse(True()).andWith(matchPredicate)

      QueryStart(StartAst(namedPaths = matchPaths), pattern, hints, Seq.empty, predicate)
  }

  def updatingStart(mandatory: Boolean) = optMan(mandatory, createStart) ~ opt(updates) ^^ {
    case starts ~ updates =>
      val startAst = starts.getOrElse(StartAst())

      QueryStart(startAst, Seq.empty, Seq.empty, updates.toSeq.flatten, True())
  }

  private def slice = opt(skip) ~ opt(limit) ^^ {
    case None ~ None => None
    case s ~ l       => Some(Slice(s, l))
  }

  def checkForAggregates(where: Predicate) {
    if (where.exists(_.isInstanceOf[AggregationExpression]))
      throw new SyntaxException("Can't use aggregate functions in the WHERE clause.")
  }

  private def expandQuery(start: QueryStart, body: Body): Query =
    body match {
      case b: BodyWith =>
        checkForAggregates(start.predicate)
        val next = expandQuery(b.nextStart, b.next)
        Query(b.returns, start.starts.startItems, start.updates ++ start.starts.updateActions, start.patterns, start.hints, start.predicate, b.aggregate, b.order, b.slice, start.starts.namedPaths, Some(next))

      case b: BodyReturn =>
        checkForAggregates(start.predicate)
        Query( b.returns, start.starts.startItems, start.updates ++ start.starts.updateActions, start.patterns, start.hints, start.predicate, b.aggregate, b.order, b.slice, start.starts.namedPaths, None)

      case NoBody() =>
        Query( Return(List()), start.starts.startItems, start.updates ++ start.starts.updateActions, Seq(), start.hints, True(), None, Seq(), None, start.starts.namedPaths, None)
    }

  private def expandQuery(b: Body): Query = b match {
    case body:BodyReturn=> Query(body.returns, Seq.empty, Seq.empty, Seq.empty, Seq.empty, True(), body.aggregate,
      body.order, body.slice, Seq.empty, None)
    case _ => throw new ThisShouldNotHappenError("Andres","This is just here to stop parser warnings")
  }


  private def extractMatches(matching: Option[(Seq[Pattern], Seq[NamedPath], Predicate)]):(Seq[Pattern],
    Seq[NamedPath], Predicate) = matching match {
    case Some((a, b, c)) => (a, b, c)
    case None            => (Seq(), Seq(), True())
  }

  private def updateCommands: Parser[(Seq[UpdateAction], StartAst)] = opt(createStart) ~ updates ^^ {
    case starts ~ updates =>
      val startAst = starts.getOrElse(StartAst())

      (updates, startAst)
  }

  private def atLeastOneUpdateCommand: Parser[(Seq[UpdateAction], StartAst)] = Parser {
    case in => updateCommands(in) match {
      case Success((changes, startAst), rest) if startAst.isEmpty && changes.isEmpty => Failure("expected at least one update", rest)
      case x                                                                           => x
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
  case class BodyReturn(slice: Option[Slice],
                        order: Seq[SortItem],
                        returns: Return,
                        aggregate: Option[Seq[AggregationExpression]]) extends Body

  /*
  If a Body is an intermediate part, either explicitly with WITH, or implicitly when first MATCHing and then updating the graph, this structure will be used.

  This structure has three parts
   */
  case class BodyWith(name:String,
                      slice: Option[Slice],
                      order: Seq[SortItem],
                      returns: Return,
                      aggregate: Option[Seq[AggregationExpression]],
                      nextStart: QueryStart,      // These items belong to the query part before the WITH delimiter
                      next: Body) extends Body

  /*This is the plug used when a query doesn't end in RETURN.*/
  case class NoBody() extends Body

  object QueryStart {
    def empty = QueryStart(StartAst(), Seq.empty, Seq.empty, Seq.empty, True())
  }

  case class QueryStart(starts: StartAst,
                        patterns: Seq[Pattern],
                        hints: Seq[StartItem with Hint],
                        updates: Seq[UpdateAction],
                        predicate: Predicate)

}