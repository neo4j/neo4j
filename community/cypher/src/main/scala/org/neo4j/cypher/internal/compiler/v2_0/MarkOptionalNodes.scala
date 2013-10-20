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
package org.neo4j.cypher.internal.compiler.v2_0

import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import scala.annotation.tailrec

/*
This class is concerned with going over the patterns in a query and explicitly marking parts of the pattern that is
optional as such, so other parts of Cypher doesn't have to infer this information
 */
object MarkOptionalNodes {

  def apply(in: AbstractQuery): AbstractQuery = in match {
    case q: Query => rewriteInferNodeOptionality(q, Set.empty)
    case u: Union => u.copy(queries = u.queries.map(q => rewriteInferNodeOptionality(q, Set.empty)))
    case _        => in
  }

  private def rewriteInferNodeOptionality(query: Query, alreadyBoundNodes: Set[String]): Query = {
    val boundStartNames: Set[String] = (for (
      startItem <- query.start;
      identifier <- startItem.identifiers)
    yield identifier._1).toSet

    val names: Set[String] = boundStartNames ++ alreadyBoundNodes

    val boundNames = collectBoundNames(query.matching, names)

    lazy val exportedNames: Set[String] = query.returns.returnItems.flatMap {
      case ReturnItem(Identifier(_), newName, _) => Some(newName)
      case AllIdentifiers()                      => boundNames
      case _                                     => None
    }.toSet

    query.copy(matching = handleOptionalsForCypher2Temporary(boundNames, query),
      tail = query.tail.map(t => rewriteInferNodeOptionality(t, exportedNames)))
  }

  /**
   * In Cypher 2.0, we want the users to explicitly mark optional nodes, but in 1.9, the node optionality is inferred.
   *
   * This is a stepping stone until users actually can mark nodes as optional. For now we skip the inference if there
   * are no start items in the first query part.
   */
  private def handleOptionalsForCypher2Temporary(boundNames: Set[String], query: Query): Seq[Pattern] = {
    if (boundNames.isEmpty || !query.matching.exists(_.optional)) query.matching
    else query.matching.map(markOptionals(boundNames))
  }

  @tailrec
  private def collectBoundNames(patterns: Seq[Pattern], names: Set[String]): Set[String] = {
    if (patterns.isEmpty) names
    else {
      val (unprocessedPatterns, newNames) = bindNames(patterns, names)
      if (unprocessedPatterns == patterns) newNames
      else collectBoundNames(unprocessedPatterns, newNames)
    }
  }

  @tailrec
  private def bindNames(patterns: Seq[Pattern], names: Set[String]): (Seq[Pattern], Set[String]) = patterns.find {
    p => p.possibleStartPoints.exists(t => names.contains(t._1)) && !p.optional
  } match {
    case Some(p) => bindNames(patterns.tail, names ++ p.possibleStartPoints.map(_._1))
    case None    => (patterns, names)
  }

  private def markOptionals(boundNames: Set[String])(part: Pattern): Pattern = part match {

    case p@RelatedTo(left, right, relName, _, _, _) if !boundNames(relName) && !boundNames(right.name) && !boundNames(left.name) =>
      p.copy(left = left.copy(optional = true), right = right.copy(optional = true), optional = true)

    case p@RelatedTo(_, right, _, _, _, true) if !boundNames(right.name) =>
      p.copy(right = right.copy(optional = true))

    case p@RelatedTo(left, _, _, _, _, true) if !boundNames(left.name) =>
      p.copy(left = left.copy(optional = true))

    case p@VarLengthRelatedTo(_, left, right, _, _, _, _, _, _) if !boundNames(left.name) && !boundNames(right.name) =>
      p.copy(left = left.copy(optional = true), right = right.copy(optional = true), optional = true)

    case p@VarLengthRelatedTo(_, _, right, _, _, _, _, _, true) if !boundNames(right.name) =>
      p.copy(right = right.copy(optional = true))

    case p@VarLengthRelatedTo(_, left, _, _, _, _, _, _, true) if !boundNames(left.name) =>
      p.copy(left = left.copy(optional = true))

    case p@ShortestPath(_, left, right, _, _, _, _, _, _) if !boundNames(left.name) && !boundNames(right.name) =>
      p.copy(left = left.copy(optional = true), right = right.copy(optional = true), optional = true)

    case p@ShortestPath(_, _, right, _, _, _, true, _, _) if !boundNames(right.name) =>
      p.copy(right = right.copy(optional = true))

    case p@ShortestPath(_, left, _, _, _, _, true, _, _) if !boundNames(left.name) =>
      p.copy(left = left.copy(optional = true))

    case _ => part
  }

}