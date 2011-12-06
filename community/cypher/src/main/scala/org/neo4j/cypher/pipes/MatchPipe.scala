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
package org.neo4j.cypher.pipes

import matching.MatchingContext
import org.neo4j.cypher.commands._
import java.lang.String
import org.neo4j.cypher.symbols.{IterableType, RelationshipType, NodeType, Identifier}

class MatchPipe(source: Pipe, patterns: Seq[Pattern], predicates:Seq[Predicate]) extends Pipe {
  val matchingContext = new MatchingContext(patterns, source.symbols, predicates)
  val identifiers = patterns.flatMap(_ match {
    case RelatedTo(left, right, rel, relType, dir, optional) => Seq(Identifier(left, NodeType()), Identifier(right, NodeType()), Identifier(rel, RelationshipType()))
    case VarLengthRelatedTo(pathName, left, right, minHops, maxHops, relType, dir, iterableRel, optional) => Seq(Identifier(left, NodeType()), Identifier(right, NodeType())) ++ iterableOfRelationships(iterableRel)
    case _ => Seq()
  })
  val symbols = source.symbols.add(identifiers:_*)

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(sourcePipeRow => {
      matchingContext.getMatches(sourcePipeRow).foreach(f)
    })
  }

  private def iterableOfRelationships(iterableRel:Option[String]): Option[Identifier] = iterableRel match {
    case None => None
    case Some(r) => Some(Identifier(r, new IterableType(RelationshipType())))
  }

  override def executionPlan(): String = source.executionPlan() + "\r\nPatternMatch(" + patterns.mkString(",") + ")"
}