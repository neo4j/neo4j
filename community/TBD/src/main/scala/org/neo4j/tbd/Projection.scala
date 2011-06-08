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
package org.neo4j.tbd

import commands.Clause
import scala.collection.JavaConverters._
import org.apache.commons.lang.StringUtils
import pipes.Pipe
import org.neo4j.graphmatching.{PatternRelationship, PatternMatch, PatternNode, PatternMatcher}
import collection.immutable.Map
import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}
import collection.Iterable

class Projection(pNodes: Map[String, PatternNode],
                 pRels: Map[String, PatternRelationship],
                 startPoints: Pipe,
                 select: Seq[Map[String, Any] => Map[String, Any]],
                 filter: Clause) extends ExecutionResult
{
  def bindStartPoints[U](x: (String, Any))
  {
    val identifier = x._1
    val entity = x._2
    entity match
    {
      case node: Node => pNodes(identifier).setAssociation(node)
      case rel: Relationship => pRels(identifier).setAssociation(rel)
    }
  }

  def getPatternMatches(fromRow: Map[String, Any]):  Iterable[PatternMatch] = {
    val startKey = fromRow.keys.head
    val startPNode = pNodes(startKey)
    val startNode = fromRow(startKey).asInstanceOf[Node]
    PatternMatcher.getMatcher.`match`(startPNode, startNode).asScala
  }

  def foreach[U](f: Map[String, Any] => U)
  {
    startPoints.foreach((fromRow) =>
    {
      fromRow.foreach( bindStartPoints(_) )

      val patternMatches = getPatternMatches(fromRow)

      patternMatches.map((aMatch) =>
      {
        val realResult: Map[String, Any] =
          pNodes.map((kv) => kv._1 -> aMatch.getNodeFor(kv._2)) ++
            pRels.map((kv) => kv._1 -> aMatch.getRelationshipFor(kv._2))

        if ( filter.isMatch(realResult) )
        {
          val r = select.map((transformer) => transformer.apply(realResult)).reduceLeft(_ ++ _)

          f.apply(r)
        }
      })

    })
  }

  //TODO: This is horrible. The Result object must know it's metadata in a state that doesn't force us to loop

}