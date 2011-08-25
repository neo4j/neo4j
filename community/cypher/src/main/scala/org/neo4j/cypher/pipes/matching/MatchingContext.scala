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
//package org.neo4j.cypher.pipes.matching
//
//import org.neo4j.cypher.commands.{RelatedTo, Pattern}
//import org.neo4j.graphmatching.{PatternNode, PatternElement}
//import org.neo4j.graphdb.Direction
//
//class MatchingContext(patterns : Seq[Pattern]) {
//
//  def getMatches(bindings : Map[String, Any]) : Seq[Map[String, Any]] = {
//    val patternGraph: Seq[PatternElement] = buildPatternGraph(bindings)
//    val (pinnedName, pinnedNode) = bindings.head
//
//    val pinnedPatternNode = patternGraph.find(_.key == pinnedName)
//
////    pinnedPatternNode.
//
//    Seq()
//  }
//
//  abstract class PatternElement(val key: String)
//
//  class PatternNode(key: String) extends PatternElement(key) {
//  }
//
//  class PatternRelationship(key: String, leftNode: PatternNode, rightNode: PatternNode, relType: Option[String],  dir: Direction) extends PatternElement(key) {
//  }
//
//  def buildPatternGraph(bindings: Map[String, Any]): Seq[PatternElement] = {
//
//    // validate pattern
//
//    val patternNodeMap: scala.collection.mutable.Map[String, PatternNode] = scala.collection.mutable.Map()
//    val patternRelMap: scala.collection.mutable.Map[String, PatternRelationship] = scala.collection.mutable.Map()
//
//    patterns.foreach(_ match {
//      case RelatedTo(left, right, rel, relType, dir) => {
//        val leftNode: PatternNode = patternNodeMap.getOrElseUpdate(left, new PatternNode(left))
//        val rightNode: PatternNode = patternNodeMap.getOrElseUpdate(right, new PatternNode(right))
//        patternRelMap(rel) = new PatternRelationship(rel, leftNode, rightNode, relType, dir)
//      }
//    })
//
//    (patternNodeMap.values ++ patternRelMap.values).toSeq
//  }
//
//}