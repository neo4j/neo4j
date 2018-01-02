/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import collection.mutable
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.SingleNode
import org.neo4j.cypher.internal.compiler.v2_3.commands.ShortestPath

object MatchPattern {

  type TUPLE = (Seq[String], Seq[MatchRelationship])

  def apply (patterns:Seq[Pattern]) : MatchPattern = if(patterns.isEmpty) {
      MatchPattern(Seq(), Seq())
    } else {
      def tuple(name:Option[String], from: String, to: String): TUPLE = {
        Seq(from, to) -> Seq(MatchRelationship(name, from, to))
      }

      val theThings: Seq[TUPLE] = patterns.map {
        case SingleNode(n, _, _)                               => Seq(n) -> Seq()
        case RelatedTo(from, to, r, _, _, _)                   => tuple(Some(r), from.name, to.name)
        case ShortestPath(_, from, to, _, _, _, _, _, _)          => tuple(None, from.name, to.name)
        case VarLengthRelatedTo(_, from, to, _, _, _, _, _, _) => tuple(None, from.name, to.name)
      }

      val (nodes, rels) = theThings.reduce( (a,b) => {
        val ((aNodes,aRels),(bNodes,bRels)) = (a,b)
        ( aNodes ++ bNodes, aRels ++ bRels )
      })

      new MatchPattern(nodes, rels)
    }
}

case class MatchRelationship(name:Option[String], from:String, to:String) {
  def contains(node: String):Boolean = from == node || to == node
}

case class MatchPattern(nodes:Seq[String], relationships:Seq[MatchRelationship]) {
  def isEmpty: Boolean = nodes.isEmpty && relationships.isEmpty
  def nonEmpty : Boolean = !isEmpty

  def possibleStartNodes : Seq[String] = nodes

  def disconnectedPatternsWithout (identifiers:Seq[String]): Seq[MatchPattern] =
    disconnectedPatterns.
      filterNot( _.containsIdentifierNamed( identifiers ) ).
      filter(_.nonEmpty)

  def containsIdentifierNamed(identifiers: Seq[String]): Boolean =
    nodes.exists(identifiers.contains) ||
    relationships.flatMap(_.name).exists(identifiers.contains)

  def disconnectedPatterns : Seq[MatchPattern] = if (nodes.isEmpty) {
    Seq(this)
  } else {

    val result = mutable.ListBuffer[MatchPattern]()
    var nodesLeft = nodes.toSet
    var relationshipsLeft = relationships.toSet

    var currentNodes= mutable.ListBuffer[String]()
    var currentRels = mutable.ListBuffer[MatchRelationship]()

    def popNextNode() {
      var current = nodesLeft.head
      nodesLeft -= current
      currentNodes += current
    }

    popNextNode()

    while(nodesLeft.nonEmpty) {

      // Find relationships that our nodes are involved in
      val (interesting, temp) = relationshipsLeft.partition( (r) => currentNodes.exists(r.contains) )
      relationshipsLeft = temp

      if(interesting.nonEmpty) {

        currentRels ++= interesting

        // Now find nodes that our new rels are connected to, that we haven't added yet
        val (interestingNodes, tempNodes) =
          nodesLeft.partition( node => currentRels.exists( r => r.contains(node)) )
        nodesLeft = tempNodes

        currentNodes ++= interestingNodes
      } else {
        // Done finding a disjoint subgraph, save it off and clear space for the next one
        result += MatchPattern(currentNodes.toSeq, currentRels.toSeq)
        currentNodes.clear()
        currentRels.clear()

        popNextNode()
      }
    }

    // Add last graph
    result += MatchPattern(currentNodes.toSeq, currentRels.toSeq)
    result.toSeq
  }

}
