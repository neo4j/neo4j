/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import scala.annotation.tailrec
import scala.collection.immutable

/*
 * Helper function that finds connected components in patterns.
 */
object connectedComponents {

  type ComponentPart = Set[Variable]
  type ConnectedComponent = Set[ComponentPart]

  //enable using the companion objects of the type aliases,
  //e.g. `ComponentPart(Variable("a"), Variable("b"),...)`
  val ComponentPart = Set
  val ConnectedComponent = Set

  def apply(patternParts: Seq[PatternPart]): IndexedSeq[ConnectedComponent] = {
    val parts: immutable.IndexedSeq[ComponentPart] = patternParts.map(_.fold(Set.empty[Variable]) {
      case NodePattern(Some(id), _, _) => list => list + id
    }).toIndexedSeq

    this.apply(parts)
  }

  def apply(parts: IndexedSeq[ComponentPart]): IndexedSeq[ConnectedComponent] = {

    @tailrec
    def loop(remaining: IndexedSeq[ComponentPart], connectedComponents: IndexedSeq[ConnectedComponent]): IndexedSeq[ConnectedComponent] = {
      if (remaining.isEmpty) connectedComponents
      else {
        val part = remaining.head

        val newConnected = connectedComponents.zipWithIndex.collectFirst {
          case (cc, index) if cc connectedTo part => connectedComponents.updated(index, cc + part)
        } getOrElse connectedComponents :+ ConnectedComponent(part)

        loop(remaining.tail, newConnected)
      }
    }
    loop(parts, IndexedSeq.empty)
  }

  implicit class RichConnectedComponent(connectedComponent: ConnectedComponent) {

    def connectedTo(part: ComponentPart) = connectedComponent.exists(c => (c intersect part).nonEmpty)

    def variables: Set[Variable] = connectedComponent.flatten
  }
}
