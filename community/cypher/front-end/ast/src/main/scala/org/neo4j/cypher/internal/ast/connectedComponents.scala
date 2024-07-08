/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternPart

import scala.annotation.tailrec
import scala.collection.immutable

/*
 * Helper function that finds connected components in patterns.
 */
object connectedComponents {

  type ComponentPart = Set[LogicalVariable]
  type ConnectedComponent = Set[ComponentPart]

  // enable using the companion objects of the type aliases,
  // e.g. `ComponentPart(Variable("a"), Variable("b"),...)`
  val ComponentPart = Set
  val ConnectedComponent = Set

  def apply(patternParts: Seq[PatternPart]): IndexedSeq[ConnectedComponent] = {
    val parts: immutable.IndexedSeq[ComponentPart] = patternParts.map(_.folder.fold(Set.empty[LogicalVariable]) {
      case NodePattern(Some(id), _, _, _) => list => list + id
    }).toIndexedSeq

    this.apply(parts)
  }

  def apply(parts: IndexedSeq[ComponentPart]): IndexedSeq[ConnectedComponent] = {

    @tailrec
    def loop(
      remaining: IndexedSeq[ComponentPart],
      connectedComponents: IndexedSeq[ConnectedComponent]
    ): IndexedSeq[ConnectedComponent] = {
      if (remaining.isEmpty) connectedComponents
      else {
        val part = remaining.head

        val (componentsConnectedByPart, componentsNotConnectedByPart) =
          connectedComponents.partition(_.connectedTo(part))

        val singleConnectedComponentForPart = componentsConnectedByPart.foldLeft(ConnectedComponent(part))(_.union(_))

        // If the first component was part of the connected components, then the new connected component is the first element in the component list
        // otherwise it is at the end.
        // The cartesian product notification assumes the first component to be the desired component to connect and reports on the rest of the components as disconnected.
        // So we need to maintain order for the first component at least.
        val newConnectedList = if (connectedComponents.headOption.exists(_.connectedTo(part))) {
          singleConnectedComponentForPart +: componentsNotConnectedByPart
        } else {
          componentsNotConnectedByPart :+ singleConnectedComponentForPart
        }

        loop(remaining.tail, newConnectedList)
      }
    }
    loop(parts, IndexedSeq.empty)
  }

  implicit class RichConnectedComponent(connectedComponent: ConnectedComponent) {

    def connectedTo(part: ComponentPart) = connectedComponent.exists(c => (c intersect part).nonEmpty)

    def variables: Set[LogicalVariable] = connectedComponent.flatten
  }
}
