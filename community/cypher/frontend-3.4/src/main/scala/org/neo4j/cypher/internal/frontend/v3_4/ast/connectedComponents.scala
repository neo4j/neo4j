/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

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
