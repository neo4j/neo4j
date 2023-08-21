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

import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Eagerly

import scala.annotation.nowarn
import scala.language.implicitConversions

object ASTAnnotationMap {

  type ASTAnnotationMap[K <: ASTNode, V] = Map[PositionedNode[K], V]

  def empty[K <: ASTNode, V]: ASTAnnotationMap[K, V] = Map.empty[PositionedNode[K], V]

  // DummyImplicit used just to disambiguate `apply` after type erasure
  def apply[K <: ASTNode, V](elems: (PositionedNode[K], V)*)(implicit
  dummyImplicit: DummyImplicit): ASTAnnotationMap[K, V] = Map(elems: _*)

  def apply[K <: ASTNode, V](elems: (K, V)*): ASTAnnotationMap[K, V] = Map(elems.map { case (k, v) =>
    (PositionedNode(k), v)
  }: _*)

  implicit class ASTAnnotationMapOps[K <: ASTNode, V](m: ASTAnnotationMap[K, V]) {

    def replaceKeys(replacements: (PositionedNode[K], PositionedNode[K])*): ASTAnnotationMap[K, V] =
      Eagerly.immutableReplaceKeys(m)(replacements: _*)
  }

  object PositionedNode {
    implicit def astNodeToPositionedNodeConverter[TFrom <: ASTNode](i: TFrom): PositionedNode[TFrom] = PositionedNode(i)
  }

  case class PositionedNode[+N <: ASTNode](node: N) {

    override def toString: String = s"PositionedNode($node@${node.position.offset})"

    @nowarn("msg=eliminated by erasure")
    def canEqual(a: Any): Boolean = {
      a.isInstanceOf[PositionedNode[N]]
    }

    @nowarn("msg=eliminated by erasure")
    override def equals(that: Any): Boolean = {
      that match {
        case that: PositionedNode[N] =>
          that.canEqual(this) &&
          this.node == that.node &&
          this.node.position == that.node.position
        case _ => false
      }
    }

    override def hashCode(): Int =
      (node, node.position).hashCode
  }
}
