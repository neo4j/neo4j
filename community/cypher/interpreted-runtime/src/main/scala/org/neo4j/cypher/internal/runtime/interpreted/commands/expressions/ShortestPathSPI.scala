/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path

sealed abstract class BaseExpander() extends Expander {

  override def addRelationshipFilter(newFilter: KernelPredicate[Entity]): Expander =
    newWith(newRelFilters = relFilters :+ newFilter)

  override def addNodeFilter(newFilter: KernelPredicate[Entity]): Expander =
    newWith(newNodeFilters = nodeFilters :+ newFilter)

  protected def newWith(
    newNodeFilters: Seq[KernelPredicate[Entity]] = nodeFilters,
    newRelFilters: Seq[KernelPredicate[Entity]] = relFilters
  ): Expander
}

case class OnlyDirectionExpander(
  override val nodeFilters: Seq[KernelPredicate[Entity]],
  override val relFilters: Seq[KernelPredicate[Entity]],
  direction: SemanticDirection
) extends BaseExpander {

  override protected def newWith(
    newNodeFilters: Seq[KernelPredicate[Entity]],
    newRelFilters: Seq[KernelPredicate[Entity]]
  ): OnlyDirectionExpander =
    copy(nodeFilters = newNodeFilters, relFilters = newRelFilters)
}

case class TypeAndDirectionExpander(
  override val nodeFilters: Seq[KernelPredicate[Entity]],
  override val relFilters: Seq[KernelPredicate[Entity]],
  typDirs: Seq[(String, SemanticDirection)]
) extends BaseExpander {

  override protected def newWith(
    newNodeFilters: Seq[KernelPredicate[Entity]],
    newRelFilters: Seq[KernelPredicate[Entity]]
  ): TypeAndDirectionExpander =
    copy(nodeFilters = newNodeFilters, relFilters = newRelFilters)

  def add(typ: String, dir: SemanticDirection): TypeAndDirectionExpander =
    copy(typDirs = typDirs :+ typ -> dir)
}

object Expanders {
  def typeDir(): TypeAndDirectionExpander = TypeAndDirectionExpander(Seq.empty, Seq.empty, Seq.empty)
  def allTypes(dir: SemanticDirection): Expander = OnlyDirectionExpander(Seq.empty, Seq.empty, dir)
}

trait ShortestPathAlgo {
  def findSinglePath(var1: Node, var2: Node): Path
  def findAllPaths(var1: Node, var2: Node): Iterable[Path]
}
