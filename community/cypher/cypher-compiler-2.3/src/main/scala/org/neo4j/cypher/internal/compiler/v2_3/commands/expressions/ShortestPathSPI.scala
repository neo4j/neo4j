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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.graphdb.{Path, Node, PropertyContainer}


trait KernelPredicate[T] {
  def test(obj: T): Boolean
}

trait Expander {
  def addRelationshipFilter(newFilter: KernelPredicate[PropertyContainer]): Expander
  def addNodeFilter(newFilter: KernelPredicate[PropertyContainer]): Expander
  def nodeFilters: Seq[KernelPredicate[PropertyContainer]]
  def relFilters: Seq[KernelPredicate[PropertyContainer]]
}

abstract class BaseExpander() extends Expander {
  def addRelationshipFilter(newFilter: KernelPredicate[PropertyContainer]): Expander =
    newWith(newRelFilters = relFilters :+ newFilter)

  def addNodeFilter(newFilter: KernelPredicate[PropertyContainer]): Expander =
    newWith(newNodeFilters = nodeFilters :+ newFilter)

  protected def newWith(newNodeFilters: Seq[KernelPredicate[PropertyContainer]] = nodeFilters,
                        newRelFilters: Seq[KernelPredicate[PropertyContainer]] = relFilters): Expander
}

case class OnlyDirectionExpander(nodeFilters: Seq[KernelPredicate[PropertyContainer]],
                                 relFilters: Seq[KernelPredicate[PropertyContainer]],
                                 direction: SemanticDirection) extends BaseExpander {

  override protected def newWith(newNodeFilters: Seq[KernelPredicate[PropertyContainer]],
                                 newRelFilters: Seq[KernelPredicate[PropertyContainer]]): OnlyDirectionExpander =
    copy(nodeFilters = newNodeFilters, relFilters = newRelFilters)
}

case class TypeAndDirectionExpander(nodeFilters: Seq[KernelPredicate[PropertyContainer]],
                                    relFilters: Seq[KernelPredicate[PropertyContainer]],
                                    typDirs: Seq[(String, SemanticDirection)]) extends BaseExpander {

  override protected def newWith(newNodeFilters: Seq[KernelPredicate[PropertyContainer]],
                                 newRelFilters: Seq[KernelPredicate[PropertyContainer]]): TypeAndDirectionExpander =
    copy(nodeFilters = newNodeFilters, relFilters = newRelFilters)

  def add(typ: String, dir: SemanticDirection): TypeAndDirectionExpander =
    copy(typDirs = typDirs :+ typ -> dir)
}

object Expander {
  def typeDirExpander(): TypeAndDirectionExpander = TypeAndDirectionExpander(Seq.empty, Seq.empty, Seq.empty)
  def expanderForAllTypes(dir: SemanticDirection): Expander = OnlyDirectionExpander(Seq.empty, Seq.empty, dir)
}

trait ShortestPathAlgo {
  def findSinglePath(var1: Node, var2: Node): Path
  def findAllPaths(var1: Node, var2: Node): Iterable[Path]
}
