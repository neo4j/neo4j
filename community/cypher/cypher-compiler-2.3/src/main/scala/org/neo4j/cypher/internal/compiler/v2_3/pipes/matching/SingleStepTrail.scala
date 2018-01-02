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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3._
import commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{PropertyContainer}

final case class SingleStepTrail(next: Trail,
                                 dir: SemanticDirection,
                                 relName: String,
                                 typ: Seq[String],
                                 start: String,
                                 relPred: Predicate,
                                 nodePred: Predicate,
                                 pattern: Pattern,
                                 originalPredicates: Seq[Predicate]) extends Trail {
  val end = next.end

  def pathDescription = next.pathDescription ++ Seq(relName, start)

  def toSteps(id: Int) = {
    val steps = next.toSteps(id + 1)

    Some(SingleStep(id, typ, dir, steps, relPred, nodePred))
  }

  val isEndPoint: Boolean = false

  val size = next.size + 1

  protected[matching] def decompose(p: Seq[PropertyContainer], m: Map[String, Any]): Iterator[(Seq[PropertyContainer], Map[String, Any])] = {
    val tail = p.tail
    if (p.isEmpty || tail.isEmpty) {
      Iterator.empty
    } else {
      val thisRel = tail.head
      val thisNode = p.head

      val a = m.get(relName)
      val b = m.get(start)

      if ((a.nonEmpty && a.get != thisRel)||(b.nonEmpty && b.get != thisNode)) {
        Iterator.empty
      } else {
        val newMap = m + (relName -> thisRel) + (start -> thisNode)
        next.decompose(tail.tail, newMap)
      }
    }
  }

  def symbols(table: SymbolTable): SymbolTable =
    next.symbols(table).add(start, CTNode).add(relName, CTRelationship)

  def contains(target: String): Boolean = next.contains(target) || target == end

  def predicates = Seq(originalPredicates) ++ next.predicates

  val patterns = next.patterns :+ pattern

  override def toString = {
    val left = if (SemanticDirection.INCOMING == dir) "<" else ""
    val right = if (SemanticDirection.OUTGOING == dir) ">" else ""
    val t = typ match {
      case List() => ""
      case x      => typ.mkString(":", "|", "")
    }

    s"($start)$left-[$relName$t WHERE $nodePred AND $relPred]-$right${next.toString}"
  }


  val nodeNames = start +: next.nodeNames

  def add(f: (String) => Trail) = copy(next = next.add(f))

  def filter(f: (Trail) => Boolean):Iterable[Trail] = Some(this).filter(f) ++ next.filter(f)
}
