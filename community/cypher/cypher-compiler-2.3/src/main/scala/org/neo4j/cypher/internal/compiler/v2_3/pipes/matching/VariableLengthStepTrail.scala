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

import org.neo4j.cypher.internal.compiler.v2_3.commands.Pattern
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.PathImpl
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.True
import org.neo4j.cypher.internal.compiler.v2_3.pipes.MutableMaps
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

final case class VariableLengthStepTrail(next: Trail,
                                         dir: SemanticDirection,
                                         projectedDir: SemanticDirection,
                                         typ: Seq[String],
                                         min: Int,
                                         max: Option[Int],
                                         path: String,
                                         relIterator: Option[String],
                                         start: String,
                                         pattern: Pattern) extends Trail {
  def contains(target: String) = false

  protected[matching] def decompose(p: Seq[PropertyContainer], m: Map[String, Any]) = {
    var idx = min
    var curr: Seq[PropertyContainer] = null
    var left: Seq[PropertyContainer] = null

    def checkRel(last: Node, r: Relationship) = (typ.contains(r.getType.name()) || typ.isEmpty) && (dir match {
      case SemanticDirection.OUTGOING => r.getStartNode == last
      case SemanticDirection.INCOMING => r.getEndNode == last
      case _                  => true
    })

    def checkPath(in: Seq[PropertyContainer]) = in.isEmpty || {
      var last: Node = in.head.asInstanceOf[Node]

      in.forall {
        case n: Node         => last = n; true
        case r: Relationship => checkRel(last, r)
      }
    }

    val x = p.splitAt(idx * 2)
    curr = x._1
    left = x._2

    var result: Seq[(Seq[PropertyContainer], Map[String, Any])] = Nil

    val newNode = p.head
    val oldNode = m.get(start)

    if (oldNode.isEmpty || oldNode.get == newNode) {
      val map = MutableMaps.create(m)

      map += (start -> newNode)

      var validRelationship = checkPath(curr)

      while (validRelationship &&
             idx <= max.getOrElse(idx) &&
             left.nonEmpty) {

        val currentPath = if (projectedDir == dir) curr :+ left.head
                          else (curr :+ left.head).reverse
        map += (path -> PathImpl(currentPath: _*))

        relIterator.foreach {
          key => map += (key -> currentPath.filter(_.isInstanceOf[Relationship]))
        }

        //Add this result to the stack
        //if our downstreams trail doesn't return anything,
        //we'll also not return anything
        result = result ++ next.decompose(left, map.toMap)

        //Get more stuff from the remaining path
        idx += 1

        val x = p.splitAt(idx * 2)
        curr = x._1
        left = x._2

        validRelationship = checkPath(curr)
      }
    }

    result.toIterator
  }

  def pathDescription = next.pathDescription ++ Seq(path, end) ++ relIterator

  val patterns = next.patterns :+ pattern

  val predicates = next.predicates

  val size = next.size + min + 1

  val isEndPoint = false

  val end = next.end

  def symbols(table: SymbolTable) = {
    val symbolTable = next.symbols(table).add(start, CTNode).add(path, CTCollection(CTRelationship))

    //If we have a rel-iterator, let's include it
    relIterator match {
      case None    => symbolTable
      case Some(r) => symbolTable.add(r, CTCollection(CTRelationship))
    }
  }

  def toSteps(id: Int): Option[ExpanderStep] = {
    val steps = next.toSteps(id + 1)

    Some(VarLengthStep(id, typ, dir, min, max, steps, True(), True()))
  }

  override def toString = {
    val left = if (SemanticDirection.INCOMING == dir) "<" else ""
    val right = if (SemanticDirection.OUTGOING == dir) ">" else ""
    val t = typ match {
      case List() => ""
      case x      => typ.mkString(":", "|", "")
    }

    val minimum = min.toString
    val maximum = if(max.nonEmpty) max.get.toString else ""

    "(%s)%s-[%s*%s..%s]-%s%s".format(start, left, t, minimum, maximum, right, next.toString)
  }

  def nodeNames = Seq(start) ++ next.nodeNames

  def add(f: (String) => Trail) = copy(next = next.add(f))

  def filter(f: (Trail) => Boolean):Iterable[Trail] = Some(this).filter(f) ++ next.filter(f)
}
