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
package cypher.cucumber.prettifier

import org.neo4j.graphdb.{PropertyContainer, GraphDatabaseService, Node, Path, Relationship, RelationshipType}
import org.scalactic.Prettifier

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.immutable.IndexedSeq

object prettifier {

  def prettify(graph: GraphDatabaseService, thing: Any) = thing match {
    case s: String => s
    case o =>
      val tx = graph.beginTx()
      try {
        val result = recurse(o)
        tx.success()
        result
      } finally {
        tx.close()
      }
  }

  private def recurse(o: Any): String = o match {
    case p: Path => prettifyPath(p, recurse)
    case n: Node => prettifyNode(n, recurse)
    case r: Relationship => prettifyRelationship(r, recurse)
    case t: RelationshipType => t.name
    case c: java.lang.Iterable[_] => "[" + c.asScala.map(recurse).mkString(", ") + "]"
    case _ => Prettifier.default(o)
  }

  // (:A:B {a: 'asd', b: 'xyz'})
  private def prettifyNode(n: Node, pf: (Any) => String): String = {
    val l = n.getLabels.asScala.map(l => s":${pf(l)}").mkString(":")
    val props = prettifyProperties(n, pf)
    s"(${List(l, props).filter(_.nonEmpty).mkString(" ")})"
  }

  // [:T {a: 'asd', b: 'xyz'}]
  private def prettifyRelationship(r: Relationship, pf: (Any) => String): String = {
    val props = prettifyProperties(r, pf)
    val relType = pf(r.getType)
    s"[:${List(relType, props).filter(_.nonEmpty).mkString(" ")}]"
  }

  private def prettifyPath(p: Path, pf: (Any) => String): String = {
    @tailrec
    def rec(acc: String, pl: List[PropertyContainer]): String = {
      pl match {
        case Nil => acc
        case (n: Node) :: Nil => s"$acc${pf(n)}"
        case (n: Node) :: (r: Relationship) :: remaining =>
          val relationship = if (r.getStartNode == n) s"-${pf(r)}->" else s"<-${pf(r)}-"
          rec(s"$acc${pf(n)}$relationship", remaining)
        case _ => throw new IllegalStateException("impossible path")
      }
    }

    rec("", p.asScala.toList)
  }

  private def prettifyProperties(n: PropertyContainer, pf: (Any) => String): String = {
    val props = n.getPropertyKeys.asScala.map(k => s"$k: ${pf(n.getProperty(k))}").mkString(", ")
    if (props.nonEmpty) s"{$props}" else ""
  }
}
