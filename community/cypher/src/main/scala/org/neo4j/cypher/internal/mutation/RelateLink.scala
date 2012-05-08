/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import collection.JavaConverters._
import org.neo4j.cypher.internal.symbols.{RelationshipType, NodeType, Identifier}
import collection.Map
import org.neo4j.graphdb._
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.{RelatePathNotUnique, CypherTypeException}


object RelateLink {
  def apply(start: String, end: String, relName: String, relType: String, dir: Direction): RelateLink =
    new RelateLink((start, Map.empty), (end, Map.empty), (relName, Map.empty), relType, dir)
}

case class RelateLink(start: (String, Map[String, Expression]), end: (String, Map[String, Expression]), rel: (String, Map[String, Expression]), relType: String, dir: Direction)
  extends GraphElementPropertyFunctions {
  lazy val relationshipType = DynamicRelationshipType.withName(relType)

  def exec(context: ExecutionContext, state: QueryState): RelateResult = {
    // We haven't yet figured out if we already have boths elements in the context
    // so let's start by finding that first

    val s = getNode(context, start.name)
    val e = getNode(context, end.name)

    (s, e) match {
      case (None, None) => CanNotAdvance()

      case (Some(startNode), None) => oneNode(startNode, context, dir, state, end)
      case (None, Some(startNode)) => oneNode(startNode, context, dir.reverse(), state, start)

      case (Some(startNode), Some(endNode)) => {
        if (context.contains(rel.name))
          Done() //We've already solved this pattern.
        else
          twoNodes(startNode, endNode, context, state)
      }
    }
  }

  // This method sees if a matching relationship already exists between two nodes
  // If any matching rels are found, they are returned. Otherwise, a new one is
  // created and returned.
  private def twoNodes(startNode: Node, endNode: Node, ctx: ExecutionContext, state: QueryState): RelateResult = {
    val rels = startNode.getRelationships(relationshipType, dir).asScala.
      filter(r => {
      r.getOtherNode(startNode) == endNode && compareWithExpectations(r, ctx, rel.properties)
    }).toList

    rels match {
      case List() => Update(UpdateWrapper(Seq(), CreateRelationshipStartItem(rel.name, Literal(startNode), Literal(endNode), relType, rel.properties)))
      case List(r) => Traverse(rel.name -> r)
      case _ => throw new RelatePathNotUnique("The pattern " + this + " produced multiple possible paths, and that is not allowed")
    }
  }

  // When only one node exists in the context, we'll traverse all the relationships of that node
  // and try to find a matching node/rel. If matches are found, they are returned. If nothing is
  // found, we'll create it and return it
  private def oneNode(startNode: Node, ctx: ExecutionContext, dir: Direction, state: QueryState, end: (String, Map[String, Expression])): RelateResult = {
    val rels = startNode.getRelationships(relationshipType, dir).asScala.filter(r => {
      val matchingRelationship = compareWithExpectations(r, ctx, rel.properties)
      if (!matchingRelationship)
        false
      else
        compareWithExpectations(r.getOtherNode(startNode), ctx, end.properties)
    })

    rels match {
      case List() => Update(createUpdateActions(dir, startNode, end): _*)
      case List(r) => Traverse(rel.name -> r, end.name -> r.getOtherNode(startNode))
      case _ => throw new RelatePathNotUnique("The pattern " + this + " produced multiple possible paths, and that is not allowed")
    }
  }


  private def createUpdateActions(dir: Direction, startNode: Node, end: (String, Map[String, Expression])): Seq[UpdateWrapper] = {
    val createRel = if (dir == Direction.OUTGOING) {
      CreateRelationshipStartItem(rel.name, Literal(startNode), Entity(end.name), relType, rel.properties)
    } else {
      CreateRelationshipStartItem(rel.name, Entity(end.name), Literal(startNode), relType, rel.properties)
    }

    val relUpdate = UpdateWrapper(Seq(end.name), createRel)
    val nodeCreate = UpdateWrapper(Seq(), CreateNodeStartItem(end.name, end.properties))

    Seq(nodeCreate, relUpdate)
  }

  def compareWithExpectations(pc: PropertyContainer, ctx: ExecutionContext, properties: Map[String, Expression]): Boolean = {
    properties.forall {
      case (k, exp) => {
        if (!pc.hasProperty(k)) {
          false
        } else {
          exp(ctx) == pc.getProperty(k)
        }
      }
    }
  }

  private def getNode(context: ExecutionContext, key: String): Option[Node] = if (key.startsWith("  UNAMED")) {
    None
  } else context.get(key).map {
    case n: Node => n
    case x => throw new CypherTypeException("Expected `" + key + "` to a node, but it is a " + x)
  }

  lazy val identifier = Seq(Identifier(start.name, NodeType()), Identifier(end.name, NodeType()), Identifier(rel.name, RelationshipType()))

  def dependencies = (propDependencies(start.properties) ++ propDependencies(end.properties) ++ propDependencies(rel.properties)).distinct

  def rewrite(f: (Expression) => Expression): RelateLink = {
    val s: (String, Map[String, Expression]) = (start.name, rewrite(start.properties, f))
    val e: (String, Map[String, Expression]) = (end.name, rewrite(end.properties, f))
    val r: (String, Map[String, Expression]) = (rel.name, rewrite(rel.properties, f))
    RelateLink(s, e, r, relType, dir)
  }

  def filter(f: (Expression) => Boolean) = Seq.empty

  //A little implicit magic to make handing these tuples a little easier
  implicit def foo2bar(x: (String, Map[String, Expression])): A = new A(x)

  class A(inner: (String, Map[String, Expression])) {
    def name = inner._1

    def properties = inner._2
  }

}