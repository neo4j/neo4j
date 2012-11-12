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
import org.neo4j.cypher.{UniquePathNotUniqueException, CypherTypeException}

case class NamedExpectation(name: String, properties: Map[String, Expression])
  extends GraphElementPropertyFunctions
  with IterableSupport {
  def this(name: String) = this(name, Map.empty)

  def compareWithExpectations(pc: PropertyContainer, ctx: ExecutionContext): Boolean = properties.forall {
    case ("*", expression) => getMapFromExpression(expression(ctx)).forall {
      case (k, value) => pc.hasProperty(k) && pc.getProperty(k) == value
    }
    case (k, exp) =>
      if (!pc.hasProperty(k)) false
      else {
        val expectationValue = exp(ctx)
        val elementValue = pc.getProperty(k)

        if (expectationValue == elementValue) true
        else isCollection(expectationValue) && isCollection(elementValue) && makeTraversable(expectationValue).toList == makeTraversable(elementValue).toList
      }
  }
}

object UniqueLink {
  def apply(start: String, end: String, relName: String, relType: String, dir: Direction): UniqueLink =
    new UniqueLink(NamedExpectation(start, Map.empty), NamedExpectation(end, Map.empty), NamedExpectation(relName, Map.empty), relType, dir)
}

case class UniqueLink(start: NamedExpectation, end: NamedExpectation, rel: NamedExpectation, relType: String, dir: Direction)
  extends GraphElementPropertyFunctions {
  lazy val relationshipType = DynamicRelationshipType.withName(relType)

  def exec(context: ExecutionContext, state: QueryState): Option[(UniqueLink, CreateUniqueResult)] = {
    // We haven't yet figured out if we already have both elements in the context
    // so let's start by finding that first

    val s = getNode(context, start.name)
    val e = getNode(context, end.name)

    (s, e) match {
      case (None, None) => Some(this->CanNotAdvance())

      case (Some(startNode), None) => oneNode(startNode, context, dir, state, end)
      case (None, Some(startNode)) => oneNode(startNode, context, dir.reverse(), state, start)

      case (Some(startNode), Some(endNode)) => {
        if (context.contains(rel.name))
          None //We've already solved this pattern.
        else
          twoNodes(startNode, endNode, context, state)
      }
    }
  }

  // This method sees if a matching relationship already exists between two nodes
  // If any matching rels are found, they are returned. Otherwise, a new one is
  // created and returned.
  private def twoNodes(startNode: Node, endNode: Node, ctx: ExecutionContext, state: QueryState): Option[(UniqueLink, CreateUniqueResult)] = {
    val rels = startNode.getRelationships(relationshipType, dir).asScala.
      filter(r => {
      r.getOtherNode(startNode) == endNode && rel.compareWithExpectations(r, ctx)
    }).toList

    rels match {
      case List() =>
        val tx = state.transaction.getOrElse(throw new RuntimeException("I need a transaction!"))

        Some(this->Update(Seq(UpdateWrapper(Seq(), CreateRelationshipStartItem(rel.name, (Literal(startNode), Map()), (Literal(endNode), Map()), relType, rel.properties))), () => {
          Seq(tx.acquireWriteLock(startNode), tx.acquireWriteLock(endNode))
        }))
      case List(r) => Some(this->Traverse(rel.name -> r))
      case _ => throw new UniquePathNotUniqueException("The pattern " + this + " produced multiple possible paths, and that is not allowed")
    }
  }

  // When only one node exists in the context, we'll traverse all the relationships of that node
  // and try to find a matching node/rel. If matches are found, they are returned. If nothing is
  // found, we'll create it and return it
  private def oneNode(startNode: Node, ctx: ExecutionContext, dir: Direction, state: QueryState, end: NamedExpectation): Option[(UniqueLink, CreateUniqueResult)] = {
    val rels = startNode.getRelationships(relationshipType, dir).asScala.filter(r => {
      rel.compareWithExpectations(r, ctx) && end.compareWithExpectations(r.getOtherNode(startNode), ctx)
    }).toList

    rels match {
      case List()  =>
        val tx = state.transaction.getOrElse(throw new RuntimeException("I need a transaction!"))
        Some(this -> Update(createUpdateActions(dir, startNode, end), () => {
          Seq(tx.acquireWriteLock(startNode))
        }))

      case List(r) => Some(this -> Traverse(rel.name -> r, end.name -> r.getOtherNode(startNode)))

      case _       => throw new UniquePathNotUniqueException("The pattern " + this + " produced multiple possible paths, and that is not allowed")
    }
  }


  private def createUpdateActions(dir: Direction, startNode: Node, end: NamedExpectation): Seq[UpdateWrapper] = {
    val createRel = if (dir == Direction.OUTGOING) {
      CreateRelationshipStartItem(rel.name, (Literal(startNode),Map()), (Entity(end.name),Map()), relType, rel.properties)
    } else {
      CreateRelationshipStartItem(rel.name, (Entity(end.name),Map()), (Literal(startNode),Map()), relType, rel.properties)
    }

    val relUpdate = UpdateWrapper(Seq(end.name), createRel)
    val nodeCreate = UpdateWrapper(Seq(), CreateNodeStartItem(end.name, end.properties))

    Seq(nodeCreate, relUpdate)
  }

  private def getNode(context: ExecutionContext, key: String): Option[Node] = if (key.startsWith("  UNAMED")) {
    None
  } else context.get(key).map {
    case n: Node => n
    case x => throw new CypherTypeException("Expected `" + key + "` to a node, but it is a " + x)
  }

  lazy val identifier = Seq(Identifier(start.name, NodeType()), Identifier(end.name, NodeType()), Identifier(rel.name, RelationshipType()))

  def dependencies = (propDependencies(start.properties) ++ propDependencies(end.properties) ++ propDependencies(rel.properties)).distinct

  def rewrite(f: (Expression) => Expression): UniqueLink = {
    val s = NamedExpectation(start.name, rewrite(start.properties, f))
    val e = NamedExpectation(end.name, rewrite(end.properties, f))
    val r = NamedExpectation(rel.name, rewrite(rel.properties, f))
    UniqueLink(s, e, r, relType, dir)
  }

  def filter(f: (Expression) => Boolean) = Seq.empty
}