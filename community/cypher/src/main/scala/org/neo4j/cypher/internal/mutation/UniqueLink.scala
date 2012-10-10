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

import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.commands._
import expressions.Identifier
import expressions.Identifier._
import expressions.Literal
import org.neo4j.cypher.internal.symbols.{RelationshipType, NodeType, SymbolTable}
import org.neo4j.graphdb.{Node, DynamicRelationshipType, Direction}
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.cypher.{SyntaxException, CypherTypeException, UniquePathNotUniqueException}
import collection.JavaConverters._
import collection.Map
import org.neo4j.cypher.internal.commands.CreateRelationshipStartItem
import org.neo4j.cypher.internal.commands.CreateNodeStartItem

object UniqueLink {
  def apply(start: String, end: String, relName: String, relType: String, dir: Direction): UniqueLink =
    new UniqueLink(NamedExpectation(start, Map.empty), NamedExpectation(end, Map.empty), NamedExpectation(relName, Map.empty), relType, dir)
}

case class UniqueLink(start: NamedExpectation, end: NamedExpectation, rel: NamedExpectation, relType: String, dir: Direction)
  extends GraphElementPropertyFunctions with Pattern {
  lazy val relationshipType = DynamicRelationshipType.withName(relType)

  def exec(context: ExecutionContext, state: QueryState): Option[(UniqueLink, CreateUniqueResult)] = {
    // We haven't yet figured out if we already have both elements in the context
    // so let's start by finding that first
    val s = getNode(context, start.name)
    val e = getNode(context, end.name)

    (s, e) match {
      case (Some(startNode), None) => oneNode(startNode, context, dir, state, end)
      case (None, Some(startNode)) => oneNode(startNode, context, dir.reverse(), state, start)

      case (Some(startNode), Some(endNode)) => {
        if (context.contains(rel.name))
          None //We've already solved this pattern.
        else
          twoNodes(startNode, endNode, context, state)
      }

      case _ => Some(this -> CanNotAdvance())
    }
  }

  // These are the nodes that have properties defined. They should always go first,
  // so any other links that use these nodes have to have them locked.
  def nodesWProps:Seq[NamedExpectation] = Seq(start,end).filter(_.properties.nonEmpty)

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
      CreateRelationshipStartItem(rel.name, (Literal(startNode),Map()), (Identifier(end.name),Map()), relType, rel.properties)
    } else {
      CreateRelationshipStartItem(rel.name, (Identifier(end.name),Map()), (Literal(startNode),Map()), relType, rel.properties)
    }

    val relUpdate = UpdateWrapper(Seq(end.name), createRel)
    val nodeCreate = UpdateWrapper(Seq(), CreateNodeStartItem(end.name, end.properties))

    Seq(nodeCreate, relUpdate)
  }

  private def getNode(context: ExecutionContext, key: String): Option[Node] = context.get(key).map {
    case n: Node => n
    case x => throw new CypherTypeException("Expected `" + key + "` to a node, but it is a " + x)
  }

  lazy val identifier2 = Seq(start.name -> NodeType(), end.name -> NodeType(), rel.name -> RelationshipType())

  def symbolTableDependencies:Set[String] = symbolTableDependencies(start.properties) ++
    symbolTableDependencies(end.properties) ++
    symbolTableDependencies(rel.properties)

  def rewrite(f: (Expression) => Expression): UniqueLink = {
    val s = NamedExpectation(start.name, rewrite(start.properties, f))
    val e = NamedExpectation(end.name, rewrite(end.properties, f))
    val r = NamedExpectation(rel.name, rewrite(rel.properties, f))
    UniqueLink(s, e, r, relType, dir)
  }

  override def toString = node(start.name) + leftArrow(dir) + relInfo + rightArrow(dir) + node(end.name)

  private def relInfo: String = {
    val relName = if (notNamed(rel.name)) "" else "`" + rel.name + "`"

    "[%s:`%s`]".format(relName, relType)
  }

  def filter(f: (Expression) => Boolean) = Seq.empty

  def assertTypes(symbols: SymbolTable) {
    checkTypes(start.properties, symbols)
    checkTypes(end.properties, symbols)
    checkTypes(rel.properties, symbols)
  }

  def optional: Boolean = false

  def possibleStartPoints = Seq(
    start.name -> NodeType(),
    end.name -> NodeType(),
    rel.name -> RelationshipType())

  def predicate = True()

  def relTypes = Seq(relType)

  def nodes = Seq(start.name, end.name)

  def rels = Seq(rel.name)

  /**
   * Either returns a unique link where the expectation is respected,
   * or throws an exception if a contradiction is found.
   * @param expectation The named expectation to follow
   * @return
   */
  def expect(expectation: NamedExpectation): UniqueLink =
    if ((expectation.name != start.name) && (expectation.name != end.name)) {
      this
    } else {
      val s = compareAndMatch(expectation, start)
      val e = compareAndMatch(expectation, end)
      copy(start = s, end = e)
    }

  private def compareAndMatch(expectation: NamedExpectation, current: NamedExpectation): NamedExpectation = {
    if (current.name == expectation.name) {
      if (current.properties.nonEmpty && current.properties != expectation.properties)
        throw new SyntaxException("`%s` can't have properties assigned to it more than once in the CREATE UNIQUE statement".format(current.name))
      else
        expectation
    } else {
      current
    }
  }
}