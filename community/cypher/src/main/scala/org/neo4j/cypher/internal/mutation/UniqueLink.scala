/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.internal.commands._
import expressions.{Expression, Identifier, Literal, Collection}
import expressions.Identifier._
import org.neo4j.cypher.internal.symbols.{RelationshipType, NodeType, SymbolTable}
import org.neo4j.graphdb.{Transaction, Node, Direction}
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.{SyntaxException, CypherTypeException, UniquePathNotUniqueException}
import collection.Map
import org.neo4j.cypher.internal.helpers.{IsMap, MapSupport}
import org.neo4j.cypher.internal.ExecutionContext
import values.LabelValue

object UniqueLink {
  def apply(start: String, end: String, relName: String, relType: String, dir: Direction): UniqueLink =
    new UniqueLink(NamedExpectation(start, Map.empty, true), NamedExpectation(end, Map.empty, true), NamedExpectation(relName, Map.empty, true), relType, dir)
}

case class UniqueLink(start: NamedExpectation, end: NamedExpectation, rel: NamedExpectation, relType: String, dir: Direction)
  extends GraphElementPropertyFunctions with Pattern with MapSupport {

  def exec(context: ExecutionContext, state: QueryState): Option[(UniqueLink, CreateUniqueResult)] = {

    def tx: Transaction = state.query.getTransaction

    def getNode(expect: NamedExpectation): Option[Node] = context.get(expect.name) match {
      case Some(n: Node)                             => Some(n)
      case Some(x)                                   => throw new CypherTypeException("Expected `%s` to a node, but it is a %s".format(expect.name, x))
      case None if expect.e.isInstanceOf[Identifier] => None
      case None => expect.e(context)(state) match {
        case n: Node  => Some(n)
        case IsMap(_) => None
        case x        => throw new CypherTypeException("Expected `%s` to a node, but it is a %s".format(expect.name, x))
      }
    }

    // This method sees if a matching relationship already exists between two nodes
    // If any matching rels are found, they are returned. Otherwise, a new one is
    // created and returned.
    def twoNodes(startNode: Node, endNode: Node): Option[(UniqueLink, CreateUniqueResult)] = {
      val rels = state.query.getRelationshipsFor(startNode, dir, Seq(relType)).
        filter(r => r.getOtherNode(startNode) == endNode && rel.compareWithExpectations(r, context, state) ).
        toList

      rels match {
        case List() =>
          val expectations = rel.getExpectations(context, state)
          val createRel = CreateRelationship(rel.name,
            RelationshipEndpoint(Literal(startNode), Map(), Seq.empty, bare = true),
            RelationshipEndpoint(Literal(endNode), Map(), Seq.empty, bare = true), relType, expectations.properties)
          Some(this->Update(Seq(UpdateWrapper(Seq(), createRel, rel.name)), () => {
            // TODO: This should not be done here. The QueryContext should take the necessary locks while reading the
            // graph. For now, let's rip out the inside of objects and get to the transaction.

            Seq(tx.acquireWriteLock(startNode), tx.acquireWriteLock(endNode))
          }))
        case List(r) => Some(this->Traverse(rel.name -> r))
        case _ => throw new UniquePathNotUniqueException("The pattern " + this + " produced multiple possible paths, and that is not allowed")
      }
    }

    // When only one node exists in the context, we'll traverse all the relationships of that node
    // and try to find a matching node/rel. If matches are found, they are returned. If nothing is
    // found, we'll create it and return it
    def oneNode(startNode: Node, dir: Direction, other: NamedExpectation): Option[(UniqueLink, CreateUniqueResult)] = {

      def createUpdateActions(): Seq[UpdateWrapper] = {
        val relExpectations = rel.getExpectations(context, state)
        val createRel = if (dir == Direction.OUTGOING) {
          CreateRelationship(rel.name,
            RelationshipEndpoint(Literal(startNode), Map(), Seq.empty, bare = true),
            RelationshipEndpoint(Identifier(other.name), Map(), Seq.empty, bare = true), relType, relExpectations.properties)
        } else {
          CreateRelationship(rel.name,
            RelationshipEndpoint(Identifier(other.name), Map(), Seq.empty, bare = true),
            RelationshipEndpoint(Literal(startNode), Map(), Seq.empty, bare = true), relType, relExpectations.properties)
        }

        val relUpdate = UpdateWrapper(Seq(other.name), createRel, createRel.key)
        val expectations = other.getExpectations(context, state)
        val nodeCreate = UpdateWrapper(Seq(), CreateNode(other.name, expectations.properties, expectations.labels), other.name)

        Seq(nodeCreate, relUpdate)
      }

      val rels = state.query.getRelationshipsFor(startNode, dir, Seq(relType)).
        filter(r => rel.compareWithExpectations(r, context, state) && other.compareWithExpectations(r.getOtherNode(startNode), context, state)).toList

      rels match {
        case List() =>
          Some(this -> Update(createUpdateActions(), () => Seq(tx.acquireWriteLock(startNode))))

        case List(r) => Some(this -> Traverse(rel.name -> r, other.name -> r.getOtherNode(startNode)))

        case _ => throw new UniquePathNotUniqueException("The pattern " + this + " produced multiple possible paths, and that is not allowed")
      }
    }


    // We haven't yet figured out if we already have both elements in the context
    // so let's start by finding that first
    val s = getNode(start)
    val e = getNode(end)

    (s, e) match {
      case (Some(startNode), None) => oneNode(startNode, dir, end)
      case (None, Some(startNode)) => oneNode(startNode, dir.reverse(), start)

      case (Some(startNode), Some(endNode)) => {
        if (context.contains(rel.name))
          None //We've already solved this pattern.
        else
          twoNodes(startNode, endNode)
      }

      case _ => Some(this -> CanNotAdvance())
    }
  }

  // These are the nodes that have properties defined. They should always go first,
  // so any other links that use these nodes have to have them locked.
  def nodesWProps:Seq[NamedExpectation] = Seq(start,end).filter(_.properties.nonEmpty)

  lazy val identifier2 = Seq(start.name -> NodeType(), end.name -> NodeType(), rel.name -> RelationshipType())

  def symbolTableDependencies:Set[String] = start.properties.symboltableDependencies ++
    end.properties.symboltableDependencies ++
    rel.properties.symboltableDependencies

  def rewrite(f: (Expression) => Expression): UniqueLink = {
    val s = NamedExpectation(start.name, start.properties.rewrite(f), start.labels.map(_.typedRewrite[LabelValue](f)), start.bare)
    val e = NamedExpectation(end.name, end.properties.rewrite(f), end.labels.map(_.typedRewrite[LabelValue](f)), end.bare)
    val r = NamedExpectation(rel.name, rel.properties.rewrite(f), rel.labels.map(_.typedRewrite[LabelValue](f)), rel.bare)
    UniqueLink(s, e, r, relType, dir)
  }

  override def toString = {
    val relInfo = {
      val relName = if (notNamed(rel.name)) "" else "`" + rel.name + "`"
      "[%s:`%s`]".format(relName, relType)
    }

    node(start.name) + leftArrow(dir) + relInfo + rightArrow(dir) + node(end.name)
  }

  def children = Seq(start.e, end.e, rel.e)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    start.properties.throwIfSymbolsMissing(symbols)
    end.properties.throwIfSymbolsMissing(symbols)
    rel.properties.throwIfSymbolsMissing(symbols)
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
  def expect(expectation: NamedExpectation): UniqueLink = {
    def compareAndMatch(current: NamedExpectation): NamedExpectation = {
      if (current.name == expectation.name) {
        if (current.properties.nonEmpty && current.properties != expectation.properties)
          throw new SyntaxException("`%s` can't have properties assigned to it more than once in the CREATE UNIQUE statement".format(current.name))
        else
          expectation
      } else {
        current
      }
    }

    if ((expectation.name != start.name) && (expectation.name != end.name)) {
      this
    } else {
      val s = compareAndMatch(start)
      val e = compareAndMatch(end)
      copy(start = s, end = e)
    }
  }
}