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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.True
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{Effects, _}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{IsMap, MapSupport, UnNamedNameGenerator}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.{CypherTypeException, SyntaxException, UniquePathNotUniqueException}
import org.neo4j.graphdb.{Node}

import scala.collection.Map

object UniqueLink {
  def apply(leftName: String, rightName: String, relName: String, relType: String, dir: SemanticDirection): UniqueLink =
    new UniqueLink(
      NamedExpectation(leftName, Map.empty),
      NamedExpectation(rightName, Map.empty),
      NamedExpectation(relName, Map.empty), relType, dir)
}

case class UniqueLink(left: NamedExpectation, right: NamedExpectation, rel: NamedExpectation, relType: String, dir: SemanticDirection)
  extends GraphElementPropertyFunctions with Pattern with MapSupport {

  def exec(context: ExecutionContext, state: QueryState): Option[(UniqueLink, CreateUniqueResult)] = {

    def orderByDir[T]( a:T, b:T, dir:SemanticDirection ):(T,T) =
      if (dir != SemanticDirection.INCOMING) (a, b) else (b, a)

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
    def twoNodes(leftNode: Node, rightNode: Node): Option[(UniqueLink, CreateUniqueResult)] = {
      val rels = state.query.getRelationshipsForIds(leftNode, dir, Some(state.query.getOptRelTypeId(relType).toSeq)).
        filter(r => r.getOtherNode(leftNode) == rightNode && rel.compareWithExpectations(r, context, state) ).
        toList

      rels match {
        case List() =>
          val expectations = rel.getExpectations(context, state)
          val (startNode, endNode) = orderByDir(leftNode, rightNode, dir)
          val createRel = CreateRelationship(rel.name,
            RelationshipEndpoint(Literal(startNode), Map(), Seq.empty),
            RelationshipEndpoint(Literal(endNode), Map(), Seq.empty), relType, expectations.properties)
          Some(this->Update(Seq(UpdateWrapper(Seq(), createRel, rel.name))))
        case List(r) => Some(this->Traverse(rel.name -> r))
        case _ => throw new UniquePathNotUniqueException("The pattern " + this + " produced multiple possible paths, and that is not allowed")
      }
    }

    // When only one node exists in the context, we'll traverse all the relationships of that node
    // and try to find a matching node/rel. If matches are found, they are returned. If nothing is
    // found, we'll create it and return it
    def oneNode(existingNode: Node, dir: SemanticDirection, other: NamedExpectation): Option[(UniqueLink, CreateUniqueResult)] = {

      def createUpdateActions(): Seq[UpdateWrapper] = {
        val relExpectations = rel.getExpectations(context, state)
        val (startExp, endExp) = orderByDir(Literal(existingNode), Identifier(other.name), if (dir == SemanticDirection.BOTH) SemanticDirection.INCOMING else dir)
        val createRel = CreateRelationship(rel.name,
          RelationshipEndpoint(startExp, Map(), Seq.empty),
          RelationshipEndpoint(endExp, Map(), Seq.empty), relType, relExpectations.properties)
        val relUpdate = UpdateWrapper(Seq(other.name), createRel, createRel.key)
        val expectations = other.getExpectations(context, state)
        val nodeCreate = UpdateWrapper(Seq(), CreateNode(other.name, expectations.properties, expectations.labels), other.name)

        Seq(nodeCreate, relUpdate)
      }

      val rels = state.query.getRelationshipsForIds(existingNode, dir, Some(state.query.getOptRelTypeId(relType).toSeq)).
        filter(r => rel.compareWithExpectations(r, context, state) && other.compareWithExpectations(r.getOtherNode(existingNode), context, state)).toList

      rels match {
        case List() =>
          Some(this -> Update(createUpdateActions()))

        case List(r) => Some(this -> Traverse(rel.name -> r, other.name -> r.getOtherNode(existingNode)))

        case _ => throw new UniquePathNotUniqueException("The pattern " + this + " produced multiple possible paths, and that is not allowed")
      }
    }


    // We haven't yet figured out if we already have both elements in the context
    // so let's start by finding that first
    (getNode(left), getNode(right)) match {
      case (Some(leftNode), None) => oneNode(leftNode, dir, right)
      case (None, Some(rightNode)) => oneNode(rightNode, dir.reversed, left)

      case (Some(leftNode), Some(rightNode)) => {
        if (context.contains(rel.name))
          None //We've already solved this pattern.
        else
          twoNodes(leftNode, rightNode)
      }

      case _ => Some(this -> CanNotAdvance())
    }
  }

  // These are the nodes that have properties defined. They should always go first,
  // so any other links that use these nodes have to have them locked.
  def nodesWProps:Seq[NamedExpectation] = Seq(left,right).filter(_.properties.nonEmpty)

  lazy val identifier2 = Seq(left.name -> CTNode, right.name -> CTNode, rel.name -> CTRelationship)

  def symbolTableDependencies:Set[String] = left.properties.symboltableDependencies ++
    right.properties.symboltableDependencies ++
    rel.properties.symboltableDependencies

  def rewrite(f: (Expression) => Expression): UniqueLink = {
    val s = NamedExpectation(left.name, left.properties.rewrite(f), left.labels.map(_.typedRewrite[KeyToken](f)))
    val e = NamedExpectation(right.name, right.properties.rewrite(f), right.labels.map(_.typedRewrite[KeyToken](f)))
    val r = NamedExpectation(rel.name, rel.properties.rewrite(f), rel.labels.map(_.typedRewrite[KeyToken](f)))
    UniqueLink(s, e, r, relType, dir)
  }

  override def toString = {
    val relInfo = {
      val relName = if (UnNamedNameGenerator.notNamed(rel.name)) rel.name.drop(9) else "`" + rel.name + "`"
      val props = if (rel.properties.isEmpty)
        ""
      else
        "{" + rel.properties.map {
          case (k, v) => k.toString + ": " + v.toString
        }.mkString(",") + "}"

      "[%s:`%s`%s]".format(relName, relType, props)
    }

    left.toString + leftArrow(dir) + relInfo + rightArrow(dir) + right.toString
  }

  def children = Seq(left.e, right.e, rel.e)

  def optional: Boolean = false

  def possibleStartPoints = Seq(
    left.name -> CTNode,
    right.name -> CTNode,
    rel.name -> CTRelationship)

  def predicate = True()

  def relTypes = Seq(relType)

  def nodes = Seq(left.name, right.name)

  def rels = Seq(rel.name)

  def names = Seq(left.name, right.name, rel.name)

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

    if ((expectation.name != left.name) && (expectation.name != right.name)) {
      this
    } else {
      copy(left = compareAndMatch(left), right = compareAndMatch(right))
    }
  }

  def effects(symbols: SymbolTable): Effects = {
    val hasBothEndNodesInScope = symbols.hasIdentifierNamed(left.name) && symbols.hasIdentifierNamed(right.name)

    if (hasBothEndNodesInScope)
      Effects(ReadsRelationships, ReadsAnyRelationshipProperty, WritesAnyRelationshipProperty, WritesRelationships)
    else
      AllEffects
  }
}
