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
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Predicate, True}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{MutableMaps, QueryState}
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, EntityNotFoundException}
import org.neo4j.graphdb._
import org.neo4j.helpers.ThisShouldNotHappenError

import scala.collection.mutable
import scala.collection.mutable.{Map => MutableMap}

trait ExpanderStep {
  def next: Option[ExpanderStep]

  def typ: Seq[String]

  def direction: SemanticDirection

  def id: Int

  def relPredicate: Predicate

  def nodePredicate: Predicate

  def createCopy(next: Option[ExpanderStep], direction: SemanticDirection, nodePredicate: Predicate): ExpanderStep

  def size: Option[Int]

  def expand(node: Node, parameters: ExecutionContext, state:QueryState): (Iterable[Relationship], Option[ExpanderStep])

  def shouldInclude(): Boolean

  /*
  The way we reverse the steps is by first creating a Seq out of the steps. In this Seq, the first element points to
  Some(second), the second to Some(third), und so weiter, until the last element points to None.

  By doing a fold left and creating copies along the way, we reverse the directions - we push in None and True as the
  first tuple, as what the first reversed step will end use as node predicate and next step. We then pass the reversed
  step, and it's original predicate on to the next step. The result is that the first element points to
  None, the second to Some(first), und so weiter, until we pop out the last step as our reversed expander
   */
  def reverse(): ExpanderStep = {
    val allSteps: Seq[ExpanderStep] = this.asSeq()

    val reversed = allSteps.foldLeft[(Option[ExpanderStep], Predicate)]((None, True())) {
      case ((lastStep: Option[ExpanderStep], lastPred: Predicate), step: ExpanderStep) =>
        val newStep = Some(step.createCopy(next = lastStep, direction = step.direction.reversed, nodePredicate = lastPred))
        (newStep, step.nodePredicate)
    }

    reversed match {
      case (Some(result), _) => result
      case _                 => throw new ThisShouldNotHappenError("Andres", "Reverse should always succeed")
    }
  }

  private def asSeq(): Seq[ExpanderStep] = {
    var allSteps = mutable.Seq[ExpanderStep]()
    var current: Option[ExpanderStep] = Some(this)

    while (current.nonEmpty) {
      val step = current.get
      allSteps = allSteps :+ step
      current = step.next
    }

    allSteps.toSeq
  }
}

abstract class MiniMapProperty(originalName: String, propertyKeyName: String) extends Expression {
  protected def calculateType(symbols: SymbolTable) = fail()

  def arguments = Nil

  def rewrite(f: (Expression) => Expression) = fail()

  def symbolTableDependencies = fail()

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = {
    val qtx = state.query
    ctx match {
      case m: MiniMap => {
        val pc = extract(m)
        try {
          pc match {
            case n: Node         => qtx.nodeOps.getProperty(n.getId, qtx.getPropertyKeyId(propertyKeyName))
            case r: Relationship => qtx.relationshipOps.getProperty(r.getId, qtx.getPropertyKeyId(propertyKeyName))
          }
        } catch {
          case x: NotFoundException =>
            throw new EntityNotFoundException("The property '%s' does not exist on %s, which was found with the identifier: %s".format(propertyKeyName, pc, originalName), x)
        }
      }
      case _          => fail()
    }
  }


  protected def fail() = throw new ThisShouldNotHappenError("Andres", "This predicate should never be used outside of the traversal matcher")

  protected def extract(m: MiniMap): PropertyContainer
}

abstract class MiniMapIdentifier() extends Expression {
  protected def calculateType(symbols: SymbolTable) = fail()

  def arguments = Nil

  def rewrite(f: (Expression) => Expression) = fail()

  def symbolTableDependencies = fail()

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ctx match {
    case m: MiniMap => extract(m)
    case _          => fail()
  }

  protected def extract(m: MiniMap): PropertyContainer

  def fail() = throw new ThisShouldNotHappenError("Andres", "This predicate should never be used outside of the traversal matcher")
}

case class NodeIdentifier() extends MiniMapIdentifier() {
  protected def extract(m: MiniMap) = m.node
}

case class RelationshipIdentifier() extends MiniMapIdentifier() {
  protected def extract(m: MiniMap) = m.relationship
}

class MiniMap(var relationship: Relationship, var node: Node, myMap: MutableMap[String, Any] = MutableMaps.empty)
  extends ExecutionContext(m = myMap) {

  override def iterator = throw new ThisShouldNotHappenError("Andres", "This method should never be used")

  override def -(key: String) = throw new ThisShouldNotHappenError("Andres", "This method should never be used")

  override protected def createWithNewMap(newMap: mutable.Map[String, Any]) = new MiniMap(relationship, node, newMap)
}
