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
package org.neo4j.cypher.internal.pipes.matching

import org.neo4j.graphdb._
import org.neo4j.cypher.internal.commands.Predicate
import collection.mutable
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.EntityNotFoundException
import org.neo4j.helpers.ThisShouldNotHappenError


trait ExpanderStep {
  def next: Option[ExpanderStep]

  def typ: Seq[String]

  def direction: Direction

  def id: Int

  def relPredicate: Predicate

  def nodePredicate: Predicate

  def createCopy(next: Option[ExpanderStep], direction: Direction, nodePredicate: Predicate): ExpanderStep

  def size: Option[Int]

  def expand(node: Node, parameters: ExecutionContext): (Iterable[Relationship], Option[ExpanderStep])

  def shouldInclude(): Boolean

  /*
  The way we reverse the steps is by first creating a Seq out of the steps. In this Seq, the first element points to
  Some(second), the second to Some(third), und so weiter, until the last element points to None.

  By doing a fold left and creating copies along the way, we reverse the directions - we push in None as what the first
  element will end up pointing to, and pass the steps to the next step. The result is that the first element points to
  None, the second to Some(first), und so weiter, until we pop out the last step as our reversed expander
   */
  def reverse(): ExpanderStep = {
    val allSteps = this.asSeq()

    val reversed = allSteps.foldLeft[Option[ExpanderStep]]((None)) {
      case (last, step) =>
        val p = step.next.map(_.nodePredicate).getOrElse(True())
        Some(step.createCopy(next = last, direction = step.direction.reverse(), nodePredicate = p))
    }

    assert(reversed.nonEmpty, "The reverse of an expander should never be empty")

    reversed.get
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

abstract class MiniMapProperty(originalName: String, prop: String) extends Expression {
  protected def calculateType(symbols: SymbolTable) = fail()

  def filter(f: (Expression) => Boolean) = fail()

  def rewrite(f: (Expression) => Expression) = fail()

  def symbolTableDependencies = fail()

  def apply(ctx: ExecutionContext) = {
    ctx match {
      case m: MiniMap => {
        val pc = extract(m)
        try {
          pc match {
            case n:Node=>ctx.state.query.nodeOps().getProperty(n, prop)
            case r:Relationship=>ctx.state.query.relationshipOps().getProperty(r, prop)
          }
        } catch {
          case x: NotFoundException =>
            throw new EntityNotFoundException("The property '%s' does not exist on %s, which was found with the identifier: %s".format(prop, pc, originalName), x)
        }
      }
      case _          => fail()
    }
  }


  protected def fail() = throw new ThisShouldNotHappenError("Andres", "This predicate should never be used outside of the traversal matcher")

  protected def extract(m: MiniMap): PropertyContainer
}

abstract class MiniMapIdentifier(originalName:String) extends Expression {
  protected def calculateType(symbols: SymbolTable) = fail()

  def filter(f: (Expression) => Boolean) = fail()

  def rewrite(f: (Expression) => Expression) = fail()

  def symbolTableDependencies = fail()

  def apply(ctx: ExecutionContext) = ctx match {
    case m: MiniMap => extract(m)
    case _          => fail()
  }

  protected def extract(m: MiniMap): PropertyContainer

  def fail() = throw new ThisShouldNotHappenError("Andres", "This predicate should never be used outside of the traversal matcher")
}

case class MiniMapRelProperty(originalName: String, prop: String) extends MiniMapProperty(originalName, prop) {
  protected def extract(m: MiniMap) = m.relationship
}

case class MiniMapNodeProperty(originalName: String, prop: String) extends MiniMapProperty(originalName, prop) {
  protected def extract(m: MiniMap) = m.node
}

case class NodeIdentifier(name:String) extends MiniMapIdentifier(name) {
  protected def extract(m: MiniMap) = m.node
}

case class RelationshipIdentifier(name:String) extends MiniMapIdentifier(name) {
  protected def extract(m: MiniMap) = m.relationship
}

case class MiniMap(var relationship: Relationship, var node: Node, myState:QueryState)
  extends ExecutionContext(state = myState) {

  override def iterator = throw new RuntimeException

  override def -(key: String) = throw new RuntimeException

  override def +[B1 >: Any](kv: (String, B1)) = throw new RuntimeException

  override def newWith(newEntries: Seq[(String, Any)]) = throw new RuntimeException

  override def newWith(newEntries: scala.collection.Map[String, Any]) = throw new RuntimeException

  override def newFrom(newEntries: Seq[(String, Any)]) = throw new RuntimeException

  override def newFrom(newEntries: scala.collection.Map[String, Any]) = throw new RuntimeException

  override def newWith(newEntry: (String, Any)) = throw new RuntimeException
}