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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.frontend.v2_3.SyntaxException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection

import scala.collection.Map

abstract sealed class AbstractPattern extends AstNode[AbstractPattern] {
  def makeOutgoing: AbstractPattern

  def parsedEntities: Seq[ParsedEntity]

  def possibleStartPoints:Seq[(String,CypherType)]

  def name:String

  def start:AbstractPattern
}


object PatternWithEnds {
  def unapply(p: AbstractPattern): Option[(ParsedEntity, ParsedEntity, Seq[String], SemanticDirection, Boolean, Option[Int], Option[String])] = p match {
    case ParsedVarLengthRelation(name, _, start, end, typ, dir, optional, None, maxHops, relIterator) => Some((start, end, typ, dir, optional, maxHops, relIterator))
    case ParsedVarLengthRelation(_, _, _, _, _, _, _, Some(x), _, _)                                  => throw new SyntaxException("Shortest path does not support a minimal length")
    case ParsedRelation(name, _, start, end, typ, dir, optional)                                      => Some((start, end, typ, dir, optional, Some(1), Some(name)))
    case _                                                                                            => None
  }
}

object ParsedEntity {
  def apply(name:String) = new ParsedEntity(name, Identifier(name), Map.empty, Seq.empty)
}

case class ParsedEntity(name: String,
                        expression: Expression,
                        props: Map[String, Expression],
                        labels: Seq[KeyToken]) extends AbstractPattern with GraphElementPropertyFunctions {
  def makeOutgoing = this

  def parsedEntities = Seq(this)

  def children: Seq[AstNode[_]] = Seq(expression) ++ props.values

  def rewrite(f: (Expression) => Expression) =
    copy(expression = expression.rewrite(f), props = props.rewrite(f), labels = labels.map(_.rewrite(f)))

  def possibleStartPoints: Seq[(String, CypherType)] = Seq(name -> CTNode)

  def start: AbstractPattern = this

  def end: AbstractPattern = this

  def asSingleNode = new SingleNode(name, labels)
}

object ParsedRelation {
  def apply(name: String, start: String, end: String, typ: Seq[String], dir: SemanticDirection): ParsedRelation =
    new ParsedRelation(name, Map.empty, ParsedEntity(start), ParsedEntity(end), typ, dir, false)
}

abstract class PatternWithPathName(val pathName: String) extends AbstractPattern {
  def rename(newName: String): PatternWithPathName
}

case class ParsedRelation(name: String,
                          props: Map[String, Expression],
                          start: ParsedEntity,
                          end: ParsedEntity,
                          typ: Seq[String],
                          dir: SemanticDirection,
                          optional: Boolean) extends PatternWithPathName(name)
with Turnable
with GraphElementPropertyFunctions {
  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def turn(start: ParsedEntity, end: ParsedEntity, dir: SemanticDirection): AbstractPattern =
    copy(start = start, end = end, dir = dir)

  def parsedEntities = Seq(start, end)

  def children: Seq[AstNode[_]] = Seq(start, end) ++ props.values

  def rewrite(f: (Expression) => Expression) =
    copy(props = props.rewrite(f), start = start.rewrite(f), end = end.rewrite(f))

  def possibleStartPoints: Seq[(String, CypherType)] =
    (start.possibleStartPoints :+ name -> CTRelationship) ++ end.possibleStartPoints
}

trait Turnable {
  def turn(start: ParsedEntity, end: ParsedEntity, dir: SemanticDirection): AbstractPattern

  // It's easier on everything if all relationships are either outgoing or both, but never incoming.
  // So we turn all patterns around, facing the same way
  def dir: SemanticDirection

  def start: ParsedEntity

  def end: ParsedEntity

  def makeOutgoing: AbstractPattern = {
    dir match {
      case SemanticDirection.INCOMING => turn(start = end, end = start, dir = SemanticDirection.OUTGOING)
      case SemanticDirection.OUTGOING => this.asInstanceOf[AbstractPattern]
      case SemanticDirection.BOTH     => (start.expression, end.expression) match {
        case (Identifier(a), Identifier(b)) if a < b  => this.asInstanceOf[AbstractPattern]
        case (Identifier(a), Identifier(b)) if a >= b => turn(start = end, end = start, dir = dir)
        case _                                        => this.asInstanceOf[AbstractPattern]
      }
    }
  }

}


case class ParsedVarLengthRelation(name: String,
                                   props: Map[String, Expression],
                                   start: ParsedEntity,
                                   end: ParsedEntity,
                                   typ: Seq[String],
                                   dir: SemanticDirection,
                                   optional: Boolean,
                                   minHops: Option[Int],
                                   maxHops: Option[Int],
                                   relIterator: Option[String])
  extends PatternWithPathName(name)
  with Turnable
  with GraphElementPropertyFunctions {
  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def turn(start: ParsedEntity, end: ParsedEntity, dir: SemanticDirection): AbstractPattern =
    copy(start = start, end = end, dir = dir)

  def parsedEntities = Seq(start, end)

  def children: Seq[AstNode[_]] = Seq(start, end) ++ props.values

  def rewrite(f: (Expression) => Expression) =
    copy(props = props.rewrite(f), start = start.rewrite(f), end = end.rewrite(f))

  def possibleStartPoints: Seq[(String, CypherType)] =
    (start.possibleStartPoints :+ name -> CTCollection(CTRelationship)) ++ end.possibleStartPoints
}

case class ParsedShortestPath(name: String,
                              props: Map[String, Expression],
                              start: ParsedEntity,
                              end: ParsedEntity,
                              typ: Seq[String],
                              dir: SemanticDirection,
                              optional: Boolean,
                              maxDepth: Option[Int],
                              single: Boolean,
                              relIterator: Option[String])
  extends PatternWithPathName(name) with GraphElementPropertyFunctions {
  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def makeOutgoing = this

  def parsedEntities = Seq(start, end)

  def children: Seq[AstNode[_]] = Seq(start, end) ++ props.values

  def rewrite(f: (Expression) => Expression) =
    copy(props = props.rewrite(f), start = start.rewrite(f), end = end.rewrite(f))

  def possibleStartPoints: Seq[(String, CypherType)] =
    (start.possibleStartPoints :+ name -> CTPath) ++ end.possibleStartPoints
}

case class ParsedNamedPath(name: String, pieces: Seq[AbstractPattern]) extends PatternWithPathName(name) {

  assert(pieces.nonEmpty)

  def rename(newName: String): PatternWithPathName = copy(name = newName)

  def makeOutgoing = this

  def parsedEntities = pieces.flatMap(_.parsedEntities)

  def children: Seq[AstNode[_]] = pieces

  def rewrite(f: (Expression) => Expression): AbstractPattern = copy(pieces = pieces.map(_.rewrite(f)))

  def possibleStartPoints: Seq[(String, CypherType)] = pieces.flatMap(_.possibleStartPoints)

  def start: AbstractPattern = pieces.head

  def end: AbstractPattern = pieces.last
}

