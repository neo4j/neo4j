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
package org.neo4j.cypher.internal.frontend.v2_3.helpers

import scala.annotation.tailrec
import scala.reflect.ClassTag

trait TreeElem[E <: TreeElem[E]] {
  self: E =>

  def children: Seq[E]
  def updateChildren(newChildren: Seq[E]): E
  def location(implicit zipper: TreeZipper[E]) = zipper(self)
}

abstract class TreeZipper[E <: TreeElem[E] : ClassTag] {

  def apply(treeElem: E) = Location(treeElem, Top)

  sealed trait Context
  case object Top extends Context
  case class TreeContext(left: List[E], parent: Location, right: List[E]) extends Context

  object Children {
    def unapply(v: Any) = implicitly[ClassTag[E]].unapply(v).map(_.children)
  }

  final case class Location(elem: E, context: Context) {
    self =>

    def isRoot = context == Top

    def isLeaf = self match {
      case Location(Children(Nil), _) => true
      case _ => false
    }

    @tailrec
    def root: Location = self match {
      case Location(_, Top) =>
        self

      case Location(_, TreeContext(left, Location(parentElem, parentContext), right)) =>
        Location(parentElem.updateChildren(left.reverse ++ List(elem) ++ right), parentContext).root
    }

    def isLeftMost = context match {
      case TreeContext(Nil, _, _) => true
      case Top => true
      case _ => false
    }

    def left: Option[Location] = context match {
      case TreeContext(Nil, _, _) =>
        None

      case TreeContext(head :: tail, parent, right) =>
        Some(Location(head, TreeContext(tail, parent, elem +: right)))

      case _ =>
        throw new IllegalStateException("Not in tree context when going left")
    }

    def leftMost: Location = context match {
      case TreeContext(Nil, _, _) =>
        self

      case TreeContext(left, parent, right) =>
        Location(left.last, TreeContext(List.empty, parent, left.init.reverse ++ List(elem) ++ right))

      case otherContext =>
        throw new IllegalStateException(s"Cannot navigate from $otherContext")
    }

    def isRightMost = context match {
      case TreeContext(_, _, Nil) => true
      case Top => true
      case _ => false
    }

    def right: Option[Location] = context match {
      case TreeContext(_, _, Nil) =>
        None

      case TreeContext(left, parent, head :: tail) =>
        Some(Location(head, TreeContext(elem +: left, parent, tail)))

      case _ =>
        throw new IllegalStateException("Not in tree context when going left")
    }

    def rightMost: Location = context match {
      case TreeContext(_, _, Nil) =>
        self

      case TreeContext(left, parent, right) =>
        Location(right.last, TreeContext(right.init.reverse ++ List(elem) ++ left, parent, List.empty))

      case otherContext =>
        throw new IllegalStateException(s"Cannot navigate from $otherContext")
    }

    def down: Option[Location] = self match {
      case Location(Children(Nil), _) =>
        None

      case Location(Children(Seq(head, tail @ _*)), _) =>
        Some(Location(head, TreeContext(Nil, self, tail.toList)))
    }

    def up: Option[Location] = self match {
      case Location(_, Top) =>
        None

      case Location(_, TreeContext(left, Location(parentElem, parentContext), right)) =>
        Some(Location(parentElem.updateChildren(left.reverse ++ List(elem) ++ right), parentContext))
    }

    def replace(newElem: E): Location =
      Location(newElem, context)

    def insertLeft(newElem: E): Option[Location] = self match {
      case Location(_, Top) =>
        None

      case Location(tree, TreeContext(left, parent, right)) =>
        Some(Location(newElem, TreeContext(left, parent, tree +: right)))
    }

    def insertRight(newElem: E): Option[Location] = self match {
      case Location(_, Top) =>
        None

      case Location(_, TreeContext(left, parent, right)) =>
        Some(Location(newElem, TreeContext(elem +: left, parent, right)))
    }

    def insertChild(newElem: E): Location =
      Location(newElem, TreeContext(elem.children.toList.reverse, self, List.empty))
  }

  implicit final class OptionalLocation(location: Option[Location]) {
    def elem = location.map(_.elem)

    def isRoot = location.map(_.isRoot)
    def isLeaf = location.map(_.isLeaf)

    def root = location.map(_.root)

    def isLeftMost = location.map(_.isLeftMost)
    def left = location.flatMap(_.left)
    def leftMost = location.map(_.leftMost)

    def isRightMost = location.map(_.isLeftMost)
    def right = location.flatMap(_.right)
    def rightMost = location.map(_.rightMost)

    def down = location.flatMap(_.down)
    def up = location.flatMap(_.up)

    def replace(replacementElem: E) = location.map(_.replace(replacementElem))
    def insertLeft(newElem: E) = location.flatMap(_.insertLeft(newElem))
    def insertRight(newElem: E) = location.flatMap(_.insertRight(newElem))
    def insertChild(newElem: E) = location.map(_.insertChild(newElem))
  }
}
