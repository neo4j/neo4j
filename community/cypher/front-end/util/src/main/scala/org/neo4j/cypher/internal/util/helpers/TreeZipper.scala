/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.helpers

import scala.annotation.tailrec
import scala.reflect.ClassTag

trait TreeElem[E <: TreeElem[E]] {
  self: E =>

  def children: Seq[E]
  def updateChildren(newChildren: Seq[E]): E
  def location(implicit zipper: TreeZipper[E]): zipper.Location = zipper(self)
}

abstract class TreeZipper[E <: TreeElem[E] : ClassTag] {

  def apply(treeElem: E): Location = Location(treeElem, Top)

  sealed trait Context
  case object Top extends Context
  case class TreeContext(left: List[E], parent: Location, right: List[E]) extends Context

  object Children {
    def unapply(v: Any): Option[Seq[E]] = implicitly[ClassTag[E]].unapply(v).map(_.children)
  }

  case class Location(elem: E, context: Context) {
    self =>

    def isRoot: Boolean = context == Top

    def isLeaf: Boolean = self match {
      case Location(Children(Seq()), _) => true
      case _                            => false
    }

    @tailrec
    final def root: Location = self match {
      case Location(_, Top) =>
        self

      case Location(_, TreeContext(left, Location(parentElem, parentContext), right)) =>
        Location(parentElem.updateChildren(left.reverse ++ List(elem) ++ right), parentContext).root
    }

    def isLeftMost: Boolean = context match {
      case TreeContext(Nil, _, _) => true
      case Top                    => true
      case _                      => false
    }

    def left: Option[Location] = context match {
      case TreeContext(Nil, _, _) =>
        None

      case TreeContext(head :: tail, parent, right) =>
        Some(Location(head, TreeContext(tail, parent, elem +: right)))

      case _ =>
        throw new IllegalStateException("Not in tree context when going left")
    }

    def leftList: List[E] = context match {
      case Top =>
        Nil

      case TreeContext(left, _, _) =>
        left
    }

    def leftMost: Location = context match {
      case TreeContext(Nil, _, _) =>
        self

      case TreeContext(left, parent, right) =>
        Location(left.last, TreeContext(List.empty, parent, left.init.reverse ++ List(elem) ++ right))

      case otherContext =>
        throw new IllegalStateException(s"Cannot navigate from $otherContext")
    }

    def isRightMost: Boolean = context match {
      case TreeContext(_, _, Nil) => true
      case Top                    => true
      case _                      => false
    }

    def right: Option[Location] = context match {
      case TreeContext(_, _, Nil) =>
        None

      case TreeContext(left, parent, head :: tail) =>
        Some(Location(head, TreeContext(elem +: left, parent, tail)))

      case _ =>
        throw new IllegalStateException("Not in tree context when going right")
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
      case Location(Children(Seq()), _) =>
        None

      case Location(Children(Seq(head, tail @ _*)), _) =>
        Some(Location(head, TreeContext(Nil, self, tail.toList)))

      case other => throw new IllegalStateException(s"Unexpected type $other")
    }

    def up: Option[Location] = self match {
      case Location(_, Top) =>
        None

      case Location(_, TreeContext(left, Location(parentElem, parentContext), right)) =>
        Some(Location(parentElem.updateChildren(left.reverse ++ List(elem) ++ right), parentContext))
    }

    def replace(newElem: E): Location =
      Location(newElem, context)

    def replaceLeftList(newLeft: List[E]): Location = self match {
      case Location(_, Top) =>
        self

      case Location(tree, TreeContext(_, parent, right)) =>
        Location(tree, TreeContext(newLeft, parent, right))
    }

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
    def elem: Option[E] = location.map(_.elem)

    def isRoot: Option[Boolean] = location.map(_.isRoot)
    def isLeaf: Option[Boolean] = location.map(_.isLeaf)

    def root: Option[Location] = location.map(_.root)

    def isLeftMost: Option[Boolean] = location.map(_.isLeftMost)
    def left: Option[Location] = location.flatMap(_.left)
    def leftList: Option[List[E]] = location.map(_.leftList)
    def leftMost: Option[Location] = location.map(_.leftMost)

    def isRightMost: Option[Boolean] = location.map(_.isLeftMost)
    def right: Option[Location] = location.flatMap(_.right)
    def rightMost: Option[Location] = location.map(_.rightMost)

    def down: Option[Location] = location.flatMap(_.down)
    def up: Option[Location] = location.flatMap(_.up)

    def replace(replacementElem: E): Option[Location] = location.map(_.replace(replacementElem))
    def replaceLeftList(replacementList: List[E]): Option[Location] = location.map(_.replaceLeftList(replacementList))
    def insertLeft(newElem: E): Option[Location] = location.flatMap(_.insertLeft(newElem))
    def insertRight(newElem: E): Option[Location] = location.flatMap(_.insertRight(newElem))
    def insertChild(newElem: E): Option[Location] = location.map(_.insertChild(newElem))
  }
}
