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

// Use an explicit implicit ctor param instead of `[E: ClassTag]` so Scala 2.13 callers
// (e.g. neo4j-ast) see a normal ctor when linking against Scala 3–compiled util.
abstract class TreeZipper[E <: TreeElem[E]](implicit val elemClassTag: ClassTag[E]) {

  def apply(treeElem: E): Location = Location(treeElem, Top)

  sealed trait Context
  final class Top private[helpers] () extends Context
  final val Top: Top = new Top
  final class TreeContext(val left: List[E], val parent: Location, val right: List[E]) extends Context

  private def children(value: Any): Option[Seq[E]] = elemClassTag.unapply(value).map(_.children)

  case class Location(elem: E, context: Context) {
    self =>

    def isRoot: Boolean = context eq Top

    def isLeaf: Boolean = children(elem).contains(Seq.empty)

    @tailrec
    final def root: Location = context match {
      case _: Top =>
        self
      case tc: TreeContext =>
        val parent = tc.parent
        Location(parent.elem.updateChildren(tc.left.reverse ++ List(elem) ++ tc.right), parent.context).root
    }

    def isLeftMost: Boolean = context match {
      case tc: TreeContext => tc.left.isEmpty
      case _: Top          => true
    }

    def left: Option[Location] = context match {
      case tc: TreeContext if tc.left.isEmpty =>
        None
      case tc: TreeContext =>
        val head = tc.left.head
        val tail = tc.left.tail
        Some(Location(head, new TreeContext(tail, tc.parent, elem +: tc.right)))
      case _ =>
        throw new IllegalStateException("Not in tree context when going left")
    }

    def leftList: List[E] = context match {
      case _: Top =>
        Nil

      case tc: TreeContext =>
        tc.left
    }

    def leftMost: Location = context match {
      case tc: TreeContext if tc.left.isEmpty =>
        self
      case tc: TreeContext =>
        val left = tc.left
        Location(left.last, new TreeContext(List.empty, tc.parent, left.init.reverse ++ List(elem) ++ tc.right))
      case otherContext =>
        throw new IllegalStateException(s"Cannot navigate from $otherContext")
    }

    def isRightMost: Boolean = context match {
      case tc: TreeContext => tc.right.isEmpty
      case _: Top          => true
    }

    def right: Option[Location] = context match {
      case tc: TreeContext if tc.right.isEmpty =>
        None
      case tc: TreeContext =>
        val head = tc.right.head
        val tail = tc.right.tail
        Some(Location(head, new TreeContext(elem +: tc.left, tc.parent, tail)))
      case _ =>
        throw new IllegalStateException("Not in tree context when going right")
    }

    def rightMost: Location = context match {
      case tc: TreeContext if tc.right.isEmpty =>
        self
      case tc: TreeContext =>
        val right = tc.right
        Location(right.last, new TreeContext(right.init.reverse ++ List(elem) ++ tc.left, tc.parent, List.empty))
      case otherContext =>
        throw new IllegalStateException(s"Cannot navigate from $otherContext")
    }

    def down: Option[Location] = children(elem) match {
      case Some(Seq()) =>
        None
      case Some(head +: tail) =>
        Some(Location(head, new TreeContext(Nil, self, tail.toList)))
      case _ =>
        throw new IllegalStateException(s"Unexpected type $elem")
    }

    def up: Option[Location] = context match {
      case _: Top =>
        None
      case tc: TreeContext =>
        val parent = tc.parent
        Some(Location(parent.elem.updateChildren(tc.left.reverse ++ List(elem) ++ tc.right), parent.context))
    }

    def replace(newElem: E): Location =
      Location(newElem, context)

    def replaceLeftList(newLeft: List[E]): Location = context match {
      case _: Top =>
        self

      case tc: TreeContext =>
        Location(elem, new TreeContext(newLeft, tc.parent, tc.right))
    }

    def insertLeft(newElem: E): Option[Location] = context match {
      case _: Top =>
        None

      case tc: TreeContext =>
        Some(Location(newElem, new TreeContext(tc.left, tc.parent, elem +: tc.right)))
    }

    def insertRight(newElem: E): Option[Location] = context match {
      case _: Top =>
        None

      case tc: TreeContext =>
        Some(Location(newElem, new TreeContext(elem +: tc.left, tc.parent, tc.right)))
    }

    def insertChild(newElem: E): Location =
      Location(newElem, new TreeContext(elem.children.toList.reverse, self, List.empty))
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
