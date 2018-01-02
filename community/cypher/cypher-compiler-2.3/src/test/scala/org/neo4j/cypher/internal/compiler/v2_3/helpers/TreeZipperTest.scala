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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import org.neo4j.cypher.internal.frontend.v2_3.helpers.{TreeElem, TreeZipper}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class TreeZipperTest extends CypherFunSuite {
  import Tapper._

  case class TestElem(name: String, children: Seq[TestElem]) extends TreeElem[TestElem] {
    def updateChildren(newChildren: Seq[TestElem]): TestElem = copy(children = newChildren.toVector)
  }

  implicit object TestElemZipper extends TreeZipper[TestElem]

  val grandChild1 = TestElem("grandchild1", Seq())
  val grandChild2 = TestElem("grandchild2", Seq())
  val child1 = TestElem("child1", Seq(grandChild1, grandChild2))
  val child2 = TestElem("child2", Seq())
  val child3 = TestElem("child3", Seq())
  val child4 = TestElem("child4", Seq())
  val root = TestElem("parent", Seq(child1, child2, child3, child4))

  test("Can get location in a single element tree") {
    root.location.elem should equal(root)
  }

  test("Can navigate to root of tree") {
    root.location.down.down.up.up.elem.get should equal(root)
  }

  test("Can navigate rightMost across children") {
    root.location.down.rightMost.elem.get should equal(child4)
  }

  test("Can navigate right across children") {
    root.location
      .down.tap { _.elem should equal(Some(child1)) }
      .right.tap { _.elem should equal(Some(child2)) }
      .right.tap { _.elem should equal(Some(child3)) }
      .right.tap { _.elem should equal(Some(child4)) }
  }

  test("Can navigate leftMost across children") {
    root.location.down.right.right.right.leftMost.elem should equal(Some(child1))
  }

  test("Can navigate left across children") {
    root.location
      .down.rightMost.tap { _.elem should equal(Some(child4)) }
      .left.tap { _.elem should equal(Some(child3)) }
      .left.tap { _.elem should equal(Some(child2)) }
      .left.tap { _.elem should equal(Some(child1)) }
  }

  test("Correctly infers tree structure") {
    def assertRole(location: TestElemZipper.Location, isRoot: Boolean, isLeftMost: Boolean, isRightMost: Boolean, isLeaf: Boolean): Unit = {
      location.isRoot should equal(isRoot)
      location.isLeftMost should equal(isLeftMost)
      location.isRightMost should equal(isRightMost)
      location.isLeaf should equal(isLeaf)
      location.root.elem should equal(root)
    }

    root
      // root
      .location.tap { assertRole(_, isRoot = true, isLeftMost = true, isRightMost = true, isLeaf = false) }

      // child1
      .down.tapSomeOrFail { assertRole(_, isRoot = false, isLeftMost = true, isRightMost = false, isLeaf = false) }

      // grandchild1
      .down.tapSomeOrFail { assertRole(_, isRoot = false, isLeftMost = true, isRightMost = false, isLeaf = true) }

      // grandchild2
      .right.tapSomeOrFail { assertRole(_, isRoot = false, isLeftMost = false, isRightMost = true, isLeaf = true) }

      // child1
      .up.tapSomeOrFail { assertRole(_, isRoot = false, isLeftMost = true, isRightMost = false, isLeaf = false) }

      // child2
      .right.tapSomeOrFail { assertRole(_, isRoot = false, isLeftMost = false, isRightMost = false, isLeaf = true) }

      // child3
      .right.tapSomeOrFail { assertRole(_, isRoot = false, isLeftMost = false, isRightMost = false, isLeaf = true) }

      // child3
      .right.tapSomeOrFail { assertRole(_, isRoot = false, isLeftMost = false, isRightMost = true, isLeaf = true) }
  }

  test("Can replace elem without siblings") {
    val newElem = TestElem("updated", Seq())

    val updatedRoot = root.location.replace(newElem)

    updatedRoot.elem should equal(newElem)
  }

  test("Can replace elem with siblings") {
    val newElem = TestElem("updated", Seq())

    val updatedRoot = root.location.down.right.right.get.replace(newElem).root

    updatedRoot.elem.children should equal(Seq(child1, child2, newElem, child4))
  }

  test("Can add new elem to left") {
    val newElem = TestElem("child0", Seq())

    val child2Location = root.location.down.right.get
    val newChildLocation = child2Location.insertLeft(newElem).get
    newChildLocation.elem should equal(newElem)
    val updatedRoot = newChildLocation.root.elem

    updatedRoot should not equal root
    updatedRoot.children should equal(Seq(child1, newElem, child2, child3, child4))
  }

  test("Can add new elem to left of leftMost") {
    val newElem = TestElem("child0", Seq())

    val child1Location = root.location.down.get
    val newChildLocation = child1Location.insertLeft(newElem).get
    newChildLocation.elem should equal(newElem)
    val updatedRoot = newChildLocation.root.elem

    updatedRoot should not equal root
    updatedRoot.children should equal(Seq(newElem, child1, child2, child3, child4))
  }

  test("Can add new elem to right") {
    val newElem = TestElem("child0", Seq())

    val child1Location = root.location.down.get
    val newChildLocation = child1Location.insertRight(newElem).get
    newChildLocation.elem should equal(newElem)
    val updatedRoot = newChildLocation.root.elem

    updatedRoot should not equal root
    updatedRoot.children should equal(Seq(child1, newElem, child2, child3, child4))
  }

  test("Can add new elem to right of rightMost") {
    val newElem = TestElem("child0", Seq())

    val child4Location = root.location.down.rightMost.get
    val newChildLocation = child4Location.insertRight(newElem).get
    newChildLocation.elem should equal(newElem)
    val updatedRoot = newChildLocation.root.elem

    updatedRoot should not equal root
    updatedRoot.children should equal(Seq(child1, child2, child3, child4, newElem))
  }

  test("Can add new only child") {
    val newElem = TestElem("child0", Seq())

    val newChildLocation = root.location.down.right.get.insertChild(newElem)
    newChildLocation.elem should equal(newElem)
    val updatedRootLocation = newChildLocation.root

    updatedRootLocation.elem should not equal root

    val updatedChild2 = updatedRootLocation.down.right.get.elem
    updatedChild2 should not equal child2
    updatedChild2.name should equal(child2.name)
    updatedChild2.children should equal(Seq(newElem))
  }

  test("Can add new child to existing children") {
    val newElem = TestElem("child0", Seq())

    val newChildLocation = root.location.insertChild(newElem)
    newChildLocation.elem should equal(newElem)
    val updatedRoot = newChildLocation.root.elem

    updatedRoot should not equal root
    updatedRoot.children should equal(Seq(child1, child2, child3, child4, newElem))
  }
}
