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
package org.neo4j.cypher.internal.commands.values

sealed abstract class Ternary(val isApplicable: Boolean, val isTrue: Boolean, val isFalse: Boolean)
  extends (Any => Boolean) {
  val negated: Ternary
  val isTrueOrNotApplicable: Boolean = isTrue

  def toApplicableOption: Option[Ternary] = if (isApplicable) Some(this) else None
  def and(other: Ternary): Ternary
  def or(other: Ternary): Ternary
  def xor(other: Ternary): Ternary
  def unapply(value: Any): Option[Ternary] = if (apply(value)) Some(this) else None
}

object IsApplicable {
  def unapply(v: Any) = Ternary(v).toApplicableOption
}

/**
 * This represents truth in ternary (3-valued) logic
 */
case object IsTrue extends Ternary(isApplicable = true, isTrue = true, isFalse = false) {
  override val negated: Ternary = IsFalse

  override def and(other: Ternary): Ternary = other
  override def or(other: Ternary): Ternary = IsTrue
  override def xor(other: Ternary): Ternary = if (other.isApplicable) Ternary(other.isFalse) else NotApplicable
  override def apply(other: Any): Boolean = true == other || IsTrue == other
  override def toString() = "true"
}

/**
 * This represents falsehood in ternary (3-valued) logic
 */
case object IsFalse extends Ternary(isApplicable = true, isTrue = false, isFalse = true) {
  override val negated: Ternary = IsTrue

  override def and(other: Ternary): Ternary = IsFalse
  override def or(other: Ternary): Ternary = other
  override def xor(other: Ternary): Ternary = if (other.isApplicable) Ternary(other.isTrue) else NotApplicable
  override def apply(other: Any): Boolean = false == other || IsFalse == other
  override def toString() = "false"
}

/**
 *  This represents unknown information or a maybe truth value in ternary (3-valued) logic
 */
case object NotApplicable extends Ternary(isApplicable = false, isTrue = false, isFalse = false) {
  override val negated: Ternary = NotApplicable
  override val isTrueOrNotApplicable: Boolean = true

  override def and(other: Ternary): Ternary = if (other.isFalse) IsFalse else NotApplicable
  override def or(other: Ternary): Ternary = if (other.isTrue) IsTrue else NotApplicable
  override def xor(other: Ternary): Ternary = NotApplicable
  override def apply(other: Any): Boolean = NotApplicable == other
  override def toString() = "<not_applicable>"
}

object Ternary {
  def apply(b: Boolean) = if (b) IsTrue else IsFalse

  def apply(v: Any): Ternary = v match {
    case b: Boolean => apply(b)
    case v: Ternary => v
    case _          => NotApplicable
  }

  val values = Set(IsTrue, IsFalse, NotApplicable)

  def forall(elements: Seq[Ternary]): Ternary = forall[Ternary](elements)(identity)

  def forall[U](elements: Seq[U])(p: U => Ternary): Ternary = {
    var foundNotApplicable = false

    for (e <- elements) {
      p(e) match {
        case IsFalse       => return IsFalse
        case NotApplicable => foundNotApplicable = true
        case IsTrue        =>
      }
    }
    if (foundNotApplicable) NotApplicable else IsTrue
  }

  def exists(elements: Seq[Ternary]): Ternary = exists[Ternary](elements)(identity)

  def exists[U](elements: Seq[U])(p: U => Ternary): Ternary = {
    for (e <- elements) {
      p(e) match {
        case IsTrue        => return IsTrue
        case NotApplicable => return NotApplicable
        case IsFalse       =>
      }
    }
    IsFalse
  }

  def none(elements: Seq[Ternary]): Ternary = none[Ternary](elements)(identity)

  def none[U](elements: Seq[U])(p: U => Ternary): Ternary = {
    for (e <- elements) {
      p(e) match {
        case IsTrue        => return IsFalse
        case NotApplicable => return NotApplicable
        case IsFalse       =>
      }
    }
    IsTrue
  }

  def single(elements: Seq[Ternary]): Ternary = single[Ternary](elements)(identity)

  def single[U](elements: Seq[U])(p: U => Ternary): Ternary = {
    var foundTrue          = false
    var foundNotApplicable = false

    for (e <- elements) {
      p(e) match {
        case IsTrue if foundTrue => return IsFalse
        case IsTrue              => foundTrue = true
        case NotApplicable       => foundNotApplicable = true
        case IsFalse             =>
      }
    }

    if (foundNotApplicable) NotApplicable else Ternary(foundTrue)
  }
}
