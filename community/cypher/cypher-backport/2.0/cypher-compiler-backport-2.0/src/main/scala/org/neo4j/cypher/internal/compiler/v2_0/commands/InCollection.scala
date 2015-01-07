/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import org.neo4j.cypher.internal.compiler.v2_0._
import expressions.{Closure, Expression}
import pipes.QueryState
import collection.Seq
import org.neo4j.cypher.internal.helpers.CollectionSupport

abstract class InCollection(collection: Expression, id: String, predicate: Predicate)
  extends Predicate
  with CollectionSupport
  with Closure {

  type CollectionPredicate[U] = ((U) => Option[Boolean]) => Option[Boolean]
  
  def seqMethod[U](f: Seq[U]): CollectionPredicate[U]

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    val seq = makeTraversable(collection(m)).toSeq

    seqMethod(seq)(item => predicate.isMatch(m.newWith(id -> item)))
  }

  def name: String

  override def toString() = name + "(" + id + " in " + collection + " where " + predicate + ")"

  def containsIsNull = predicate.containsIsNull

  override def children = Seq(collection, predicate)

  def arguments: scala.Seq[Expression] = Seq(collection)

  def symbolTableDependencies = symbolTableDependencies(collection, predicate, id)
}

case class AllInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {

  private def forAll[U](collectionValue: Seq[U])(predicate: (U => Option[Boolean])): Option[Boolean] = {
    var result: Option[Boolean] = Some(true)

    for (item <- collectionValue) {
      predicate(item) match {
        case Some(false) => return Some(false)
        case None        => result = None
        case _           =>
      }
    }

    result
  }

  def seqMethod[U](value: Seq[U]): CollectionPredicate[U] = forAll(value)
  def name = "all"

  def rewrite(f: (Expression) => Expression) =
    f(AllInCollection(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}

case class AnyInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {

  private def exists[U](collectionValue: Seq[U])(predicate: (U => Option[Boolean])): Option[Boolean] = {
    var result: Option[Boolean] = Some(false)

    for (item <- collectionValue) {
      predicate(item) match {
        case Some(true) => return Some(true)
        case None       => result = None
        case _          =>
      }
    }

    result
  }

  def seqMethod[U](value: Seq[U]): CollectionPredicate[U] = exists(value)

  def name = "any"

  def rewrite(f: (Expression) => Expression) =
    f(AnyInCollection(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}

case class NoneInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {

  private def none[U](collectionValue: Seq[U])(predicate: (U => Option[Boolean])): Option[Boolean] = {
    var result: Option[Boolean] = Some(true)

    for (item <- collectionValue) {
      predicate(item) match {
        case Some(true) => return Some(false)
        case None       => result = None
        case _          =>
      }
    }

    result
  }

  def seqMethod[U](value: Seq[U]): CollectionPredicate[U] = none(value)

  def name = "none"

  def rewrite(f: (Expression) => Expression) =
    f(NoneInCollection(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}

case class SingleInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {

  private def single[U](collectionValue: Seq[U])(predicate: (U => Option[Boolean])): Option[Boolean] = {
    var matched = false

    for (item <- collectionValue) {
      predicate(item) match {
        case Some(true) if matched => return Some(false)
        case Some(true)            => matched = true
        case None                  => return None
        case _                     =>
      }
    }

    Some(matched)
  }

  def seqMethod[U](value: Seq[U]): CollectionPredicate[U] = single(value)

  def name = "single"

  def rewrite(f: (Expression) => Expression) =
    f(SingleInCollection(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}
