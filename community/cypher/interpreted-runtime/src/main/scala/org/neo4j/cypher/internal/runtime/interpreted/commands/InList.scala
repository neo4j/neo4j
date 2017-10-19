/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Closure, Expression}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue

import scala.collection.Seq

abstract class InList(collectionExpression: Expression, id: String, predicate: Predicate)
  extends Predicate
  with ListSupport
  with Closure {

  type CollectionPredicate = ((AnyValue) => Option[Boolean]) => Option[Boolean]

  def seqMethod(f: ListValue): CollectionPredicate

  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val list = collectionExpression(m, state)

    if (list == Values.NO_VALUE) None
    else {
      val seq = makeTraversable(list)

      seqMethod(seq)(item => predicate.isMatch(m.newWith1(id, item), state))
    }
  }

  def name: String

  override def toString() = name + "(" + id + " in " + collectionExpression + " where " + predicate + ")"

  def containsIsNull = predicate.containsIsNull

  override def children = Seq(collectionExpression, predicate)

  def arguments: scala.Seq[Expression] = Seq(collectionExpression)

  def symbolTableDependencies = symbolTableDependencies(collectionExpression, predicate, id)
}

case class AllInList(collection: Expression, symbolName: String, inner: Predicate)
  extends InList(collection, symbolName, inner) {

  private def forAll(collectionValue: ListValue)(predicate: (AnyValue => Option[Boolean])): Option[Boolean] = {
    var result: Option[Boolean] = Some(true)

    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next())  match {
        case Some(false) => return Some(false)
        case None        => result = None
        case _           =>
      }
    }
    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = forAll(value)
  def name = "all"

  def rewrite(f: (Expression) => Expression) =
    f(AllInList(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}

case class AnyInList(collection: Expression, symbolName: String, inner: Predicate)
  extends InList(collection, symbolName, inner) {

  private def exists(collectionValue: ListValue)(predicate: (AnyValue => Option[Boolean])): Option[Boolean] = {
    var result: Option[Boolean] = Some(false)
    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next()) match {
        case Some(true) => return Some(true)
        case None       => result = None
        case _          =>
      }
    }
    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = exists(value)

  def name = "any"

  def rewrite(f: (Expression) => Expression) =
    f(AnyInList(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}

case class NoneInList(collection: Expression, symbolName: String, inner: Predicate)
  extends InList(collection, symbolName, inner) {

  private def none(collectionValue: ListValue)(predicate: (AnyValue => Option[Boolean])): Option[Boolean] = {
    var result: Option[Boolean] = Some(true)

    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next()) match {
        case Some(true) => return Some(false)
        case None       => result = None
        case _          =>
      }
    }

    result
  }

  def seqMethod(value: ListValue): CollectionPredicate = none(value)

  def name = "none"

  def rewrite(f: (Expression) => Expression) =
    f(NoneInList(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}

case class SingleInList(collection: Expression, symbolName: String, inner: Predicate)
  extends InList(collection, symbolName, inner) {

  private def single(collectionValue: ListValue)(predicate: (AnyValue => Option[Boolean])): Option[Boolean] = {
    var matched = false
    val iterator = collectionValue.iterator()
    while(iterator.hasNext) {
      predicate(iterator.next()) match {
        case Some(true) if matched => return Some(false)
        case Some(true)            => matched = true
        case None                  => return None
        case _                     =>
      }
    }

    Some(matched)
  }

  def seqMethod(value: ListValue): CollectionPredicate = single(value)

  def name = "single"

  def rewrite(f: (Expression) => Expression) =
    f(SingleInList(
      collection = collection.rewrite(f),
      symbolName = symbolName,
      inner = inner.rewriteAsPredicate(f)))
}
