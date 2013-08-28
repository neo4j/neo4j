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
package org.neo4j.cypher.internal.commands

import collection.Seq
import expressions.{Closure, Expression}
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.commands.values.{NotBound, NotApplicable, Ternary}

abstract class InCollection(collection: Expression, id: String, predicate: Predicate)
  extends TernaryPredicate
  with CollectionSupport
  with Closure {

  def seqMethod[U](f: Seq[U])(p: (U) => Ternary): Ternary

  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary = {
    val value = collection(m)
    value match {
      case NotBound => NotApplicable
      case coll     =>
        val seq = makeTraversable(coll).toSeq
        seqMethod(seq)(item => predicate.ternaryIsMatch(m.newWith(id -> item)))
    }
  }

  def name: String

  override def toString() = name + "(" + id + " in " + collection + " where " + predicate + ")"

  def containsIsNull = predicate.containsIsNull

  def children = Seq(collection, predicate)

  def assertInnerTypes(symbols: SymbolTable) {
    val innerType = collection.evaluateType(CollectionType(AnyType()), symbols).iteratedType
    predicate.throwIfSymbolsMissing(symbols.add(id, innerType))
  }

  def symbolTableDependencies = symbolTableDependencies(collection, predicate, id)
}

case class AllInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {
  override def seqMethod[U](f: Seq[U])(p: (U) => Ternary): Ternary = Ternary.forall(f)(p)

  def name = "all"

  def rewrite(f: (Expression) => Expression) = AllInCollection(collection.rewrite(f), symbolName, inner.rewrite(f))
}

case class AnyInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {
  override def seqMethod[U](f: Seq[U])(p: (U) => Ternary): Ternary = Ternary.exists(f)(p)

  def name = "any"

  def rewrite(f: (Expression) => Expression) = AnyInCollection(collection.rewrite(f), symbolName, inner.rewrite(f))
}

case class NoneInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {
  override def seqMethod[U](f: Seq[U])(p: (U) => Ternary): Ternary = Ternary.none(f)(p)

  def name = "none"

  def rewrite(f: (Expression) => Expression) = NoneInCollection(collection.rewrite(f), symbolName, inner.rewrite(f))
}

case class SingleInCollection(collection: Expression, symbolName: String, inner: Predicate)
  extends InCollection(collection, symbolName, inner) {
  override def seqMethod[U](f: Seq[U])(p: (U) => Ternary): Ternary = Ternary.single(f)(p)

  def name = "single"

  def rewrite(f: (Expression) => Expression) = SingleInCollection(collection.rewrite(f), symbolName, inner.rewrite(f))
}