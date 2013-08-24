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

import org.scalatest.Assertions
import org.junit.Test

class TernaryTest extends Assertions {

  val someValue: String = "23"

  @Test
  def should_know_all_values() {
    assert(Set(IsTrue, IsFalse, NotApplicable) === Ternary.values)
  }

  @Test
  def should_compute_ternary_inversed() {
    assert(IsTrue === IsFalse.negated)
    assert(IsFalse === IsTrue.negated)
    assert(NotApplicable === NotApplicable.negated)
  }

  @Test
  def should_compute_ternary_and() {
    assert(IsTrue === (IsTrue `and` IsTrue))
    assert(IsFalse === (IsTrue `and` IsFalse))
    assert(NotApplicable === (IsTrue `and` NotApplicable))
    assert(IsFalse === (IsFalse `and` IsTrue))
    assert(IsFalse === (IsFalse `and` IsFalse))
    assert(IsFalse === (IsFalse `and` NotApplicable))
    assert(NotApplicable === (NotApplicable `and` IsTrue))
    assert(IsFalse === (NotApplicable `and` IsFalse))
    assert(NotApplicable === (NotApplicable `and` NotApplicable))
  }

  @Test
  def should_have_symmetric_ternary_and() {
    for (a <- Ternary.values)
      for (b <- Ternary.values)
        assert((a `and` b) === (b `and` a))
  }

  @Test
  def should_compute_ternary_or() {
    assert(IsTrue === (IsTrue `or` IsTrue))
    assert(IsTrue === (IsTrue `or` IsFalse))
    assert(IsTrue === (IsTrue `or` NotApplicable))
    assert(IsTrue === (IsFalse `or` IsTrue))
    assert(IsFalse === (IsFalse `or` IsFalse))
    assert(NotApplicable === (IsFalse `or` NotApplicable))
    assert(IsTrue === (NotApplicable `or` IsTrue))
    assert(NotApplicable === (NotApplicable `or` IsFalse))
    assert(NotApplicable === (NotApplicable `or` NotApplicable))
  }

  @Test
  def should_have_symmetric_ternary_or() {
    for (a <- Ternary.values)
      for (b <- Ternary.values)
        assert((a `or` b) === (b `or` a))
  }

  @Test
  def should_compute_ternary_xor() {
    assert(IsFalse === (IsTrue `xor` IsTrue))
    assert(IsTrue === (IsTrue `xor` IsFalse))
    assert(NotApplicable === (IsTrue `xor` NotApplicable))
    assert(IsTrue === (IsFalse `xor` IsTrue))
    assert(IsFalse === (IsFalse `xor` IsFalse))
    assert(NotApplicable === (IsFalse `xor` NotApplicable))
    assert(NotApplicable === (NotApplicable `xor` IsTrue))
    assert(NotApplicable === (NotApplicable `xor` IsFalse))
    assert(NotApplicable === (NotApplicable `xor` NotApplicable))
  }

  @Test
  def should_have_symmetric_ternary_xor() {
    for (a <- Ternary.values)
      for (b <- Ternary.values)
        assert((a `xor` b) === (b `xor` a))
  }

  @Test
  def should_compute_isTrueOrUnknown() {
    for (a <- Ternary.values)
      assert( (a.isTrue || !a.isApplicable) === a.isTrueOrNotApplicable)
  }

  @Test
  def should_compute_toKnownOption() {
    for (a <- Ternary.values)
      assert(a.isApplicable === a.toApplicableOption.exists(_.isApplicable))
  }

  @Test
  def should_convert_fromBoolean() {
    assert(IsTrue === Ternary(b = true))
    assert(IsFalse === Ternary(b = false))
  }

  @Test
  def should_convert_fromValue() {
    assert(IsTrue === Ternary(v = true))
    assert(IsTrue === Ternary(v = IsTrue))
    assert(IsFalse === Ternary(v = false))
    assert(IsFalse === Ternary(v = IsFalse))
    assert(NotApplicable === Ternary(v = NotApplicable))
    assert(NotApplicable === Ternary(v = null))
    assert(NotApplicable === Ternary(v = NotBound))
    assert(NotApplicable === Ternary(v = someValue))
  }

  @Test
  def should_identify_truth() {
    identifies(IsTrue, true)
    identifies(IsTrue, IsTrue)
    assert(!IsTrue(false))
    assert(!IsTrue(IsFalse))
    assert(!IsTrue(NotApplicable))
    assert(!IsTrue(null))
    assert(!IsTrue(someValue))
  }

  @Test
  def should_identify_falsehood() {
    identifies(IsFalse, false)
    identifies(IsFalse, IsFalse)
    assert(!IsFalse(true))
    assert(!IsFalse(IsTrue))
    assert(!IsFalse(NotApplicable))
    assert(!IsFalse(null))
    assert(!IsFalse(someValue))
  }

  @Test
  def should_identify_not_applicable() {
    identifies(NotApplicable, NotApplicable)
    assert(!NotApplicable(someValue))
    assert(!NotApplicable(null))
    assert(!NotApplicable(true))
    assert(!NotApplicable(IsTrue))
    assert(!NotApplicable(false))
    assert(!NotApplicable(IsFalse))
  }

  @Test
  def should_evaluate_forall() {
    assert( IsTrue == Ternary.forall(Seq()) )
    assert( IsTrue == Ternary.forall(Seq(IsTrue)) )
    assert( NotApplicable == Ternary.forall(Seq(NotApplicable)) )
    assert( NotApplicable == Ternary.forall(Seq(IsTrue, NotApplicable)) )
    assert( IsFalse == Ternary.forall(Seq(IsFalse, NotApplicable)) )
    assert( IsFalse == Ternary.forall(Seq(IsTrue, IsFalse)) )
    assert( IsFalse == Ternary.forall(Seq(NotApplicable, IsFalse)) )
  }

  @Test
  def should_evaluate_exists() {
    assert( IsFalse == Ternary.exists(Seq()) )
    assert( IsTrue == Ternary.exists(Seq(IsTrue)) )
    assert( NotApplicable == Ternary.exists(Seq(NotApplicable)) )
    assert( IsTrue == Ternary.exists(Seq(IsTrue, NotApplicable)) )
    assert( NotApplicable == Ternary.exists(Seq(IsFalse, NotApplicable)) )
    assert( IsTrue == Ternary.exists(Seq(IsFalse, IsTrue)) )
  }

  @Test
  def should_evaluate_none() {
    assert( IsTrue == Ternary.none(Seq()) )
    assert( IsFalse == Ternary.none(Seq(IsTrue)) )
    assert( NotApplicable == Ternary.none(Seq(NotApplicable)) )
    assert( IsFalse == Ternary.none(Seq(IsTrue, NotApplicable)) )
    assert( NotApplicable == Ternary.none(Seq(IsFalse, NotApplicable)) )
    assert( IsFalse == Ternary.none(Seq(IsFalse, IsTrue)) )
  }

  @Test
  def should_evaluate_single() {
    assert( IsFalse == Ternary.single(Seq()) )
    assert( IsTrue == Ternary.single(Seq(IsTrue)) )
    assert( NotApplicable == Ternary.single(Seq(NotApplicable)) )
    assert( NotApplicable == Ternary.single(Seq(IsTrue, NotApplicable)) )
    assert( NotApplicable == Ternary.single(Seq(IsFalse, NotApplicable)) )
    assert( IsTrue == Ternary.single(Seq(IsFalse, IsTrue)) )
    assert( IsFalse == Ternary.single(Seq(IsFalse, IsTrue, IsTrue)) )
    assert( IsFalse == Ternary.single(Seq(IsFalse, IsTrue, IsTrue, NotApplicable)) )
  }

  @Test
  def should_print_nicely() {
    assert("true" === IsTrue.toString())
    assert("false" === IsFalse.toString())
    assert("<not_applicable>" === NotApplicable.toString())
  }

  private def identifies(tern: Ternary, value: Any) {
    assert(tern(value))
    value match {
      case tern(matched) => assert(tern === matched)
    }
  }
}