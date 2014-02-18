/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.parser

import org.neo4j.cypher.internal.compiler.v2_1._
import org.parboiled.Context
import org.parboiled.scala._
import org.parboiled.support.IndexRange

trait Base extends Parser {

  def OpChar = rule("an operator char") { anyOf("|^&<>=?!:+-*/%~") }

  def Decimal = rule { (optional(Integer) ~ "." ~ Digits).memoMismatches }
  def Integer = rule { (optional("-") ~ UnsignedInteger).memoMismatches }
  def UnsignedInteger = rule { (("1" - "9") ~ Digits | Digit).memoMismatches }
  def Exponent = rule { ((Decimal | Integer) ~ "E" ~ Integer).memoMismatches }
  def Digits = rule { oneOrMore(Digit) }
  def Digit = rule { "0" - "9" }
  def HexDigit = rule { "0" - "9" | "a" - "f" | "A" - "Z" }

  def CommaSep = rule("','") { WS ~ ch(',') ~ WS }

  def WS = rule("whitespace") {
    zeroOrMore(
        (oneOrMore(WSChar) memoMismatches)
      | (ch('/').label("comment") ~ (
          ch('*') ~ zeroOrMore(!"*/" ~ ANY) ~ "*/"
        | ch('/') ~ zeroOrMore(!anyOf("\n\r") ~ ANY) ~ (optional(ch('\r')) ~ ch('\n') | EOI)
      ) memoMismatches)
    )
  }.suppressNode

  def keyword(string: String): Rule0 = {
    def word(string: String): Rule0 = group(ignoreCase(string).label(string.toUpperCase) ~ !IdentifierPart)
    val strings = string.trim.split(' ')
    group(strings.tail.foldLeft(word(strings.head)) {
      (acc, s) => acc ~ WS ~ word(s)
    })
  }
  def operator(string: String) = group(string ~ !OpChar)

  def push[R](f: InputPosition => R): Rule1[R] = pushFromContext(ctx => f(ContextPosition(ctx)))

  def position = withContext((_: IndexRange, ctx: Context[Any]) => ContextPosition(ctx))

  implicit class RichRule0(r: Rule0) {
    def ~~(other: Rule0): Rule0 = r ~ WS ~ other
    def ~~[A](other: Rule1[A]): Rule1[A] = r ~ WS ~ other
    def ~~[A, B](other: Rule2[A, B]): Rule2[A, B] = r ~ WS ~ other
    def ~~[A, B, C](other: Rule3[A, B, C]): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[A, B, C, D](other: Rule4[A, B, C, D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[A, B, C, D, E](other: Rule5[A, B, C, D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[A, B, C, D, E, F](other: Rule6[A, B, C, D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[A, B, C, D, E, F, G](other: Rule7[A, B, C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[A, B](other: ReductionRule1[A, B]): ReductionRule1[A, B] = r ~ WS ~ other
    def ~~[A, B, C](other: ReductionRule2[A, B, C]): ReductionRule2[A, B, C] = r ~ WS ~ other

    def ~>>>[R](f: String => InputPosition => R): Rule1[R] = r ~> withContext((s: String, ctx) => f(s)(ContextPosition(ctx)))
    def ~~>>[Z, R](f: (Z) => (InputPosition => R)): ReductionRule1[Z, R] =
      r ~~> withContext((z: Z, ctx) => f(z)(ContextPosition(ctx)))
    def ~~>>[Y, Z, R](f: (Y, Z) => (InputPosition => R)): ReductionRule2[Y, Z, R] =
      r ~~> withContext((y: Y, z: Z, ctx) => f(y, z)(ContextPosition(ctx)))
  }

  implicit class RichString(s: String) {
    def ~~(other: Rule0): Rule0 = s ~ WS ~ other
    def ~~[A](other: String): Rule0 = s ~ WS ~ other
    def ~~[A](other: Rule1[A]): Rule1[A] = s ~ WS ~ other
    def ~~[A, B](other: Rule2[A, B]): Rule2[A, B] = s ~ WS ~ other
    def ~~[A, B, C](other: Rule3[A, B, C]): Rule3[A, B, C] = s ~ WS ~ other
    def ~~[A, B, C, D](other: Rule4[A, B, C, D]): Rule4[A, B, C, D] = s ~ WS ~ other
    def ~~[A, B, C, D, E](other: Rule5[A, B, C, D, E]): Rule5[A, B, C, D, E] = s ~ WS ~ other
    def ~~[A, B, C, D, E, F](other: Rule6[A, B, C, D, E, F]): Rule6[A, B, C, D, E, F] = s ~ WS ~ other
    def ~~[A, B, C, D, E, F, G](other: Rule7[A, B, C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = s ~ WS ~ other

    def ~~[A, B](other: ReductionRule1[A, B]): ReductionRule1[A, B] = s ~ WS ~ other
    def ~~[A, B, C](other: ReductionRule2[A, B, C]): ReductionRule2[A, B, C] = s ~ WS ~ other
  }

  implicit class RichRule1[+A](r: Rule1[A]) {
    def ~~(other: Rule0): Rule1[A] = r ~ WS ~ other
    def ~~[B](other: Rule1[B]): Rule2[A, B] = r ~ WS ~ other
    def ~~[B, C](other: Rule2[B, C]): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[B, C, D](other: Rule3[B, C, D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[B, C, D, E](other: Rule4[B, C, D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[B, C, D, E, F](other: Rule5[B, C, D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[B, C, D, E, F, G](other: Rule6[B, C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[B](other: ReductionRule1[A, B]): Rule1[B] = r ~ WS ~ other

    def ~~>>[R](f: (A) => (InputPosition => R)): Rule1[R] =
      r ~~> withContext((a: A, ctx) => f(a)(ContextPosition(ctx)))
    def ~~>>[Z, R](f: (Z, A) => (InputPosition => R)): ReductionRule1[Z, R] =
      r ~~> withContext((z: Z, a: A, ctx) => f(z, a)(ContextPosition(ctx)))
    def ~~>>[Y, Z, R](f: (Y, Z, A) => (InputPosition => R)): ReductionRule2[Y, Z, R] =
      r ~~> withContext((y: Y, z: Z, a: A, ctx) => f(y, z, a)(ContextPosition(ctx)))
  }

  implicit class RichRule2[+A, +B](r: Rule2[A, B]) {
    def ~~(other: Rule0): Rule2[A, B] = r ~ WS ~ other
    def ~~[C](other: Rule1[C]): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[C, D](other: Rule2[C, D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[C, D, E](other: Rule3[C, D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[C, D, E, F](other: Rule4[C, D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[C, D, E, F, G](other: Rule5[C, D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[BB >: B, C](other: ReductionRule1[BB, C]): Rule2[A, C] = r ~ WS ~ other
    def ~~[AA >: A, BB >: B, C](other: ReductionRule2[AA, BB, C]): Rule1[C] = r ~ WS ~ other

    def ~~>>[R](f: (A, B) => InputPosition => R): Rule1[R] =
      r ~~> withContext((a: A, b: B, ctx) => f(a, b)(ContextPosition(ctx)))
    def ~~>>[Z, R](f: (Z, A, B) => InputPosition => R): ReductionRule1[Z, R] =
      r ~~> withContext((z: Z, a: A, b: B, ctx) => f(z, a, b)(ContextPosition(ctx)))
    def ~~>>[Y, Z, R](f: (Y, Z, A, B) => InputPosition => R): ReductionRule2[Y, Z, R] =
      r ~~> withContext((y: Y, z: Z, a: A, b: B, ctx) => f(y, z, a, b)(ContextPosition(ctx)))
  }

  implicit class RichRule3[+A, +B, +C](r: Rule3[A, B, C]) {
    def ~~(other: Rule0): Rule3[A, B, C] = r ~ WS ~ other
    def ~~[D](other: Rule1[D]): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[D, E](other: Rule2[D, E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[D, E, F](other: Rule3[D, E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[D, E, F, G](other: Rule4[D, E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](f: (A, B, C) => InputPosition => R): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, ctx) => f(a, b, c)(ContextPosition(ctx)))
  }

  implicit class RichRule4[+A, +B, +C, +D](r: Rule4[A, B, C, D]) {
    def ~~(other: Rule0): Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[E](other: Rule1[E]): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[E, F](other: Rule2[E, F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[E, F, G](other: Rule3[E, F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](f: (A, B, C, D) => InputPosition => R): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, d: D, ctx) => f(a, b, c, d)(ContextPosition(ctx)))
  }

  implicit class RichRule5[+A, +B, +C, +D, +E](r: Rule5[A, B, C, D, E]) {
    def ~~(other: Rule0): Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[F](other: Rule1[F]): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[F, G](other: Rule2[F, G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](f: (A, B, C, D, E) => (InputPosition => R)): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, d: D, e: E, ctx) => f(a, b, c, d, e)(ContextPosition(ctx)))
  }

  implicit class RichRule6[+A, +B, +C, +D, +E, +F](r: Rule6[A, B, C, D, E, F]) {
    def ~~(other: Rule0): Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[G](other: Rule1[G]): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~>>[R](func: (A, B, C, D, E, F) => (InputPosition => R)): Rule1[R] =
      r ~~> withContext((a: A, b: B, c: C, d: D, e: E, f: F, ctx) => func(a, b, c, d, e, f)(ContextPosition(ctx)))
  }

  implicit class RichRule7[+A, +B, +C, +D, +E, +F, +G](r: Rule7[A, B, C, D, E, F, G]) {
    def ~~(other: Rule0): Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other
  }

  implicit class RichReductionRule1[-A, +B](r: ReductionRule1[A, B]) {
    def ~~(other: Rule0): ReductionRule1[A, B] = r ~ WS ~ other
  }

  implicit class RichReductionRule2[-A, -B, +C](r: ReductionRule2[A, B, C]) {
    def ~~(other: Rule0): ReductionRule2[A, B, C] = r ~ WS ~ other
  }
}
