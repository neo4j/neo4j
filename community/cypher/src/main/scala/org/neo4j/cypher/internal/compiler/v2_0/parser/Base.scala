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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0._
import org.parboiled.Context
import org.parboiled.scala._

trait Base extends Parser {

  def IdentifierCharacter = rule("an identifier character") { (Letter | ch('_') | Digit | ch('\'')) memoMismatches } suppressSubnodes

  def OperatorCharacter = rule("an operator char") { anyOf("|^&<>=!:+-*/%~") }

  def LegacyPropertyOperator = rule("") { group(anyOf("?!") ~ !OperatorCharacter) }

  def WordCharacter = rule { (Letter | ch('_') | Digit) memoMismatches }

  def Decimal = rule { (optional(Integer) ~ "." ~ Digits) memoMismatches }
  def Integer = rule { (optional("-") ~ UnsignedInteger) memoMismatches }
  def UnsignedInteger = rule { (("1" - "9") ~ Digits | Digit) memoMismatches }
  def Digits = rule { oneOrMore(Digit) }
  def Digit = rule { "0" - "9" }
  def HexDigit = rule { "0" - "9" | "a" - "f" | "A" - "Z" }

  def Letter = rule { (AscLetter) memoMismatches } // TODO: unicode
  def AscLetter = rule { "a" - "z" | "A" - "Z" }

  def CommaSep = rule("','") { WS ~ ch(',') ~ WS }

  def WS = rule("whitespace") {
    zeroOrMore(
        (oneOrMore(WSCharacter) memoMismatches)
      | (ch('/').label("comment") ~ (
          ch('*') ~ zeroOrMore(!("*/") ~ ANY) ~ "*/"
        | ch('/') ~ zeroOrMore(!anyOf("\n\r") ~ ANY) ~ ("\r\n" | ch('\r') | ch('\n') | EOI)
      ) memoMismatches)
    )
  } suppressNode
  def WB = rule { !(WordCharacter) } suppressNode
  def WSCharacter = rule("whitespace") { anyOf(" \n\r\t\f") }

  def keyword(string: String) : Rule0 = group(ignoreCase(string).label(string.toUpperCase) ~ WB)
  def keyword(firstString: String, strings: String*) : Rule0 =
      group(strings.foldLeft(keyword(firstString) ~ (_ : Rule0))((prev, s) => prev(WS ~ keyword(s)) ~ _)(EMPTY))
  def operator(string: String) = group(string ~ !(OperatorCharacter))

  def identifier = withContext((s: String, ctx: Context[Any]) => {
    ast.Identifier(s, ContextToken(ctx))
  })

  def token[IndexRange, A] = withContext((_: IndexRange, ctx: Context[Any]) => ContextToken(ctx))
  def t[V, A](inner: ((V, InputToken) => A)) = withContext((v: V, ctx: Context[Any]) => inner(v, ContextToken(ctx)))
  def t[V1, V2, A](inner: ((V1, V2, InputToken) => A)) = withContext((v1: V1, v2: V2, ctx: Context[Any]) => inner(v1, v2, ContextToken(ctx)))
  def t[V1, V2, V3, A](inner: ((V1, V2, V3, InputToken) => A)) = withContext((v1: V1, v2: V2, v3: V3, ctx: Context[Any]) => inner(v1, v2, v3, ContextToken(ctx)))
  def t[V1, V2, V3, V4, A](inner: ((V1, V2, V3, V4, InputToken) => A)) = withContext((v1: V1, v2: V2, v3: V3, v4: V4, ctx: Context[Any]) => inner(v1, v2, v3, v4, ContextToken(ctx)))
  def t(inner: Rule0) : Rule1[InputToken] = inner ~>> token
  def t[V](inner: Rule1[V]) : Rule2[V, InputToken] = inner ~>> token
  def t[V1, V2](inner: Rule2[V1, V2]) : Rule3[V1, V2, InputToken] = inner ~>> token
  def t[V1, V2, V3](inner: Rule3[V1, V2, V3]) : Rule4[V1, V2, V3, InputToken] = inner ~>> token
  def t[V1, V2, V3, V4](inner: Rule4[V1, V2, V3, V4]) : Rule5[V1, V2, V3, V4, InputToken] = inner ~>> token

  def rt[V, A](inner: ((V, InputToken) => A)) =
      withContext((v: V, start: Int, end: Int, ctx: Context[Any]) => inner(v, ContextToken(ctx, start, end)))
  def rt[V1, V2, A](inner: ((V1, V2, InputToken) => A)) =
      withContext((v1: V1, start: Int, v2: V2, end: Int, ctx: Context[Any]) => inner(v1, v2, ContextToken(ctx, start, end)))
  def rt[V1, V2, V3, A](inner: ((V1, V2, V3, InputToken) => A)) =
      withContext((v1: V1, start: Int, v2: V2, v3: V3, end: Int, ctx: Context[Any]) => inner(v1, v2, v3, ContextToken(ctx, start, end)))
  def rt[V1, V2, V3, V4, A](inner: ((V1, V2, V3, V4, InputToken) => A)) =
      withContext((v1: V1, start: Int, v2: V2, v3: V3, v4: V4, end: Int, ctx: Context[Any]) => inner(v1, v2, v3, v4, ContextToken(ctx, start, end)))

  implicit class WSRule0(r: Rule0) {
    def ~~(other: Rule0) : Rule0 = r ~ WS ~ other
    def ~~[A](other: Rule1[A]) : Rule1[A] = r ~ WS ~ other
    def ~~[A, B](other: Rule2[A, B]) : Rule2[A, B] = r ~ WS ~ other
    def ~~[A, B, C](other: Rule3[A, B, C]) : Rule3[A, B, C] = r ~ WS ~ other
    def ~~[A, B, C, D](other: Rule4[A, B, C, D]) : Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[A, B, C, D, E](other: Rule5[A, B, C, D, E]) : Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[A, B, C, D, E, F](other: Rule6[A, B, C, D, E, F]) : Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[A, B, C, D, E, F, G](other: Rule7[A, B, C, D, E, F, G]) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[A, B](other: ReductionRule1[A, B]) : ReductionRule1[A, B] = r ~ WS ~ other
    def ~~[A, B, C](other: ReductionRule2[A, B, C]) : ReductionRule2[A, B, C] = r ~ WS ~ other
  }

  implicit class WSSRule0(s: String) {
    def ~~(other: Rule0) : Rule0 = s ~ WS ~ other
    def ~~[A](other: String) : Rule0 = s ~ WS ~ other
    def ~~[A](other: Rule1[A]) : Rule1[A] = s ~ WS ~ other
    def ~~[A, B](other: Rule2[A, B]) : Rule2[A, B] = s ~ WS ~ other
    def ~~[A, B, C](other: Rule3[A, B, C]) : Rule3[A, B, C] = s ~ WS ~ other
    def ~~[A, B, C, D](other: Rule4[A, B, C, D]) : Rule4[A, B, C, D] = s ~ WS ~ other
    def ~~[A, B, C, D, E](other: Rule5[A, B, C, D, E]) : Rule5[A, B, C, D, E] = s ~ WS ~ other
    def ~~[A, B, C, D, E, F](other: Rule6[A, B, C, D, E, F]) : Rule6[A, B, C, D, E, F] = s ~ WS ~ other
    def ~~[A, B, C, D, E, F, G](other: Rule7[A, B, C, D, E, F, G]) : Rule7[A, B, C, D, E, F, G] = s ~ WS ~ other

    def ~~[A, B](other: ReductionRule1[A, B]) : ReductionRule1[A, B] = s ~ WS ~ other
    def ~~[A, B, C](other: ReductionRule2[A, B, C]) : ReductionRule2[A, B, C] = s ~ WS ~ other
  }

  implicit class WSRule1[+A](r: Rule1[A]) {
    def ~~(other: Rule0) : Rule1[A] = r ~ WS ~ other
    def ~~[B](other: Rule1[B]) : Rule2[A, B] = r ~ WS ~ other
    def ~~[B, C](other: Rule2[B, C]) : Rule3[A, B, C] = r ~ WS ~ other
    def ~~[B, C, D](other: Rule3[B, C, D]) : Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[B, C, D, E](other: Rule4[B, C, D, E]) : Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[B, C, D, E, F](other: Rule5[B, C, D, E, F]) : Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[B, C, D, E, F, G](other: Rule6[B, C, D, E, F, G]) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[B](other: ReductionRule1[A, B]) : Rule1[B] = r ~ WS ~ other
  }

  implicit class WSRule2[+A, +B](r: Rule2[A, B]) {
    def ~~(other: Rule0) : Rule2[A, B] = r ~ WS ~ other
    def ~~[C](other: Rule1[C]) : Rule3[A, B, C] = r ~ WS ~ other
    def ~~[C, D](other: Rule2[C, D]) : Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[C, D, E](other: Rule3[C, D, E]) : Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[C, D, E, F](other: Rule4[C, D, E, F]) : Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[C, D, E, F, G](other: Rule5[C, D, E, F, G]) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other

    def ~~[BB >: B, C](other: ReductionRule1[BB, C]) : Rule2[A, C] = r ~ WS ~ other
    def ~~[AA >: A, BB >: B, C](other: ReductionRule2[AA, BB, C]) : Rule1[C] = r ~ WS ~ other
  }

  implicit class WSRule3[+A, +B, +C](r: Rule3[A, B, C]) {
    def ~~(other: Rule0) : Rule3[A, B, C] = r ~ WS ~ other
    def ~~[D](other: Rule1[D]) : Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[D, E](other: Rule2[D, E]) : Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[D, E, F](other: Rule3[D, E, F]) : Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[D, E, F, G](other: Rule4[D, E, F, G]) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other
  }

  implicit class WSRule4[+A, +B, +C, +D](r: Rule4[A, B, C, D]) {
    def ~~(other: Rule0) : Rule4[A, B, C, D] = r ~ WS ~ other
    def ~~[E](other: Rule1[E]) : Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[E, F](other: Rule2[E, F]) : Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[E, F, G](other: Rule3[E, F, G]) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other
  }

  implicit class WSRule5[+A, +B, +C, +D, +E](r: Rule5[A, B, C, D, E]) {
    def ~~(other: Rule0) : Rule5[A, B, C, D, E] = r ~ WS ~ other
    def ~~[F](other: Rule1[F]) : Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[F, G](other: Rule2[F, G]) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other
  }

  implicit class WSRule6[+A, +B, +C, +D, +E, +F](r: Rule6[A, B, C, D, E, F]) {
    def ~~(other: Rule0) : Rule6[A, B, C, D, E, F] = r ~ WS ~ other
    def ~~[G](other: Rule1[G]) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other
  }

  implicit class WSRule7[+A, +B, +C, +D, +E, +F, +G](r: Rule7[A, B, C, D, E, F, G]) {
    def ~~(other: Rule0) : Rule7[A, B, C, D, E, F, G] = r ~ WS ~ other
  }

  implicit class WSReductionRule1[-A, +B](r: ReductionRule1[A, B]) {
    def ~~(other: Rule0) : ReductionRule1[A, B] = r ~ WS ~ other
  }

  implicit class WSReductionRule2[-A, -B, +C](r: ReductionRule2[A, B, C]) {
    def ~~(other: Rule0) : ReductionRule2[A, B, C] = r ~ WS ~ other
  }
}
