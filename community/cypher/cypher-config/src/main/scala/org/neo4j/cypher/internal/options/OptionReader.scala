/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.options

import language.experimental.macros
import magnolia.CaseClass
import magnolia.Magnolia
import magnolia.SealedTrait
import org.neo4j.cypher.internal.config.CypherConfiguration


trait OptionReader[T] {
  def read(input: OptionReader.Input): OptionReader.Result[T]

  def map[R](func: T => R): OptionReader[R] =
    (input: OptionReader.Input) => read(input).map(func)
}

object OptionReader {
  case class Input(
    config: CypherConfiguration,
    keyValues: Set[(String, String)],
  ) {
    def extract(key: String): Result[Set[String]] =
      Result(copy(keyValues = keyValues.filterNot(_._1 == key)), keyValues.filter(_._1 == key).map(_._2))
  }

  object Input {
    def apply(config: CypherConfiguration, keyValues: Set[(String, String)]): Input =
      new Input(config, keyValues.map { case (k, v) => (canonical(k), canonical(v)) })
  }

  def canonical(str: String): String = str.toLowerCase

  def matches(a: String, b: String): Boolean = canonical(a) == canonical(b)

  case class Result[T](
    remainder: Input,
    result: T,
  ) {
    def map[R](transform: T => R): Result[R] =
      copy(result = transform(result))
  }

  // Magnolia generic derivation
  // Check out the tutorial at https://propensive.com/opensource/magnolia/tutorial

  type Typeclass[T] = OptionReader[T]

  /**
   * Generic OptionReader for any case class (given that there are OptionReader:s for all its parameter types)
   * that reads each parameter in turn, passing on the remaining input to the next OptionReader
   */
  def combine[T](caseClass: CaseClass[OptionReader, T]): OptionReader[T] = {
    (input: Input) =>
      val results = caseClass.parameters.foldLeft(Result(input, Seq[Any]())) { case (in, p) =>
        val result = p.typeclass.read(in.remainder)
        Result(result.remainder, in.result :+ result.result)
      }
      results.map(caseClass.rawConstruct)
  }

  implicit def derive[T]: OptionReader[T] = macro Magnolia.gen[T]
}
