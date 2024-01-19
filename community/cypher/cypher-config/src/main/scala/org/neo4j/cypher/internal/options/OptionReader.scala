/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.options

import magnolia1.CaseClass
import magnolia1.Magnolia
import org.neo4j.cypher.internal.config.CypherConfiguration

import java.util.Locale

import scala.language.experimental.macros

/**
 * Reads a value of T by consuming part of the input,
 * returning the value and the remainder of input
 */
trait OptionReader[T] {
  def read(input: OptionReader.Input): OptionReader.Result[T]

  def map[R](func: T => R): OptionReader[R] =
    (input: OptionReader.Input) => read(input).map(func)
}

object OptionReader {

  /** Input to OptionReader:s */
  case class Input(
    config: CypherConfiguration,
    keyValues: Set[(String, String)]
  ) {

    /** Grab all values from keyValues mapped for the given key. Return the values and the remaining input */
    def extract(key: String): Result[Set[String]] = {
      val (hits, misses) = keyValues.partition(_._1 == key)
      Result(copy(keyValues = misses), hits.map(_._2))
    }
  }

  object Input {

    /** Creates an input with canonical keys and values */
    def apply(config: CypherConfiguration, keyValues: Set[(String, String)]): Input =
      new Input(config, keyValues.map { case (k, v) => (canonical(k), canonical(v)) })
  }

  /**
   * Output from OptionReader:s
   *
   * @param remainder remaining input that has not been read yet
   * @param result    a read value
   */
  case class Result[T](
    remainder: Input,
    result: T
  ) {

    def map[R](transform: T => R): Result[R] =
      copy(result = transform(result))
  }

  def canonical(str: String): String = str.toLowerCase(Locale.ROOT)

  // Magnolia generic derivation
  // Check out the tutorial at https://propensive.com/opensource/magnolia/tutorial

  type Typeclass[T] = OptionReader[T]

  /**
   * Generic OptionReader for any case class (given that there are OptionReader:s for all its parameter types)
   * that reads each parameter in turn, passing on the remaining input to the next OptionReader
   */
  def join[T](caseClass: CaseClass[OptionReader, T]): OptionReader[T] = {
    (input: Input) =>
      val results = caseClass.parameters.foldLeft(Result(input, Seq[Any]())) { case (in, p) =>
        val result = p.typeclass.read(in.remainder)
        Result(result.remainder, in.result :+ result.result)
      }
      results.map(caseClass.rawConstruct)
  }

  implicit def derive[T]: OptionReader[T] = macro Magnolia.gen[T]
}
