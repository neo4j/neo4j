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

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.graphdb.config.Setting

object CypherOption {
  val DEFAULT: String = "default"
}

abstract class CypherOption(inputName: String) {
  val name: String = OptionReader.canonical(inputName)

  def companion: CypherOptionCompanion[_ <: CypherOption]

  def render: String = if (this == companion.default) "" else name

  def cacheKey: String = render
}

abstract class CypherKeyValueOption(inputName: String) extends CypherOption(inputName) {
  override def render: String = if (this == companion.default) "" else s"${companion.name}=$name"
}

abstract class CypherOptionCompanion[Opt <: CypherOption](
  val name: String,
  setting: Option[Setting[_]],
  cypherConfigField: Option[CypherConfiguration => Opt],
) {
  self: Product =>

  private val key = OptionReader.canonical(name)

  def default: Opt

  /**
   * When overriding this, make sure it is not defined as val, to avoid hitting this:
   * https://stackoverflow.com/questions/28151338/case-object-gets-initialized-to-null-how-is-that-even-possible
   */
  def values: Set[Opt]

  def fromConfig(configuration: Config): Opt = {
    setting.map(s => configuration.get(s))
           .map(_.toString)
           .map(fromValue)
           .getOrElse(default)
  }

  /**
   * An OptionReader for reading exactly one value of this option
   * Fails if there are more than one.
   */
  def singleOptionReader(): OptionReader[Opt] =
    (input: OptionReader.Input) =>
      input.extract(key)
           .map(values => fromValues(values).headOption)
           .map(value => value.getOrElse(fromCypherConfiguration(input.config)))

  /**
   * An OptionReader for reading all values of this option
   */
  def multiOptionReader(): OptionReader[Set[Opt]] =
    (input: OptionReader.Input) =>
      input.extract(key)
           .map(values => values.map(fromValue))

  private def fromCypherConfiguration(configuration: CypherConfiguration): Opt =
    cypherConfigField.map(f => f(configuration))
                     .getOrElse(default)

  private def fromValues(input: Set[String]): Set[Opt] = input.size match {
    case 0 => Set.empty
    case 1 => Set(fromValue(input.head))
    case _ => conflictingValuesError()
  }

  protected def fromValue(input: String): Opt = OptionReader.canonical(input) match {
    case CypherOption.DEFAULT => default
    case value                => values.find(_.name == value).getOrElse(invalidValueError(input))

  }

  private def invalidValueError(input: String): Nothing =
    throw new InvalidCypherOption(s"$input is not a valid option for $name. Valid options are: ${values.map(_.name).mkString(", ")}")

  private def conflictingValuesError(): Nothing =
    throw new InvalidCypherOption(s"Can't specify multiple conflicting values for $name")
}
