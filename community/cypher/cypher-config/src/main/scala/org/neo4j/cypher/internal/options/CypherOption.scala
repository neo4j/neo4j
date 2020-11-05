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
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.graphdb.config.Setting

object CypherOption {
  val DEFAULT: String = "default"

  def asCanonicalName(name: String): String = name.toLowerCase
}

abstract class CypherOption(inputName: String) {
  val name: String = CypherOption.asCanonicalName(inputName)

  def companion: CypherOptionCompanion[_ <: CypherOption]

  def render: String = if (this == companion.default) "" else name
}

abstract class CypherKeyValueOption(inputName: String) extends CypherOption(inputName) {
  override def companion: CypherKeyValueOptionCompanion[_ <: CypherKeyValueOption]

  override def render: String = if (this == companion.default) "" else s"${companion.key}=$name"
}

abstract class CypherOptionCompanion[Opt <: CypherOption](
  val name: String,
  setting: Option[Setting[_]],
  cypherConfigField: Option[CypherConfiguration => Opt],
) {
  self: Product =>

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

  def fromCypherConfiguration(configuration: CypherConfiguration): Opt =
    cypherConfigField.map(f => f(configuration))
                     .getOrElse(default)

  def fromValues(values: Set[String]): Set[Opt] = values.size match {
    case 0 => Set.empty
    case 1 => Set(fromValue(values.head))
    case _ => throw new InvalidCypherOption(s"Can't specify multiple conflicting values for $name")
  }

  protected def fromValue(value: String): Opt = findByExactName(CypherOption.asCanonicalName(value)).getOrElse {
    throw new InvalidCypherOption(s"$value is not a valid option for $name. Valid options are: ${values.map(_.name).mkString(", ")}")
  }

  private def findByExactName(name: String) = if (CypherOption.DEFAULT == name)
    Some(default)
  else
    values.find(_.name == name)
}

abstract class CypherKeyValueOptionCompanion[Opt <: CypherOption](
  val key: String,
  setting: Option[Setting[_]],
  cypherConfigField: Option[CypherConfiguration => Opt],
) extends CypherOptionCompanion[Opt](key, setting, cypherConfigField) {
  self: Product =>
}
