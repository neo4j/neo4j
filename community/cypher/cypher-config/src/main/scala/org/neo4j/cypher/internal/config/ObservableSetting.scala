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
package org.neo4j.cypher.internal.config

import org.neo4j.configuration.Config
import org.neo4j.configuration.SettingChangeListener
import org.neo4j.function.Observable
import org.neo4j.function.Observer
import org.neo4j.graphdb.config.Setting

import java.io.Closeable

/**
 * An adapter between the general Observable interface and the setting change listener mechanism
 */
class ObservableSetting[T](config: Config, setting: Setting[T]) extends Observable[T] {
  def latestValue: T = config.get(setting)

  def subscribe(observer: Observer[T]): Closeable = {
    val listener: SettingChangeListener[T] = (_, newValue) => observer.onChange(newValue)
    config.addListener(setting, listener)
    () => config.removeListener(setting, listener)
  }
}
