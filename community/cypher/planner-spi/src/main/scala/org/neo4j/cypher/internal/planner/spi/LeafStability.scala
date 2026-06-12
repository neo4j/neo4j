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
package org.neo4j.cypher.internal.planner.spi

/**
 * The stable-iterator classification of a leaf plan, recorded by the planner for the runtime and for
 * eagerness analysis.
 */
enum LeafStability {

  /**
   * No stable-iterator optimisation applies: a non-MVCC storage format, or a leaf the planner does not mark
   * (read-only queries, non-leftmost leaves).
   */
  case NonMvcc

  /**
   * MVCC storage with an empty transaction state. The leaf is a stable iterator: the runtime may ask the
   * kernel to skip transaction-state changes, and eagerness analysis may rely on its stability.
   */
  case MvccEmptyTx

  /**
   * MVCC storage with a non-empty transaction state. The leaf is NOT a stable iterator: eagerness analysis
   * must treat it as unstable (e.g. plan an additional Eager).
   */
  case MvccNonEmptyTx
}
