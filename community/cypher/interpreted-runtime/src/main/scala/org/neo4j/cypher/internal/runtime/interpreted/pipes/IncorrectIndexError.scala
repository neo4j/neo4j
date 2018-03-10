/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

/**
  * Exception which signals that an index seek was attempted with a predicate set that cannot
  * match any node. This is not how index lookup should ever work, but added it in order not to change behaviour
  * while refactoring. Hoping to change this soon.
  *
  * The only time when this is reasonable is when the non-match can be statically determined at compile
  * time, and in that case maybe a warning is enough?
  */
// TODO: remove this error and just return no rows for there cases.
@Deprecated
class IncorrectIndexError extends IllegalArgumentException(
  "Cannot compare a property against both numbers and strings. They are incomparable.")
