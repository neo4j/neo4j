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
package org.neo4j.helpers;

/**
 * Generic function interface with 2 input parameters.
 *
 * This is deprecated, use {@link org.neo4j.function.BiFunction} instead.
 *
 * @param <T1> the type of the first input item
 * @param <T2> the type of the second input item
 * @param <R> the type of the mapped item
 */
@Deprecated
public interface Function2<T1, T2, R> extends org.neo4j.function.Function2<T1, T2, R>
{
}
