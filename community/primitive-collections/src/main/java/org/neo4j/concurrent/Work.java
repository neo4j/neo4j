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
package org.neo4j.concurrent;

/**
 * <p>
 *     A unit of work that can be applied to the given type of material, or combined with other like-typed units of
 *     work.
 * </p>
 * <p>
 *     These types of work must exhibit a number of properties, for their use in the WorkSync to be correct:
 * </p>
 * <ul>
 *     <li>
 *         <strong>Commutativity</strong><br>
 *         The order of operands must not matter:
 *         <code>a.combine(b)  =  b.combine(a)</code>
 *     </li>
 *     <li>
 *         <strong>Associativity</strong><br>
 *         The order of operations must not matter:
 *         <code>a.combine(b.combine(c))  =  a.combine(b).combine(c)</code>
 *     </li>
 *     <li>
 *         <strong>Effect Transitivity</strong><br>
 *         Work-combining must not change work outcome:
 *         <code>a.combine(b).apply(m)  =  { a.apply(m) ; b.apply(m) }</code>
 *     </li>
 * </ul>
 * @see WorkSync
 * @param <Material> The type of material to work with.
 * @param <W> The concrete type of work being performed.
 */
public interface Work<Material, W extends Work<Material,W>>
{
    /**
     * <p>
     *     Combine this unit of work with the given unit of work, and produce a unit of work that represents the
     *     aggregate of the work.
     * </p>
     * <p>
     *     It is perfectly fine for a unit to build up internal state that aggregates the work it is combined with,
     *     and then return itself. This is perhaps useful for reducing the allocation rate a little.
     * </p>
     */
    W combine( W work );

    /**
     * Apply this unit of work to the given material.
     */
    void apply( Material material );
}
