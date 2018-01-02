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
package org.neo4j.function;

/**
 * Represents an operation upon two long-valued operands and producing a long-valued result. This is the primitive type specialization of {@link
 * ThrowingBinaryOperator} for long.
 *
 * @param <E> the type of exception that may be thrown from the operator
 */
public interface ThrowingLongBinaryOperator<E extends Exception>
{
    /**
     * Applies this operator to the given operand.
     *
     * @param left the first operand
     * @param right the second operand
     * @return the operator result
     * @throws E an exception if the operator fails
     */
    long applyAsLong( long left, long right ) throws E;
}
