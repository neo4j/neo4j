/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.values.utils;

/**
 * {@code TemporalArithmeticException} is thrown when arithmetic operations of temporal values and
 * durations are unsuccessful. Examples of such arithmetic operations include adding a
 * {@code TemporalValue} and {@code DurationValue} and subtracting a {@code DurationValue} from a
 * {@code TemporalValue}.
 */
public class TemporalArithmeticException extends ValuesException
{
    public TemporalArithmeticException( String errorMsg, Throwable cause )
    {
        super( errorMsg, cause );
    }
}
