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
 * {@code InvalidValuesArgumentException} is thrown when trying to pass in an invalid argument to
 * a {@code PointValue}, {@code TemporalValue} or {@code DurationValue} method. Examples of such
 * cases include trying to pass an invalid CRS to a {@code PointValue} and trying to pass a
 * temporal unit out of range when creating a {@code TemporalValue}, e.g. specifying {@code month: 13}.
 */
public class InvalidValuesArgumentException extends ValuesException
{
    public InvalidValuesArgumentException( String errorMsg )
    {
        super( errorMsg );
    }

    public InvalidValuesArgumentException( String errorMsg, Throwable cause )
    {
        super( errorMsg, cause );
    }
}
