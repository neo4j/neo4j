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
 * {@code TemporalParseException} is thrown if parsing of a TemporalValue is unsuccessful.
 * The constructor parameters {@code parsedData} and {@code errorIndex} can optionally be provided
 * in order to conform with Java's {@code DateTimeParseException} and {@code SyntaxException}.
 */
public class TemporalParseException extends ValuesException
{
    private String parsedData;
    private int errorIndex;

    public TemporalParseException( String errorMsg, Throwable cause )
    {
        super( errorMsg, cause );
    }

    public TemporalParseException( String errorMsg, String parsedData, int errorIndex )
    {
        super( errorMsg );
        this.parsedData = parsedData;
        this.errorIndex = errorIndex;
    }

    public TemporalParseException( String errorMsg, String parsedData, int errorIndex, Throwable cause )
    {
        super( errorMsg, cause );
        this.parsedData = parsedData;
        this.errorIndex = errorIndex;
    }

    public String getParsedData()
    {
        return parsedData;
    }

    public int getErrorIndex()
    {
        return errorIndex;
    }
}
