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
package org.neo4j.internal.kernel.api.procs;

import java.util.stream.Stream;

/**
 * This class captures information about the context in which a procedure was called. For example if it was called within Cypher it might
 * have a YIELD clause with only a few of the available fields requested, in which case procedure authors can optimize their procedures to
 * skip calculating and returning the unused fields.
 */
public class ProcedureCallContext
{
    private final String[] outputFieldNames;
    private final boolean calledFromCypher;

    public ProcedureCallContext( String[] outputFieldNames, boolean calledFromCypher )
    {
        this.outputFieldNames = outputFieldNames;
        this.calledFromCypher = calledFromCypher;
    }

    /*
     * Get a stream of all the field names the procedure was requested to yield
     */
    public Stream<String> outputFields()
    {
        return Stream.of( outputFieldNames );
    }

    /*
     * Indicates whether the procedure was called via a complete Cypher stack.
     * Check this to make sure you are not in a testing environment.
     * When this is false, we cannot make use of the information in outputFields().
     */
    public boolean isCalledFromCypher()
    {
        return calledFromCypher;
    }

    /* Can be used for testing purposes */
    public static ProcedureCallContext EMPTY = new ProcedureCallContext( new String[]{}, false );
}
