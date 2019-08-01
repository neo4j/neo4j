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
 * Has information about the context in which the procedure has been called in.
 * Can for instance be used by procedures to save calculating fields that are not yielded afterwards
 */
public class ProcedureCallContext
{
    private final String[] yieldFieldNames;
    private final boolean isUsedFromCypher;

    public ProcedureCallContext( String[] fieldNames, boolean isUsedFromCypher )
    {
        yieldFieldNames = fieldNames;
        this.isUsedFromCypher = isUsedFromCypher;
    }

    /*
     * Get a stream of all the field names the procedure was requested to yield
     */
    public Stream<String> getStreamOfYieldFieldNames()
    {
        return Stream.of( yieldFieldNames );
    }

    /*
     * Indicates whether the procedure was called via a complete Cypher stack. Check this to make sure you are not in a testing environment
     */
    public boolean isUsed()
    {
        return isUsedFromCypher;
    }

    /* Can be used for testing purposes */
    public static ProcedureCallContext EMPTY = new ProcedureCallContext( new String[]{}, false );
}
