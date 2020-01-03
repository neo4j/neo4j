/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.runtime.statemachine.impl;

import org.neo4j.bolt.runtime.statemachine.StatementMetadata;

public class AutoCommitStatementMetadata implements StatementMetadata
{
    private final String[] fieldNames;

    public AutoCommitStatementMetadata( String[] fieldNames )
    {
        this.fieldNames = fieldNames;
    }

    @Override
    public String[] fieldNames()
    {
        return fieldNames;
    }

    @Override
    public int queryId()
    {
        return ABSENT_QUERY_ID;
    }
}
