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
package org.neo4j.kernel.impl.storemigration;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;

/**
 * Provides source information when reading from a neo4j store. Mostly for store migration purposes
 * where {@link BatchImporter} is used to port the data.
 */
class StoreSourceTraceability implements SourceTraceability
{
    private final String description;
    private final int recordSize;
    private long id;

    public StoreSourceTraceability( String description, int recordSize )
    {
        this.description = description;
        this.recordSize = recordSize;
    }

    @Override
    public String sourceDescription()
    {
        return description;
    }

    @Override
    public long lineNumber()
    {
        return id;
    }

    @Override
    public long position()
    {
        return id*recordSize;
    }

    public void atId( long id )
    {
        this.id = id;
    }
}
