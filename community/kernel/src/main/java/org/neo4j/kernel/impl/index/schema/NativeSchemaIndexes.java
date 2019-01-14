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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.pagecache.PageCache;

import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_FAILED;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_ONLINE;
import static org.neo4j.kernel.impl.index.schema.NativeSchemaIndexPopulator.BYTE_POPULATING;

public class NativeSchemaIndexes
{
    private  NativeSchemaIndexes()
    {}

    public static InternalIndexState readState( PageCache pageCache, File indexFile ) throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, indexFile, headerReader );
        switch ( headerReader.state )
        {
        case BYTE_FAILED:
            return InternalIndexState.FAILED;
        case BYTE_ONLINE:
            return InternalIndexState.ONLINE;
        case BYTE_POPULATING:
            return InternalIndexState.POPULATING;
        default:
            throw new IllegalStateException( "Unexpected initial state byte value " + headerReader.state );
        }
    }

    static String readFailureMessage( PageCache pageCache, File indexFile )
            throws IOException
    {
        NativeSchemaIndexHeaderReader headerReader = new NativeSchemaIndexHeaderReader();
        GBPTree.readHeader( pageCache, indexFile, headerReader );
        return headerReader.failureMessage;
    }
}
