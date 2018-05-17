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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexProvider.Monitor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class StringIndexProviderTest extends NativeIndexProviderTest
{
    @Override
    IndexProvider newProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory dir,
                               Monitor monitor, RecoveryCleanupWorkCollector collector )
    {
        return new StringIndexProvider( pageCache, fs, dir, monitor, collector, false );
    }

    @Override
    IndexProvider newReadOnlyProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory dir,
                                       Monitor monitor, RecoveryCleanupWorkCollector collector )
    {
        return new StringIndexProvider( pageCache, fs, dir, monitor, collector, true );
    }

    @Override
    protected InternalIndexState expectedStateOnNonExistingSubIndex()
    {
        return InternalIndexState.POPULATING;
    }

    @Override
    protected Value someValue()
    {
        return Values.stringValue( "abc" );
    }
}
