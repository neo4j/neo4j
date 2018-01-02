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
package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.kvstore.State;
import org.neo4j.logging.LogProvider;

@State( State.Strategy.READ_ONLY_CONCURRENT_HASH_MAP )
public class ReadOnlyCountsTracker extends CountsTracker
{
    public ReadOnlyCountsTracker( LogProvider logProvider, FileSystemAbstraction fileSystem, PageCache pageCache,
                                  Config config, File baseFile )
    {
        super( logProvider, fileSystem, pageCache, config, baseFile );
    }

    @Override
    public long rotate( long txId ) throws IOException
    {
        return -1;
    }
}
