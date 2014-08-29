/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.stress;

import java.io.File;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

public class PageCacheStresser
{
    private final int maxPages;
    private final int recordsPerPage;
    private final int numberOfThreads;
    private RecordVerifierUpdater recordVerifierUpdater;

    public PageCacheStresser( int maxPages, int recordsPerPage, int numberOfThreads )
    {
        this.maxPages = maxPages;
        this.recordsPerPage = recordsPerPage;
        this.numberOfThreads = numberOfThreads;
        this.recordVerifierUpdater = new RecordVerifierUpdater( this.numberOfThreads );
    }

    public void stress( PageCache pageCache, Condition condition ) throws Exception
    {
        File file = Files.createTempFile( "pagecachestress", ".bin" ).toFile();
        file.deleteOnExit();

        PagedFile pagedFile = pageCache.map( file, recordsPerPage * recordVerifierUpdater.getRecordSize() );

        List<Updater> updaters = new LinkedList<>();
        for ( int threadNumber = 0; threadNumber < numberOfThreads; threadNumber++ )
        {
            updaters.add(
                    new Updater(
                            pagedFile, condition, maxPages, recordsPerPage, recordVerifierUpdater, threadNumber
                    )
            );
        }

        ExecutorService executorService = Executors.newFixedThreadPool( numberOfThreads );
        for ( Future<Updater> future : executorService.invokeAll( updaters ) )
        {
            future.get().verifyCounts();
        }

        new Verifier().verify( pagedFile, maxPages, recordsPerPage, recordVerifierUpdater );

        pageCache.unmap( file );
    }

    public int getRecordSize()
    {
        return recordVerifierUpdater.getRecordSize();
    }
}
