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

import static java.nio.file.Paths.get;
import static org.neo4j.io.pagecache.stress.StressTestRecord.SizeOfCounter;

public class PageCacheStresser
{
    private final int maxPages;
    private final int recordsPerPage;
    private final int numberOfThreads;

    private final String workingDirectory;

    public PageCacheStresser( int maxPages, int recordsPerPage, int numberOfThreads, String workingDirectory )
    {
        this.maxPages = maxPages;
        this.recordsPerPage = recordsPerPage;
        this.numberOfThreads = numberOfThreads;
        this.workingDirectory = workingDirectory;
    }

    public void stress( PageCache pageCache, Condition condition, CountKeeperFactory countKeeperFactory ) throws Exception
    {
        File file = Files.createTempFile( get( workingDirectory ), "pagecacheundertest", ".bin" ).toFile();
        file.deleteOnExit();
        PagedFile pagedFile = pageCache.map( file, recordsPerPage * (numberOfThreads + 1) * SizeOfCounter );

        ChecksumVerifier checksumVerifier = new ChecksumVerifier( recordsPerPage, numberOfThreads );

        List<RecordStresser> recordStressers = prepare( condition, countKeeperFactory, pagedFile, checksumVerifier );

        execute( recordStressers );

        countKeeperFactory.createVerifier().verifyCounts( pagedFile );
        checksumVerifier.verifyChecksums( pagedFile );

        pagedFile.close();
    }

    private List<RecordStresser> prepare( Condition condition, CountKeeperFactory countKeeperFactory, PagedFile pagedFile, ChecksumVerifier checksumVerifier )
    {
        CountUpdater countUpdater = new CountUpdater( numberOfThreads );

        List<RecordStresser> recordStressers = new LinkedList<>();
        for ( int threadNumber = 0; threadNumber < numberOfThreads; threadNumber++ )
        {
            recordStressers.add(
                    new RecordStresser(
                            pagedFile,
                            condition,
                            checksumVerifier,
                            countUpdater,
                            countKeeperFactory.createRecordKeeper(),
                            maxPages,
                            recordsPerPage,
                            threadNumber

                    )
            );
        }
        return recordStressers;
    }

    private void execute( List<RecordStresser> recordStressers ) throws InterruptedException,
            java.util.concurrent.ExecutionException
    {
        ExecutorService executorService = Executors.newFixedThreadPool( numberOfThreads );

        try
        {
            for ( Future<Void> future : executorService.invokeAll( recordStressers ) )
            {
                future.get();
            }
        }
        finally
        {
            executorService.shutdown();
        }
    }
}
