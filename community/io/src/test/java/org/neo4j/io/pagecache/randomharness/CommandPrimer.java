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
package org.neo4j.io.pagecache.randomharness;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;

class CommandPrimer
{
    private final Random rng;
    private final MuninnPageCache cache;
    private final File[] files;
    private final Map<File,PagedFile> fileMap;
    private final Map<File,List<Integer>> recordsWrittenTo;
    private final List<File> mappedFiles;
    private final Set<File> filesTouched;
    private final int filePageCount;
    private final int filePageSize;
    private final RecordFormat recordFormat;

    public CommandPrimer(
            Random rng,
            MuninnPageCache cache,
            File[] files,
            Map<File,PagedFile> fileMap,
            int filePageCount,
            int filePageSize,
            RecordFormat recordFormat )
    {
        this.rng = rng;
        this.cache = cache;
        this.files = files;
        this.fileMap = fileMap;
        this.filePageCount = filePageCount;
        this.filePageSize = filePageSize;
        this.recordFormat = recordFormat;
        mappedFiles = new ArrayList<>();
        mappedFiles.addAll( fileMap.keySet() );
        filesTouched = new HashSet<>();
        filesTouched.addAll( mappedFiles );
        recordsWrittenTo = new HashMap<>();
        for ( File file : files )
        {
            recordsWrittenTo.put( file, new ArrayList<Integer>() );
        }
    }

    public List<File> getMappedFiles()
    {
        return mappedFiles;
    }

    public Set<File> getFilesTouched()
    {
        return filesTouched;
    }

    public Action prime( Command command )
    {
        switch ( command )
        {
        case FlushCache: return flushCache();
        case FlushFile: return flushFile();
        case MapFile: return mapFile();
        case UnmapFile: return unmapFile();
        case ReadRecord: return readRecord();
        case WriteRecord: return writeRecord();
        default: throw new IllegalArgumentException( "Unknown command: " + command );
        }
    }

    private Action flushCache()
    {
        return new Action( Command.FlushCache, "" )
        {
            @Override
            public void perform() throws Exception
            {
                cache.flushAndForce();
            }
        };
    }

    private Action flushFile()
    {
        if ( mappedFiles.size() > 0 )
        {
            final File file = mappedFiles.get( rng.nextInt( mappedFiles.size() ) );
            return new Action( Command.FlushFile, "[file=%s]", file.getName() )
            {
                @Override
                public void perform() throws Exception
                {
                    PagedFile pagedFile = fileMap.get( file );
                    if ( pagedFile != null )
                    {
                        pagedFile.flushAndForce();
                    }
                }
            };
        }
        return new Action( Command.FlushFile, "[no files mapped to flush]" )
        {
            @Override
            public void perform() throws Exception
            {
            }
        };
    }

    private Action mapFile()
    {
        final File file = files[rng.nextInt( files.length )];
        mappedFiles.add( file );
        filesTouched.add( file );
        return new Action( Command.MapFile, "[file=%s]", file )
        {
            @Override
            public void perform() throws Exception
            {
                fileMap.put( file, cache.map( file, filePageSize ) );
            }
        };
    }

    private Action unmapFile()
    {
        if ( mappedFiles.size() > 0 )
        {
            final File file = mappedFiles.remove( rng.nextInt( mappedFiles.size() ) );
            return new Action( Command.UnmapFile, "[file=%s]", file )
            {
                @Override
                public void perform() throws Exception
                {
                    fileMap.get( file ).close();
                }
            };
        }
        return null;
    }

    private Action readRecord()
    {
        int mappedFilesCount = mappedFiles.size();
        if ( mappedFilesCount == 0 )
        {
            return null;
        }
        final File file = mappedFiles.get( rng.nextInt( mappedFilesCount ) );
        List<Integer> recordsWritten = recordsWrittenTo.get( file );
        int recordSize = recordFormat.getRecordSize();
        int recordsPerPage = cache.pageSize() / recordSize;
        final int recordId =
                recordsWritten.isEmpty()?
                rng.nextInt( filePageCount * recordsPerPage )
                : recordsWritten.get( rng.nextInt( recordsWritten.size() ) );
        final int pageId = recordId / recordsPerPage;
        final int pageOffset = (recordId % recordsPerPage) * recordSize;
        final Record expectedRecord = recordFormat.createRecord( file, recordId );
        return new Action( Command.ReadRecord,
                "[file=%s, recordId=%s, pageId=%s, pageOffset=%s, expectedRecord=%s]",
                file, recordId, pageId, pageOffset, expectedRecord )
        {
            @Override
            public void perform() throws Exception
            {
                PagedFile pagedFile = fileMap.get( file );
                if ( pagedFile != null )
                {
                    try ( PageCursor cursor = pagedFile.io( pageId, PagedFile.PF_SHARED_LOCK ) )
                    {
                        if ( cursor.next() )
                        {
                            cursor.setOffset( pageOffset );
                            Record actualRecord = recordFormat.readRecord( cursor );
                            assertThat( actualRecord, isOneOf( expectedRecord, recordFormat.zeroRecord() ) );
                        }
                    }
                }
            }
        };
    }

    private Action writeRecord()
    {
        int mappedFilesCount = mappedFiles.size();
        if ( mappedFilesCount == 0 )
        {
            return null;
        }
        final File file = mappedFiles.get( rng.nextInt( mappedFilesCount ) );
        filesTouched.add( file );
        int recordSize = 16;
        int recordsPerPage = cache.pageSize() / recordSize;
        final int recordId = rng.nextInt( filePageCount * recordsPerPage );
        recordsWrittenTo.get( file ).add( recordId );
        final int pageId = recordId / recordsPerPage;
        final int pageOffset = (recordId % recordsPerPage) * recordSize;
        final Record record = recordFormat.createRecord( file, recordId );
        return new Action(  Command.WriteRecord,
                "[file=%s, recordId=%s, pageId=%s, pageOffset=%s, record=%s]",
                file, recordId, pageId, pageOffset, record )
        {
            @Override
            public void perform() throws Exception
            {
                PagedFile pagedFile = fileMap.get( file );
                if ( pagedFile != null )
                {
                    try ( PageCursor cursor = pagedFile.io( pageId, PagedFile.PF_EXCLUSIVE_LOCK ) )
                    {
                        if ( cursor.next() )
                        {
                            cursor.setOffset( pageOffset );
                            recordFormat.write( record, cursor );
                        }
                    }
                }
            }
        };
    }
}
