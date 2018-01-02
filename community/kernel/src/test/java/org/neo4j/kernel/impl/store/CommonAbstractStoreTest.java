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
package org.neo4j.kernel.impl.store;

import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class CommonAbstractStoreTest
{
    private static final NullLogProvider LOG = NullLogProvider.getInstance();
    private final IdGeneratorFactory idGeneratorFactory = mock( IdGeneratorFactory.class );
    private final PageCache pageCache = mock( PageCache.class );
    private final Config config = new Config();
    private final File storeFile = new File( "store" );
    private final IdType idType = IdType.RELATIONSHIP; // whatever

    @Test
    public void shouldCloseStoreFileFirstAndIdGeneratorAfter() throws Throwable
    {
        // given
        PagedFile storePagedFile = mock( PagedFile.class );
        when( pageCache.map( eq( storeFile ), anyInt() ) ).thenReturn( storePagedFile );
        IdGenerator idGenerator = mock(
                IdGenerator.class );
        when( idGeneratorFactory.open( any( File.class ), eq( idType ), anyInt() ) )
                .thenReturn( idGenerator );
        CommonAbstractStore store = new TheStore( storeFile, config, idType, idGeneratorFactory, pageCache, LOG );
        store.initialise( false );

        // this is needed to forget all interaction with the mocks during the construction of the store
        reset( storePagedFile, idGenerator );

        InOrder inOrder = inOrder( storePagedFile, idGenerator );

        // when
        store.close();

        // then
        inOrder.verify( storePagedFile, times( 1 ) ).close();
        inOrder.verify( idGenerator, times( 1 ) ).close();
    }

    private static class TheStore extends CommonAbstractStore
    {
        public TheStore( File fileName, Config configuration, IdType idType, IdGeneratorFactory idGeneratorFactory,
                PageCache pageCache, LogProvider logProvider )
        {
            super( fileName, configuration, idType, idGeneratorFactory, pageCache, logProvider );
        }

        @Override
        protected String getTypeDescriptor()
        {
            return null;
        }

        @Override
        protected void initialiseNewStoreFile( PagedFile file ) throws IOException
        {
        }

        @Override
        protected void readAndVerifyBlockSize() throws IOException
        {
        }

        @Override
        protected boolean isInUse( byte inUseByte )
        {
            return false;
        }

        @Override
        public int getRecordSize()
        {
            return 10;
        }

        @Override
        public long scanForHighId()
        {
            return 42;
        }
    }
}
