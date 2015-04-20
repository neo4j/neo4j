/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;

public class PageCacheRule extends ExternalResource
{
    private PageCache pageCache;
    private final boolean automaticallyProduceInconsistentReads;

    public PageCacheRule()
    {
        automaticallyProduceInconsistentReads = true;
    }

    public PageCacheRule( boolean automaticallyProduceInconsistentReads )
    {
        this.automaticallyProduceInconsistentReads = automaticallyProduceInconsistentReads;
    }

    public PageCache getPageCache( FileSystemAbstraction fs )
    {
        Map<String,String> settings = new HashMap<>();
        settings.put( GraphDatabaseSettings.pagecache_memory.name(), "8M" );
        return getPageCache( fs, new Config( settings ) );
    }

    public PageCache getPageCache( FileSystemAbstraction fs, Config config )
    {
        if ( pageCache != null )
        {
            try
            {
                pageCache.close();
            }
            catch ( IOException e )
            {
                throw new AssertionError(
                        "Failed to stop existing PageCache prior to creating a new one", e );
            }
        }

        pageCache = StandalonePageCacheFactory.createPageCache( fs, config );

        if ( automaticallyProduceInconsistentReads )
        {
            return withInconsistentReads( pageCache );
        }
        return pageCache;
    }

    @Override
    protected void after( boolean success )
    {
        if ( pageCache != null )
        {
            try
            {
                pageCache.close();
            }
            catch ( IOException e )
            {
                throw new AssertionError( "Failed to stop PageCache after test", e );
            }
            pageCache = null;
        }
    }

    /**
     * Returns a decorated PageCache where the next page read from a read page cursor will be
     * inconsistent if the given AtomicBoolean is set to 'true'. The AtomicBoolean is automatically
     * switched to 'false' when the inconsistent read is performed, to prevent code from looping
     * forever.
     */
    public PageCache withInconsistentReads( PageCache pageCache, AtomicBoolean nextReadIsInconsistent )
    {
        InconsistentReadDecision decision = new AtomicInconsistentReadDecision( nextReadIsInconsistent );
        return new PossiblyInconsistentPageCache( pageCache, decision );
    }

    /**
     * Returns a decorated PageCache where the read page cursors will randomly produce inconsistent
     * reads with a ~50% probability.
     */
    public PageCache withInconsistentReads( PageCache pageCache )
    {
        InconsistentReadDecision decision = new RandomInconsistentReadDecision();
        return new PossiblyInconsistentPageCache( pageCache, decision );
    }

    private static interface InconsistentReadDecision
    {
        boolean isNextReadInconsistent();
    }

    private static class AtomicInconsistentReadDecision implements InconsistentReadDecision
    {
        private final AtomicBoolean nextReadIsInconsistent;

        public AtomicInconsistentReadDecision( AtomicBoolean nextReadIsInconsistent )
        {
            this.nextReadIsInconsistent = nextReadIsInconsistent;
        }

        @Override
        public boolean isNextReadInconsistent()
        {
            return nextReadIsInconsistent.getAndSet( false );
        }
    }

    private static class RandomInconsistentReadDecision implements InconsistentReadDecision
    {
        @Override
        public boolean isNextReadInconsistent()
        {
            return ThreadLocalRandom.current().nextBoolean();
        }
    }

    private static class PossiblyInconsistentPageCache implements PageCache
    {
        private final PageCache pageCache;
        private final InconsistentReadDecision decision;

        public PossiblyInconsistentPageCache( PageCache pageCache, InconsistentReadDecision decision )
        {
            this.pageCache = pageCache;
            this.decision = decision;
        }

        @Override
        public PagedFile map( File file, int pageSize ) throws IOException
        {
            PagedFile pagedFile = pageCache.map( file, pageSize );
            return new PossiblyInconsistentPagedFile( pagedFile, decision );
        }

        @Override
        public void flushAndForce() throws IOException
        {
            pageCache.flushAndForce();
        }

        @Override
        public void close() throws IOException
        {
            pageCache.close();
        }

        @Override
        public int pageSize()
        {
            return pageCache.pageSize();
        }

        @Override
        public int maxCachedPages()
        {
            return pageCache.maxCachedPages();
        }
    }

    private static class PossiblyInconsistentPagedFile implements PagedFile
    {
        private final PagedFile pagedFile;
        private final InconsistentReadDecision decision;

        public PossiblyInconsistentPagedFile(
                PagedFile pagedFile, InconsistentReadDecision decision )
        {
            this.pagedFile = pagedFile;
            this.decision = decision;
        }

        @Override
        public String toString()
        {
            return "PossiblyInconsistent:" + pagedFile;
        }

        @Override
        public PageCursor io( long pageId, int pf_flags ) throws IOException
        {
            PageCursor cursor = pagedFile.io( pageId, pf_flags );
            if ( (pf_flags & PF_SHARED_LOCK) == PF_SHARED_LOCK )
            {
                return new PossiblyInconsistentPageCursor( cursor, decision );
            }
            return cursor;
        }

        @Override
        public int pageSize()
        {
            return pagedFile.pageSize();
        }

        @Override
        public void flushAndForce() throws IOException
        {
            pagedFile.flushAndForce();
        }

        @Override
        public void force() throws IOException
        {
            pagedFile.force();
        }

        @Override
        public long getLastPageId() throws IOException
        {
            return pagedFile.getLastPageId();
        }

        @Override
        public void close() throws IOException
        {
            pagedFile.close();
        }
    }

    private static class PossiblyInconsistentPageCursor implements PageCursor
    {
        private final PageCursor cursor;
        private final InconsistentReadDecision decision;
        private boolean currentReadIsInconsistent;

        public PossiblyInconsistentPageCursor(
                PageCursor cursor, InconsistentReadDecision decision )
        {
            this.cursor = cursor;
            this.decision = decision;
        }

        @Override
        public byte getByte()
        {
            return currentReadIsInconsistent? 0 : cursor.getByte();
        }

        @Override
        public byte getByte( int offset )
        {
            return currentReadIsInconsistent? 0 : cursor.getByte( offset );
        }

        @Override
        public void putByte( byte value )
        {
            cursor.putByte( value );
        }

        @Override
        public void putByte( int offset, byte value )
        {
            cursor.putByte( offset, value );
        }

        @Override
        public long getLong()
        {
            return currentReadIsInconsistent? 0 : cursor.getLong();
        }

        @Override
        public long getLong( int offset )
        {
            return currentReadIsInconsistent? 0 : cursor.getLong( offset );
        }

        @Override
        public void putLong( long value )
        {
            cursor.putLong( value );
        }

        @Override
        public void putLong( int offset, long value )
        {
            cursor.putLong( offset, value );
        }

        @Override
        public int getInt()
        {
            return currentReadIsInconsistent? 0 : cursor.getInt();
        }

        @Override
        public int getInt( int offset )
        {
            return currentReadIsInconsistent? 0 : cursor.getInt( offset );
        }

        @Override
        public void putInt( int value )
        {
            cursor.putInt( value );
        }

        @Override
        public void putInt( int offset, int value )
        {
            cursor.putInt( offset, value );
        }

        @Override
        public long getUnsignedInt()
        {
            return currentReadIsInconsistent? 0 : cursor.getUnsignedInt();
        }

        @Override
        public long getUnsignedInt( int offset )
        {
            return currentReadIsInconsistent? 0 : cursor.getUnsignedInt( offset );
        }

        @Override
        public void getBytes( byte[] data )
        {
            if ( !currentReadIsInconsistent )
            {
                cursor.getBytes( data );
            }
        }

        @Override
        public void putBytes( byte[] data )
        {
            cursor.putBytes( data );
        }

        @Override
        public short getShort()
        {
            return currentReadIsInconsistent? 0 : cursor.getShort();
        }

        @Override
        public short getShort( int offset )
        {
            return currentReadIsInconsistent? 0 : cursor.getShort( offset );
        }

        @Override
        public void putShort( short value )
        {
            cursor.putShort( value );
        }

        @Override
        public void putShort( int offset, short value )
        {
            cursor.putShort( offset, value );
        }

        @Override
        public void setOffset( int offset )
        {
            cursor.setOffset( offset );
        }

        @Override
        public int getOffset()
        {
            return cursor.getOffset();
        }

        @Override
        public long getCurrentPageId()
        {
            return cursor.getCurrentPageId();
        }

        @Override
        public int getCurrentPageSize()
        {
            return cursor.getCurrentPageSize();
        }

        @Override
        public File getCurrentFile()
        {
            return cursor.getCurrentFile();
        }

        @Override
        public void rewind() throws IOException
        {
            cursor.rewind();
        }

        @Override
        public boolean next() throws IOException
        {
            currentReadIsInconsistent = decision.isNextReadInconsistent();
            return cursor.next();
        }

        @Override
        public boolean next( long pageId ) throws IOException
        {
            currentReadIsInconsistent = decision.isNextReadInconsistent();
            return cursor.next( pageId );
        }

        @Override
        public void close()
        {
            cursor.close();
        }

        @Override
        public boolean shouldRetry() throws IOException
        {
            if ( currentReadIsInconsistent )
            {
                currentReadIsInconsistent = false;
                cursor.shouldRetry();
                return true;
            }
            return cursor.shouldRetry();
        }
    }
}
