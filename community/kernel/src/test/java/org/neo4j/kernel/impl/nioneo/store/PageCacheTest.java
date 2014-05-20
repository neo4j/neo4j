package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.legacy.WindowPoolPageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class PageCacheTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private final File storeFile = new File( "myStore" );

    @Test
    public void shouldMapAndDoIO() throws Exception
    {
        // Given
        PageCache cache = newPageCache();

        // When
        PagedFile mappedFile = cache.map( storeFile, 1024, 33 );

        // Then I should be able to write to the file
        PageCursor cursor = cache.newCursor();
        mappedFile.pin( cursor, PageLock.WRITE, 4 );

        cursor.setOffset( 33 );
        byte[] expected = "Hello, cruel world".getBytes( "UTF-8" );
        cursor.putBytes( expected );
        cursor.putByte( (byte) 13 );
        cursor.putInt( 1337 );
        cursor.putLong( 7331 );

        // Then I should be able to read from the file
        cursor.setOffset( 33 );
        byte[] actual = new byte[expected.length];
        cursor.getBytes( actual );

        assertThat( actual, equalTo( expected ) );
        assertThat(cursor.getByte(), equalTo((byte)13));
        assertThat(cursor.getInt(), equalTo(1337));
        assertThat(cursor.getLong(), equalTo(7331l));
    }

    private PageCache newPageCache() throws IOException
    {
        EphemeralFileSystemAbstraction fs = fsRule.get();
        return new WindowPoolPageCache( new DefaultWindowPoolFactory( new Monitors(), new Config() ), fs );
    }
}
