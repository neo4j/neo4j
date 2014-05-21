package org.neo4j.io.pagecache.impl.standard;

import org.junit.Test;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.impl.common.OffsetTrackingCursor;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.neo4j.io.pagecache.impl.standard.PageTable.PageIO;
import static org.neo4j.io.pagecache.impl.standard.PageTable.PinnablePage;

public class StandardPagedFileTest
{

    @Test
    public void shouldLoadPage() throws Exception
    {
        // Given
        PageCursor cursor = new OffsetTrackingCursor();
        PageTable table = mock(PageTable.class);
        PinnablePage page = mock( PinnablePage.class );
        when( table.load( any( PageIO.class), 12, PageLock.READ ) ).thenReturn( page );
        StoreChannel channel = mock( StoreChannel.class);
        StandardPagedFile file = new StandardPagedFile(table, channel);

        // When
        file.pin( cursor, PageLock.READ, 12 );

        // Then
        verify(table).load( any(PageIO.class), eq(12), eq(PageLock.READ) );
    }


}
