package org.neo4j.io.pagecache;

import java.io.File;
import java.io.IOException;

public interface PageCache
{
    PagedFile map( File file, int pageSize, int transitionalPeriodRecordSize ) throws IOException;

    PageCursor newCursor();

    int getPageCount();
    int getPageSize();
    
    void close();
}
