package org.neo4j.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface SCIndex extends Closeable
{
    public static final String filePrefix = "shortcut.index.";
    public static final String indexFileSuffix = ".bin";
    public static final String metaFileSuffix = ".meta";

    SCIndexDescription getDescription();

    void insert( long[] key, long[] value ) throws IOException;

    void seek( Seeker seeker, List<SCResult> resultList) throws IOException;
}
