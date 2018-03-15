package org.neo4j.kernel.api.impl.fulltext.lucene;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.DatabaseIndex;

public interface FulltextIndex extends DatabaseIndex
{
    @Override
    FulltextIndexReader getIndexReader() throws IOException;
}
