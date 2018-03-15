package org.neo4j.kernel.api.impl.fulltext.lucene;

import java.io.IOException;

import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.storageengine.api.schema.IndexReader;

public interface FulltextIndex extends DatabaseIndex<FulltextIndexReader>
{
    @Override
    FulltextIndexReader getIndexReader() throws IOException;
}
