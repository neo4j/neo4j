/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import java.io.IOException;
import java.util.Set;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;

class WritableFulltext extends WritableAbstractDatabaseIndex<LuceneFulltext>
{
    private PartitionedIndexWriter indexWriter;

    WritableFulltext( LuceneFulltext luceneFulltext )
    {
        super( luceneFulltext );
    }

    @Override
    public void open() throws IOException
    {
        super.open();
        indexWriter = luceneIndex.getIndexWriter( this );
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        indexWriter = null;
    }

    @Override
    public void drop() throws IOException
    {
        super.drop();
        indexWriter = null;
    }

    PartitionedIndexWriter getIndexWriter()
    {
        return indexWriter;
    }

    Set<String> getProperties()
    {
        return luceneIndex.getProperties();
    }

    void setPopulated()
    {
        luceneIndex.setPopulated();
    }

    void setFailed()
    {
        luceneIndex.setFailed();
    }

    ReadOnlyFulltext getIndexReader() throws IOException
    {
        return luceneIndex.getIndexReader();
    }

    String getAnalyzerName()
    {
        return luceneIndex.getAnalyzerName();
    }

    void saveConfiguration( long lastCommittedTransactionId ) throws IOException
    {
        luceneIndex.saveConfiguration( lastCommittedTransactionId );
    }

    InternalIndexState getState()
    {
        return luceneIndex.getState();
    }

    void awaitNoReaders() throws InterruptedException
    {
        luceneIndex.awaitNoReaders();
    }
}
