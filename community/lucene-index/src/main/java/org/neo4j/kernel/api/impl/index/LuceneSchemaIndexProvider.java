/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.SchemaIndexProvider;
import org.neo4j.kernel.impl.api.index.IndexPopulator;
import org.neo4j.kernel.impl.api.index.IndexWriter;

@Service.Implementation(SchemaIndexProvider.class)
public class LuceneSchemaIndexProvider extends SchemaIndexProvider
{
    static final String KEY_STATUS = "status";
    static final String ONLINE = "online";
    private final DirectoryFactory directoryFactory;
    
    public LuceneSchemaIndexProvider()
    {
        this( DirectoryFactory.PERSISTENT );
    }
    
    public LuceneSchemaIndexProvider( DirectoryFactory directoryFactory )
    {
        super( "lucene", 1 );
        this.directoryFactory = directoryFactory;
    }
    
    private File dir( File directory, long indexId )
    {
        return new File( directory, "" + indexId );
    }

    @Override
    public IndexPopulator getPopulator( long indexId )
    {
        return new LuceneIndexPopulator( dir( rootDirectory, indexId ), directoryFactory, 10000 );
    }

    @Override
    public IndexWriter getWriter( long indexId )
    {
        return new IndexWriter.Adapter()
        {
        };
    }

    @Override
    public InternalIndexState getInitialState( long indexId )
    {
        try
        {
            Directory directory = directoryFactory.open( dir( rootDirectory, indexId ) );
            if ( !IndexReader.indexExists( directory ) )
                return InternalIndexState.NON_EXISTENT;
            Map<String, String> commitData = IndexReader.getCommitUserData( directory );
            return ONLINE.equals( commitData.get( KEY_STATUS ) ) ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
