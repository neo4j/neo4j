/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.index.impl.lucene;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.index.IndexConnectionBroker;
import org.neo4j.kernel.impl.index.ReadOnlyIndexConnectionBroker;
import org.neo4j.kernel.impl.transaction.TxModule;

public class LuceneIndexProvider extends IndexProvider
{
    private static final String KEY_PROVIDER = "provider";
    private static final String KEY_TYPE = "type";
    public static final String SERVICE_NAME = "lucene";
    
    public static final Map<String, String> EXACT_CONFIG =
            Collections.unmodifiableMap( MapUtil.stringMap(
                    KEY_PROVIDER, SERVICE_NAME, KEY_TYPE, "exact" ) );
    
    public static final Map<String, String> FULLTEXT_CONFIG =
            Collections.unmodifiableMap( MapUtil.stringMap(
                    KEY_PROVIDER, SERVICE_NAME, KEY_TYPE, "fulltext" ) );
    
    public static final int DEFAULT_LAZY_THRESHOLD = 100;
    private static final String DATA_SOURCE_NAME = "lucene-index";
    
    private IndexConnectionBroker<LuceneXaConnection> broker;
    private LuceneDataSource dataSource;
    private GraphDatabaseService graphDb;
    final int lazynessThreshold = DEFAULT_LAZY_THRESHOLD;
    
    public LuceneIndexProvider()
    {
        super( SERVICE_NAME );
    }
    
    public LuceneIndexProvider( GraphDatabaseService db )
    {
        this();
        load( db, ((AbstractGraphDatabase) db).getConfig() );
    }
    
    IndexConnectionBroker<LuceneXaConnection> broker()
    {
        return this.broker;
    }
    
    LuceneDataSource dataSource()
    {
        return this.dataSource;
    }
    
    GraphDatabaseService graphDb()
    {
        return this.graphDb;
    }
    
    @Override
    protected void load( KernelData kernel )
    {
        try
        {
            load( kernel.graphDatabase(), kernel.getConfig() );
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    private void load( GraphDatabaseService db, Config config )
    {
        this.graphDb = db;
        TxModule txModule = config.getTxModule();
        boolean isReadOnly = ((AbstractGraphDatabase) graphDb).isReadOnly();
        Map<Object, Object> params = new HashMap<Object, Object>( config.getParams() );
        params.put( "read_only", isReadOnly );
        dataSource = (LuceneDataSource) txModule.registerDataSource( DATA_SOURCE_NAME,
                LuceneDataSource.class.getName(), LuceneDataSource.DEFAULT_BRANCH_ID,
                params, true );
        broker = isReadOnly ?
                new ReadOnlyIndexConnectionBroker<LuceneXaConnection>( txModule.getTxManager() ) :
                new ConnectionBroker( txModule.getTxManager(), dataSource );
    }
    
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.NODE,
                dataSource.nodeEntityType, indexName, config );
        synchronized ( dataSource.indexes )
        {
            LuceneIndex index = dataSource.indexes.get( identifier );
            if ( index == null )
            {
                index = new LuceneIndex.NodeIndex( this, identifier );
                dataSource.indexes.put( identifier, index );
            }
            return index;
        }
        
//        return new LuceneIndex.NodeIndex( this, identifier );
    }

    public RelationshipIndex relationshipIndex( String indexName, Map<String, String> config )
    {
        IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.RELATIONSHIP,
                dataSource.relationshipEntityType, indexName, config );
        synchronized ( dataSource.indexes )
        {
            LuceneIndex index = dataSource.indexes.get( identifier );
            if ( index == null )
            {
                index = new LuceneIndex.RelationshipIndex( this, identifier );
                dataSource.indexes.put( identifier, index );
            }
            return (RelationshipIndex) index;
        }
//
//        return new LuceneIndex.RelationshipIndex( this, identifier );
    }
    
    public Map<String, String> fillInDefaults( Map<String, String> source )
    {
        Map<String, String> result = source != null ?
                new HashMap<String, String>( source ) : new HashMap<String, String>();
        String type = result.get( "type" );
        if ( type == null )
        {
            type = "exact";
            result.put( "type", type );
        }
        if ( type.equals( "fulltext" ) )
        {
            if ( !result.containsKey( "to_lower_case" ) )
            {
                result.put( "to_lower_case", "true" );
            }
        }
        return result;
    }
    
    @Override
    public String getDataSourceName()
    {
        return DATA_SOURCE_NAME;
    }
}
