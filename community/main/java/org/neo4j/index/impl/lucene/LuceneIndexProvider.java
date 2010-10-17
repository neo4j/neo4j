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

import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
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
    
    final ConnectionBroker broker;
    final LuceneDataSource dataSource;
    final int lazynessThreshold = DEFAULT_LAZY_THRESHOLD;
    final GraphDatabaseService graphDb;
    
    public LuceneIndexProvider( final GraphDatabaseService graphDb )
    {
        super( SERVICE_NAME );
        this.graphDb = graphDb;

        Config config = getGraphDbConfig();
        TxModule txModule = config.getTxModule();
        boolean isReadOnly = isReadOnly( graphDb );
        Map<Object, Object> params = new HashMap<Object, Object>( config.getParams() );
        params.put( "read_only", isReadOnly );
        params.put( "index_config",
//                config.getIndexStore() );
                new HashMap<String, Map<String, String>>() );
        dataSource = (LuceneDataSource) txModule.registerDataSource( DATA_SOURCE_NAME,
                LuceneDataSource.class.getName(), LuceneDataSource.DEFAULT_BRANCH_ID,
                params, true );
        broker = isReadOnly ?
                new ReadOnlyConnectionBroker( txModule.getTxManager(), dataSource ) :
                new ConnectionBroker( txModule.getTxManager(), dataSource );
    }
    
    private static boolean isReadOnly( GraphDatabaseService graphDb )
    {
        return graphDb instanceof EmbeddedReadOnlyGraphDatabase;
    }
    
    private Config getGraphDbConfig()
    {
        return ((AbstractGraphDatabase) graphDb).getConfig();
    }
    
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        IndexIdentifier identifier = new IndexIdentifier( LuceneCommand.NODE,
                dataSource.nodeEntityType, indexName, config( indexName, config ) );
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
                dataSource.relationshipEntityType, indexName, config( indexName, config ) );
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
    
    private Map<String, String> config( final String indexName, Map<String, String> config )
    {
        final Pair<Map<String, String>, Boolean> result = dataSource.indexStore.getIndexConfig( indexName,
                config, null, LuceneConfigDefaultsFiller.INSTANCE );
        if ( result.other() )
        {
            Thread creator = new Thread()
            {
                @Override
                public void run()
                {
                    Transaction tx = graphDb.beginTx();
                    try
                    {
                        LuceneXaConnection connection = (LuceneXaConnection) dataSource.getXaConnection();
                        javax.transaction.Transaction javaxTx = getGraphDbConfig().getTxModule().getTxManager().getTransaction();
                        javaxTx.enlistResource( connection.getXaResource() );
                        try
                        {
                            connection.getLuceneTx().createIndex( indexName, result.first() );
                        }
                        finally
                        {
                            javaxTx.delistResource( connection.getXaResource(), XAResource.TMSUCCESS );
                        }
                        tx.success();
                    }
                    catch ( Exception e )
                    {
                        e.printStackTrace();
                    }
                    finally
                    {
                        tx.finish();
                    }
                }
            };
            creator.start();
            try
            {
                creator.join();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
        return result.first();
    }
}
