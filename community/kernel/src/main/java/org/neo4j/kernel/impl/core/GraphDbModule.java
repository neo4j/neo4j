/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.MeasureDoNothing;
import org.neo4j.kernel.impl.cache.SoftCacheProvider;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;

public class GraphDbModule
{
    private static final String DEFAULT_CACHE_TYPE = SoftCacheProvider.NAME;
    private static Logger log = Logger.getLogger( GraphDbModule.class.getName() );

    private boolean startIsOk = true;

    private static final int INDEX_COUNT = 2500;

    private final GraphDatabaseService graphDbService;
    private final TransactionManager transactionManager;
    private final LockManager lockManager;
    private final EntityIdGenerator idGenerator;

    private NodeManager nodeManager;

    private MeasureDoNothing monitorGc;

    private boolean readOnly = false;

    public GraphDbModule( GraphDatabaseService graphDb, LockManager lockManager, TransactionManager transactionManager,
            EntityIdGenerator idGenerator, boolean readOnly )
    {
        this.graphDbService = graphDb;
        this.lockManager = lockManager;
        this.transactionManager = transactionManager;
        this.idGenerator = idGenerator;
        this.readOnly = readOnly;
    }

    public void init()
    {
    }

    public void start( LockReleaser lockReleaser, PersistenceManager persistenceManager,
            RelationshipTypeCreator relTypeCreator, DiagnosticsManager diagnostics, Map<Object, Object> params,
            Caches caches )
    {
        if ( !startIsOk )
        {
            return;
        }

        CacheProvider cacheProvider = null;
        Map<String, CacheProvider> cacheProviders = new HashMap<String, CacheProvider>();
        for ( CacheProvider provider : Service.load( CacheProvider.class ) )
        {
            cacheProviders.put( provider.getName(), provider );
        }
        String cacheTypeName = (String) params.get( Config.CACHE_TYPE );
        if ( cacheTypeName == null )
        {
            cacheTypeName = DEFAULT_CACHE_TYPE;
        }
        cacheProvider = cacheProviders.get( cacheTypeName );
        if ( cacheProvider == null ) throw new IllegalArgumentException( "No cache type '" + cacheTypeName + "'" );

        caches.configure( cacheProvider, params );
        Cache<NodeImpl> nodeCache = diagnostics.tryAppendProvider( caches.node() );
        Cache<RelationshipImpl> relCache = diagnostics.tryAppendProvider( caches.relationship() );

        if ( !readOnly )
        {
            nodeManager = new NodeManager( graphDbService, lockManager, lockReleaser, transactionManager,
                    persistenceManager, idGenerator, relTypeCreator, cacheProvider, diagnostics, params, nodeCache,
                    relCache );
        }
        else
        {
            nodeManager = new ReadOnlyNodeManager( graphDbService, lockManager, lockReleaser, transactionManager,
                    persistenceManager, idGenerator, cacheProvider, diagnostics, params, nodeCache, relCache );
        }
        // load and verify from PS
        NameData[] relTypes = null;
        NameData[] propertyIndexes = null;
        // beginTx();
        relTypes = persistenceManager.loadAllRelationshipTypes();
        propertyIndexes = persistenceManager.loadPropertyIndexes( INDEX_COUNT );
        // commitTx();
        nodeManager.addRawRelationshipTypes( relTypes );
        nodeManager.addPropertyIndexes( propertyIndexes );
        if ( propertyIndexes.length < INDEX_COUNT )
        {
            nodeManager.setHasAllpropertyIndexes( true );
        }
        nodeManager.start( params );

        startGCMonitor( params );

        startIsOk = false;
    }

    private void startGCMonitor( Map<Object, Object> params )
    {
        int monitor_wait_time = 100;
        try
        {
            String value = (String) params.get( Config.GC_MONITOR_WAIT_TIME );
            if ( value != null )
            {
                monitor_wait_time = Integer.parseInt( value );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        int monitor_threshold = 200;
        try
        {
            String value = (String) params.get( Config.GC_MONITOR_THRESHOLD );
            if ( value != null )
            {
                monitor_threshold = Integer.parseInt( value );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        StringLogger logger = (StringLogger) params.get( StringLogger.class );
        monitorGc = new MeasureDoNothing( logger, monitor_wait_time, monitor_threshold );
        monitorGc.start();
    }

    private void beginTx()
    {
        try
        {
            transactionManager.begin();
        }
        catch ( NotSupportedException e )
        {
            throw new TransactionFailureException(
                "Unable to begin transaction.", e );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException(
                "Unable to begin transaction.", e );
        }
    }

    private void commitTx()
    {
        try
        {
            transactionManager.commit();
        }
        catch ( SecurityException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( IllegalStateException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( RollbackException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( HeuristicMixedException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( HeuristicRollbackException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
        catch ( SystemException e )
        {
            throw new TransactionFailureException( "Failed to commit.", e );
        }
    }

    public void setReferenceNodeId( Long nodeId )
    {
        nodeManager.setReferenceNodeId( nodeId.longValue() );
        try
        {
            nodeManager.getReferenceNode();
        }
        catch ( NotFoundException e )
        {
            log.warning( "Reference node[" + nodeId + "] not valid." );
        }
    }

    public Long getCurrentReferenceNodeId()
    {
        try
        {
            return nodeManager.getReferenceNode().getId();
        }
        catch ( NotFoundException e )
        {
            return -1L;
        }
    }

    public Node createNewReferenceNode()
    {
        Node node = nodeManager.createNode();
        nodeManager.setReferenceNodeId( node.getId() );
        return node;
    }

    public void reload( Map<Object,Object> params )
    {
        throw new UnsupportedOperationException();
    }

    public void stop()
    {
        nodeManager.clearPropertyIndexes();
        nodeManager.clearCache();
        nodeManager.stop();
        monitorGc.stopMeasuring();
    }

    public void destroy()
    {
    }

    public NodeManager getNodeManager()
    {
        return this.nodeManager;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return nodeManager.getRelationshipTypes();
    }

    protected Caches createCaches( StringLogger logger )
    {
        return new DefaultCaches( logger );
    }
}
