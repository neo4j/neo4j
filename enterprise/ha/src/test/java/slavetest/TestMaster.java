/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package slavetest;

import org.junit.Ignore;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.StoreWriter;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.FakeMasterBroker;
import org.neo4j.kernel.ha.FakeSlaveBroker;
import org.neo4j.kernel.ha.IdAllocation;
import org.neo4j.kernel.ha.LockResult;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

/**
 * A {@link Master} which goes together with {@link FakeMasterBroker} / {@link FakeSlaveBroker}
 * and their rigid nature in that they cannot discover a new master themselves. Only used in
 * {@link SingleJvmTest} and friends.
 */
@Ignore( "Not a test" )
class TestMaster implements Master
{
    private final Master actual;
    private GraphDatabaseAPI db;

    TestMaster( Master actual, GraphDatabaseAPI db )
    {
        this.actual = actual;
        this.db = db;
    }
    
    public GraphDatabaseAPI getGraphDb()
    {
        return db;
    }
    
    // This is for testing purposes, mainly because of the rigid broker mocking it has
    public void setGraphDb( GraphDatabaseAPI db )
    {
        this.db = db;
    }

    public Response<IdAllocation> allocateIds( IdType idType )
    {
        return actual.allocateIds( idType );
    }

    public Response<Integer> createRelationshipType( RequestContext context, String name )
    {
        return actual.createRelationshipType( context, name );
    }

    public Response<Void> initializeTx( RequestContext context )
    {
        return actual.initializeTx( context );
    }

    public Response<LockResult> acquireNodeWriteLock( RequestContext context, long... nodes )
    {
        return actual.acquireNodeWriteLock( context, nodes );
    }

    public Response<LockResult> acquireNodeReadLock( RequestContext context, long... nodes )
    {
        return actual.acquireNodeReadLock( context, nodes );
    }

    public Response<LockResult> acquireGraphWriteLock( RequestContext context )
    {
        return actual.acquireGraphWriteLock( context );
    }

    public Response<LockResult> acquireGraphReadLock( RequestContext context )
    {
        return actual.acquireGraphReadLock( context );
    }

    public Response<LockResult> acquireRelationshipWriteLock( RequestContext context,
            long... relationships )
    {
        return actual.acquireRelationshipWriteLock( context, relationships );
    }

    public Response<LockResult> acquireRelationshipReadLock( RequestContext context,
            long... relationships )
    {
        return actual.acquireRelationshipReadLock( context, relationships );
    }

    public Response<Long> commitSingleResourceTransaction( RequestContext context, String resource,
            TxExtractor txGetter )
    {
        return actual.commitSingleResourceTransaction( context, resource, txGetter );
    }

    public Response<Void> finishTransaction( RequestContext context, boolean success )
    {
        return actual.finishTransaction( context, success );
    }

    public Response<Void> pullUpdates( RequestContext context )
    {
        return actual.pullUpdates( context );
    }

    public Response<Pair<Integer, Long>> getMasterIdForCommittedTx( long txId, StoreId myStoreId )
    {
        return actual.getMasterIdForCommittedTx( txId, myStoreId );
    }

    public Response<Void> copyStore( RequestContext context, StoreWriter writer )
    {
        return actual.copyStore( context, writer );
    }

    public void shutdown()
    {
        actual.shutdown();
    }

    public Response<LockResult> acquireIndexWriteLock( RequestContext context, String index,
            String key )
    {
        return actual.acquireIndexWriteLock( context, index, key );
    }

    public Response<LockResult> acquireIndexReadLock( RequestContext context, String index, String key )
    {
        return actual.acquireIndexReadLock( context, index, key );
    }

    @Override
    public Response<Void> copyTransactions( RequestContext context, String dsName, long startTxId,
            long endTxId )
    {
        return actual.copyTransactions( context, dsName, startTxId, endTxId );
    }
    
    @Override
    public Response<Void> pushTransaction( RequestContext context, String resourceName, long tx )
    {
        return actual.pushTransaction( context, resourceName, tx );
    }
}
