/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.fulltext;

import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.FulltextSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.lang.String.format;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.AWAIT_REFRESH;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.FULLTEXT_CREATE;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asNodeLabelStr;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asPropertiesStrList;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProceduresUtil.asRelationshipTypeStr;
import static org.neo4j.scheduler.JobMonitoringParams.NOT_MONITORED;

class EventuallyConsistentFulltextProceduresTest extends FulltextProceduresTestSupport
{
    @ExtensionCallback
    @Override
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        super.configure( builder );
        builder.setConfig( FulltextSettings.eventually_consistent, true );
    }

    @Test
    void fulltextIndexesMustBeEventuallyConsistentByDefaultWhenThisIsConfigured() throws InterruptedException
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.execute( format( FULLTEXT_CREATE, DEFAULT_NODE_IDX_NAME, asNodeLabelStr( LABEL.name() ), asPropertiesStrList( PROP, "otherprop" ) ) );
            tx.execute( format( FULLTEXT_CREATE, DEFAULT_REL_IDX_NAME, asRelationshipTypeStr( REL.name() ), asPropertiesStrList( PROP ) ) );
            tx.commit();
        }
        awaitIndexesOnline();

        // Prevent index updates from being applied to eventually consistent indexes.
        BinaryLatch indexUpdateBlocker = new BinaryLatch();
        db.getDependencyResolver().resolveDependency( JobScheduler.class ).schedule( Group.INDEX_UPDATING, NOT_MONITORED, indexUpdateBlocker::await );

        LongHashSet nodeIds = new LongHashSet();
        long relId;
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = tx.createNode( LABEL );
                node1.setProperty( PROP, "bla bla" );
                Node node2 = tx.createNode( LABEL );
                node2.setProperty( "otherprop", "bla bla" );
                Relationship relationship = node1.createRelationshipTo( node2, REL );
                relationship.setProperty( PROP, "bla bla" );
                nodeIds.add( node1.getId() );
                nodeIds.add( node2.getId() );
                relId = relationship.getId();
                tx.commit();
            }

            // Index updates are still blocked for eventually consistent indexes, so we should not find anything at this point.
            assertQueryFindsIdsInOrder( db, true, DEFAULT_NODE_IDX_NAME, "bla" );
            assertQueryFindsIdsInOrder( db, false, DEFAULT_REL_IDX_NAME, "bla" );
        }
        finally
        {
            // Uncork the eventually consistent fulltext index updates.
            Thread.sleep( 10 );
            indexUpdateBlocker.release();
        }
        // And wait for them to apply.
        try ( Transaction transaction = db.beginTx() )
        {
            transaction.execute( AWAIT_REFRESH ).close();
            transaction.commit();
        }

        // Now we should see our data.
        assertQueryFindsIds( db, true, DEFAULT_NODE_IDX_NAME, "bla", nodeIds );
        assertQueryFindsIds( db, false, DEFAULT_REL_IDX_NAME, "bla", newSetWith( relId ) );
    }
}
