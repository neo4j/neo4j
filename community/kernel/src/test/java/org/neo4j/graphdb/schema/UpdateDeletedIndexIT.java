/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb.schema;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.Race;
import org.neo4j.test.TestLabels;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.neo4j.test.Race.throwing;

public class UpdateDeletedIndexIT
{
    private static final TestLabels LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";
    private static final int NODES = 100;
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldHandleUpdateRemovalOfLabelConcurrentlyWithIndexDrop() throws Throwable
    {
        // given
        long[] nodes = createNodes();
        IndexDefinition indexDefinition = createIndex();

        // when
        Race race = new Race();
        race.addContestant( indexDropper( indexDefinition ), 1 );
        for ( int i = 0; i < NODES; i++ )
        {
            race.addContestant( nodeLabelRemover( nodes[i] ) );
        }

        // then
        race.go();
    }

    @Test
    public void shouldHandleDeleteNodeConcurrentlyWithIndexDrop() throws Throwable
    {
        // given
        long[] nodes = createNodes();
        IndexDefinition indexDefinition = createIndex();

        // when
        Race race = new Race();
        race.addContestant( indexDropper( indexDefinition ), 1 );
        for ( int i = 0; i < NODES; i++ )
        {
            race.addContestant( nodeDeleter( nodes[i] ) );
        }

        // then
        race.go();
    }

    @Test
    public void shouldHandleRemovePropertyConcurrentlyWithIndexDrop() throws Throwable
    {
        // given
        long[] nodes = createNodes();
        IndexDefinition indexDefinition = createIndex();

        // when
        Race race = new Race();
        race.addContestant( indexDropper( indexDefinition ), 1 );
        for ( int i = 0; i < NODES; i++ )
        {
            race.addContestant( nodePropertyRemover( nodes[i] ) );
        }

        // then
        race.go();
    }

    @Test
    public void shouldHandleNodeDetachDeleteConcurrentlyWithIndexDrop() throws Throwable
    {
        // given
        long[] nodes = createNodes();
        IndexDefinition indexDefinition = createIndex();

        // when
        Race race = new Race();
        race.addContestant( indexDropper( indexDefinition ), 1 );
        for ( int i = 0; i < NODES; i++ )
        {
            race.addContestant( nodeDetachDeleter( nodes[i] ) );
        }

        // then
        race.go();
    }

    private Runnable indexDropper( IndexDefinition indexDefinition )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                indexDefinition.drop();
                tx.success();
            }
        };
    }

    private Runnable nodeLabelRemover( long nodeId )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.getNodeById( nodeId ).removeLabel( LABEL );
                tx.success();
            }
        };
    }

    private Runnable nodeDeleter( long nodeId )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.getNodeById( nodeId ).delete();
                tx.success();
            }
        };
    }

    private Runnable nodeDetachDeleter( long nodeId )
    {
        return throwing( () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class ).getKernelTransactionBoundToThisThread(
                        true ).dataWrite().nodeDetachDelete( nodeId );
                tx.success();
            }
        } );
    }

    private Runnable nodePropertyRemover( long nodeId )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.getNodeById( nodeId ).removeProperty( KEY );
                tx.success();
            }
        };
    }

    private long[] createNodes()
    {
        long[] nodes = new long[NODES];
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < NODES; i++ )
            {
                Node node = db.createNode( LABEL );
                node.setProperty( KEY, i );
                nodes[i] = node.getId();
            }
            tx.success();
        }
        return nodes;
    }

    private IndexDefinition createIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < NODES; i++ )
            {
                db.createNode( LABEL ).setProperty( KEY, i );
            }
            tx.success();
        }
        IndexDefinition indexDefinition;
        try ( Transaction tx = db.beginTx() )
        {
            indexDefinition = db.schema().indexFor( LABEL ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.success();
        }
        return indexDefinition;
    }
}
