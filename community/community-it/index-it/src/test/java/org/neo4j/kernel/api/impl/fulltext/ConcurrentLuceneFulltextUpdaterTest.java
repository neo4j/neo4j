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
package org.neo4j.kernel.api.impl.fulltext;

import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.test.Race;
import org.neo4j.test.rule.RepeatRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.storageengine.api.EntityType.NODE;

/**
 * Concurrent updates and index changes should result in valid state, and not create conflicts or exceptions during
 * commit.
 */
public class ConcurrentLuceneFulltextUpdaterTest extends LuceneFulltextTestSupport
{
    private final int aliceThreads = 10;
    private final int bobThreads = 10;
    private final int nodesCreatedPerThread = 10;
    private Race race;

    @Override
    protected RepeatRule createRepeatRule()
    {
        return new RepeatRule( false, 3 );
    }

    @Before
    public void createRace()
    {
        race = new Race();
    }

    private SchemaDescriptor getNewDescriptor( String[] entityTokens )
    {
        return fulltextAdapter.schemaFor( NODE, entityTokens, settings, "otherProp" );
    }

    private SchemaDescriptor getExistingDescriptor( String[] entityTokens )
    {
        return fulltextAdapter.schemaFor( NODE, entityTokens, settings, PROP );
    }

    private IndexReference createInitialIndex( SchemaDescriptor descriptor ) throws Exception
    {
        IndexReference index;
        try ( KernelTransactionImplementation transaction = getKernelTransaction() )
        {
            SchemaWrite schemaWrite = transaction.schemaWrite();
            index = schemaWrite.indexCreate( descriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), Optional.of( "nodes" ) );
            transaction.success();
        }
        await( index );
        return index;
    }

    private void raceContestantsAndVerifyResults( SchemaDescriptor newDescriptor, Runnable aliceWork, Runnable changeConfig, Runnable bobWork ) throws Throwable
    {
        race.addContestants( aliceThreads, aliceWork );
        race.addContestant( changeConfig );
        race.addContestants( bobThreads, bobWork );
        race.go();
        Thread.sleep( 100 );
        await( IndexDescriptorFactory.forSchema( newDescriptor, Optional.of( "nodes" ), FulltextIndexProviderFactory.DESCRIPTOR ) );
        try ( Transaction tx = db.beginTx() )
        {
            KernelTransaction ktx = kernelTransaction( tx );
            ScoreEntityIterator bob = fulltextAdapter.query( ktx, "nodes", "bob" );
            assertEquals( bobThreads * nodesCreatedPerThread, bob.stream().count() );
            ScoreEntityIterator alice = fulltextAdapter.query( ktx, "nodes", "alice" );
            assertEquals( 0, alice.stream().count() );
        }
    }

    private Runnable work( int iterations, ThrowingAction<Exception> work )
    {
        return () ->
        {
            try
            {
                for ( int i = 0; i < iterations; i++ )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        work.apply();
                        tx.success();
                    }
                }
            }
            catch ( Exception e )
            {
                throw new AssertionError( e );
            }
        };
    }

    private ThrowingAction<Exception> dropAndReCreateIndex( IndexReference descriptor, SchemaDescriptor newDescriptor )
    {
        return () ->
        {
            try ( KernelTransactionImplementation transaction = getKernelTransaction() )
            {
                SchemaWrite schemaWrite = transaction.schemaWrite();
                schemaWrite.indexDrop( descriptor );
                schemaWrite.indexCreate( newDescriptor, FulltextIndexProviderFactory.DESCRIPTOR.name(), Optional.of( "nodes" ) );
                transaction.success();
            }
        };
    }

    @Test
    public void labelledNodesCoreAPI() throws Throwable
    {
        Label label = Label.label( "LABEL" );
        String[] entityTokens = {label.name()};
        SchemaDescriptor descriptor = getExistingDescriptor( entityTokens );
        SchemaDescriptor newDescriptor = getNewDescriptor( entityTokens );
        IndexReference initialIndex = createInitialIndex( descriptor );

        Runnable aliceWork = work( nodesCreatedPerThread, () -> db.getNodeById( createNodeIndexableByPropertyValue( LABEL, "alice" ) ).addLabel( label ) );
        Runnable bobWork = work( nodesCreatedPerThread, () -> db.getNodeById( createNodeWithProperty( LABEL, "otherProp", "bob" ) ).addLabel( label ) );
        Runnable changeConfig = work( 1, dropAndReCreateIndex( initialIndex, newDescriptor ) );
        raceContestantsAndVerifyResults( newDescriptor, aliceWork, changeConfig, bobWork );
    }

    @Test
    public void labelledNodesCypherCurrent() throws Throwable
    {
        Label label = Label.label( "LABEL" );
        String[] entityTokens = {label.name()};
        SchemaDescriptor descriptor = getExistingDescriptor( entityTokens );
        SchemaDescriptor newDescriptor = getNewDescriptor( entityTokens );
        IndexReference initialIndex = createInitialIndex( descriptor );

        Runnable aliceWork = work( nodesCreatedPerThread, () -> db.execute( "create (:LABEL {" + PROP + ": \"alice\"})" ).close() );
        Runnable bobWork = work( nodesCreatedPerThread, () -> db.execute( "create (:LABEL {otherProp: \"bob\"})" ).close() );
        Runnable changeConfig = work( 1, dropAndReCreateIndex( initialIndex, newDescriptor ) );
        raceContestantsAndVerifyResults( newDescriptor, aliceWork, changeConfig, bobWork );
    }

    @Test
    public void labelledNodesCypher31() throws Throwable
    {
        Label label = Label.label( "LABEL" );
        String[] entityTokens = {label.name()};
        SchemaDescriptor descriptor = getExistingDescriptor( entityTokens );
        SchemaDescriptor newDescriptor = getNewDescriptor( entityTokens );
        IndexReference initialIndex = createInitialIndex( descriptor );

        Runnable aliceWork = work( nodesCreatedPerThread, () -> db.execute( "CYPHER 3.1 create (:LABEL {" + PROP + ": \"alice\"})" ).close() );
        Runnable bobWork = work( nodesCreatedPerThread, () -> db.execute( "CYPHER 3.1 create (:LABEL {otherProp: \"bob\"})" ).close() );
        Runnable changeConfig = work( 1, dropAndReCreateIndex( initialIndex, newDescriptor ) );
        raceContestantsAndVerifyResults( newDescriptor, aliceWork, changeConfig, bobWork );
    }

    @Test
    public void labelledNodesCypher23() throws Throwable
    {
        Label label = Label.label( "LABEL" );
        String[] entityTokens = {label.name()};
        SchemaDescriptor descriptor = getExistingDescriptor( entityTokens );
        SchemaDescriptor newDescriptor = getNewDescriptor( entityTokens );
        IndexReference initialIndex = createInitialIndex( descriptor );

        Runnable aliceWork = work( nodesCreatedPerThread, () -> db.execute( "CYPHER 2.3 create (:LABEL {" + PROP + ": \"alice\"})" ).close() );
        Runnable bobWork = work( nodesCreatedPerThread, () -> db.execute( "CYPHER 2.3 create (:LABEL {otherProp: \"bob\"})" ).close() );
        Runnable changeConfig = work( 1, dropAndReCreateIndex( initialIndex, newDescriptor ) );
        raceContestantsAndVerifyResults( newDescriptor, aliceWork, changeConfig, bobWork );
    }

    @Test
    public void labelledNodesCypherRule() throws Throwable
    {
        Label label = Label.label( "LABEL" );
        String[] entityTokens = {label.name()};
        SchemaDescriptor descriptor = getExistingDescriptor( entityTokens );
        SchemaDescriptor newDescriptor = getNewDescriptor( entityTokens );
        IndexReference initialIndex = createInitialIndex( descriptor );

        Runnable aliceWork = work( nodesCreatedPerThread, () -> db.execute( "CYPHER planner=rule create (:LABEL {" + PROP + ": \"alice\"})" ).close() );
        Runnable bobWork = work( nodesCreatedPerThread, () -> db.execute( "CYPHER planner=rule create (:LABEL {otherProp: \"bob\"})" ).close() );
        Runnable changeConfig = work( 1, dropAndReCreateIndex( initialIndex, newDescriptor ) );
        raceContestantsAndVerifyResults( newDescriptor, aliceWork, changeConfig, bobWork );
    }
}
