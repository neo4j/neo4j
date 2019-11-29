/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.index.SabotageNativeIndex.nativeIndexDirectoryStructure;

public class IndexFailureOnStartupTest
{
    private static final Label PERSON = Label.label( "Person" );

    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final DbmsRule db = new EmbeddedDbmsRule().startLazily();

    @Test
    public void failedIndexShouldRepairAutomatically() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( PERSON ).on( "name" ).create();
            tx.commit();
        }
        awaitIndexesOnline( 5, SECONDS );
        createNamed( PERSON, "Johan" );
        // when - we restart the database in a state where the index is not operational
        db.restartDatabase( new SabotageNativeIndex( random.random() ) );
        // then - the database should still be operational
        createNamed( PERSON, "Lars" );
        awaitIndexesOnline( 5, SECONDS );
        indexStateShouldBe( ONLINE );
        assertFindsNamed( PERSON, "Lars" );
    }

    @Test
    public void shouldNotBeAbleToViolateConstraintWhenBackingIndexFailsToOpen() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( PERSON ).assertPropertyIsUnique( "name" ).create();
            tx.commit();
        }
        createNamed( PERSON, "Lars" );
        // when - we restart the database in a state where the index is not operational
        db.restartDatabase( new SabotageNativeIndex( random.random() ) );
        // then - we must not be able to violate the constraint
        createNamed( PERSON, "Johan" );
        Throwable failure = null;
        try
        {
            createNamed( PERSON, "Lars" );
        }
        catch ( Throwable e )
        {
            // this must fail, otherwise we have violated the constraint
            failure = e;
        }
        assertNotNull( failure );
        indexStateShouldBe( ONLINE );
    }

    @Test
    public void shouldArchiveFailedIndex() throws Exception
    {
        // given
        db.withSetting( GraphDatabaseSettings.archive_failed_index, true );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( PERSON );
            node.setProperty( "name", "Fry" );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( PERSON );
            node.setProperty( "name", Values.pointValue( CoordinateReferenceSystem.WGS84, 1, 2 ) );
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( PERSON ).assertPropertyIsUnique( "name" ).create();
            tx.commit();
        }
        assertThat( archiveFile() ).isNull();

        // when
        db.restartDatabase( new SabotageNativeIndex( random.random() ) );

        // then
        indexStateShouldBe( ONLINE );
        assertThat( archiveFile() ).isNotNull();
    }

    private File archiveFile()
    {
        File indexDir = nativeIndexDirectoryStructure( db.databaseLayout() ).rootDirectory();
        File[] files = indexDir.listFiles( pathname -> pathname.isFile() && pathname.getName().startsWith( "archive-" ) );
        if ( files == null || files.length == 0 )
        {
            return null;
        }
        assertEquals( 1, files.length );
        return files[0];
    }

    private void awaitIndexesOnline( int timeout, TimeUnit unit )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( timeout, unit );
            tx.commit();
        }
    }

    private void assertFindsNamed( Label label, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( "Must be able to find node created while index was offline",
                    tx.findNode( label, "name", name ) );
            tx.commit();
        }
    }

    private void indexStateShouldBe( Schema.IndexState value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( IndexDefinition index : tx.schema().getIndexes() )
            {
                assertThat( tx.schema().getIndexState( index ) ).isEqualTo( value );
            }
            tx.commit();
        }
    }

    private void createNamed( Label label, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode( label ).setProperty( "name", name );
            tx.commit();
        }
    }
}
