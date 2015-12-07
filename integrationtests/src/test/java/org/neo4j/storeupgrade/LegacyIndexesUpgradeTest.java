/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.storeupgrade;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.index.impl.lucene.legacy.LuceneIndexImplementation.*;

public class LegacyIndexesUpgradeTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    public void setUp() throws IOException
    {
        FileUtils.deleteRecursively( testDir.graphDbDir() );
    }

    @Test
    public void successfulMigrationWithoutLegacyIndexes() throws Exception
    {
        prepareStore( "empty-legacy-index-db.zip" );
        startDbAndCheckData();
    }

    @Test
    public void successfulMigrationLegacyIndexes() throws Exception
    {
        prepareStore( "legacy-index-db.zip" );

        startDbAndCheckData();

        checkIndexData();

        checkMigrationProgressFeedback();
    }

    @Test
    public void migrationShouldFailIfUpgradeNotAllowed() throws IOException
    {
        prepareStore( "legacy-index-db.zip" );
        expectedException.expect( new NestedThrowableMatcher( UpgradeNotAllowedByConfigurationException.class ) );

        startDatabase( false );
    }

    private void startDbAndCheckData()
    {
        GraphDatabaseService db = startDatabase( true );
        try
        {
            checkDbAccessible( db );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void checkDbAccessible( GraphDatabaseService db )
    {
        try ( Transaction transaction = db.beginTx() )
        {
            assertNotNull( db.getNodeById( 1 ) );
            transaction.success();
        }
    }

    private GraphDatabaseService startDatabase(boolean allowUpgread)
    {
        GraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( testDir.graphDbDir() );
        builder.setConfig( GraphDatabaseSettings.allow_store_upgrade, Boolean.toString( allowUpgread ));
        builder.setConfig( GraphDatabaseSettings.pagecache_memory, "8m" );
        return builder.newGraphDatabase();
    }

    private void checkIndexData()
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( testDir.graphDbDir() );
        try
        {
            IntFunction<String> keyFactory = basicKeyFactory();
            Factory<Node> readNodes = readNodes( db );
            readIndex( db, nodeIndex( db, "node-1", EXACT_CONFIG ), readNodes, keyFactory, stringValues() );
            readIndex( db, nodeIndex( db, "node-2", EXACT_CONFIG ), readNodes, keyFactory, intValues() );
            readIndex( db, nodeIndex( db, "node-3", FULLTEXT_CONFIG ), readNodes, keyFactory, stringValues() );
            readIndex( db, nodeIndex( db, "node-4", FULLTEXT_CONFIG ), readNodes, keyFactory, longValues() );
            Factory<Relationship> relationships = readRelationships( db );
            readIndex( db, relationshipIndex( db, "rel-1", EXACT_CONFIG ), relationships, keyFactory, stringValues() );
            readIndex( db, relationshipIndex( db, "rel-2", EXACT_CONFIG ), relationships, keyFactory, floatValues() );
            readIndex( db, relationshipIndex( db, "rel-3", FULLTEXT_CONFIG ), relationships, keyFactory, stringValues() );
            readIndex( db, relationshipIndex( db, "rel-4", FULLTEXT_CONFIG ), relationships, keyFactory, doubleValues() );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void prepareStore(String store) throws IOException
    {
        Unzip.unzip( getClass(), store, testDir.graphDbDir() );
    }

    private IntFunction<Object> intValues()
    {
        return ValueContext::numeric;
    }

    private IntFunction<Object> longValues()
    {
        return value -> ValueContext.numeric( (long) value );
    }

    private IntFunction<Object> floatValues()
    {
        return value -> ValueContext.numeric( (float) value );
    }

    private IntFunction<Object> doubleValues()
    {
        return value -> ValueContext.numeric( (double) value );
    }

    private IntFunction<Object> stringValues()
    {
        return value -> "value balue " + value;
    }

    private Factory<Node> readNodes( final GraphDatabaseService db )
    {
        return new Factory<Node>()
        {
            private long id;

            @Override
            public Node newInstance()
            {
                return db.getNodeById( id++ );
            }
        };
    }

    private Factory<Relationship> readRelationships( final GraphDatabaseService db )
    {
        return new Factory<Relationship>()
        {
            private long id;

            @Override
            public Relationship newInstance()
            {
                return db.getRelationshipById( id++ );
            }
        };
    }

    private Index<Node> nodeIndex( GraphDatabaseService db, String name, Map<String,String> config )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Index<Node> index = db.index().forNodes( name, config );
            tx.success();
            return index;
        }
    }

    private RelationshipIndex relationshipIndex( GraphDatabaseService db, String name, Map<String,String> config )
    {
        try ( Transaction tx = db.beginTx() )
        {
            RelationshipIndex index = db.index().forRelationships( name, config );
            tx.success();
            return index;
        }
    }

    private <ENTITY extends PropertyContainer> void readIndex( GraphDatabaseService db, Index<ENTITY> index,
            Factory<ENTITY> entityFactory, IntFunction<String> keyFactory, IntFunction<Object> valueFactory )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                ENTITY entity = entityFactory.newInstance();
                String key = keyFactory.apply( i );
                Object value = valueFactory.apply( i );
                assertEquals( entity, single( (Iterator<ENTITY>) index.get( key, value ) ) );
            }
            tx.success();
        }
    }

    private IntFunction<String> basicKeyFactory()
    {
        return value -> "key-" + (value % 3);
    }

    private class NestedThrowableMatcher extends TypeSafeMatcher<Throwable>
    {
        private final Class<? extends Throwable> expectedType;

        public NestedThrowableMatcher( Class<? extends Throwable> expectedType )
        {
            this.expectedType = expectedType;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "expect " )
                    .appendValue( expectedType )
                    .appendText( " to be exception cause." );
        }

        @Override
        protected boolean matchesSafely( Throwable item )
        {
            Throwable currentThrowable = item;
            do
            {
                if ( expectedType.isInstance( currentThrowable ) )
                {
                    return true;
                }
                currentThrowable = currentThrowable.getCause();
            }
            while ( currentThrowable != null );
            return false;
        }
    }

    private void checkMigrationProgressFeedback()
    {
        suppressOutput.getOutputVoice().containsMessage( "Starting upgrade of database" );
        suppressOutput.getOutputVoice().containsMessage( "Successfully finished upgrade of database" );
        suppressOutput.getOutputVoice().containsMessage( "10%" );
        suppressOutput.getOutputVoice().containsMessage( "100%" );
    }
}
