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
package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.fail;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class TestExceptionTypeOnInvalidIds
{
    private static final long SMALL_POSSITIVE_INTEGER = 5;
    private static final long SMALL_NEGATIVE_INTEGER = -5;
    private static final long BIG_POSSITIVE_INTEGER = Integer.MAX_VALUE;
    private static final long BIG_NEGATIVE_INTEGER = Integer.MIN_VALUE;
    private static final long SMALL_POSSITIVE_LONG = ((long) Integer.MAX_VALUE) + 1;
    private static final long SMALL_NEGATIVE_LONG = -((long) Integer.MIN_VALUE) - 1;
    private static final long BIG_POSSITIVE_LONG = Long.MAX_VALUE;
    private static final long BIG_NEGATIVE_LONG = Long.MIN_VALUE;
    private static GraphDatabaseService graphdb;
    private static GraphDatabaseService graphDbReadOnly;
    private Transaction tx;

    @BeforeClass
    public static void createDatabase()
    {
        graphdb = new TestGraphDatabaseFactory().newEmbeddedDatabase( getRandomStoreDir() );
        File storeDir = getRandomStoreDir();
        new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();
        graphDbReadOnly = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).
            setConfig( GraphDatabaseSettings.read_only, TRUE ).
            newGraphDatabase();
    }

    private static File getRandomStoreDir()
    {
        return new File( "target/var/id_test/" + UUID.randomUUID() );
    }

    @AfterClass
    public static void destroyDatabase()
    {
        graphDbReadOnly.shutdown();
        graphDbReadOnly = null;
        graphdb.shutdown();
        graphdb = null;
    }

    @Before
    public void startTransaction()
    {
        tx = graphdb.beginTx();
    }

    @After
    public void endTransaction()
    {
        tx.close();
        tx = null;
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallPossitiveInteger()
    {
        getNodeById( SMALL_POSSITIVE_INTEGER );
        getNodeByIdReadOnly( SMALL_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallNegativeInteger()
    {
        getNodeById( SMALL_NEGATIVE_INTEGER );
        getNodeByIdReadOnly( SMALL_NEGATIVE_INTEGER );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigPossitiveInteger()
    {
        getNodeById( BIG_POSSITIVE_INTEGER );
        getNodeByIdReadOnly( BIG_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigNegativeInteger()
    {
        getNodeById( BIG_NEGATIVE_INTEGER );
        getNodeByIdReadOnly( BIG_NEGATIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallPossitiveLong()
    {
        getNodeById( SMALL_POSSITIVE_LONG );
        getNodeByIdReadOnly( SMALL_POSSITIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getNodeBySmallNegativeLong()
    {
        getNodeById( SMALL_NEGATIVE_LONG );
        getNodeByIdReadOnly( SMALL_NEGATIVE_LONG );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigPossitiveLong()
    {
        getNodeById( BIG_POSSITIVE_LONG );
        getNodeByIdReadOnly( BIG_POSSITIVE_LONG );
    }

    /* finds the node with id=0, since that what the id truncates to */
    @Test( expected = NotFoundException.class )
    public void getNodeByBigNegativeLong()
    {
        getNodeById( BIG_NEGATIVE_LONG );
        getNodeByIdReadOnly( BIG_NEGATIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallPossitiveInteger()
    {
        getRelationshipById( SMALL_POSSITIVE_INTEGER );
        getRelationshipByIdReadOnly( SMALL_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallNegativeInteger()
    {
        getRelationshipById( SMALL_NEGATIVE_INTEGER );
        getRelationshipByIdReadOnly( SMALL_POSSITIVE_INTEGER );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigPossitiveInteger()
    {
        getRelationshipById( BIG_POSSITIVE_INTEGER );
        getRelationshipByIdReadOnly( BIG_POSSITIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigNegativeInteger()
    {
        getRelationshipById( BIG_NEGATIVE_INTEGER );
        getRelationshipByIdReadOnly( BIG_NEGATIVE_INTEGER );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallPossitiveLong()
    {
        getRelationshipById( SMALL_POSSITIVE_LONG );
        getRelationshipByIdReadOnly( SMALL_POSSITIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipBySmallNegativeLong()
    {
        getRelationshipById( SMALL_NEGATIVE_LONG );
        getRelationshipByIdReadOnly( SMALL_NEGATIVE_LONG );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigPossitiveLong()
    {
        getRelationshipById( BIG_POSSITIVE_LONG );
        getRelationshipByIdReadOnly( BIG_POSSITIVE_LONG );
    }

    /* behaves as expected */
    @Test( expected = NotFoundException.class )
    public void getRelationshipByBigNegativeLong()
    {
        getRelationshipById( BIG_NEGATIVE_LONG );
        getRelationshipByIdReadOnly( BIG_NEGATIVE_LONG );
    }

    private void getNodeById( long index )
    {
        Node value = graphdb.getNodeById( index );
        fail( String.format( "Returned Node [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }

    private void getNodeByIdReadOnly( long index )
    {
        Node value = graphDbReadOnly.getNodeById( index );
        fail( String.format( "Returned Node [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }

    private void getRelationshipById( long index )
    {
        Relationship value = graphdb.getRelationshipById( index );
        fail( String.format( "Returned Relationship [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }

    private void getRelationshipByIdReadOnly( long index )
    {
        Relationship value = graphDbReadOnly.getRelationshipById( index );
        fail( String.format( "Returned Relationship [0x%x] for index 0x%x (int value: 0x%x)",
                value.getId(), index, (int) index ) );
    }
}
