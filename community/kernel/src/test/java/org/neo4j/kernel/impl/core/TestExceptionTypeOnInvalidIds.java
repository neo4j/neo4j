/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.read_only;
import static org.neo4j.kernel.configuration.Settings.TRUE;

class TestExceptionTypeOnInvalidIds
{
    private static final long SMALL_POSSITIVE_INTEGER = 5;
    private static final long SMALL_NEGATIVE_INTEGER = -5;
    private static final long BIG_POSSITIVE_INTEGER = MAX_VALUE;
    private static final long BIG_NEGATIVE_INTEGER = MIN_VALUE;
    private static final long SMALL_POSSITIVE_LONG = ((long) MAX_VALUE) + 1;
    private static final long SMALL_NEGATIVE_LONG = -((long) MIN_VALUE) - 1;
    private static final long BIG_POSSITIVE_LONG = Long.MAX_VALUE;
    private static final long BIG_NEGATIVE_LONG = Long.MIN_VALUE;
    private static GraphDatabaseService graphdb;
    private static GraphDatabaseService graphDbReadOnly;
    private Transaction tx;

    @BeforeAll
    static void createDatabase()
    {
        graphdb = new TestGraphDatabaseFactory().newEmbeddedDatabase( getRandomStoreDir() );
        File storeDir = getRandomStoreDir();
        new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();
        graphDbReadOnly = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).
                setConfig( read_only, TRUE ).
                newGraphDatabase();
    }

    private static File getRandomStoreDir()
    {
        return new File( "target/var/id_test/" + randomUUID() );
    }

    @AfterAll
    static void destroyDatabase()
    {
        graphDbReadOnly.shutdown();
        graphDbReadOnly = null;
        graphdb.shutdown();
        graphdb = null;
    }

    @BeforeEach
    void startTransaction()
    {
        tx = graphdb.beginTx();
    }

    @AfterEach
    void endTransaction()
    {
        tx.close();
        tx = null;
    }

    /* behaves as expected */
    @Test
    void getNodeBySmallPossitiveInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( SMALL_POSSITIVE_INTEGER );
            getNodeByIdReadOnly( SMALL_POSSITIVE_INTEGER );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getNodeBySmallNegativeInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( SMALL_NEGATIVE_INTEGER );
            getNodeByIdReadOnly( SMALL_NEGATIVE_INTEGER );
        } );
    }

    /* behaves as expected */
    @Test
    void getNodeByBigPossitiveInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( BIG_POSSITIVE_INTEGER );
            getNodeByIdReadOnly( BIG_POSSITIVE_INTEGER );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getNodeByBigNegativeInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( BIG_NEGATIVE_INTEGER );
            getNodeByIdReadOnly( BIG_NEGATIVE_INTEGER );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getNodeBySmallPossitiveLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( SMALL_POSSITIVE_LONG );
            getNodeByIdReadOnly( SMALL_POSSITIVE_LONG );
        } );
    }

    /* behaves as expected */
    @Test
    void getNodeBySmallNegativeLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( SMALL_NEGATIVE_LONG );
            getNodeByIdReadOnly( SMALL_NEGATIVE_LONG );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getNodeByBigPossitiveLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( BIG_POSSITIVE_LONG );
            getNodeByIdReadOnly( BIG_POSSITIVE_LONG );
        } );
    }

    /* finds the node with id=0, since that what the id truncates to */
    @Test
    void getNodeByBigNegativeLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getNodeById( BIG_NEGATIVE_LONG );
            getNodeByIdReadOnly( BIG_NEGATIVE_LONG );
        } );
    }

    /* behaves as expected */
    @Test
    void getRelationshipBySmallPossitiveInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( SMALL_POSSITIVE_INTEGER );
            getRelationshipByIdReadOnly( SMALL_POSSITIVE_INTEGER );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getRelationshipBySmallNegativeInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( SMALL_NEGATIVE_INTEGER );
            getRelationshipByIdReadOnly( SMALL_POSSITIVE_INTEGER );
        } );
    }

    /* behaves as expected */
    @Test
    void getRelationshipByBigPossitiveInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( BIG_POSSITIVE_INTEGER );
            getRelationshipByIdReadOnly( BIG_POSSITIVE_INTEGER );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getRelationshipByBigNegativeInteger()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( BIG_NEGATIVE_INTEGER );
            getRelationshipByIdReadOnly( BIG_NEGATIVE_INTEGER );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getRelationshipBySmallPossitiveLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( SMALL_POSSITIVE_LONG );
            getRelationshipByIdReadOnly( SMALL_POSSITIVE_LONG );
        } );
    }

    /* behaves as expected */
    @Test
    void getRelationshipBySmallNegativeLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( SMALL_NEGATIVE_LONG );
            getRelationshipByIdReadOnly( SMALL_NEGATIVE_LONG );
        } );
    }

    /* throws IllegalArgumentException instead of NotFoundException */
    @Test
    void getRelationshipByBigPossitiveLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( BIG_POSSITIVE_LONG );
            getRelationshipByIdReadOnly( BIG_POSSITIVE_LONG );
        } );
    }

    /* behaves as expected */
    @Test
    void getRelationshipByBigNegativeLong()
    {
        assertThrows( NotFoundException.class, () -> {
            getRelationshipById( BIG_NEGATIVE_LONG );
            getRelationshipByIdReadOnly( BIG_NEGATIVE_LONG );
        } );
    }

    private void getNodeById( long index )
    {
        Node value = graphdb.getNodeById( index );
        fail( format( "Returned Node [0x%x] for index 0x%x (int value: 0x%x)", value.getId(), index, (int) index ) );
    }

    private void getNodeByIdReadOnly( long index )
    {
        Node value = graphDbReadOnly.getNodeById( index );
        fail( format( "Returned Node [0x%x] for index 0x%x (int value: 0x%x)", value.getId(), index, (int) index ) );
    }

    private void getRelationshipById( long index )
    {
        Relationship value = graphdb.getRelationshipById( index );
        fail( format( "Returned Relationship [0x%x] for index 0x%x (int value: 0x%x)", value.getId(), index,
                (int) index ) );
    }

    private void getRelationshipByIdReadOnly( long index )
    {
        Relationship value = graphDbReadOnly.getRelationshipById( index );
        fail( format( "Returned Relationship [0x%x] for index 0x%x (int value: 0x%x)", value.getId(), index,
                (int) index ) );
    }
}
