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
package org.neo4j.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class Neo4jTestCase
{
    private static GraphDatabaseService graphDb;
    private Transaction tx;

    @BeforeAll
    public static void setUpDb()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterAll
    public static void tearDownDb()
    {
        graphDb.shutdown();
    }

    @BeforeEach
    public void setUpTest()
    {
        tx = graphDb.beginTx();
    }

    @AfterEach
    public void tearDownTest()
    {
        if ( !manageMyOwnTxFinish() )
        {
            finishTx( true );
        }
    }

    protected boolean manageMyOwnTxFinish()
    {
        return false;
    }

    protected void finishTx( boolean commit )
    {
        if ( tx == null )
        {
            return;
        }

        if ( commit )
        {
            tx.success();
        }
        tx.close();
        tx = null;
    }

    protected Transaction beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
        return tx;
    }

    public static void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : Objects.requireNonNull( file.listFiles() ) )
            {
                deleteFileOrDirectory( child );
            }
        }
        assertTrue( file.delete(), "delete " + file );
    }

    protected static GraphDatabaseService graphDb()
    {
        return graphDb;
    }

    public static <T> void assertContains( Collection<T> collection,
        T... expectedItems )
    {
        String collectionString = join( ", ", collection.toArray() );
        assertEquals( expectedItems.length, collection.size(), collectionString );
        for ( T item : expectedItems )
        {
            assertTrue( collection.contains( item ) );
        }
    }

    public static <T> void assertContains( Iterable<T> items, T... expectedItems )
    {
        assertContains( asCollection( items ), expectedItems );
    }

    public static <T> void assertContainsInOrder( Collection<T> collection,
            T... expectedItems )
    {
        String collectionString = join( ", ", collection.toArray() );
        assertEquals( expectedItems.length, collection.size(), collectionString );
        Iterator<T> itr = collection.iterator();
        for ( int i = 0; itr.hasNext(); i++ )
        {
            assertEquals( expectedItems[i], itr.next() );
        }
    }

    public static <T> void assertContainsInOrder( Iterable<T> collection,
            T... expectedItems )
    {
        assertContainsInOrder( asCollection( collection ), expectedItems );
    }

    public static <T> Collection<T> asCollection( Iterable<T> iterable )
    {
        List<T> list = new ArrayList<>();
        for ( T item : iterable )
        {
            list.add( item );
        }
        return list;
    }

    public static <T> String join( String delimiter, T... items )
    {
        StringBuilder buffer = new StringBuilder();
        for ( T item : items )
        {
            if ( buffer.length() > 0 )
            {
                buffer.append( delimiter );
            }
            buffer.append( item.toString() );
        }
        return buffer.toString();
    }
}
