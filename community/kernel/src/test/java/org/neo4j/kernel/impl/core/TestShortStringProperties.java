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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.GraphTransactionRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.hasProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;

public class TestShortStringProperties
{
    @ClassRule
    public static DatabaseRule graphdb = new ImpermanentDatabaseRule();

    @Rule
    public GraphTransactionRule tx = new GraphTransactionRule( graphdb );

    public void commit()
    {
        tx.success();
    }

    private void newTx()
    {
        tx.success();
        tx.begin();
    }

    private static final String LONG_STRING = "this is a really long string, believe me!";

    @Test
    public void canAddMultipleShortStringsToTheSameNode()
    {
        long recordCount = dynamicRecordsInUse();
        Node node = graphdb.getGraphDatabaseAPI().createNode();
        node.setProperty( "key", "value" );
        node.setProperty( "reverse", "esrever" );
        commit();
        assertEquals( recordCount, dynamicRecordsInUse() );
        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( "value" )  ) );
        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "reverse" ).withValue( "esrever" )  ) );
    }

    @Test
    public void canAddShortStringToRelationship()
    {
        long recordCount = dynamicRecordsInUse();
        GraphDatabaseService db = graphdb.getGraphDatabaseAPI();
        Relationship rel = db.createNode().createRelationshipTo( db.createNode(), withName( "REL_TYPE" ) );
        rel.setProperty( "type", "dimsedut" );
        commit();
        assertEquals( recordCount, dynamicRecordsInUse() );
        assertThat( rel, inTx( db, hasProperty( "type" ).withValue( "dimsedut" ) ) );
    }

    @Test
    public void canUpdateShortStringInplace()
    {
        try
        {
            long recordCount = dynamicRecordsInUse();
            long propCount = propertyRecordsInUse();
            Node node = graphdb.getGraphDatabaseAPI().createNode();
            node.setProperty( "key", "value" );

            newTx();

            assertEquals( recordCount, dynamicRecordsInUse() );
            assertEquals( propCount + 1, propertyRecordsInUse() );
            assertEquals( "value", node.getProperty( "key" ) );

            node.setProperty( "key", "other" );
            commit();

            assertEquals( recordCount, dynamicRecordsInUse() );
            assertEquals( propCount + 1, propertyRecordsInUse() );
            assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( "other" )  ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void canReplaceLongStringWithShortString()
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.getGraphDatabaseAPI().createNode();
        node.setProperty( "key", LONG_STRING );
        newTx();

        assertEquals( recordCount + 1, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( LONG_STRING, node.getProperty( "key" ) );

        node.setProperty( "key", "value" );
        commit();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( "value" )  ) );
    }

    @Test
    public void canReplaceShortStringWithLongString()
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        Node node = graphdb.getGraphDatabaseAPI().createNode();
        node.setProperty( "key", "value" );
        newTx();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );

        node.setProperty( "key", LONG_STRING );
        commit();

        assertEquals( recordCount + 1, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertThat( node, inTx( graphdb.getGraphDatabaseAPI(), hasProperty( "key" ).withValue( LONG_STRING )  ) );
    }

    @Test
    public void canRemoveShortStringProperty()
    {
        long recordCount = dynamicRecordsInUse();
        long propCount = propertyRecordsInUse();
        GraphDatabaseService db = graphdb.getGraphDatabaseAPI();
        Node node = db.createNode();
        node.setProperty( "key", "value" );
        newTx();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount + 1, propertyRecordsInUse() );
        assertEquals( "value", node.getProperty( "key" ) );

        node.removeProperty( "key" );
        commit();

        assertEquals( recordCount, dynamicRecordsInUse() );
        assertEquals( propCount, propertyRecordsInUse() );
        assertThat( node, inTx( db, not( hasProperty( "key" ) ) ) );
    }

    @Test
    public void canEncodeEmptyString()
    {
        assertCanEncode( "" );
    }

    @Test
    public void canEncodeReallyLongString()
    {
        assertCanEncode( "                    " ); // 20 spaces
        assertCanEncode( "                " ); // 16 spaces
    }

    @Test
    public void canEncodeFifteenSpaces()
    {
        assertCanEncode( "               " );
    }

    @Test
    public void canEncodeNumericalString()
    {
        assertCanEncode( "0123456789+,'.-" );
        assertCanEncode( " ,'.-0123456789" );
        assertCanEncode( "+ '.0123456789-" );
        assertCanEncode( "+, 0123456789.-" );
        assertCanEncode( "+,0123456789' -" );
        assertCanEncode( "+0123456789,'. " );
        // IP(v4) numbers
        assertCanEncode( "192.168.0.1" );
        assertCanEncode( "127.0.0.1" );
        assertCanEncode( "255.255.255.255" );
    }

    @Test
    public void canEncodeTooLongStringsWithCharsInDifferentTables()
    {
        assertCanEncode( "____________+" );
        assertCanEncode( "_____+_____" );
        assertCanEncode( "____+____" );
        assertCanEncode( "HELLO world" );
        assertCanEncode( "Hello_World" );
    }

    @Test
    public void canEncodeUpToNineEuropeanChars()
    {
        // Shorter than 10 chars
        assertCanEncode( "fågel" ); // "bird" in Swedish
        assertCanEncode( "påfågel" ); // "peacock" in Swedish
        assertCanEncode( "påfågelö" ); // "peacock island" in Swedish
        assertCanEncode( "påfågelön" ); // "the peacock island" in Swedish
        // 10 chars
        assertCanEncode( "påfågelöar" ); // "peacock islands" in Swedish
    }

    @Test
    public void canEncodeEuropeanCharsWithPunctuation()
    {
        assertCanEncode( "qHm7 pp3" );
        assertCanEncode( "UKKY3t.gk" );
    }

    @Test
    public void canEncodeAlphanumerical()
    {
        assertCanEncode( "1234567890" ); // Just a sanity check
        assertCanEncodeInBothCasings( "HelloWor1d" ); // There is a number there
        assertCanEncode( "          " ); // Alphanum is the first that can encode 10 spaces
        assertCanEncode( "_ _ _ _ _ " ); // The only available punctuation
        assertCanEncode( "H3Lo_ or1D" ); // Mixed case + punctuation
        assertCanEncode( "q1w2e3r4t+" ); // + is not in the charset
    }

    @Test
    public void canEncodeHighUnicode()
    {
        assertCanEncode( "\u02FF" );
        assertCanEncode( "hello\u02FF" );
    }

    @Test
    public void canEncodeLatin1SpecialChars()
    {
        assertCanEncode( "#$#$#$#" );
        assertCanEncode( "$hello#" );
    }

    @Test
    public void canEncodeTooLongLatin1String()
    {
        assertCanEncode( "#$#$#$#$" );
    }

    @Test
    public void canEncodeLowercaseAndUppercaseStringsUpTo12Chars()
    {
        assertCanEncodeInBothCasings( "hello world" );
        assertCanEncode( "hello_world" );
        assertCanEncode( "_hello_world" );
        assertCanEncode( "hello::world" );
        assertCanEncode( "hello//world" );
        assertCanEncode( "hello world" );
        assertCanEncode( "http://ok" );
        assertCanEncode( "::::::::" );
        assertCanEncode( " _.-:/ _.-:/" );
    }

    private void assertCanEncodeInBothCasings( String string )
    {
        assertCanEncode( string.toLowerCase() );
        assertCanEncode( string.toUpperCase() );
    }

    private void assertCanEncode( String string )
    {
        encode( string, true );
    }

    private void encode( String string, boolean isShort )
    {
        long recordCount = dynamicRecordsInUse();
        Node node = graphdb.getGraphDatabaseAPI().createNode();
        node.setProperty( "key", string );
        newTx();
        if ( isShort )
        {
            assertEquals( recordCount, dynamicRecordsInUse() );
        }
        else
        {
            assertTrue( recordCount < dynamicRecordsInUse() );
        }
        assertEquals( string, node.getProperty( "key" ) );
    }

    private long propertyRecordsInUse()
    {
        return AbstractNeo4jTestCase.numberOfRecordsInUse( propertyStore() );
    }

    private long dynamicRecordsInUse()
    {
        return AbstractNeo4jTestCase.numberOfRecordsInUse( propertyStore().getStringStore() );
    }

    private PropertyStore propertyStore()
    {
        return graphdb.getGraphDatabaseAPI().getDependencyResolver().resolveDependency( RecordStorageEngine.class)
                .testAccessNeoStores().getPropertyStore();
    }
}
