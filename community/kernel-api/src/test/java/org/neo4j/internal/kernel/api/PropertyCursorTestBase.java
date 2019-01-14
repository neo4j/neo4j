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
package org.neo4j.internal.kernel.api;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class PropertyCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    @SuppressWarnings( "SpellCheckingInspection" )
    private static final String LONG_STRING = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque "
            + "eget nibh cursus, efficitur risus non, ultrices justo. Nulla laoreet eros mi, non molestie magna "
            + "luctus in. Fusce nibh neque, tristique ultrices laoreet et, aliquet non dolor. Donec ultrices nisi "
            + "eget urna luctus volutpat. Vivamus hendrerit eget justo vel scelerisque. Morbi interdum volutpat diam,"
            + " et cursus arcu efficitur consectetur. Cras vitae facilisis ipsum, vitae ullamcorper orci. Nullam "
            + "tristique ante sed nibh consequat posuere. Curabitur mauris nisl, condimentum ac varius vel, imperdiet"
            + " a neque. Sed euismod condimentum nisl, vel efficitur turpis tempus id.\n"
            + "\n"
            + "Sed in tempor arcu. Suspendisse molestie rutrum risus a dignissim. Donec et orci non diam tincidunt "
            + "sollicitudin non id nisi. Aliquam vehicula imperdiet viverra. Cras et lacinia eros. Etiam imperdiet ac"
            + " dolor ut tristique. Phasellus ut lacinia ex. Pellentesque habitant morbi tristique senectus et netus "
            + "et malesuada fames ac turpis egestas. Integer libero justo, tincidunt ut felis non, interdum "
            + "consectetur mauris. Cras eu felis ante. Sed dapibus nulla urna, at elementum tortor ultricies pretium."
            + " Maecenas sed augue non urna consectetur fringilla vitae eu libero. Vivamus interdum bibendum risus, "
            + "quis luctus eros.\n"
            + "\n"
            + "Sed neque augue, fermentum sit amet iaculis ut, porttitor ac odio. Phasellus et sapien non sapien "
            + "consequat fermentum accumsan non dolor. Integer eget pellentesque lectus, vitae lobortis ante. Nam "
            + "elementum, dui ut finibus rutrum, purus mauris efficitur purus, efficitur tempus ante metus bibendum "
            + "velit. Curabitur commodo, risus et eleifend facilisis, eros augue posuere tortor, eu dictum erat "
            + "tortor consectetur orci. Fusce a velit dignissim, tempus libero nec, faucibus risus. Nullam pharetra "
            + "mauris sit amet volutpat facilisis. Pellentesque habitant morbi tristique senectus et netus et "
            + "malesuada fames ac turpis egestas. Praesent lacinia non felis ut lobortis.\n"
            + "\n"
            + "Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Sed eu nisi dui"
            + ". Suspendisse imperdiet lorem vel eleifend faucibus. Mauris non venenatis metus. Aenean neque magna, "
            + "rhoncus vel velit in, dictum convallis leo. Phasellus pulvinar eu sapien ac vehicula. Praesent "
            + "placerat augue quam, egestas vehicula velit porttitor in. Vivamus velit metus, pellentesque quis "
            + "fermentum et, porta quis velit. Curabitur sed lacus quis nibh convallis tincidunt.\n"
            + "\n"
            + "Etiam eu elit eget dolor dignissim lacinia. Vivamus tortor ex, dapibus id elementum non, suscipit ac "
            + "nisl. Aenean vel tempor libero, eu venenatis elit. Nunc nec velit eu odio interdum pellentesque sed et"
            + " eros. Nam quis mi in metus tristique aliquam. Nullam facilisis dapibus lacus, nec lacinia velit. "
            + "Proin massa enim, accumsan ac libero at, iaculis sodales tellus. Vivamus fringilla justo sed luctus "
            + "tincidunt. Sed placerat fringilla ex, vel placerat sem faucibus eget. Vestibulum semper dui sit amet "
            + "efficitur blandit. Donec eu tellus velit. Etiam a mi nec massa euismod posuere. Cras eget lacus leo.";

    private static long bare, byteProp, shortProp, intProp, inlineLongProp, longProp,
            floatProp, doubleProp, trueProp, falseProp, charProp, emptyStringProp, shortStringProp, longStringProp,
            utf8Prop, smallArray, bigArray, pointProp, dateProp, allProps;

    private static String chinese = "造Unicode之";
    private static Value pointValue = Values.pointValue( CoordinateReferenceSystem.Cartesian, 10, 20 );
    private static Value dateValue = Values.temporalValue( LocalDate.of( 2018, 7, 26 ) );

    protected boolean supportsBigProperties()
    {
        return true;
    }

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            bare = graphDb.createNode().getId();

            byteProp = createNodeWithProperty( graphDb, "byteProp", (byte) 13 );
            shortProp = createNodeWithProperty( graphDb, "shortProp", (short) 13 );
            intProp = createNodeWithProperty( graphDb, "intProp", 13 );
            inlineLongProp = createNodeWithProperty( graphDb, "inlineLongProp", 13L );
            longProp = createNodeWithProperty( graphDb, "longProp", Long.MAX_VALUE );

            floatProp = createNodeWithProperty( graphDb, "floatProp", 13.0f );
            doubleProp = createNodeWithProperty( graphDb, "doubleProp", 13.0 );

            trueProp = createNodeWithProperty( graphDb, "trueProp", true );
            falseProp = createNodeWithProperty( graphDb, "falseProp", false );

            charProp = createNodeWithProperty( graphDb, "charProp", 'x' );
            emptyStringProp = createNodeWithProperty( graphDb, "emptyStringProp", "" );
            shortStringProp = createNodeWithProperty( graphDb, "shortStringProp", "hello" );
            longStringProp = createNodeWithProperty( graphDb, "longStringProp", LONG_STRING );
            utf8Prop = createNodeWithProperty( graphDb, "utf8Prop", chinese );

            smallArray = createNodeWithProperty( graphDb, "smallArray", new int[]{1, 2, 3, 4} );
            bigArray = createNodeWithProperty( graphDb, "bigArray", new String[]{LONG_STRING} );

            pointProp = createNodeWithProperty( graphDb, "pointProp", pointValue );
            dateProp = createNodeWithProperty( graphDb, "dateProp", dateValue );

            Node all = graphDb.createNode();
            // first property record
            all.setProperty( "byteProp", (byte) 13 );
            all.setProperty( "shortProp", (short) 13 );
            all.setProperty( "intProp", 13 );
            all.setProperty( "inlineLongProp", 13L );
            // second property record
            all.setProperty( "longProp", Long.MAX_VALUE );
            all.setProperty( "floatProp", 13.0f );
            all.setProperty( "doubleProp", 13.0 );
            //                  ^^^
            // third property record halfway through double?
            all.setProperty( "trueProp", true );
            all.setProperty( "falseProp", false );

            all.setProperty( "charProp", 'x' );
            all.setProperty( "emptyStringProp", "" );
            all.setProperty( "shortStringProp", "hello" );
            if ( supportsBigProperties() )
            {
                all.setProperty( "longStringProp", LONG_STRING );
            }
            all.setProperty( "utf8Prop", chinese );

            if ( supportsBigProperties() )
            {
                all.setProperty( "smallArray", new int[]{1, 2, 3, 4} );
                all.setProperty( "bigArray", new String[]{LONG_STRING} );
            }

            all.setProperty( "pointProp", pointValue );
            all.setProperty( "dateProp", dateProp );

            allProps = all.getId();

            tx.success();
        }
    }

    private long createNodeWithProperty( GraphDatabaseService graphDb, String propertyKey, Object value )
    {
        Node p = graphDb.createNode();
        p.setProperty( propertyKey, value );
        return p.getId();
    }

    @Test
    public void shouldNotAccessNonExistentProperties()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor(); PropertyCursor props = cursors.allocatePropertyCursor() )
        {
            // when
            read.singleNode( bare, node );
            assertTrue( "node by reference", node.next() );
            assertFalse( "no properties", node.hasProperties() );

            node.properties( props );
            assertFalse( "no properties by direct method", props.next() );

            read.nodeProperties( node.nodeReference(), node.propertiesReference(), props );
            assertFalse( "no properties via property ref", props.next() );

            assertFalse( "only one node", node.next() );
        }
    }

    @Test
    public void shouldAccessSingleProperty()
    {
        assertAccessSingleProperty( byteProp, Values.of( (byte) 13 ), ValueGroup.NUMBER );
        assertAccessSingleProperty( shortProp, Values.of( (short) 13 ), ValueGroup.NUMBER );
        assertAccessSingleProperty( intProp, Values.of( 13 ), ValueGroup.NUMBER );
        assertAccessSingleProperty( inlineLongProp, Values.of( 13L ), ValueGroup.NUMBER );
        assertAccessSingleProperty( longProp, Values.of( Long.MAX_VALUE ), ValueGroup.NUMBER );
        assertAccessSingleProperty( floatProp, Values.of( 13.0f ), ValueGroup.NUMBER );
        assertAccessSingleProperty( doubleProp, Values.of( 13.0 ), ValueGroup.NUMBER );
        assertAccessSingleProperty( trueProp, Values.of( true ), ValueGroup.BOOLEAN );
        assertAccessSingleProperty( falseProp, Values.of( false ), ValueGroup.BOOLEAN );
        assertAccessSingleProperty( charProp, Values.of( 'x' ), ValueGroup.TEXT );
        assertAccessSingleProperty( emptyStringProp, Values.of( "" ), ValueGroup.TEXT );
        assertAccessSingleProperty( shortStringProp, Values.of( "hello" ), ValueGroup.TEXT );
        if ( supportsBigProperties() )
        {
            assertAccessSingleProperty( longStringProp, Values.of( LONG_STRING ), ValueGroup.TEXT );
        }
        assertAccessSingleProperty( utf8Prop, Values.of( chinese ), ValueGroup.TEXT );
        if ( supportsBigProperties() )
        {
            assertAccessSingleProperty( smallArray, Values.of( new int[]{1, 2, 3, 4} ), ValueGroup.NUMBER_ARRAY );
            assertAccessSingleProperty( bigArray, Values.of( new String[]{LONG_STRING} ), ValueGroup.TEXT_ARRAY );
        }
        assertAccessSingleProperty( pointProp, Values.of( pointValue ), ValueGroup.GEOMETRY );
        assertAccessSingleProperty( dateProp, Values.of( dateValue ), ValueGroup.DATE );
    }

    @Test
    public void shouldAccessAllNodeProperties()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor(); PropertyCursor props = cursors.allocatePropertyCursor() )
        {
            // when
            read.singleNode( allProps, node );
            assertTrue( "node by reference", node.next() );
            assertTrue( "has properties", node.hasProperties() );

            node.properties( props );
            Set<Object> values = new HashSet<>();
            while ( props.next() )
            {
                values.add( props.propertyValue().asObject() );
            }

            assertTrue( "byteProp", values.contains( (byte) 13 ) );
            assertTrue( "shortProp", values.contains( (short) 13 ) );
            assertTrue( "intProp", values.contains( 13 ) );
            assertTrue( "inlineLongProp", values.contains( 13L ) );
            assertTrue( "longProp", values.contains( Long.MAX_VALUE ) );
            assertTrue( "floatProp", values.contains( 13.0f ) );
            assertTrue( "doubleProp", values.contains( 13.0 ) );
            assertTrue( "trueProp", values.contains( true ) );
            assertTrue( "falseProp", values.contains( false ) );
            assertTrue( "charProp", values.contains( 'x' ) );
            assertTrue( "emptyStringProp", values.contains( "" ) );
            assertTrue( "shortStringProp", values.contains( "hello" ) );
            assertTrue( "utf8Prop", values.contains( chinese ) );
            if ( supportsBigProperties() )
            {
                assertTrue( "longStringProp", values.contains( LONG_STRING ) );
                assertThat( "smallArray", values, hasItem( intArray( 1, 2, 3, 4 ) ) );
                assertThat( "bigArray", values, hasItem( arrayContaining( LONG_STRING ) ) );
            }
            assertTrue( "pointProp", values.contains( pointValue ) );

            int expected = supportsBigProperties() ? 18 : 15;
            assertEquals( "number of values", expected, values.size() );
        }
    }

    private void assertAccessSingleProperty( long nodeId, Object expectedValue, ValueGroup expectedValueType )
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor(); PropertyCursor props = cursors.allocatePropertyCursor() )
        {
            // when
            read.singleNode( nodeId, node );
            assertTrue( "node by reference", node.next() );
            assertTrue( "has properties", node.hasProperties() );

            node.properties( props );
            assertTrue( "has properties by direct method", props.next() );
            assertEquals( "correct value", expectedValue, props.propertyValue() );
            assertEquals( "correct value type ", expectedValueType, props.propertyType() );
            assertFalse( "single property", props.next() );

            read.nodeProperties( node.nodeReference(), node.propertiesReference(), props );
            assertTrue( "has properties via property ref", props.next() );
            assertEquals( "correct value", expectedValue, props.propertyValue() );
            assertFalse( "single property", props.next() );
        }
    }

    private static TypeSafeMatcher<int[]> intArray( int... content )
    {
        return new TypeSafeMatcher<int[]>()
        {
            @Override
            protected boolean matchesSafely( int[] item )
            {
                if ( item.length != content.length )
                {
                    return false;
                }
                for ( int i = 0; i < content.length; i++ )
                {
                    if ( item[i] != content[i] )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( content );
            }
        };
    }
}
