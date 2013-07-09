/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

/**
 * Ensures that arrays are returned as primitives, even if boxed values were passed. We need to check each
 * primitive type, for nodes and relationships, for inlined and non inlined arrays, whether the were set
 * anew or replacing an existing one.
 *
 * This test tests what {@link org.neo4j.kernel.api.properties.PropertyConversionTest} but is specific to arrays and
 * tests at the API level instead of the PropertyConversion unit level.
 */
public class TestArrayProperties extends AbstractNeo4jTestCase
{
    @Test
    public void testConsistentReturnTypeForBooleanNonInline()
    {
        Boolean[] boxed = new Boolean[100];
        boolean[] primitive = new boolean[100];
        for ( int i = 0; i < boxed.length; i++ )
        {
            boxed[i] = i%2 == 0;
            primitive[i] = i % 2 == 0;
        }

        testConsistentReturnTypeForBoolean( boxed, primitive );
    }

    @Test
    public void testConsistentReturnTypeForBooleanInline()
    {
        testConsistentReturnTypeForBoolean( new Boolean[]{true, false}, new boolean[]{true, false} );
    }

    @Test
    public void testConsistentReturnTypeForByteNonInline()
    {
        Byte[] boxed = new Byte[100];
        byte[] primitive = new byte[100];
        for ( byte i = 0; i < boxed.length; i++ )
        {
            boxed[i] = i;
            primitive[i] = i;
        }

        testConsistentReturnTypeForByte( boxed, primitive );
    }

    @Test
    public void testConsistentReturnTypeForByteInline()
    {
        testConsistentReturnTypeForByte( new Byte[]{1}, new byte[]{1} );
    }

    @Test
    public void testConsistentReturnTypeForCharNonInline()
    {
        Character[] boxed = new Character[100];
        char[] primitive = new char[100];
        for ( char c = 'a', i = 0; i < boxed.length; i++, c++ )
        {
            boxed[i] = c;
            primitive[i] = c;
        }

        testConsistentReturnTypeForCharacter( boxed, primitive );
    }

    @Test
    public void testConsistentReturnTypeForCharInline()
    {
        testConsistentReturnTypeForCharacter( new Character[]{'a'}, new char[]{'a'} );
    }

    @Test
    public void testConsistentReturnTypeForShortNonInline()
    {
        Short[] boxed = new Short[100];
        short[] primitive = new short[100];
        for ( short i = 0; i < boxed.length; i++ )
        {
            boxed[i] = i;
            primitive[i] = i;
        }

        testConsistentReturnTypeForShort( boxed, primitive );
    }

    @Test
    public void testConsistentReturnTypeForShortInline()
    {
        testConsistentReturnTypeForShort( new Short[]{1}, new short[]{1} );
    }

    @Test
    public void testConsistentReturnTypeForIntegerNonInline()
    {
        testConsistentReturnTypeForInteger( new Integer[]{-1, -2, -3, -4, -5, -6, -7, -8, -9, -10},
                new int[]{-10, -11, -12, -13, -14, -15, -16, -17, -18, -19, -20} );
    }

    @Test
    public void testConsistentReturnTypeForIntegerInline()
    {
        testConsistentReturnTypeForInteger( new Integer[]{1}, new int[]{2} );
    }

    @Test
    public void testConsistentReturnTypeForLongNonInline()
    {
        testConsistentReturnTypeForLong( new Long[]{-1L, -2L, -3L, -4L, -5L}, new long[]{-10L, -11L, -12L, -13L,
                -14L} );
    }

    @Test
    public void testConsistentReturnTypeForLongInline()
    {
        testConsistentReturnTypeForLong( new Long[]{1L}, new long[]{2L} );
    }

    @Test
    public void testConsistentReturnTypeForFloatNonInline()
    {
        testConsistentReturnTypeForFloat( new Float[]{-1F, -2F, -3F, -4F, -5F, -6F, -7F, -8F, -9F, -10F},
                new float[]{-10F, -11F, -12F, -13F, -14F, -15F, -16F, -17F, -18F, -19F, -20F} );
    }

    @Test
    public void testConsistentReturnTypeForFloatInline()
    {
        testConsistentReturnTypeForFloat( new Float[]{1F}, new float[]{2F} );
    }

    @Test
    public void testConsistentReturnTypeForDoubleNonInline()
    {
        testConsistentReturnTypeForDouble( new Double[]{-1D, -2D, -3D, -4D, -5D},
                new double[]{-10D, -11D, -12D, -13D, -14D} );
    }

    @Test
    public void testConsistentReturnTypeForDoubleInline()
    {
        testConsistentReturnTypeForDouble( new Double[]{1D}, new double[]{2D} );
    }

    @Test
    public void testNullValueInArrayThrowsException()
    {
        Node theNode = getGraphDb().createNode();
        try
        {
            theNode.setProperty( "willNeverBeSet", new String[] {"null", null, "not null"} );
            fail("Should have thrown NullPointerException if property value array member is null");
        }
        catch ( IllegalArgumentException e )
        {
            // pretty
        }
    }

    //======
    // Private methods that execute the actual tests follow
    //======

    private static final String THE_ARRAY_BOXED_PROP_NAME = "theArrayBoxed";
    private static final String THE_ARRAY_PRIMITIVE_PROP_NAME = "theArrayPrimitive";

    private void testConsistentReturnTypeForBoolean( Boolean[] theArrayBoxed, boolean[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Boolean[] theArrayBoxed, boolean[] theArrayPrimitive )
    {
        boolean[] retrievedTheArrayBoxed = (boolean[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        boolean[] retrievedTheArrayPrimitive = (boolean[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].booleanValue(), retrievedTheArrayBoxed[i] );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i] );
        }
    }

    private void testConsistentReturnTypeForByte( Byte[] theArrayBoxed, byte[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Byte[] theArrayBoxed, byte[] theArrayPrimitive )
    {
        byte[] retrievedTheArrayBoxed = (byte[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        byte[] retrievedTheArrayPrimitive = (byte[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].byteValue(), retrievedTheArrayBoxed[i] );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i] );
        }
    }

    private void testConsistentReturnTypeForCharacter( Character[] theArrayBoxed, char[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Character[] theArrayBoxed, char[] theArrayPrimitive )
    {
        char[] retrievedTheArrayBoxed = (char[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        char[] retrievedTheArrayPrimitive = (char[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].charValue(), retrievedTheArrayBoxed[i] );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i] );
        }
    }

    private void testConsistentReturnTypeForShort( Short[] theArrayBoxed, short[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Short[] theArrayBoxed, short[] theArrayPrimitive )
    {
        short[] retrievedTheArrayBoxed = (short[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        short[] retrievedTheArrayPrimitive = (short[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].intValue(), retrievedTheArrayBoxed[i] );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i] );
        }
    }

    private void testConsistentReturnTypeForInteger( Integer[] theArrayBoxed, int[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Integer[] theArrayBoxed, int[] theArrayPrimitive )
    {
        int[] retrievedTheArrayBoxed = (int[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        int[] retrievedTheArrayPrimitive = (int[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].intValue(), retrievedTheArrayBoxed[i] );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i] );
        }
    }

    private void testConsistentReturnTypeForLong( Long[] theArrayBoxed, long[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Long[] theArrayBoxed, long[] theArrayPrimitive )
    {
        long[] retrievedTheArrayBoxed = (long[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        long[] retrievedTheArrayPrimitive = (long[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].longValue(), retrievedTheArrayBoxed[i] );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i] );
        }
    }

    private void testConsistentReturnTypeForFloat( Float[] theArrayBoxed, float[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Float[] theArrayBoxed, float[] theArrayPrimitive )
    {
        float[] retrievedTheArrayBoxed = (float[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        float[] retrievedTheArrayPrimitive = (float[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].floatValue(), retrievedTheArrayBoxed[i], 0.0 );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i], 0.0 );
        }
    }

    private void testConsistentReturnTypeForDouble( Double[] theArrayBoxed, double[] theArrayPrimitive )
    {
        Node theNode = getGraphDb().createNode();
        Node theOtherNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label" ) );
        Relationship theOtherRelationship = theNode.createRelationshipTo( theOtherNode, DynamicRelationshipType.withName( "Label2" ) );

        setupEntities( theNode, theOtherNode, theRelationship, theOtherRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theRelationship, theArrayBoxed, theArrayPrimitive );

        checkProperties( theOtherNode, theArrayBoxed, theArrayPrimitive );
        checkProperties( theOtherRelationship, theArrayBoxed, theArrayPrimitive );
    }

    private void checkProperties( PropertyContainer theThing, Double[] theArrayBoxed, double[] theArrayPrimitive )
    {
        double[] retrievedTheArrayBoxed = (double[]) theThing.getProperty( THE_ARRAY_BOXED_PROP_NAME );
        double[] retrievedTheArrayPrimitive = (double[]) theThing.getProperty( THE_ARRAY_PRIMITIVE_PROP_NAME );
        for ( int i = 0; i < theArrayBoxed.length; i++ )
        {
            assertEquals( "boxed index " + i + " did not match", theArrayBoxed[i].doubleValue(), retrievedTheArrayBoxed[i], 0.0 );
            assertEquals( "primitive index " + i + " did not match", theArrayPrimitive[i], retrievedTheArrayPrimitive[i], 0.0 );
        }
    }

    private void testArrayValueIsCopied( long[] theArray )
    {
        long original0 = theArray[0];
        Node theNode = getGraphDb().createNode();
        Relationship theRelationship = theNode.createRelationshipTo( getGraphDb().createNode(), DynamicRelationshipType.withName( "Label" ) );
        theNode.setProperty( "theArray", theArray );
        theRelationship.setProperty( "theArray", theArray );
        theArray[0] = original0 + 1;
        assertFalse( theArray == theNode.getProperty( "theArray" ) );
        assertEquals( original0, ((long[]) theNode.getProperty( "theArray" ))[0] );
        assertFalse( theArray == theRelationship.getProperty( "theArray" ) );
        assertEquals( original0, ((long[]) theRelationship.getProperty( "theArray" ))[0] );

        commit();
        clearCache();
        assertFalse( theArray == theNode.getProperty( "theArray" ) );
        assertEquals( original0, ((long[]) theNode.getProperty( "theArray" ))[0] );
        assertFalse( theArray == theRelationship.getProperty( "theArray" ) );
        assertEquals( original0, ((long[]) theRelationship.getProperty( "theArray" ))[0] );
    }

    private void setupEntities(Node theNode, Node theOtherNode, Relationship theRelationship,
                               Relationship theOtherRelationship, Object theArrayBoxed, Object theArrayPrimitive )
    {
        theOtherNode.setProperty( THE_ARRAY_BOXED_PROP_NAME, "temp" );
        theOtherNode.setProperty( THE_ARRAY_PRIMITIVE_PROP_NAME, "temp" );

        theOtherRelationship.setProperty( THE_ARRAY_BOXED_PROP_NAME, "temp" );
        theOtherRelationship.setProperty( THE_ARRAY_PRIMITIVE_PROP_NAME, "temp" );

        commit();
        newTransaction();

        theNode.setProperty( THE_ARRAY_BOXED_PROP_NAME, theArrayBoxed );
        theNode.setProperty( THE_ARRAY_PRIMITIVE_PROP_NAME, theArrayPrimitive );
        theRelationship.setProperty( THE_ARRAY_BOXED_PROP_NAME, theArrayBoxed );
        theRelationship.setProperty( THE_ARRAY_PRIMITIVE_PROP_NAME, theArrayPrimitive );

        theOtherNode.setProperty( THE_ARRAY_BOXED_PROP_NAME, theArrayBoxed );
        theOtherNode.setProperty( THE_ARRAY_PRIMITIVE_PROP_NAME, theArrayPrimitive );
        theOtherRelationship.setProperty( THE_ARRAY_BOXED_PROP_NAME, theArrayBoxed );
        theOtherRelationship.setProperty( THE_ARRAY_PRIMITIVE_PROP_NAME, theArrayPrimitive );
    }
}