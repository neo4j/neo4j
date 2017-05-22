/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.values;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.neo4j.values.Values.booleanArrayValue;
import static org.neo4j.values.Values.booleanValue;
import static org.neo4j.values.Values.byteArrayValue;
import static org.neo4j.values.Values.byteValue;
import static org.neo4j.values.Values.charArrayValue;
import static org.neo4j.values.Values.charValue;
import static org.neo4j.values.Values.doubleArrayValue;
import static org.neo4j.values.Values.doubleValue;
import static org.neo4j.values.Values.floatArrayValue;
import static org.neo4j.values.Values.floatValue;
import static org.neo4j.values.Values.intArrayValue;
import static org.neo4j.values.Values.intValue;
import static org.neo4j.values.Values.longArrayValue;
import static org.neo4j.values.Values.longValue;
import static org.neo4j.values.Values.shortArrayValue;
import static org.neo4j.values.Values.shortValue;
import static org.neo4j.values.Values.stringArrayValue;
import static org.neo4j.values.Values.stringValue;

public class ValuesTest
{
    @Test
    public void shouldBeEqualToItself()
    {
        assertEqual( booleanValue( false ), booleanValue( false ) );
        assertEqual( byteValue( (byte)0 ), byteValue( (byte)0 ) );
        assertEqual( shortValue( (short)0 ), shortValue( (short)0 ) );
        assertEqual( intValue( 0 ), intValue( 0 ) );
        assertEqual( longValue( 0 ), longValue( 0 ) );
        assertEqual( floatValue( 0.0f ), floatValue( 0.0f ) );
        assertEqual( doubleValue( 0.0 ), doubleValue( 0.0 ) );
        assertEqual( stringValue( "" ), stringValue( "" ) );

        assertEqual( booleanValue( true ), booleanValue( true ) );
        assertEqual( byteValue( (byte)1 ), byteValue( (byte)1 ) );
        assertEqual( shortValue( (short)1 ), shortValue( (short)1 ) );
        assertEqual( intValue( 1 ), intValue( 1 ) );
        assertEqual( longValue( 1 ), longValue( 1 ) );
        assertEqual( floatValue( 1.0f ), floatValue( 1.0f ) );
        assertEqual( doubleValue( 1.0 ), doubleValue( 1.0 ) );
        assertEqual( charValue( 'x' ), charValue( 'x' ) );
        assertEqual( stringValue( "hi" ), stringValue( "hi" ) );

        assertEqual( booleanArrayValue( new boolean[]{} ), booleanArrayValue( new boolean[]{} ) );
        assertEqual( byteArrayValue( new byte[]{} ), byteArrayValue( new byte[]{} ) );
        assertEqual( shortArrayValue( new short[]{} ), shortArrayValue( new short[]{} ) );
        assertEqual( intArrayValue( new int[]{} ), intArrayValue( new int[]{} ) );
        assertEqual( longArrayValue( new long[]{} ), longArrayValue( new long[]{} ) );
        assertEqual( floatArrayValue( new float[]{} ), floatArrayValue( new float[]{} ) );
        assertEqual( doubleArrayValue( new double[]{} ), doubleArrayValue( new double[]{} ) );
        assertEqual( charArrayValue( new char[]{} ), charArrayValue( new char[]{} ) );
        assertEqual( stringArrayValue( new String[]{} ), stringArrayValue( new String[]{} ) );

        assertEqual( booleanArrayValue( new boolean[]{true} ), booleanArrayValue( new boolean[]{true} ) );
        assertEqual( byteArrayValue( new byte[]{1} ), byteArrayValue( new byte[]{1} ) );
        assertEqual( shortArrayValue( new short[]{1} ), shortArrayValue( new short[]{1} ) );
        assertEqual( intArrayValue( new int[]{1} ), intArrayValue( new int[]{1} ) );
        assertEqual( longArrayValue( new long[]{1} ), longArrayValue( new long[]{1} ) );
        assertEqual( floatArrayValue( new float[]{1.0f} ), floatArrayValue( new float[]{1.0f} ) );
        assertEqual( doubleArrayValue( new double[]{1.0} ), doubleArrayValue( new double[]{1.0} ) );
        assertEqual( charArrayValue( new char[]{'x'} ), charArrayValue( new char[]{'x'} ) );
        assertEqual( stringArrayValue( new String[]{"hi"} ), stringArrayValue( new String[]{"hi"} ) );
    }

    @Test
    public void shouldHandleCoercion()
    {

    }

    private void assertEqual( Value a, Value b )
    {
        assertTrue( "should be equal", a.equals( b ) );
        assertTrue( "should be equal", b.equals( a ) );
        assertTrue( "should have same has", a.hashCode() == b.hashCode() );
    }
}
