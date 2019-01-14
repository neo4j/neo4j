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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.hamcrest.Matcher;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

class FusionIndexTestHelp
{
    private static LabelSchemaDescriptor indexKey = SchemaDescriptorFactory.forLabel( 0, 0 );
    private static LabelSchemaDescriptor compositeIndexKey = SchemaDescriptorFactory.forLabel( 0, 0, 1 );

    private static final Value[] stringValues = new Value[]
            {
                    Values.stringValue( "abc" ),
                    Values.stringValue( "abcdefghijklmnopqrstuvwxyzåäö" ),
                    Values.charValue( 'S' ),
            };
    private static final Value[] numberValues = new Value[]
            {
                    Values.byteValue( (byte) 1 ),
                    Values.shortValue( (short) 2 ),
                    Values.intValue( 3 ),
                    Values.longValue( 4 ),
                    Values.floatValue( 5.6f ),
                    Values.doubleValue( 7.8 )
            };
    private static final Value[] pointValues = new Value[]
            {
                    Values.pointValue( CoordinateReferenceSystem.Cartesian, 123.0, 456.0 ),
                    Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 123.0, 456.0, 789.0 ),
                    Values.pointValue( CoordinateReferenceSystem.WGS84, 13.2, 56.8 )
            };
    private static final Value[] temporalValues = new Value[]
            {
                    DateValue.epochDate( 1 ),
                    DateValue.epochDate( 10000 )
            };
    private static final Value[] otherValues = new Value[]
            {
                    Values.booleanValue( true ),
                    Values.booleanArray( new boolean[2] ),
                    Values.byteArray( new byte[]{1, 2} ),
                    Values.shortArray( new short[]{3, 4} ),
                    Values.intArray( new int[]{5, 6} ),
                    Values.longArray( new long[]{7, 8} ),
                    Values.floatArray( new float[]{9.10f, 11.12f} ),
                    Values.doubleArray( new double[]{13.14, 15.16} ),
                    Values.charArray( new char[2] ),
                    Values.stringArray( "a", "b" ),
                    Values.pointArray( pointValues ),
                    Values.NO_VALUE
            };

    static Value[] valuesSupportedByString()
    {
        return stringValues;
    }

    static Value[] valuesSupportedByNumber()
    {
        return numberValues;
    }

    static Value[] valuesSupportedBySpatial()
    {
        return pointValues;
    }

    static Value[] valuesSupportedByTemporal()
    {
        return temporalValues;
    }

    static Value[] valuesNotSupportedBySpecificIndex()
    {
        return otherValues;
    }

    static Value[] allValues()
    {
        List<Value> values = new ArrayList<>();
        for ( Value[] group : valuesByGroup() )
        {
            values.addAll( Arrays.asList( group ) );
        }
        return values.toArray( new Value[values.size()] );
    }

    static Value[][] valuesByGroup()
    {
        return new Value[][]
                {
                        FusionIndexTestHelp.valuesSupportedByString(),
                        FusionIndexTestHelp.valuesSupportedByNumber(),
                        FusionIndexTestHelp.valuesSupportedBySpatial(),
                        FusionIndexTestHelp.valuesSupportedByTemporal(),
                        FusionIndexTestHelp.valuesNotSupportedBySpecificIndex()
                };
    }

    static void verifyCallFail( Exception expectedFailure, Callable failingCall )
    {
        try
        {
            failingCall.call();
            fail( "Should have failed" );
        }
        catch ( Exception e )
        {
            assertSame( expectedFailure, e );
        }
    }

    static IndexEntryUpdate<LabelSchemaDescriptor> add( Value... value )
    {
        switch ( value.length )
        {
        case 1:
            return IndexEntryUpdate.add( 0, indexKey, value );
        case 2:
            return IndexEntryUpdate.add( 0, compositeIndexKey, value );
        default:
            return null;
        }
    }

    static IndexEntryUpdate<LabelSchemaDescriptor> remove( Value... value )
    {
        switch ( value.length )
        {
        case 1:
            return IndexEntryUpdate.remove( 0, indexKey, value );
        case 2:
            return IndexEntryUpdate.remove( 0, compositeIndexKey, value );
        default:
            return null;
        }
    }

    static IndexEntryUpdate<LabelSchemaDescriptor> change( Value[] before, Value[] after )
    {
        return IndexEntryUpdate.change( 0, compositeIndexKey, before, after );
    }

    static IndexEntryUpdate<LabelSchemaDescriptor> change( Value before, Value after )
    {
        return IndexEntryUpdate.change( 0, indexKey, before, after );
    }

    static void verifyOtherIsClosedOnSingleThrow( AutoCloseable failingCloseable, AutoCloseable fusionCloseable, AutoCloseable... successfulCloseables )
            throws Exception
    {
        IOException failure = new IOException( "fail" );
        doThrow( failure ).when( failingCloseable ).close();

        // when
        try
        {
            fusionCloseable.close();
            fail( "Should have failed" );
        }
        catch ( IOException ignore )
        {
        }

        // then
        for ( AutoCloseable successfulCloseable : successfulCloseables )
        {
            verify( successfulCloseable, Mockito.times( 1 ) ).close();
        }
    }

    static void verifyFusionCloseThrowOnSingleCloseThrow( AutoCloseable failingCloseable, AutoCloseable fusionCloseable )
            throws Exception
    {
        IOException expectedFailure = new IOException( "fail" );
        doThrow( expectedFailure ).when( failingCloseable ).close();
        try
        {
            fusionCloseable.close();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            assertSame( expectedFailure, e );
        }
    }

    static void verifyFusionCloseThrowIfAllThrow( AutoCloseable fusionCloseable, AutoCloseable... autoCloseables ) throws Exception
    {
        // given
        IOException[] failures = new IOException[autoCloseables.length];
        for ( int i = 0; i < autoCloseables.length; i++ )
        {
            failures[i] = new IOException( "unknown" );
            doThrow( failures[i] ).when( autoCloseables[i] ).close();
        }

        try
        {
            // when
            fusionCloseable.close();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            List<Matcher<? super IOException>> matchers = new ArrayList<>();
            for ( IOException failure : failures )
            {
                matchers.add( sameInstance( failure ) );
            }
            assertThat( e, anyOf( matchers ) );
        }
    }
}
