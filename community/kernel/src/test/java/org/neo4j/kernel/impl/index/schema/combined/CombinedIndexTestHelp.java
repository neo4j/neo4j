package org.neo4j.kernel.impl.index.schema.combined;

import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.Callable;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

class CombinedIndexTestHelp
{
    private static final Value[] numberValues = new Value[]
            {
                    Values.byteValue( (byte) 1 ),
                    Values.shortValue( (short) 2 ),
                    Values.intValue( 3 ),
                    Values.longValue( 4 ),
                    Values.floatValue( 5.6f ),
                    Values.doubleValue( 7.8 )
            };
    private static final Value[] otherValues = new Value[]
            {
                    Values.booleanValue( true ),
                    Values.charValue( 'a' ),
                    Values.stringValue( "bcd" ),
                    Values.booleanArray( new boolean[2] ),
                    Values.byteArray( new byte[]{1, 2} ),
                    Values.shortArray( new short[]{3, 4} ),
                    Values.intArray( new int[]{5, 6} ),
                    Values.longArray( new long[]{7, 8} ),
                    Values.floatArray( new float[]{9.10f, 11.12f} ),
                    Values.doubleArray( new double[]{13.14, 15.16} ),
                    Values.charArray( new char[2] ),
                    Values.stringArray( new String[2] ),
                    Values.NO_VALUE
            };

    static Value[] valuesSupportedByBoost()
    {
        return numberValues;
    }

    static Value[] valuesNotSupportedByBoost()
    {
        return otherValues;
    }

    static Value[] allValues()
    {
        return ArrayUtils.addAll( numberValues, otherValues );
    }

    static void verifyCallFail( Exception expectedFailure, Callable failingCall ) throws Exception
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
}
