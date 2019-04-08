package org.neo4j.kernel.api.impl.schema;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NumberFieldTest
{
    @Test
    void encodingLongsAndDoubles()
    {
        for ( int i = 1; i < Integer.MAX_VALUE; i++ )
        {
            BytesRef longEncode = NumberField.encode( (long) i );
            BytesRef doubleEncode = NumberField.encode( (double) i );
            if ( !longEncode.equals( doubleEncode ) )
            {
                byte[] bytes = new byte[Double.BYTES];
                DoublePoint.encodeDimension( i, bytes, 0 );
                BytesRef reference = new BytesRef( bytes );
                fail( "Expected encodings of " + i + " to be equal, but they were double = " + doubleEncode + ", long = " + longEncode +
                                " (DoublePoint reference = " + reference + ")" );
            }
        }
    }
}