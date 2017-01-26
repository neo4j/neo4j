package org.neo4j.cypher.internal.codegen;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.cypher.internal.frontend.v3_2.IncomparableValuesException;

import static java.lang.String.format;

public class CompiledOrderabilityUtilsTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public static Object[] values = new Object[]{
            // OTHER
//            PropertyValueComparison.LOWEST_OBJECT,
//            new Object(),

            // STRING
            "",
//            Character.MIN_VALUE,
            " ",
            "20",
            "x",
            "y",
//            Character.MIN_HIGH_SURROGATE,
//            Character.MAX_HIGH_SURROGATE,
//            Character.MIN_LOW_SURROGATE,
//            Character.MAX_LOW_SURROGATE,
//            Character.MAX_VALUE,

            // BOOLEAN
            false,
            true,

            // NUMBER
            Double.NEGATIVE_INFINITY,
            -Double.MAX_VALUE,
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            Integer.MIN_VALUE,
            Short.MIN_VALUE,
            Byte.MIN_VALUE,
            0,
            Double.MIN_VALUE,
            Double.MIN_NORMAL,
            Float.MIN_VALUE,
            Float.MIN_NORMAL,
            1L,
            1.1d,
            1.2f,
            Math.E,
            Math.PI,
            (byte) 10,
            (short) 20,
            Byte.MAX_VALUE,
            Short.MAX_VALUE,
            Integer.MAX_VALUE,
            9007199254740992D,
            9007199254740993L,
            Long.MAX_VALUE,
            Float.MAX_VALUE,
            Double.MAX_VALUE,
            Double.POSITIVE_INFINITY,
            Double.NaN
    };

    @Test
    public void shouldOrderValuesCorrectly()
    {
        for ( int i = 2; i < values.length; i++ )
        {
            for ( int j = 2; j < values.length; j++ )
            {
                Object left = values[i];
                Object right = values[j];

                int cmpPos = sign( i - j );
                int cmpVal = sign( compare(  left, right ) );

//                System.out.println( format( "%s (%d), %s (%d) => %d (%d)", left, i, right, j, cmpLeft, i - j ) );

                if ( cmpPos != cmpVal)
                {
                    throw new AssertionError( format(
                            "Comparing %s against %s does not agree with their positions in the sorted list (%d and " +
                            "%d)",
                            left, right, i, j
                    ) );
                }
            }
        }
    }

    private <T> int compare( T left, T right )
    {
        try
        {
            int cmp1 = CompiledOrderabilityUtils.compare( left, right );
            int cmp2 = CompiledOrderabilityUtils.compare( right, left );
            if ( sign( cmp1 ) != -sign( cmp2 ) )
            {
                throw new AssertionError( format( "Comparator is not symmetric on %s and %s", left, right ) );
            }
            return cmp1;
        }
        catch ( IncomparableValuesException e )
        {
            throw new AssertionError( format("Failed to compare %s:%s and %s:%s", left,left.getClass().getName(), right,right.getClass().getName()), e );
        }
    }

    private int sign( int value )
    {
        return value == 0 ? 0 : (value < 0 ? -1 : +1);
    }
}