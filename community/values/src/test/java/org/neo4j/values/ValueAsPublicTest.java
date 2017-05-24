package org.neo4j.values;

import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValueAsPublicTest
{

    Iterable<AsPublicTest> scalars = Arrays.asList(
            shouldGivePublic( Values.byteValue( (byte)1 ), (byte)1 ),
            shouldGivePublic( Values.shortValue( (short)2 ), (short)2 ),
            shouldGivePublic( Values.intValue( 3 ), 3 ),
            shouldGivePublic( Values.longValue( 4L ), 4L ),
            shouldGivePublic( Values.floatValue( 5.0f ), 5.0f ),
            shouldGivePublic( Values.doubleValue( 6.0 ), 6.0 ),
            shouldGivePublic( Values.booleanValue( false ), false ),
            shouldGivePublic( Values.charValue( 'a' ), 'a' ),
            shouldGivePublic( Values.stringValue( "b" ), "b" ),
            shouldGivePublic( Values.lazyStringValue( () -> "c"  ), "c" )
        );

    @Test
    public void shouldProvideScalarValueAsPublic()
    {
        for ( AsPublicTest test : scalars )
        {
            test.assertGeneratesPublic();
        }
    }

    // DIRECT ARRAYS

    @Test
    public void shouldProvideDirectByteArrayAsPublic()
    {
        byte[] inStore = {1};
        Value value = Values.byteArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return byte[]", asPublic instanceof byte[] );

        byte[] arr = (byte[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (byte[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectShortArrayAsPublic()
    {
        short[] inStore = {1};
        Value value = Values.shortArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return short[]", asPublic instanceof short[] );

        short[] arr = (short[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (short[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectIntArrayAsPublic()
    {
        int[] inStore = {1};
        Value value = Values.intArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return int[]", asPublic instanceof int[] );

        int[] arr = (int[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (int[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectLongArrayAsPublic()
    {
        long[] inStore = {1};
        Value value = Values.longArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return long[]", asPublic instanceof long[] );

        long[] arr = (long[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (long[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectFloatArrayAsPublic()
    {
        float[] inStore = {1};
        Value value = Values.floatArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return float[]", asPublic instanceof float[] );

        float[] arr = (float[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (float[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectDoubleArrayAsPublic()
    {
        double[] inStore = {1};
        Value value = Values.doubleArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return double[]", asPublic instanceof double[] );

        double[] arr = (double[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (double[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectCharArrayAsPublic()
    {
        char[] inStore = {'a'};
        Value value = Values.charArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return char[]", asPublic instanceof char[] );

        char[] arr = (char[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = 'b';
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (char[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectStringArrayAsPublic()
    {
        String[] inStore = {"a"};
        Value value = Values.stringArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return String[]", asPublic instanceof String[] );

        String[] arr = (String[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = "b";
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (String[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideDirectBooleanArrayAsPublic()
    {
        boolean[] inStore = {true};
        Value value = Values.booleanArray( inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return boolean[]", asPublic instanceof boolean[] );

        boolean[] arr = (boolean[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = false;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (boolean[])value.asPublic() ) );
    }

    // LAZY ARRAYS

    @Test
    public void shouldProvideLazyByteArrayAsPublic()
    {
        byte[] inStore = {1};
        Value value = Values.lazyByteArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return byte[]", asPublic instanceof byte[] );

        byte[] arr = (byte[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (byte[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyShortArrayAsPublic()
    {
        short[] inStore = {1};
        Value value = Values.lazyShortArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return short[]", asPublic instanceof short[] );

        short[] arr = (short[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (short[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyIntArrayAsPublic()
    {
        int[] inStore = {1};
        Value value = Values.lazyIntArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return int[]", asPublic instanceof int[] );

        int[] arr = (int[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (int[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyLongArrayAsPublic()
    {
        long[] inStore = {1};
        Value value = Values.lazyLongArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return long[]", asPublic instanceof long[] );

        long[] arr = (long[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (long[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyFloatArrayAsPublic()
    {
        float[] inStore = {1};
        Value value = Values.lazyFloatArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return float[]", asPublic instanceof float[] );

        float[] arr = (float[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (float[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyDoubleArrayAsPublic()
    {
        double[] inStore = {1};
        Value value = Values.lazyDoubleArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return double[]", asPublic instanceof double[] );

        double[] arr = (double[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = -1;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (double[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyCharArrayAsPublic()
    {
        char[] inStore = {'a'};
        Value value = Values.lazyCharArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return char[]", asPublic instanceof char[] );

        char[] arr = (char[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = 'b';
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (char[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyStringArrayAsPublic()
    {
        String[] inStore = {"a"};
        Value value = Values.lazyStringArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return String[]", asPublic instanceof String[] );

        String[] arr = (String[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = "b";
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (String[])value.asPublic() ) );
    }

    @Test
    public void shouldProvideLazyBooleanArrayAsPublic()
    {
        boolean[] inStore = {true};
        Value value = Values.lazyBooleanArray( () -> inStore );
        Object asPublic = value.asPublic();
        assertTrue( "should return boolean[]", asPublic instanceof boolean[] );

        boolean[] arr = (boolean[]) asPublic;
        assertTrue( "should have same values", Arrays.equals( inStore, arr ) );

        arr[0] = false;
        assertFalse( "should not modify inStore array", Arrays.equals( inStore, arr ) );
        assertTrue( "should still generate inStore array", Arrays.equals( inStore, (boolean[])value.asPublic() ) );
    }

    private AsPublicTest shouldGivePublic( Value value, Object asPublic )
    {
        return new AsPublicTest( value, asPublic );
    }

    private class AsPublicTest
    {
        private final Value value;
        private final Object expected;

        private AsPublicTest( Value value, Object expected )
        {
            this.value = value;
            this.expected = expected;
        }

        void assertGeneratesPublic()
        {
            assertThat( value.asPublic(), equalTo( expected ) );
        }
    }
}
