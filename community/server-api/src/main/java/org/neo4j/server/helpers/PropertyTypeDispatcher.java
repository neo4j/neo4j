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
package org.neo4j.server.helpers;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.helpers.collection.ArrayIterator;

/*
 * THIS CLASS SHOULD BE MOVED TO KERNEL ASAP!!!
 */
public abstract class PropertyTypeDispatcher<K, T>
{
    public static abstract class PropertyArray<A, T> implements Iterable<T>
    {
        private PropertyArray()
        {
        }

        public abstract int length();

        public abstract A getClonedArray();

        public abstract Class<?> getType();
    }

    public static void consumeProperties( PropertyTypeDispatcher<String, Void> dispatcher,
            PropertyContainer entity )
    {
        for ( Map.Entry<String, Object> property : entity.getAllProperties().entrySet() )
        {
            dispatcher.dispatch( property.getValue(), property.getKey() );
        }
    }

    @SuppressWarnings( "boxing" )
    public final T dispatch( Object property, K param )
    {
        if( property == null) 
        {
            return dispatchNullProperty( param );
        } else if ( property instanceof String ) 
        {
            return dispatchStringProperty( (String) property, param );
        }
        else if ( property instanceof Number )
        {
            return dispatchNumberProperty( (Number) property, param );
        }
        else if ( property instanceof Boolean )
        {
            return dispatchBooleanProperty( (Boolean) property, param );
        }
        else if ( property instanceof Character )
        {
            return dispatchCharacterProperty( (Character) property, param );
        }
        else if ( property instanceof String[] )
        {
            return dispatchStringArrayProperty( (String[]) property, param );
        }
        else if ( property instanceof Object[] )
        {
            return dispatchOtherArray( (Object[]) property, param );
        }
        else
        {
            Class<?> propertyType = property.getClass();
            if ( propertyType.isArray() && propertyType.getComponentType().isPrimitive() )
            {
                return dispatchPrimitiveArray( property, param );
            }
            else
            {
                return dispatchOtherProperty( property, param );
            }
        }
    }

    private T dispatchPrimitiveArray( Object property, K param )
    {
        if ( property instanceof byte[] )
        {
            return dispatchByteArrayProperty( (byte[]) property, param );
        }
        else if ( property instanceof char[] )
        {
            return dispatchCharacterArrayProperty( (char[]) property, param );
        }
        else if ( property instanceof boolean[] )
        {
            return dispatchBooleanArrayProperty( (boolean[]) property, param );
        }
        else if ( property instanceof long[] )
        {
            return dispatchLongArrayProperty( (long[]) property, param );
        }
        else if ( property instanceof double[] )
        {
            return dispatchDoubleArrayProperty( (double[]) property, param );
        }
        else if ( property instanceof int[] )
        {
            return dispatchIntegerArrayProperty( (int[]) property, param );
        }
        else if ( property instanceof short[] )
        {
            return dispatchShortArrayProperty( (short[]) property, param );
        }
        else if ( property instanceof float[] )
        {
            return dispatchFloatArrayProperty( (float[]) property, param );
        }
        else
        {
            throw new Error( "Unsupported primitive array type: " + property.getClass() );
        }
    }

    protected T dispatchOtherArray( Object[] property, K param )
    {
        if ( property instanceof Byte[] )
        {
            return dispatchByteArrayProperty( (Byte[]) property, param );
        }
        else if ( property instanceof Character[] )
        {
            return dispatchCharacterArrayProperty( (Character[]) property, param );
        }
        else if ( property instanceof Boolean[] )
        {
            return dispatchBooleanArrayProperty( (Boolean[]) property, param );
        }
        else if ( property instanceof Long[] )
        {
            return dispatchLongArrayProperty( (Long[]) property, param );
        }
        else if ( property instanceof Double[] )
        {
            return dispatchDoubleArrayProperty( (Double[]) property, param );
        }
        else if ( property instanceof Integer[] )
        {
            return dispatchIntegerArrayProperty( (Integer[]) property, param );
        }
        else if ( property instanceof Short[] )
        {
            return dispatchShortArrayProperty( (Short[]) property, param );
        }
        else if ( property instanceof Float[] )
        {
            return dispatchFloatArrayProperty( (Float[]) property, param );
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported property array type: "
                                                + property.getClass() );
        }
    }

    @SuppressWarnings( "boxing" )
    protected T dispatchNumberProperty( Number property, K param )
    {
        if ( property instanceof Long )
        {
            return dispatchLongProperty( (Long) property, param );
        }
        else if ( property instanceof Integer )
        {
            return dispatchIntegerProperty( (Integer) property, param );
        }
        else if ( property instanceof Double )
        {
            return dispatchDoubleProperty( (Double) property, param );
        }
        else if ( property instanceof Float )
        {
            return dispatchFloatProperty( (Float) property, param );
        }
        else if ( property instanceof Short )
        {
            return dispatchShortProperty( (Short) property, param );
        }
        else if ( property instanceof Byte )
        {
            return dispatchByteProperty( (Byte) property, param );
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported property type: " + property.getClass() );
        }
    }

    protected T dispatchNullProperty( K param ) {
        return null;
    }
    
    @SuppressWarnings( "boxing" )
    protected abstract T dispatchByteProperty( byte property, K param );

    @SuppressWarnings( "boxing" )
    protected abstract T dispatchCharacterProperty( char property, K param );

    @SuppressWarnings( "boxing" )
    protected abstract T dispatchShortProperty( short property, K param );

    @SuppressWarnings( "boxing" )
    protected abstract T dispatchIntegerProperty( int property, K param );

    @SuppressWarnings( "boxing" )
    protected abstract T dispatchLongProperty( long property, K param );

    @SuppressWarnings( "boxing" )
    protected abstract T dispatchFloatProperty( float property, K param );

    @SuppressWarnings( "boxing" )
    protected abstract T dispatchDoubleProperty( double property, K param );

    @SuppressWarnings( "boxing" )
    protected abstract T dispatchBooleanProperty( boolean property, K param );
    
    protected T dispatchOtherProperty( Object property, K param) {
        throw new IllegalArgumentException( "Unsupported property type: "
                + property.getClass() );
    }

    protected T dispatchByteArrayProperty( final byte[] property, K param )
    {
        return dispatchByteArrayProperty( new PrimitiveArray<byte[], Byte>()
        {
            @Override
            public byte[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Byte item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchCharacterArrayProperty( final char[] property, K param )
    {
        return dispatchCharacterArrayProperty( new PrimitiveArray<char[], Character>()
        {
            @Override
            public char[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Character item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchShortArrayProperty( final short[] property, K param )
    {
        return dispatchShortArrayProperty( new PrimitiveArray<short[], Short>()
        {
            @Override
            public short[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Short item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchIntegerArrayProperty( final int[] property, K param )
    {
        return dispatchIntegerArrayProperty( new PrimitiveArray<int[], Integer>()
        {
            @Override
            public int[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Integer item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchLongArrayProperty( final long[] property, K param )
    {
        return dispatchLongArrayProperty( new PrimitiveArray<long[], Long>()
        {
            @Override
            public long[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Long item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchFloatArrayProperty( final float[] property, K param )
    {
        return dispatchFloatArrayProperty( new PrimitiveArray<float[], Float>()
        {
            @Override
            public float[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Float item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchDoubleArrayProperty( final double[] property, K param )
    {
        return dispatchDoubleArrayProperty( new PrimitiveArray<double[], Double>()
        {
            @Override
            public double[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Double item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchBooleanArrayProperty( final boolean[] property, K param )
    {
        return dispatchBooleanArrayProperty( new PrimitiveArray<boolean[], Boolean>()
        {
            @Override
            public boolean[] getClonedArray()
            {
                return property.clone();
            }

            @Override
            public int length()
            {
                return property.length;
            }

            @Override
            @SuppressWarnings( "boxing" )
            protected Boolean item( int offset )
            {
                return property[offset];
            }

            @Override
            public Class<?> getType()
            {
                return property.getClass();
            }
        }, param );
    }

    protected T dispatchByteArrayProperty( final Byte[] property, K param )
    {
        return dispatchByteArrayProperty( new BoxedArray<byte[], Byte>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public byte[] getClonedArray()
            {
                byte[] result = new byte[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected T dispatchCharacterArrayProperty( final Character[] property, K param )
    {
        return dispatchCharacterArrayProperty( new BoxedArray<char[], Character>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public char[] getClonedArray()
            {
                char[] result = new char[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected T dispatchShortArrayProperty( final Short[] property, K param )
    {
        return dispatchShortArrayProperty( new BoxedArray<short[], Short>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public short[] getClonedArray()
            {
                short[] result = new short[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected T dispatchIntegerArrayProperty( final Integer[] property, K param )
    {
        return dispatchIntegerArrayProperty( new BoxedArray<int[], Integer>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public int[] getClonedArray()
            {
                int[] result = new int[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected T dispatchLongArrayProperty( final Long[] property, K param )
    {
        return dispatchLongArrayProperty( new BoxedArray<long[], Long>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public long[] getClonedArray()
            {
                long[] result = new long[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected T dispatchFloatArrayProperty( final Float[] property, K param )
    {
        return dispatchFloatArrayProperty( new BoxedArray<float[], Float>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public float[] getClonedArray()
            {
                float[] result = new float[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected T dispatchDoubleArrayProperty( final Double[] property, K param )
    {
        return dispatchDoubleArrayProperty( new BoxedArray<double[], Double>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public double[] getClonedArray()
            {
                double[] result = new double[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected T dispatchBooleanArrayProperty( final Boolean[] property, K param )
    {
        return dispatchBooleanArrayProperty( new BoxedArray<boolean[], Boolean>( property )
        {
            @Override
            @SuppressWarnings( "boxing" )
            public boolean[] getClonedArray()
            {
                boolean[] result = new boolean[property.length];
                for ( int i = 0; i < result.length; i++ )
                    result[i] = property[i];
                return result;
            }
        }, param );
    }

    protected abstract T dispatchStringProperty( String property, K param );

    protected T dispatchStringArrayProperty( final String[] property, K param )
    {
        return dispatchStringArrayProperty( new BoxedArray<String[], String>( property )
        {
            @Override
            public String[] getClonedArray()
            {
                return property.clone();
            }
        }, param );
    }

    protected T dispatchStringArrayProperty( PropertyArray<String[], String> array, K param )
    {
        return dispatchArray( array, param );
    }

    protected T dispatchByteArrayProperty( PropertyArray<byte[], Byte> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchCharacterArrayProperty( PropertyArray<char[], Character> array, K param )
    {
        return dispatchArray( array, param );
    }

    protected T dispatchShortArrayProperty( PropertyArray<short[], Short> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchIntegerArrayProperty( PropertyArray<int[], Integer> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchLongArrayProperty( PropertyArray<long[], Long> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchFloatArrayProperty( PropertyArray<float[], Float> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchDoubleArrayProperty( PropertyArray<double[], Double> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchBooleanArrayProperty( PropertyArray<boolean[], Boolean> array, K param )
    {
        return dispatchArray( array, param );
    }

    protected T dispatchNumberArray( PropertyArray<?, ? extends Number> array, K param )
    {
        return dispatchArray( array, param );
    }

    protected T dispatchArray( PropertyArray<?, ?> array, K param )
    {
        throw new UnsupportedOperationException( "Unhandled array type: " + array.getType() );
    }

    private static abstract class BoxedArray<A, T> extends PropertyArray<A, T>
    {
        private final T[] array;

        BoxedArray( T[] array )
        {
            this.array = array;
        }

        @Override
        public int length()
        {
            return array.length;
        }

        @Override
        public Iterator<T> iterator()
        {
            return new ArrayIterator<T>( array );
        }

        @Override
        public Class<?> getType()
        {
            return array.getClass();
        }
    }

    private static abstract class PrimitiveArray<A, T> extends PropertyArray<A, T>
    {
        @Override
        public Iterator<T> iterator()
        {
            return new Iterator<T>()
            {
                final int size = length();
                int pos = 0;

                @Override
                public boolean hasNext()
                {
                    return pos < size;
                }

                @Override
                public T next()
                {
                    return item( pos++ );
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException(
                            "Cannot remove element from primitive array." );
                }
            };
        }

        protected abstract T item( int offset );
    }
}
