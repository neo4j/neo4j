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
package org.neo4j.server.helpers;

import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.helpers.collection.ArrayIterator;

/*
 * THIS CLASS SHOULD BE MOVED TO KERNEL ASAP!!!
 */
public abstract class PropertyTypeDispatcher<K, T>
{
    public abstract static class PropertyArray<T> implements Iterable<T>
    {
        private PropertyArray()
        {
        }

        public abstract int length();

        public abstract Class<?> getType();
    }

    public static void consumeProperties( PropertyTypeDispatcher<String, Void> dispatcher,
            Entity entity )
    {
        for ( Map.Entry<String, Object> property : entity.getAllProperties().entrySet() )
        {
            dispatcher.dispatch( property.getValue(), property.getKey() );
        }
    }

    @SuppressWarnings( "boxing" )
    public final T dispatch( Object property, K param )
    {
        if ( property == null )
        {
            return dispatchNullProperty();
        }
        else if ( property instanceof String )
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
        else if ( property instanceof Point )
        {
          return dispatchPointProperty( (Point) property, param );
        }
        else if ( property instanceof Temporal )
        {
            return dispatchTemporalProperty( (Temporal) property, param );
        }
        else if ( property instanceof TemporalAmount )
        {
            return dispatchTemporalAmountProperty( (TemporalAmount) property, param );
        }
        else if ( property instanceof String[] )
        {
            return dispatchStringArrayProperty( (String[]) property, param );
        }
        else if ( property instanceof Point[] )
        {
            return dispatchPointArrayProperty( (Point[]) property, param );
        }
        else if ( property instanceof Temporal[] )
        {
            return dispatchTemporalArrayProperty( (Temporal[]) property, param );

        }
        else if ( property instanceof TemporalAmount[] )
        {
            return dispatchTemporalAmountArrayProperty( (TemporalAmount[]) property, param );
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
                return dispatchOtherProperty( property );
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

    private T dispatchOtherArray( Object[] property, K param )
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
    private T dispatchNumberProperty( Number property, K param )
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

    private T dispatchNullProperty()
    {
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

    //not abstract in order to not break existing code, since this was fixed in point release
    protected T dispatchPointProperty( Point property, K param )
    {
        return dispatchOtherProperty( property );
    }

    //not abstract in order to not break existing code, since this was fixed in point release
    protected T dispatchTemporalProperty( Temporal property, K param )
    {
        return dispatchOtherProperty( property );
    }

    //not abstract in order to not break existing code, since this was fixed in point release
    protected T dispatchTemporalAmountProperty( TemporalAmount property, K param )
    {
        return dispatchOtherProperty( property );
    }

    private T dispatchOtherProperty( Object property )
    {
        throw new IllegalArgumentException( "Unsupported property type: " + property.getClass() );
    }

    private T dispatchByteArrayProperty( final byte[] property, K param )
    {
        return dispatchByteArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchCharacterArrayProperty( final char[] property, K param )
    {
        return dispatchCharacterArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchShortArrayProperty( final short[] property, K param )
    {
        return dispatchShortArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchIntegerArrayProperty( final int[] property, K param )
    {
        return dispatchIntegerArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchLongArrayProperty( final long[] property, K param )
    {
        return dispatchLongArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchFloatArrayProperty( final float[] property, K param )
    {
        return dispatchFloatArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchDoubleArrayProperty( final double[] property, K param )
    {
        return dispatchDoubleArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchBooleanArrayProperty( final boolean[] property, K param )
    {
        return dispatchBooleanArrayProperty( new PrimitiveArray<>()
        {
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

    private T dispatchByteArrayProperty( final Byte[] property, K param )
    {
        return dispatchByteArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchCharacterArrayProperty( final Character[] property, K param )
    {
        return dispatchCharacterArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchShortArrayProperty( final Short[] property, K param )
    {
        return dispatchShortArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchIntegerArrayProperty( final Integer[] property, K param )
    {
        return dispatchIntegerArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchLongArrayProperty( final Long[] property, K param )
    {
        return dispatchLongArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchFloatArrayProperty( final Float[] property, K param )
    {
        return dispatchFloatArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchDoubleArrayProperty( final Double[] property, K param )
    {
        return dispatchDoubleArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchBooleanArrayProperty( final Boolean[] property, K param )
    {
        return dispatchBooleanArrayProperty( new BoxedArray<>( property ), param );
    }

    protected abstract T dispatchStringProperty( String property, K param );

    protected T dispatchStringArrayProperty( final String[] property, K param )
    {
        return dispatchStringArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchStringArrayProperty( PropertyArray<String> array, K param )
    {
        return dispatchArray( array );
    }

    protected T dispatchPointArrayProperty( final Point[] property, K param )
    {
        return dispatchPointArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchPointArrayProperty( PropertyArray<Point> array, K param )
    {
        return dispatchArray( array );
    }

    private T dispatchTemporalArrayProperty( PropertyArray<Temporal> array, K param )
    {
        return dispatchArray( array );
    }

    protected T dispatchTemporalArrayProperty( final Temporal[] property, K param )
    {
        return dispatchTemporalArrayProperty( new BoxedArray<>( property ), param );
    }

    private T dispatchTemporalAmountArrayProperty( PropertyArray<TemporalAmount> array, K param )
    {
        return dispatchArray( array );
    }

    protected T dispatchTemporalAmountArrayProperty( final TemporalAmount[] property, K param )
    {
        return dispatchTemporalAmountArrayProperty( new BoxedArray<>( property ), param );
    }

    protected T dispatchByteArrayProperty( PropertyArray<Byte> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchCharacterArrayProperty( PropertyArray<Character> array, K param )
    {
        return dispatchArray( array );
    }

    protected T dispatchShortArrayProperty( PropertyArray<Short> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchIntegerArrayProperty( PropertyArray<Integer> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchLongArrayProperty( PropertyArray<Long> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchFloatArrayProperty( PropertyArray<Float> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchDoubleArrayProperty( PropertyArray<Double> array, K param )
    {
        return dispatchNumberArray( array, param );
    }

    protected T dispatchBooleanArrayProperty( PropertyArray<Boolean> array, K param )
    {
        return dispatchArray( array );
    }

    private T dispatchNumberArray( PropertyArray<? extends Number> array, K param )
    {
        return dispatchArray( array );
    }

    private T dispatchArray( PropertyArray<?> array )
    {
        throw new UnsupportedOperationException( "Unhandled array type: " + array.getType() );
    }

    private static final class BoxedArray<T> extends PropertyArray<T>
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
            return new ArrayIterator<>( array );
        }

        @Override
        public Class<?> getType()
        {
            return array.getClass();
        }
    }

    private abstract static class PrimitiveArray<T> extends PropertyArray<T>
    {
        @Override
        public Iterator<T> iterator()
        {
            return new Iterator<>()
            {
                final int size = length();
                int pos;

                @Override
                public boolean hasNext()
                {
                    return pos < size;
                }

                @Override
                public T next()
                {
                    if ( !hasNext() )
                    {
                        throw new NoSuchElementException();
                    }
                    return item( pos++ );
                }

                @Override
                public void remove()
                {
                    throw new UnsupportedOperationException( "Cannot remove element from primitive array." );
                }
            };
        }

        protected abstract T item( int offset );
    }
}
