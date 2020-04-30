/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.stream;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.values.AnyValue;

public class Records
{

    public static Record empty()
    {
        return of( List.of() );
    }

    public static Record of( List<AnyValue> values )
    {
        return new ListRecord( values );
    }

    public static Record of( AnyValue[] values )
    {
        return new ListRecord( List.of( values ) );
    }

    public static Record join( Record lhs, Record rhs )
    {
        return new JoinedRecord( lhs, rhs );
    }

    public static Record lazy( int size, Supplier<Record> recordSupplier )
    {
        return new LazyConvertingRecord( size, recordSupplier );
    }

    public static Map<String,AnyValue> asMap( Record record, List<String> columns )
    {
        HashMap<String,AnyValue> map = new HashMap<>();
        for ( int i = 0; i < columns.size(); i++ )
        {
            map.put( columns.get( i ), record.getValue( i ) );
        }
        return map;
    }

    public static Iterator<AnyValue> iterator( Record record )
    {
        return new Iterator<>()
        {
            private int i;

            @Override
            public boolean hasNext()
            {
                return i < record.size();
            }

            @Override
            public AnyValue next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return record.getValue( i++ );
            }
        };
    }

    public static Iterable<AnyValue> iterable( Record record )
    {
        return () -> iterator( record );
    }

    public static Stream<AnyValue> stream( Record record )
    {
        return StreamSupport.stream( iterable( record ).spliterator(), false );
    }

    public static String show( Record record )
    {
        return stream( record )
                .map( Object::toString )
                .collect( Collectors.joining( ", ", "[", "]" ) );
    }

    private static class ListRecord extends Record
    {

        private final List<AnyValue> values;

        private ListRecord( List<AnyValue> values )
        {
            this.values = values;
        }

        @Override
        public AnyValue getValue( int offset )
        {
            return values.get( offset );
        }

        @Override
        public int size()
        {
            return values.size();
        }
    }

    private static class JoinedRecord extends Record
    {
        private final Record lhs;
        private final Record rhs;

        private JoinedRecord( Record lhs, Record rhs )
        {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public AnyValue getValue( int offset )
        {
            if ( offset < lhs.size() )
            {
                return lhs.getValue( offset );
            }
            else
            {
                return rhs.getValue( offset - lhs.size() );
            }
        }

        @Override
        public int size()
        {
            return lhs.size() + rhs.size();
        }
    }

    private static class LazyConvertingRecord extends Record
    {

        private final int size;
        private final Supplier<Record> recordSupplier;
        private Record convertedRecord;

        LazyConvertingRecord( int size, Supplier<Record> recordSupplier )
        {
            this.size = size;
            this.recordSupplier = recordSupplier;
        }

        @Override
        public AnyValue getValue( int offset )
        {
            maybeConvert();
            return convertedRecord.getValue( offset );
        }

        @Override
        public int size()
        {
            return size;
        }

        private void maybeConvert()
        {
            if ( convertedRecord == null )
            {
                convertedRecord = recordSupplier.get();
            }
        }
    }
}
