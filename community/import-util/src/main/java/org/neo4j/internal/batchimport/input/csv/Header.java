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
package org.neo4j.internal.batchimport.input.csv;

import java.io.PrintStream;
import java.util.Arrays;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.values.storable.CSVHeaderInformation;

/**
 * Header of tabular/csv data input, specifying meta data about values in each "column", for example
 * semantically of which {@link Type} they are and which {@link Extractor type of value} they are.
 */
public class Header
{
    public interface Factory
    {
        default Header create( CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups )
        {
            return create( dataSeeker, configuration, idType, groups, NO_MONITOR );
        }

        /**
         * @param dataSeeker {@link CharSeeker} containing the data. Usually there's a header for us
         * to read at the very top of it.
         * @param configuration {@link Configuration} specific to the format of the data.
         * @param idType type of values we expect the ids to be.
         * @param groups {@link Groups} to register groups in.
         * @return the created {@link Header}.
         */
        Header create( CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups, Monitor monitor );

        /**
         * @return whether or not this header is already defined. If this returns {@code false} then the header
         * will be read from the top of the data stream.
         */
        boolean isDefined();
    }

    private final Entry[] entries;

    public Header( Entry... entries )
    {
        this.entries = entries;
    }

    public Entry[] entries()
    {
        return entries;
    }

    @Override
    public String toString()
    {
        return Arrays.toString( entries );
    }

    public Header( Header other )
    {
        this.entries = new Entry[other.entries.length];
        for ( int i = 0; i < other.entries.length; i++ )
        {
            this.entries[i] = new Entry( other.entries[i] );
        }
    }

    public static class Entry
    {
        private final String name;
        private final Type type;
        private final Group group;
        private final Extractor<?> extractor;
        // This can be used to encapsulate the parameters set in the header for spatial and temporal columns
        private final CSVHeaderInformation optionalParameter;

        public Entry( String name, Type type, Group group, Extractor<?> extractor )
        {
            this.name = name;
            this.type = type;
            this.group = group;
            this.extractor = extractor;
            this.optionalParameter = null;
        }

        public Entry( String name, Type type, Group group, Extractor<?> extractor, CSVHeaderInformation optionalParameter )
        {
            this.name = name;
            this.type = type;
            this.group = group;
            this.extractor = extractor;
            this.optionalParameter = optionalParameter;
        }

        @Override
        public String toString()
        {
            if ( optionalParameter == null )
            {
                return (name != null ? name : "") + ":" + (type == Type.PROPERTY ? extractor.name().toLowerCase() : type.name()) +
                        (group() != Group.GLOBAL ? "(" + group().name() + ")" : "");
            }
            else
            {
                return (name != null ? name : "") + ":" +
                        (type == Type.PROPERTY ? extractor.name().toLowerCase() + "[" + optionalParameter + "]" : type.name()) +
                        (group() != Group.GLOBAL ? "(" + group().name() + ")" : "");
            }
        }

        public Extractor<?> extractor()
        {
            return extractor;
        }

        public Type type()
        {
            return type;
        }

        public Group group()
        {
            return group != null ? group : Group.GLOBAL;
        }

        public String name()
        {
            return name;
        }

        CSVHeaderInformation optionalParameter()
        {
            return optionalParameter;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            if ( name != null )
            {
                result = prime * result + name.hashCode();
            }
            result = prime * result + type.hashCode();
            if ( group != null )
            {
                result = prime * result + group.hashCode();
            }
            result = prime * result + extractor.hashCode();
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null || getClass() != obj.getClass() )
            {
                return false;
            }
            Entry other = (Entry) obj;
            return nullSafeEquals( name, other.name ) && type == other.type &&
                    nullSafeEquals( group, other.group ) && extractorEquals( extractor, other.extractor ) &&
                    nullSafeEquals( optionalParameter, other.optionalParameter );
        }

        Entry( Entry other )
        {
            this( other.name, other.type, other.group, other.extractor != null ? other.extractor.clone() : null, other.optionalParameter );
        }

        private static boolean nullSafeEquals( Object o1, Object o2 )
        {
            return o1 == null || o2 == null ? o1 == o2 : o1.equals( o2 );
        }

        private static boolean extractorEquals( Extractor<?> first, Extractor<?> other )
        {
            if ( first == null || other == null )
            {
                return first == other;
            }
            return first.getClass().equals( other.getClass() );
        }
    }

    public interface Monitor
    {
        /**
         * Notifies that a type has been normalized.
         *
         * @param sourceDescription description of source file or stream that the header entry is defined in.
         * @param header name of the header entry.
         * @param fromType the type specified in the header in the source.
         * @param toType the type which will be used instead of the specified type.
         */
        void typeNormalized( String sourceDescription, String header, String fromType, String toType );
    }

    public static final Monitor NO_MONITOR = ( source, header, from, to ) -> {};

    public static class PrintingMonitor implements Monitor
    {
        private final PrintStream out;
        private boolean first = true;

        PrintingMonitor( PrintStream out )
        {
            this.out = out;
        }

        @Override
        public void typeNormalized( String sourceDescription, String name, String fromType, String toType )
        {
            if ( first )
            {
                out.println( "Type normalization:" );
                first = false;
            }

            out.println( String.format( "  Property type of '%s' normalized from '%s' --> '%s' in %s", name, fromType, toType, sourceDescription ) );
        }
    }
}
