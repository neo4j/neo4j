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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.util.Arrays;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.values.storable.CSVHeaderInformation;

/**
 * Header of tabular/csv data input, specifying meta data about values in each "column", for example
 * semantically of which {@link Type} they are and which {@link Extractor type of value} they are.
 */
public class Header implements Cloneable
{
    public interface Factory
    {
        /**
         * @param dataSeeker {@link CharSeeker} containing the data. Usually there's a header for us
         * to read at the very top of it.
         * @param configuration {@link Configuration} specific to the format of the data.
         * @param idType type of values we expect the ids to be.
         * @param groups {@link Groups} to register groups in.
         * @return the created {@link Header}.
         */
        Header create( CharSeeker dataSeeker, Configuration configuration, IdType idType, Groups groups );

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

    public Entry entry( Type type )
    {
        Entry result = null;
        for ( Entry entry : entries )
        {
            if ( entry.type() == type )
            {
                if ( result != null )
                {
                    throw new IllegalStateException( "Multiple header entries of type " + type );
                }
                result = entry;
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return Arrays.toString( entries );
    }

    @Override
    public Header clone()
    {
        Entry[] entries = new Entry[this.entries.length];
        for ( int i = 0; i < entries.length; i++ )
        {
            entries[i] = this.entries[i].clone();
        }
        return new Header( entries );
    }

    public static class Entry implements Cloneable
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

        public CSVHeaderInformation optionalParameter()
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

        @Override
        public Entry clone()
        {
            return new Entry( name, type, group, extractor != null ? extractor.clone() : null, optionalParameter );
        }

        private boolean nullSafeEquals( Object o1, Object o2 )
        {
            return o1 == null || o2 == null ? o1 == o2 : o1.equals( o2 );
        }

        private boolean extractorEquals( Extractor<?> first, Extractor<?> other )
        {
            if ( first == null || other == null )
            {
                return first == other;
            }
            return first.getClass().equals( other.getClass() );
        }
    }
}
