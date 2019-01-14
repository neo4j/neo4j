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
package org.neo4j.internal.kernel.api.procs;

/**
 * See also type_system.txt in the cypher code base, this is a mapping of that type system definition
 * down to this level. Ideally this becomes canonical.
 *
 * This should also move to replace the specialized type handling in packstream, or be tied to it in some
 * way to ensure a strict mapping.
 */
public class Neo4jTypes
{
    public static final AnyType NTAny = new AnyType();
    public static final TextType NTString = new TextType();
    public static final NumberType NTNumber = new NumberType();
    public static final IntegerType NTInteger = new IntegerType();
    public static final FloatType NTFloat = new FloatType();
    public static final BooleanType NTBoolean = new BooleanType();
    public static final MapType NTMap = new MapType();
    public static final ByteArrayType NTByteArray = new ByteArrayType();
    public static final NodeType NTNode = new NodeType();
    public static final RelationshipType NTRelationship = new RelationshipType();
    public static final PathType NTPath = new PathType();
    public static final GeometryType NTGeometry = new GeometryType();
    public static final PointType NTPoint = new PointType();
    public static final DateTimeType NTDateTime = new DateTimeType();
    public static final LocalDateTimeType NTLocalDateTime = new LocalDateTimeType();
    public static final DateType NTDate = new DateType();
    public static final TimeType NTTime = new TimeType();
    public static final LocalTimeType NTLocalTime = new LocalTimeType();
    public static final DurationType NTDuration = new DurationType();

    private Neo4jTypes()
    {
    }

    public static ListType NTList( AnyType innerType )
    {
        return new ListType( innerType );
    }

    public static class AnyType
    {
        private final String name;

        public AnyType()
        {
            this( "ANY?" );
        }

        protected AnyType( String name )
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public static class TextType extends AnyType
    {
        public TextType()
        {
            super( "STRING?" );
        }
    }

    public static class NumberType extends AnyType
    {
        public NumberType()
        {
            super( "NUMBER?" );
        }

        protected NumberType( String name )
        {
            super( name );
        }
    }

    public static class IntegerType extends NumberType
    {
        public IntegerType()
        {
            super( "INTEGER?" );
        }
    }

    public static class FloatType extends NumberType
    {
        public FloatType()
        {
            super( "FLOAT?" );
        }
    }

    public static class BooleanType extends AnyType
    {
        public BooleanType()
        {
            super( "BOOLEAN?" );
        }
    }

    public static class ListType extends AnyType
    {
        /** The type of values in this collection */
        private final AnyType innerType;

        public ListType( AnyType innerType )
        {
            super( "LIST? OF " + innerType );
            this.innerType = innerType;
        }

        public AnyType innerType()
        {
            return innerType;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ListType listType = (ListType) o;
            return innerType.equals( listType.innerType );
        }

        @Override
        public int hashCode()
        {
            return innerType.hashCode();
        }
    }

    public static class MapType extends AnyType
    {
        public MapType()
        {
            super( "MAP?" );
        }

        protected MapType( String name )
        {
            super( name );
        }
    }

    public static class ByteArrayType extends AnyType
    {
        public ByteArrayType()
        {
            super( "BYTEARRAY?" );
        }

        protected ByteArrayType( String name )
        {
            super( name );
        }
    }

    public static class NodeType extends MapType
    {
        public NodeType()
        {
            super( "NODE?" );
        }
    }

    public static class RelationshipType extends MapType
    {
        public RelationshipType()
        {
            super( "RELATIONSHIP?" );
        }
    }

    public static class PathType extends AnyType
    {
        public PathType()
        {
            super( "PATH?" );
        }
    }

    public static class GeometryType extends AnyType
    {
        public GeometryType()
        {
            super( "GEOMETRY?" );
        }
    }

    public static class PointType extends AnyType
    {
        public PointType()
        {
            super( "POINT?" );
        }
    }

    public static class DateTimeType extends AnyType
    {
        public DateTimeType()
        {
            super( "DATETIME?" );
        }
    }

    public static class LocalDateTimeType extends AnyType
    {
        public LocalDateTimeType()
        {
            super( "LOCALDATETIME?" );
        }
    }

    public static class DateType extends AnyType
    {
        public DateType()
        {
            super( "DATE?" );
        }
    }

    public static class TimeType extends AnyType
    {
        public TimeType()
        {
            super( "TIME?" );
        }
    }

    public static class LocalTimeType extends AnyType
    {
        public LocalTimeType()
        {
            super( "LOCALTIME?" );
        }
    }

    public static class DurationType extends AnyType
    {
        public DurationType()
        {
            super( "DURATION?" );
        }
    }
}
