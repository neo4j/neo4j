/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.storageengine.api;

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
    public static final NodeType NTNode = new NodeType();
    public static final RelationshipType NTRelationship = new RelationshipType();
    public static final PathType NTPath = new PathType();
    public static ListType NTList( AnyType innerType ) { return new ListType( innerType ); }

    public static class AnyType
    {
        private final String name;

        public AnyType()
        {
            this( "Any" );
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
            super( "String" );
        }
    }

    public static class NumberType extends AnyType
    {
        public NumberType()
        {
            super( "Number" );
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
            super( "Integer" );
        }
    }

    public static class FloatType extends NumberType
    {
        public FloatType()
        {
            super( "Float");
        }
    }

    public static class BooleanType extends AnyType
    {
        public BooleanType()
        {
            super( "Boolean" );
        }
    }

    public static class ListType extends AnyType
    {
        /** The type of values in this collection */
        private final AnyType innerType;

        public ListType( AnyType innerType )
        {
            super( "Collection[" + innerType.toString() + "]" );
            this.innerType = innerType;
        }

        public AnyType innerType()
        {
            return innerType;
        }
    }

    public static class MapType extends AnyType
    {
        public MapType()
        {
            super( "Map" );
        }

        protected MapType( String name )
        {
            super( name );
        }
    }

    public static class NodeType extends MapType
    {
        public NodeType()
        {
            super( "Node" );
        }
    }

    public static class RelationshipType extends MapType
    {
        public RelationshipType()
        {
            super( "Relationship" );
        }
    }

    public static class PathType extends AnyType
    {
        public PathType()
        {
            super( "Path" );
        }
    }
}
