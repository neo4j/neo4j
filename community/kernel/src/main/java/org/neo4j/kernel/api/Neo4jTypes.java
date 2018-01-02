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
package org.neo4j.kernel.api;

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
    public static final TextType NTText = new TextType();
    public static final NumberType NTNumber = new NumberType();
    public static final IntegerType NTInteger = new IntegerType();
    public static final FloatType NTFloat = new FloatType();
    public static final BooleanType NTBoolean = new BooleanType();
    public static final MapType NTMap = new MapType();
    public static final NodeType NTNode = new NodeType();
    public static final RelationshipType NTRelationship = new RelationshipType();
    public static final PathType NTPath = new PathType();
    public static ListType NTList( AnyType innerType ) { return new ListType( innerType ); }

    public static final int ORD_ANY = 0;
    public static final int ORD_TEXT = 1;
    public static final int ORD_NUMBER = 2;
    public static final int ORD_INTEGER = 3;
    public static final int ORD_FLOAT = 4;
    public static final int ORD_BOOLEAN = 5;
    public static final int ORD_LIST = 6;
    public static final int ORD_MAP = 7;
    public static final int ORD_NODE = 8;
    public static final int ORD_RELATIONSHIP = 9;
    public static final int ORD_PATH = 10;

    public static class AnyType
    {
        /**
         * A unique simple number assigned to each type, used when representing types in
         * various forms. This is expected to be durably stored and potentially public,
         * don't change ordinals.
         */
        private final int ordinal;

        private final String name;

        public int ordinal()
        {
            return ordinal;
        }

        public AnyType()
        {
            this( ORD_ANY, "Any" );
        }

        protected AnyType( int ordinal, String name )
        {
            this.ordinal = ordinal;
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
            super( ORD_TEXT, "Text" );
        }
    }

    public static class NumberType extends AnyType
    {
        public NumberType()
        {
            super( ORD_NUMBER, "Number" );
        }

        protected NumberType( int ordinal, String name )
        {
            super( ordinal, name );
        }
    }

    public static class IntegerType extends NumberType
    {
        public IntegerType()
        {
            super( ORD_INTEGER, "Integer" );
        }
    }

    public static class FloatType extends NumberType
    {
        public FloatType()
        {
            super( ORD_FLOAT, "Float");
        }
    }

    public static class BooleanType extends AnyType
    {
        public BooleanType()
        {
            super( ORD_BOOLEAN, "Boolean" );
        }
    }

    public static class ListType extends AnyType
    {
        /** The type of values in this collection */
        private final AnyType innerType;

        public ListType( AnyType innerType )
        {
            super( ORD_LIST, "Collection[" + innerType.toString() + "]" );
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
            super( ORD_MAP, "Map" );
        }

        protected MapType(int ordinal, String name)
        {
            super( ordinal, name );
        }
    }

    public static class NodeType extends MapType
    {
        public NodeType()
        {
            super( ORD_NODE, "Node" );
        }
    }

    public static class RelationshipType extends MapType
    {
        public RelationshipType()
        {
            super( ORD_RELATIONSHIP, "Relationship" );
        }
    }

    public static class PathType extends AnyType
    {
        public PathType()
        {
            super( ORD_PATH, "Path" );
        }
    }
}
