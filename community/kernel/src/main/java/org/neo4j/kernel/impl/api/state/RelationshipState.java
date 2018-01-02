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
package org.neo4j.kernel.impl.api.state;

import java.util.Iterator;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Supplier;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.cursor.TxAllPropertyCursor;
import org.neo4j.kernel.impl.api.cursor.TxSinglePropertyCursor;

/**
 * Represents the transactional changes to a relationship.
 *
 * @see PropertyContainerState
 */
public interface RelationshipState extends PropertyContainerState
{
    long getId();

    <EX extends Exception> boolean accept( RelationshipVisitor<EX> visitor ) throws EX;

    class Mutable extends PropertyContainerState.Mutable implements RelationshipState
    {
        private long startNode = -1;
        private long endNode = -1;
        private int type = -1;

        private Mutable( long id )
        {
            super( id, EntityType.RELATIONSHIP );
        }

        public void setMetaData( long startNode, long endNode, int type )
        {
            this.startNode = startNode;
            this.endNode = endNode;
            this.type = type;
        }

        @Override
        public <EX extends Exception> boolean accept( RelationshipVisitor<EX> visitor ) throws EX
        {
            if ( type != -1 )
            {
                visitor.visit( getId(), type, startNode, endNode );
                return true;
            }
            return false;
        }
    }

    abstract class Defaults extends StateDefaults<Long, RelationshipState, RelationshipState.Mutable>
    {
        @Override
        Mutable createValue( Long id, TxState state )
        {
            return new Mutable( id );
        }

        @Override
        RelationshipState defaultValue()
        {
            return DEFAULT;
        }

        private static final RelationshipState DEFAULT = new RelationshipState()
        {
            private UnsupportedOperationException notDefined( String field )
            {
                return new UnsupportedOperationException( field + " not defined" );
            }

            @Override
            public long getId()
            {
                throw notDefined( "id" );
            }

            @Override
            public <EX extends Exception> boolean accept( RelationshipVisitor<EX> visitor ) throws EX
            {
                return false;
            }

            @Override
            public Iterator<DefinedProperty> addedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<DefinedProperty> changedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<Integer> removedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<DefinedProperty> addedAndChangedProperties()
            {
                return IteratorUtil.emptyIterator();
            }

            @Override
            public Iterator<DefinedProperty> augmentProperties( Iterator<DefinedProperty> iterator )
            {
                return iterator;
            }

            @Override
            public Cursor<PropertyItem> augmentPropertyCursor( Supplier<TxAllPropertyCursor> propertyCursor,
                    Cursor<PropertyItem> cursor )
            {
                return cursor;
            }

            @Override
            public Cursor<PropertyItem> augmentSinglePropertyCursor( Supplier<TxSinglePropertyCursor> propertyCursor,
                    Cursor<PropertyItem> cursor,
                    int propertyKeyId )
            {
                return cursor;
            }

            @Override
            public void accept( Visitor visitor )
            {
            }
        };
    }
}
