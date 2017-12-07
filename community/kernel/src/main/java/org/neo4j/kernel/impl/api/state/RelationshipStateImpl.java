/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.RelationshipState;

import static java.util.Collections.emptyIterator;

public class RelationshipStateImpl extends PropertyContainerStateImpl implements RelationshipState
{
    private long startNode = -1;
    private long endNode = -1;
    private int type = -1;

    RelationshipStateImpl( long id )
    {
        super( id );
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

    public abstract static class Defaults extends StateDefaults<Long, RelationshipState, RelationshipStateImpl>
    {
        @Override
        RelationshipStateImpl createValue( Long id, TxState state )
        {
            return new RelationshipStateImpl( id );
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
            public Iterator<StorageProperty> addedProperties()
            {
                return emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> changedProperties()
            {
                return emptyIterator();
            }

            @Override
            public Iterator<Integer> removedProperties()
            {
                return emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> addedAndChangedProperties()
            {
                return emptyIterator();
            }

            @Override
            public Iterator<StorageProperty> augmentProperties( Iterator<StorageProperty> iterator )
            {
                return iterator;
            }

            @Override
            public void accept( Visitor visitor ) throws ConstraintValidationException
            {
            }

            @Override
            public boolean hasPropertyChanges()
            {
                return false;
            }

            @Override
            public StorageProperty getChangedProperty( int propertyKeyId )
            {
                return null;
            }

            @Override
            public StorageProperty getAddedProperty( int propertyKeyId )
            {
                return null;
            }

            @Override
            public boolean isPropertyChangedOrRemoved( int propertyKey )
            {
                return false;
            }

            @Override
            public boolean isPropertyRemoved( int propertyKeyId )
            {
                return false;
            }
        };
    }
}
