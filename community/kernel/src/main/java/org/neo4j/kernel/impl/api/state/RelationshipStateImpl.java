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
package org.neo4j.kernel.impl.api.state;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.util.Iterator;

import org.neo4j.kernel.impl.util.collection.CollectionsFactory;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.values.storable.Value;

import static java.util.Collections.emptyIterator;

class RelationshipStateImpl extends PropertyContainerStateImpl implements RelationshipState
{
    static final RelationshipState EMPTY = new RelationshipState()
    {
        @Override
        public long getId()
        {
            throw new UnsupportedOperationException( "id not defined" );
        }

        @Override
        public <EX extends Exception> boolean accept( RelationshipVisitor<EX> visitor )
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
        public IntIterable removedProperties()
        {
            return IntSets.immutable.empty();
        }

        @Override
        public Iterator<StorageProperty> addedAndChangedProperties()
        {
            return emptyIterator();
        }

        @Override
        public boolean hasPropertyChanges()
        {
            return false;
        }

        @Override
        public boolean isPropertyChangedOrRemoved( int propertyKey )
        {
            return false;
        }

        @Override
        public Value propertyValue( int propertyKey )
        {
            return null;
        }
    };

    private long startNode = -1;
    private long endNode = -1;
    private int type = -1;

    RelationshipStateImpl( long id, CollectionsFactory collectionsFactory )
    {
        super( id, collectionsFactory );
    }

    void setMetaData( long startNode, long endNode, int type )
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
