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
package org.neo4j.kernel.api.explicitindex;

import java.util.Set;

import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.values.storable.Value;

/**
 * Abstract interface for accessing legacy auto indexing facilities for a given type of entity (nodes or relationships)
 *
 * Instances have three main concerns:
 * - Controlling if auto indexing of the underlying entity type (node/relationship) is enabled or disabled
 * - Controlling which properties are being indexed currently
 * - Tracking updates
 *
 * @see AutoIndexing
 * @see org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexOperations
 */
public interface AutoIndexOperations
{
    void propertyAdded( ExplicitIndexWrite write, long entityId, int propertyKeyId, Value value )
            throws AutoIndexingKernelException;

    void propertyChanged( ExplicitIndexWrite write, long entityId, int propertyKeyId, Value oldValue, Value newValue )
            throws AutoIndexingKernelException;

    void propertyRemoved( ExplicitIndexWrite write, long entityId, int propertyKey )
            throws AutoIndexingKernelException;

    void entityRemoved( ExplicitIndexWrite write, long entityId ) throws AutoIndexingKernelException;

    boolean enabled();

    void enabled( boolean enabled );

    void startAutoIndexingProperty( String propName );

    void stopAutoIndexingProperty( String propName );

    Set<String> getAutoIndexedProperties();

    /**
     * Instance of {@link AutoIndexOperations} that throws {@link UnsupportedOperationException} when any of its methods is invoked
     */
    AutoIndexOperations UNSUPPORTED = new AutoIndexOperations()
    {

        @Override
        public void propertyAdded( ExplicitIndexWrite write, long entityId, int propertyKeyId, Value value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void propertyChanged( ExplicitIndexWrite write, long entityId, int propertyKeyId, Value oldValue,
                Value newValue )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void propertyRemoved( ExplicitIndexWrite write, long entityId, int propertyKey )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void entityRemoved( ExplicitIndexWrite write, long entityId )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean enabled()
        {
            return false;
        }

        @Override
        public void enabled( boolean enabled )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startAutoIndexingProperty( String propName )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void stopAutoIndexingProperty( String propName )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getAutoIndexedProperties()
        {
            throw new UnsupportedOperationException();
        }
    };
}
