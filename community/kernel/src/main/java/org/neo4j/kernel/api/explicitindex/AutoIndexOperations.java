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
package org.neo4j.kernel.api.explicitindex;

import java.util.Set;

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
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
    void propertyAdded( DataWriteOperations ops, long entityId, int propertyKeyId, Value value ) throws AutoIndexingKernelException;
    void propertyChanged( DataWriteOperations ops, long entityId, int propertyKeyId, Value oldValue, Value newValue )
        throws AutoIndexingKernelException;
    void propertyRemoved( DataWriteOperations ops, long entityId, int propertyKey )
        throws AutoIndexingKernelException;

    void entityRemoved( DataWriteOperations ops, long entityId ) throws AutoIndexingKernelException;

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
        public void propertyAdded( DataWriteOperations ops, long entityId, int propertyKeyId, Value value )
                throws AutoIndexingKernelException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void propertyChanged( DataWriteOperations ops, long entityId, int propertyKeyId, Value oldValue,
                Value newValue ) throws AutoIndexingKernelException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void propertyRemoved( DataWriteOperations ops, long entityId, int propertyKey ) throws
                AutoIndexingKernelException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void entityRemoved( DataWriteOperations ops, long entityId ) throws AutoIndexingKernelException
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
