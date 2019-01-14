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
package org.neo4j.kernel.impl.api.explicitindex;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.AutoIndexingKernelException;
import org.neo4j.internal.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.values.storable.Value;

public class InternalAutoIndexOperations implements AutoIndexOperations
{
    public enum EntityType
    {
        NODE
                {
                    @Override
                    public void add( ExplicitIndexWrite ops, long entityId, String keyName, Object value )
                            throws KernelException
                    {
                        ops.nodeAddToExplicitIndex( InternalAutoIndexing.NODE_AUTO_INDEX, entityId, keyName, value );
                    }

                    @Override
                    public void remove( ExplicitIndexWrite ops, long entityId, String keyName, Object value )
                            throws ExplicitIndexNotFoundKernelException
                    {
                        ops.nodeRemoveFromExplicitIndex( InternalAutoIndexing.NODE_AUTO_INDEX, entityId, keyName, value );
                    }

                    @Override
                    public void remove( ExplicitIndexWrite ops, long entityId, String keyName )
                            throws ExplicitIndexNotFoundKernelException
                    {
                        ops.nodeRemoveFromExplicitIndex( InternalAutoIndexing.NODE_AUTO_INDEX, entityId, keyName );
                    }

                    @Override
                    public void remove( ExplicitIndexWrite ops, long entityId )
                            throws ExplicitIndexNotFoundKernelException
                    {
                        ops.nodeRemoveFromExplicitIndex( InternalAutoIndexing.NODE_AUTO_INDEX, entityId );
                    }

                    @Override
                    public void ensureIndexExists( ExplicitIndexWrite ops )

                    {
                        ops.nodeExplicitIndexCreateLazily( InternalAutoIndexing.NODE_AUTO_INDEX, null );
                    }
                },
        RELATIONSHIP
                {
                    @Override
                    public void add( ExplicitIndexWrite ops, long entityId, String keyName, Object value )
                            throws KernelException
                    {
                        ops.relationshipAddToExplicitIndex( InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX, entityId,
                                keyName, value );
                    }

                    @Override
                    public void remove( ExplicitIndexWrite ops, long entityId, String keyName, Object value )
                            throws KernelException
                    {
                        ops.relationshipRemoveFromExplicitIndex( InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX,
                                entityId, keyName, value );
                    }

                    @Override
                    public void remove( ExplicitIndexWrite ops, long entityId, String keyName )
                            throws KernelException
                    {
                        ops.relationshipRemoveFromExplicitIndex( InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX,
                                entityId, keyName );
                    }

                    @Override
                    public void remove( ExplicitIndexWrite ops, long entityId )
                            throws KernelException
                    {
                        ops.relationshipRemoveFromExplicitIndex( InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX, entityId );
                    }

                    @Override
                    public void ensureIndexExists( ExplicitIndexWrite ops )
                    {
                        ops.relationshipExplicitIndexCreateLazily( InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX, null );
                    }
                };

        public abstract void add( ExplicitIndexWrite ops, long entityId, String keyName, Object value )
                throws KernelException;

        public abstract void remove( ExplicitIndexWrite ops, long entityId, String keyName, Object value )
                throws KernelException;

        public abstract void remove( ExplicitIndexWrite ops, long entityId, String keyName )
                throws KernelException;

        public abstract void remove( ExplicitIndexWrite ops, long entityId )
                throws KernelException;

        public abstract void ensureIndexExists( ExplicitIndexWrite write );
    }

    private AtomicReference<Set<String>> propertyKeysToInclude = new AtomicReference<>( Collections.emptySet() );

    private final PropertyKeyTokenHolder propertyKeyLookup;
    private final EntityType type;

    private volatile boolean enabled;
    private volatile boolean indexCreated;

    public InternalAutoIndexOperations( PropertyKeyTokenHolder propertyKeyLookup, EntityType type )
    {
        this.propertyKeyLookup = propertyKeyLookup;
        this.type = type;
    }

    @Override
    public void propertyAdded( ExplicitIndexWrite ops, long entityId, int propertyKeyId, Value value ) throws
            AutoIndexingKernelException
    {
        if ( enabled )
        {
            try
            {
                String name = propertyKeyLookup.getTokenById( propertyKeyId ).name();
                if ( propertyKeysToInclude.get().contains( name ) )
                {
                    ensureIndexExists( ops );
                    type.add( ops, entityId, name, value.asObject() );
                }
            }
            catch ( KernelException e )
            {
                throw new AutoIndexingKernelException( e );
            }
            catch ( TokenNotFoundException e )
            {
                // TODO: TokenNotFoundException was added before there was a kernel. It should be converted to a
                // KernelException now
                throw new AutoIndexingKernelException( new PropertyKeyIdNotFoundKernelException( propertyKeyId, e ) );
            }
        }
    }

    @Override
    public void propertyChanged( ExplicitIndexWrite ops, long entityId, int propertyKeyId, Value oldValue,
            Value newValue )
            throws AutoIndexingKernelException
    {
        if ( enabled )
        {
            try
            {
                String name = propertyKeyLookup.getTokenById( propertyKeyId ).name();
                if ( propertyKeysToInclude.get().contains( name ) )
                {
                    ensureIndexExists( ops );
                    type.remove( ops, entityId, name, oldValue.asObject() );
                    type.add( ops, entityId, name, newValue.asObject() );
                }
            }
            catch ( KernelException e )
            {
                throw new AutoIndexingKernelException( e );
            }
            catch ( TokenNotFoundException e )
            {
                // TODO: TokenNotFoundException was added before there was a kernel. It should be converted to a
                // KernelException now
                throw new AutoIndexingKernelException( new PropertyKeyIdNotFoundKernelException( propertyKeyId, e ) );
            }
        }
    }

    @Override
    public void propertyRemoved( ExplicitIndexWrite ops, long entityId, int propertyKey )
            throws AutoIndexingKernelException
    {
        if ( enabled )
        {
            try
            {
                String name = propertyKeyLookup.getTokenById( propertyKey ).name();
                if ( propertyKeysToInclude.get().contains( name ) )
                {
                    ensureIndexExists( ops );
                    type.remove( ops, entityId, name );
                }
            }
            catch ( KernelException e )
            {
                throw new AutoIndexingKernelException( e );
            }
            catch ( TokenNotFoundException e )
            {
                // TODO: TokenNotFoundException was added before there was a kernel. It should be converted to a
                // KernelException now
                throw new AutoIndexingKernelException( new PropertyKeyIdNotFoundKernelException( propertyKey, e ) );
            }
        }
    }

    @Override
    public void entityRemoved( ExplicitIndexWrite ops, long entityId ) throws AutoIndexingKernelException
    {
        if ( enabled )
        {
            try
            {
                ensureIndexExists( ops );
                type.remove( ops, entityId );
            }
            catch ( KernelException e )
            {
                throw new AutoIndexingKernelException( e );
            }
        }
    }

    // Trap door needed to keep this as an enum
    void replacePropertyKeysToInclude( List<String> propertyKeysToIncludeNow )
    {
        Set<String> copiedPropertyKeysToIncludeNow = new HashSet<>( propertyKeysToIncludeNow.size() );
        copiedPropertyKeysToIncludeNow.addAll( propertyKeysToIncludeNow );
        this.propertyKeysToInclude.set( copiedPropertyKeysToIncludeNow );
    }

    @Override
    public void enabled( boolean enabled )
    {
        this.enabled = enabled;
    }

    @Override
    public boolean enabled()
    {
        return enabled;
    }

    @Override
    public void startAutoIndexingProperty( String propName )
    {
        propertyKeysToInclude.getAndUpdate( current ->
        {
            Set<String> updated = new HashSet<>();
            updated.addAll( current );
            updated.add( propName );
            return updated;
        });
    }

    @Override
    public void stopAutoIndexingProperty( String propName )
    {
        propertyKeysToInclude.getAndUpdate( current ->
        {
            Set<String> updated = new HashSet<>();
            updated.addAll( current );
            updated.remove( propName );
            return updated;
        });
    }

    @Override
    public Set<String> getAutoIndexedProperties()
    {
        return Collections.unmodifiableSet( propertyKeysToInclude.get() );
    }

    private void ensureIndexExists( ExplicitIndexWrite ops )
    {
        // Known racy, but this is safe because ensureIndexExists is concurrency safe, we just want to avoid calling it
        // for every single write we make.
        if ( !indexCreated )
        {
            type.ensureIndexExists( ops );
            indexCreated = true;
        }
    }
}
