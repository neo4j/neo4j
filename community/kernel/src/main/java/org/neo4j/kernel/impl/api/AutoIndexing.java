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
package org.neo4j.kernel.impl.api;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.legacyindex.AutoIndexingKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;

import static org.neo4j.kernel.impl.api.AutoIndexing.AutoIndexOperations.EntityType.NODE;
import static org.neo4j.kernel.impl.api.AutoIndexing.AutoIndexOperations.EntityType.RELATIONSHIP;

/**
 * This gets notified whenever there are changes to entities and their properties, and given a runtime-configurable set of rules
 * then automatically triggers writes to two special legacy indexes - eg. it automatically keeps these indexes up to date.
 */
public class AutoIndexing
{
    public static final String NODE_AUTO_INDEX = "node_auto_index";
    public static final String RELATIONSHIP_AUTO_INDEX = "relationship_auto_index";

    private final AutoIndexOperations nodes;
    private final AutoIndexOperations relationships;

    public AutoIndexing( Config config, PropertyKeyTokenHolder propertyKeyLookup )
    {
        this.nodes = new AutoIndexOperations( propertyKeyLookup, NODE );
        this.relationships = new AutoIndexOperations( propertyKeyLookup, RELATIONSHIP );

        this.nodes.enabled( config.get( GraphDatabaseSettings.node_auto_indexing ) );
        this.nodes.propertyKeysToInclude.addAll( config.get( GraphDatabaseSettings.node_keys_indexable ) );
        this.relationships.enabled( config.get( GraphDatabaseSettings.relationship_auto_indexing ) );
        this.relationships.propertyKeysToInclude.addAll( config.get( GraphDatabaseSettings.relationship_keys_indexable ) );
    }

    public AutoIndexOperations nodes()
    {
        return nodes;
    }

    public AutoIndexOperations relationships()
    {
        return relationships;
    }

    public static class AutoIndexOperations
    {
        enum EntityType
        {
            NODE
            {
                @Override
                public void add( DataWriteOperations ops, long entityId, String keyName, Object value )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.nodeAddToLegacyIndex( NODE_AUTO_INDEX, entityId, keyName, value );
                }

                @Override
                public void remove( DataWriteOperations ops, long entityId, String keyName, Object value )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.nodeRemoveFromLegacyIndex( NODE_AUTO_INDEX, entityId, keyName, value );
                }

                @Override
                public void remove( DataWriteOperations ops, long entityId, String keyName )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.nodeRemoveFromLegacyIndex( NODE_AUTO_INDEX, entityId, keyName );
                }

                @Override
                public void remove( DataWriteOperations ops, long entityId )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.nodeRemoveFromLegacyIndex( NODE_AUTO_INDEX, entityId );
                }

                @Override
                public void ensureIndexExists( DataWriteOperations ops ) throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.nodeLegacyIndexCreateLazily( NODE_AUTO_INDEX, null );
                }
            },
            RELATIONSHIP
            {
                @Override
                public void add( DataWriteOperations ops, long entityId, String keyName, Object value )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.relationshipAddToLegacyIndex( RELATIONSHIP_AUTO_INDEX, entityId, keyName, value );
                }

                @Override
                public void remove( DataWriteOperations ops, long entityId, String keyName, Object value )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.relationshipRemoveFromLegacyIndex( RELATIONSHIP_AUTO_INDEX, entityId, keyName, value );
                }

                @Override
                public void remove( DataWriteOperations ops, long entityId, String keyName )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.relationshipRemoveFromLegacyIndex( RELATIONSHIP_AUTO_INDEX, entityId, keyName );
                }

                @Override
                public void remove( DataWriteOperations ops, long entityId )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.relationshipRemoveFromLegacyIndex( RELATIONSHIP_AUTO_INDEX, entityId );
                }

                @Override
                public void ensureIndexExists( DataWriteOperations ops )
                        throws LegacyIndexNotFoundKernelException, EntityNotFoundException
                {
                    ops.relationshipLegacyIndexCreateLazily( RELATIONSHIP_AUTO_INDEX, null );
                }
            };

            public abstract void add( DataWriteOperations ops, long entityId, String keyName, Object value )
                    throws LegacyIndexNotFoundKernelException, EntityNotFoundException;

            public abstract void remove( DataWriteOperations ops, long entityId, String keyName, Object value )
                    throws LegacyIndexNotFoundKernelException, EntityNotFoundException;

            public abstract void remove( DataWriteOperations ops, long entityId, String keyName )
                    throws LegacyIndexNotFoundKernelException, EntityNotFoundException;

            public abstract void remove( DataWriteOperations ops, long entityId )
                    throws LegacyIndexNotFoundKernelException, EntityNotFoundException;

            public abstract void ensureIndexExists( DataWriteOperations ops )
                    throws LegacyIndexNotFoundKernelException, EntityNotFoundException;
        }

        private final Set<String> propertyKeysToInclude = new HashSet<>();
        private final PropertyKeyTokenHolder propertyKeyLookup;
        private final EntityType type;

        private volatile boolean enabled;
        private volatile boolean indexCreated;

        AutoIndexOperations( PropertyKeyTokenHolder propertyKeyLookup, EntityType type )
        {
            this.propertyKeyLookup = propertyKeyLookup;
            this.type = type;
        }

        public void propertyAdded( DataWriteOperations ops, long entityId, Property property ) throws AutoIndexingKernelException
        {
            if ( enabled )
            {
                try
                {
                    String name = propertyKeyLookup.getTokenById( property.propertyKeyId() ).name();
                    if ( propertyKeysToInclude.contains( name ) )
                    {
                        ensureIndexExists( ops );
                        type.add( ops, entityId, name, property.value() );
                    }
                }
                catch ( LegacyIndexNotFoundKernelException | EntityNotFoundException | PropertyNotFoundException e )
                {
                    throw new AutoIndexingKernelException( e );
                }
                catch ( TokenNotFoundException e )
                {
                    // TODO: TokenNotFoundException was added before there was a kernel. It should be converted to a KernelException now
                    throw new AutoIndexingKernelException( new PropertyKeyIdNotFoundKernelException( property.propertyKeyId(), e ) );
                }
            }
        }

        public void propertyChanged( DataWriteOperations ops, long entityId, Property oldProperty, Property newProperty )
                throws AutoIndexingKernelException
        {
            if ( enabled )
            {
                try
                {
                    String name = propertyKeyLookup.getTokenById( oldProperty.propertyKeyId() ).name();
                    if ( propertyKeysToInclude.contains( name ) )
                    {
                        ensureIndexExists( ops );
                        type.remove( ops, entityId, name, oldProperty.value() );
                        type.add( ops, entityId, name, newProperty.value() );
                    }
                }
                catch ( LegacyIndexNotFoundKernelException | EntityNotFoundException | PropertyNotFoundException e )
                {
                    throw new AutoIndexingKernelException( e );
                }
                catch ( TokenNotFoundException e )
                {
                    // TODO: TokenNotFoundException was added before there was a kernel. It should be converted to a KernelException now
                    throw new AutoIndexingKernelException( new PropertyKeyIdNotFoundKernelException( oldProperty.propertyKeyId(), e ) );
                }
            }
        }

        public void propertyRemoved( DataWriteOperations ops, long entityId, int propertyKey )
                throws AutoIndexingKernelException
        {
            if ( enabled )
            {
                try
                {
                    String name = propertyKeyLookup.getTokenById( propertyKey ).name();
                    if ( propertyKeysToInclude.contains( name ) )
                    {
                        ensureIndexExists( ops );
                        type.remove( ops, entityId );
                    }
                }
                catch ( LegacyIndexNotFoundKernelException | EntityNotFoundException e )
                {
                    throw new AutoIndexingKernelException( e );
                }
                catch ( TokenNotFoundException e )
                {
                    // TODO: TokenNotFoundException was added before there was a kernel. It should be converted to a KernelException now
                    throw new AutoIndexingKernelException( new PropertyKeyIdNotFoundKernelException( propertyKey, e ) );
                }
            }
        }

        public void entityRemoved( DataWriteOperations ops, long entityId ) throws AutoIndexingKernelException
        {
            if( enabled )
            {
                try
                {
                    ensureIndexExists( ops );
                    type.remove( ops, entityId );
                }
                catch ( LegacyIndexNotFoundKernelException | EntityNotFoundException e )
                {
                    throw new AutoIndexingKernelException( e );
                }
            }
        }

        public void enabled( boolean enabled )
        {
            this.enabled = enabled;
        }

        public boolean enabled()
        {
            return enabled;
        }

        public void startAutoIndexingProperty( String propName )
        {
            propertyKeysToInclude.add(propName);
        }

        public void stopAutoIndexingProperty( String propName )
        {
            propertyKeysToInclude.remove( propName );
        }

        public Set<String> getAutoIndexedProperties()
        {
            return Collections.unmodifiableSet( propertyKeysToInclude );
        }

        private void ensureIndexExists( DataWriteOperations ops ) throws LegacyIndexNotFoundKernelException, EntityNotFoundException
        {
            // Known racy, but this is safe because ensureIndexExists is concurrency safe, we just want to avoid calling it
            // for every single write we make.
            if( !indexCreated )
            {
                type.ensureIndexExists( ops );
                indexCreated = true;
            }
        }
    }
}
