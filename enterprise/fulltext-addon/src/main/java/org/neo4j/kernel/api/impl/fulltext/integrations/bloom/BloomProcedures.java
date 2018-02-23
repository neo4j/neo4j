/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.integrations.bloom;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.api.impl.fulltext.integrations.kernel.FulltextAccessor;
import org.neo4j.kernel.api.impl.fulltext.lucene.FulltextQueryHelper;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.EntityType;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;

/**
 * Procedures for querying the bloom fulltext addon.
 */
public class BloomProcedures
{
    public static final String BLOOM_RELATIONSHIPS = "bloomRelationships";
    public static final String BLOOM_NODES = "bloomNodes";

    @Context
    public FulltextProvider provider;

    @Context
    public KernelTransaction tx;

    @Context
    public FulltextAccessor accessor;

    @Description( "Await the completion of any background index population or updates" )
    @Procedure( name = "bloom.awaitPopulation", mode = READ )
    public void awaitPopulation() throws Exception
    {
        await( BLOOM_NODES );
        await( BLOOM_RELATIONSHIPS );
    }

    private void await( String indexName ) throws IndexPopulationFailedKernelException
    {
        //TODO real await
        try ( Statement stmt = tx.acquireStatement() )
        {
            //noinspection StatementWithEmptyBody
            IndexDescriptor descriptor = stmt.readOperations().indexGetForName( indexName );
            InternalIndexState state;
            while ( (state = stmt.readOperations().indexGetState( descriptor )) != InternalIndexState.ONLINE )
            {
                if ( state == InternalIndexState.FAILED )
                {
                    StatementTokenNameLookup lookup = new StatementTokenNameLookup( stmt.readOperations() );
                    throw new IndexPopulationFailedKernelException( descriptor.schema(), descriptor.userDescription( lookup ),
                            "Population of index " + indexName + " has failed." );
                }
            }
        }
        catch ( IndexNotFoundKernelException | SchemaRuleNotFoundException e )
        {
            //This is fine
        }
    }

    @Description( "Returns the node property keys indexed by the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.getIndexedNodePropertyKeys", mode = READ )
    public Stream<PropertyOutput> getIndexedNodePropertyKeys() throws Exception
    {
        try ( Statement stmt = tx.acquireStatement() )
        {
            ReadOperations readOperations = stmt.readOperations();
            return accessor.propertyKeyStrings( readOperations.indexGetForName( BLOOM_NODES ) ).map( PropertyOutput::new );
        }
    }

    @Description( "Set the node property keys to index" )
    @Procedure( name = "bloom.setIndexedNodePropertyKeys", mode = SCHEMA )
    public void setIndexedNodePropertyKeys( @Name( "propertyKeys" ) List<String> propertyKeys ) throws Exception
    {
        try ( Statement stmt = tx.acquireStatement() )
        {
            ReadOperations readOperations = stmt.readOperations();
            stmt.schemaWriteOperations().indexDrop( readOperations.indexGetForName( BLOOM_NODES ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            //this is fine
        }
        try ( Statement stmt = tx.acquireStatement() )
        {
            IndexDescriptor indexDescriptor = accessor.indexDescriptorFor( BLOOM_NODES, EntityType.NODE, new String[0], propertyKeys.toArray( new String[0] ) );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( indexDescriptor );
        }
    }

    @Description( "Returns the relationship property keys indexed by the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.getIndexedRelationshipPropertyKeys", mode = READ )
    public Stream<PropertyOutput> getIndexedRelationshipPropertyKeys() throws Exception
    {
        try ( Statement stmt = tx.acquireStatement() )
        {
            ReadOperations readOperations = stmt.readOperations();
            return accessor.propertyKeyStrings( readOperations.indexGetForName( BLOOM_RELATIONSHIPS ) ).map( PropertyOutput::new );
        }
    }

    @Description( "Set the relationship property keys to index" )
    @Procedure( name = "bloom.setIndexedRelationshipPropertyKeys", mode = SCHEMA )
    public void setIndexedRelationshipPropertyKeys( @Name( "propertyKeys" ) List<String> propertyKeys ) throws Exception
    {
        try ( Statement stmt = tx.acquireStatement() )
        {
            ReadOperations readOperations = stmt.readOperations();
            stmt.schemaWriteOperations().indexDrop( readOperations.indexGetForName( BLOOM_RELATIONSHIPS ) );
        }
        catch ( SchemaRuleNotFoundException e )
        {
            //this is fine
        }
        try ( Statement stmt = tx.acquireStatement() )
        {
            IndexDescriptor indexDescriptor =
                    accessor.indexDescriptorFor( BLOOM_RELATIONSHIPS, EntityType.RELATIONSHIP, new String[0], propertyKeys.toArray( new String[0] ) );
            stmt.schemaWriteOperations().nonSchemaIndexCreate( indexDescriptor );
        }
    }

    @Description( "Check the status of the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.indexStatus", mode = READ )
    public Stream<StatusOutput> indexStatus() throws Exception
    {
        InternalIndexState nodeIndexState;
        InternalIndexState relationshipIndexState;
        try ( Statement stmt = tx.acquireStatement() )
        {
            ReadOperations readOperations = stmt.readOperations();
            nodeIndexState = readOperations.indexGetState( readOperations.indexGetForName( BLOOM_NODES ) );
            relationshipIndexState = readOperations.indexGetState( readOperations.indexGetForName( BLOOM_RELATIONSHIPS ) );
        }
        return Stream.of( nodeIndexState, relationshipIndexState ).map( StatusOutput::new );
    }

    @Description( "Query the Bloom fulltext index for nodes" )
    @Procedure( name = "bloom.searchNodes", mode = READ )
    public Stream<EntityOutput> bloomFulltextNodes(
            @Name( "terms" ) List<String> terms,
            @Name( value = "fuzzy", defaultValue = "true" ) boolean fuzzy,
            @Name( value = "matchAll", defaultValue = "false" ) boolean matchAll ) throws Exception
    {
        return queryAsStream( terms, BLOOM_NODES, fuzzy, matchAll );
    }

    @Description( "Query the Bloom fulltext index for relationships" )
    @Procedure( name = "bloom.searchRelationships", mode = READ )
    public Stream<EntityOutput> bloomFulltextRelationships(
            @Name( "terms" ) List<String> terms,
            @Name( value = "fuzzy", defaultValue = "true" ) boolean fuzzy,
            @Name( value = "matchAll", defaultValue = "false" ) boolean matchAll ) throws Exception
    {
        return queryAsStream( terms, BLOOM_RELATIONSHIPS, fuzzy, matchAll );
    }

    private Stream<EntityOutput> queryAsStream( List<String> terms, String indexName, boolean fuzzy, boolean matchAll )
            throws IOException, IndexNotFoundKernelException
    {
        String query = FulltextQueryHelper.createQuery( terms, fuzzy, matchAll );
        Iterator<EntityOutput> iterator = PrimitiveLongCollections.map( EntityOutput::new, accessor.query( indexName, query ) );
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( iterator, Spliterator.ORDERED ), false );
    }

    public static class EntityOutput
    {
        public final long entityid;

        public EntityOutput( long entityid )
        {
            this.entityid = entityid;
        }
    }

    public static class PropertyOutput
    {
        public final String propertyKey;

        public PropertyOutput( String propertykey )
        {
            this.propertyKey = propertykey;
        }
    }

    public class StatusOutput
    {
        public final String state;

        public StatusOutput( InternalIndexState internalIndexState )
        {
            switch ( internalIndexState )
            {
            case POPULATING:
                state = "POPULATING";
                break;
            case ONLINE:
                state = "ONLINE";
                break;
            case FAILED:
                state = "FAILED";
                break;
            default:
                throw new IllegalArgumentException( String.format( "Illegal index state %s", internalIndexState ) );
            }
        }
    }
}
