/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.impl.fulltext.FulltextProvider;
import org.neo4j.kernel.api.impl.fulltext.ReadOnlyFulltext;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.NODES;
import static org.neo4j.kernel.api.impl.fulltext.FulltextProvider.FulltextIndexType.RELATIONSHIPS;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_NODES;
import static org.neo4j.kernel.api.impl.fulltext.integrations.bloom.BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS;
import static org.neo4j.procedure.Mode.READ;

/**
 * Procedures for querying the bloom fulltext addon.
 */
public class BloomProcedures
{
    @Context
    public FulltextProvider provider;

    @Description( "Await the completion of any background index population or updates" )
    @Procedure( name = "db.fulltext.bloomAwaitPopulation", mode = READ )
    public void awaitPopulation() throws Exception
    {
        provider.awaitPopulation();
    }

    @Description( "Returns the property keys indexed by the bloom addon" )
    @Procedure( name = "db.fulltext.bloomFulltextProperties", mode = READ )
    public Stream<PropertyOutput> bloomFulltextProperties() throws Exception
    {
        return provider.getProperties( BLOOM_NODES, NODES ).stream().map( PropertyOutput::new );
    }

    @Description( "Check the status of the bloom addon" )
    @Procedure( name = "db.fulltext.bloomFulltextStatus", mode = READ )
    public Stream<StatusOutput> bloomFulltextStatus() throws Exception
    {
        List<InternalIndexState> states = Arrays.asList( provider.getState( BLOOM_NODES, NODES ), provider.getState( BLOOM_RELATIONSHIPS, RELATIONSHIPS ) );
        return states.stream().map( StatusOutput::new );
    }

    @Description( "Queries the bloom index for nodes" )
    @Procedure( name = "db.fulltext.bloomFulltextNodes", mode = READ )
    public Stream<EntityOutput> bloomFulltextNodes( @Name( "terms" ) List<String> terms, @Name( "fuzzy" ) boolean fuzzy,
            @Name( "Require that all terms are matched" ) boolean matchAll ) throws Exception
    {
        try ( ReadOnlyFulltext indexReader = provider.getReader( BLOOM_NODES, NODES ) )
        {
            return queryAsStream( terms, indexReader, fuzzy, matchAll );
        }
    }

    @Description( "Queries the bloom index for relationships" )
    @Procedure( name = "db.fulltext.bloomFulltextRelationships", mode = READ )
    public Stream<EntityOutput> bloomFulltextRelationships( @Name( "terms" ) List<String> terms, @Name( "fuzzy" ) boolean fuzzy,
            @Name( "Require that all terms are matched" ) boolean matchAll ) throws Exception
    {
        try ( ReadOnlyFulltext indexReader = provider.getReader( BLOOM_RELATIONSHIPS, RELATIONSHIPS ) )
        {
            return queryAsStream( terms, indexReader, fuzzy, matchAll );
        }
    }

    private Stream<EntityOutput> queryAsStream( List<String> terms, ReadOnlyFulltext indexReader, boolean fuzzy, boolean matchAll )
    {
        PrimitiveLongIterator primitiveLongIterator;
        if ( fuzzy )
        {
            primitiveLongIterator = indexReader.fuzzyQuery( terms, matchAll );
        }
        else
        {
            primitiveLongIterator = indexReader.query( terms, matchAll );
        }
        Iterator<EntityOutput> iterator = PrimitiveLongCollections.map( EntityOutput::new, primitiveLongIterator );
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
