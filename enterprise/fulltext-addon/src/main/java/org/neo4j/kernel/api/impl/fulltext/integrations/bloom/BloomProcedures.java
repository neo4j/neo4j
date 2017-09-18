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

import java.io.IOException;
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
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

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
    public void awaitPopulation() throws IOException
    {
        provider.awaitPopulation();
    }

    @Description( "Queries the bloom index for nodes" )
    @Procedure( name = "db.fulltext.bloomFulltextNodes", mode = READ )
    public Stream<Output> bloomFulltextNodes( @Name( "terms" ) List<String> terms ) throws Exception
    {
        try ( ReadOnlyFulltext indexReader = provider.getReader( BLOOM_NODES, FulltextProvider.FulltextIndexType.NODES ) )
        {
            return queryAsStream( terms, indexReader );
        }
    }

    @Description( "Queries the bloom index for relationships" )
    @Procedure( name = "db.fulltext.bloomFulltextRelationships", mode = READ )
    public Stream<Output> bloomFulltextRelationships( @Name( "terms" ) List<String> terms ) throws Exception
    {
        try ( ReadOnlyFulltext indexReader = provider.getReader( BLOOM_RELATIONSHIPS, FulltextProvider.FulltextIndexType.RELATIONSHIPS ) )
        {
            return queryAsStream( terms, indexReader );
        }
    }

    private Stream<Output> queryAsStream( List<String> terms, ReadOnlyFulltext indexReader )
    {
        PrimitiveLongIterator primitiveLongIterator = indexReader.fuzzyQuery( terms.toArray( new String[0] ) );
        Iterator<Output> iterator = PrimitiveLongCollections.map( Output::new, primitiveLongIterator );
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( iterator, Spliterator.ORDERED ), false );

    }

    public static class Output
    {
        public long entityid;

        public Output( long entityid )
        {
            this.entityid = entityid;
        }
    }
}
