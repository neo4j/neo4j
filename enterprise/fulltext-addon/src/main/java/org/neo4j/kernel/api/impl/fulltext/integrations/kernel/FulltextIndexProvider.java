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
package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.fulltext.lucene.FulltextFactory;
import org.neo4j.kernel.api.impl.fulltext.lucene.WritableFulltext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.NonSchemaSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.storageengine.api.EntityType;

class FulltextIndexProvider extends IndexProvider<FulltextIndexDescriptor> implements FulltextAccessor
{
    private final FulltextFactory factory;
    private final PropertyKeyTokenHolder propertyKeyTokenHolder;
    private final LabelTokenHolder labelTokenHolder;
    private final RelationshipTypeTokenHolder relationshipTypeTokenHolder;
    private final Map<FulltextIndexDescriptor,FulltextIndexAccessor> accessors;
    private final Map<String,FulltextIndexAccessor> accessorsByName;

    FulltextIndexProvider( Descriptor descriptor, int priority, IndexDirectoryStructure.Factory directoryStructureFactory, FileSystemAbstraction fileSystem,
            String analyzerClassName, PropertyKeyTokenHolder propertyKeyTokenHolder, LabelTokenHolder labelTokenHolder,
            RelationshipTypeTokenHolder relationshipTypeTokenHolder ) throws IOException
    {
        super( descriptor, priority, directoryStructureFactory );
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        this.labelTokenHolder = labelTokenHolder;
        this.relationshipTypeTokenHolder = relationshipTypeTokenHolder;
        factory = new FulltextFactory( fileSystem, directoryStructure().rootDirectory(), analyzerClassName );
        accessors = new HashMap<>();
        accessorsByName = new HashMap<>();
    }

    public PrimitiveLongIterator query( IndexDescriptor descriptor, String query ) throws IOException
    {
        return accessors.get( descriptor ).query(query);
    }

    @Override
    public FulltextIndexDescriptor indexDescriptorFor( SchemaDescriptor schema, String name )
    {
        try
        {
            return new FulltextIndexDescriptor( schema, name, propertyKeyTokenHolder );
        }
        catch ( TokenNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public IndexPopulator getPopulator( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        WritableFulltext fulltextIndex = new WritableFulltext(factory.createFulltextIndex( descriptor ));
        return new FulltextIndexPopulator( indexId, descriptor, samplingConfig, fulltextIndex );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, FulltextIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        WritableFulltext fulltextIndex = new WritableFulltext(factory.createFulltextIndex( descriptor ));
        fulltextIndex.open();

        FulltextIndexAccessor fulltextIndexAccessor = new FulltextIndexAccessor( fulltextIndex, descriptor );
        accessors.put( descriptor, fulltextIndexAccessor );
        accessorsByName.put( descriptor.identifier(), fulltextIndexAccessor );
        return fulltextIndexAccessor;
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        return "TODO Failure";
    }

    @Override
    public InternalIndexState getInitialState( long indexId, FulltextIndexDescriptor descriptor )
    {
        System.out.println( "internal index state" );
        return InternalIndexState.ONLINE;
    }

    @Override
    public IndexCapability getCapability( FulltextIndexDescriptor indexDescriptor )
    {
        return IndexCapability.NO_CAPABILITY;
    }

    @Override
    public boolean compatible( IndexDescriptor indexDescriptor )
    {
        return indexDescriptor instanceof FulltextIndexDescriptor;
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        //TODO store migration
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    @Override
    public Stream<String> propertyKeyStrings( IndexDescriptor descriptor )
    {
        return Arrays.stream( descriptor.schema().getPropertyIds() ).mapToObj( id -> propertyKeyTokenHolder.getTokenByIdOrNull( id ).name() );
    }

    @Override
    public IndexDescriptor indexDescriptorFor( String name, EntityType type, String[] entityTokens, String... properties )
    {
        int[] entityTokenIds;
        if ( type== EntityType.NODE )
        {
            entityTokenIds = Arrays.stream( entityTokens ).mapToInt( labelTokenHolder::getOrCreateId ).toArray();
        }
        else {
            entityTokenIds = Arrays.stream( entityTokens ).mapToInt( relationshipTypeTokenHolder::getOrCreateId ).toArray();
        }
        int[] propertyIds = Arrays.stream(properties).mapToInt( propertyKeyTokenHolder::getOrCreateId ).toArray();
        return indexDescriptorFor( new NonSchemaSchemaDescriptor( entityTokenIds, type, propertyIds ), name );
    }

    @Override
    public PrimitiveLongIterator query( String indexName, String queryString ) throws IOException
    {
        return accessorsByName.get( indexName ).query( queryString );
    }
}
