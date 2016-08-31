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
package org.neo4j.unsafe.impl.batchimport;

import java.io.IOException;

import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static org.neo4j.unsafe.impl.batchimport.input.InputCache.MAIN;

/**
 * Counts number of relationships per node that is going to be imported by {@link RelationshipStage} later.
 * Dense node threshold is calculated based on these counts, so that correct relationship representation can be written
 * per node. Steps:
 *
 * <ol>
 * <li>{@link InputIteratorBatcherStep} reading from {@link InputIterator} produced from {@link Input#relationships()}.
 * </li>
 * <li>{@link InputEntityCacherStep} alternatively {@link InputCache caches} this input data
 * (all the {@link InputRelationship input relationships}) if the iterator doesn't support
 * {@link InputIterable#supportsMultiplePasses() multiple passes}.</li>
 * <li>{@link RelationshipTypeCheckerStep} keeps track of all different types of all
 * {@link InputRelationship input relationships} so that the upcoming relationship import knows which
 * types to import, i.e. how to split the import.</li>
 * <li>{@link RelationshipPreparationStep} looks up {@link InputRelationship#startNode() start node input id} /
 * {@link InputRelationship#endNode() end node input id} from {@link IdMapper} and attaches to the batches going
 * through because that lookup is costly and this step can be parallelized.</li>
 * <li>{@link CalculateRelationshipsStep} simply counts the input relationships going through and in the
 * end sets that count as high id in relationship store, this to more predictably create secondary record units
 * for those records that require it.</li>
 * <li>For each node id {@link NodeRelationshipCache#incrementCount(long) updates the node->relationship cache}
 * so that in the end we will know how many relationships each node in the import will have and hence also
 * which nodes will have a dense representation in the store.</li>
 * </ol>
 */
public class CalculateDenseNodesStage extends Stage
{
    private RelationshipTypeCheckerStep typer;

    public CalculateDenseNodesStage( Configuration config, InputIterable<InputRelationship> relationships,
            NodeRelationshipCache cache, IdMapper idMapper,
            Collector badCollector, InputCache inputCache,
            BatchingNeoStores neoStores ) throws IOException
    {
        super( "Calculate dense nodes", config );
        add( new InputIteratorBatcherStep<>( control(), config,
                relationships.iterator(), InputRelationship.class ) );
        if ( !relationships.supportsMultiplePasses() )
        {
            add( new InputEntityCacherStep<>( control(), config, inputCache.cacheRelationships( MAIN ) ) );
        }
        add( typer = new RelationshipTypeCheckerStep( control(), config, neoStores.getRelationshipTypeRepository() ) );
        add( new RelationshipPreparationStep( control(), config, idMapper ) );
        add( new CalculateRelationshipsStep( control(), config, neoStores.getRelationshipStore() ) );
        add( new CalculateDenseNodesStep( control(), config, cache, badCollector ) );
    }

    /*
     * @see RelationshipTypeCheckerStep#getRelationshipTypes(int)
     */
    public Object[] getRelationshipTypes( long belowOrEqualToThreshold )
    {
        return typer.getRelationshipTypes( belowOrEqualToThreshold );
    }
}
