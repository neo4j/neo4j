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
package org.neo4j.procedure.builtin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class BuiltInProcedures
{
    private static final int NOT_EXISTING_INDEX_ID = -1;
    static final long LONG_FIELD_NOT_CALCULATED = -1;  // the user should not even see this because that column should be filtered away (not yielded)

    @Context
    public KernelTransaction tx;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Context
    public ProcedureCallContext callContext;

    @Description( "List all labels in the database and their total count." )
    @Procedure( name = "db.labels", mode = READ )
    public Stream<LabelResult> listLabels()
    {
        boolean shouldCount = !callContext.isCalledFromCypher() || callContext.outputFields().anyMatch( name -> name.equals( "nodeCount" ) );
        List<LabelResult> labelResults =
                TokenAccess.LABELS.all( tx ).stream().map( label ->
                {
                    int labelId = tx.tokenRead().nodeLabel( label.name() );
                    long count = shouldCount ? tx.dataRead().countsForNode( labelId ) : LONG_FIELD_NOT_CALCULATED;
                    return new LabelResult( label, count );
                } ).collect( Collectors.toList() );
        return labelResults.stream();
    }

    @Description( "List all property keys in the database." )
    @Procedure( name = "db.propertyKeys", mode = READ )
    public Stream<PropertyKeyResult> listPropertyKeys()
    {
        List<PropertyKeyResult> propertyKeys =
                TokenAccess.PROPERTY_KEYS.all( tx ).stream().map( PropertyKeyResult::new ).collect( Collectors.toList() );
        return propertyKeys.stream();
    }

    @Description( "List all relationship types in the database and their total count." )
    @Procedure( name = "db.relationshipTypes", mode = READ )
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        boolean shouldCount = !callContext.isCalledFromCypher() || callContext.outputFields().anyMatch( name -> name.equals( "relationshipCount" ) );
        List<RelationshipTypeResult> relationshipTypes =
                TokenAccess.RELATIONSHIP_TYPES.all( tx ).stream().map( type ->
                {
                    int typeId = tx.tokenRead().relationshipType( type.name() );
                    long count = shouldCount ? tx.dataRead().countsForRelationship( ANY_LABEL, typeId, ANY_LABEL ) : LONG_FIELD_NOT_CALCULATED;
                    return new RelationshipTypeResult( type, count );
                } ).collect( Collectors.toList() );
        return relationshipTypes.stream();
    }

    @Description( "List all indexes in the database." )
    @Procedure( name = "db.indexes", mode = READ )
    public Stream<IndexResult> listIndexes()
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            TokenRead tokenRead = tx.tokenRead();
            TokenNameLookup tokenLookup = new SilentTokenNameLookup( tokenRead );
            IndexingService indexingService = resolver.resolveDependency( IndexingService.class );

            SchemaReadCore schemaRead = tx.schemaRead().snapshot();
            List<IndexDescriptor> indexes = asList( schemaRead.indexesGetAll() );
            indexes.sort( Comparator.comparing( a -> a.schema().userDescription( tokenLookup ) ) );

            ArrayList<IndexResult> result = new ArrayList<>();
            for ( IndexDescriptor index : indexes )
            {
                SchemaDescriptor schema = index.schema();
                long id = getIndexId( indexingService, schema );
                String name = index.getName();
                IndexStatus status = getIndexStatus( schemaRead, index );
                String uniqueness = IndexUniqueness.getUniquenessOf( index );
                String type = IndexType.getIndexTypeOf( index );
                String entityType = IndexEntityType.entityTypeOf( index );
                List<String> labelsOrTypes = Arrays.asList( tokenLookup.entityTokensGetNames( schema.entityType(), schema.getEntityTokenIds() ) );
                List<String> properties = propertyNames( tokenLookup, index );
                String provider = index.getIndexProvider().name();

                // todo Move to indexDetails procedure
                // String description = "INDEX ON " + schema.userDescription( tokens );
                // String failureMessage = internalIndexState == InternalIndexState.FAILED ? schemaRead.indexGetFailure( index ) : "";
                result.add( new IndexResult( id, name, status.state, status.populationProgress, uniqueness, type, entityType, labelsOrTypes, properties,
                        provider ) );
            }
            result.sort( Comparator.comparing( r -> r.name ) );
            return result.stream();
        }
    }

    private static IndexStatus getIndexStatus( SchemaReadCore schemaRead, IndexDescriptor index )
    {
        IndexStatus status = new IndexStatus();
        try
        {
            InternalIndexState internalIndexState = schemaRead.indexGetState( index );
            status.state = internalIndexState.toString();
            PopulationProgress progress = schemaRead.indexGetPopulationProgress( index );
            status.populationProgress = progress.toIndexPopulationProgress().getCompletedPercentage();
            status.failureMessage = internalIndexState == InternalIndexState.FAILED ? schemaRead.indexGetFailure( index ) : "";
        }
        catch ( IndexNotFoundKernelException e )
        {
            status.state = "NOT FOUND";
            status.populationProgress = 0;
            status.failureMessage = "Index not found. It might have been concurrently dropped.";
        }
        return status;
    }

    private static class IndexStatus
    {
        String state;
        String failureMessage;
        float populationProgress;
    }

    @Description( "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\"))." )
    @Procedure( name = "db.awaitIndex", mode = READ )
    public void awaitIndex( @Name( "index" ) String index,
            @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.awaitIndexByPattern( index, timeout, TimeUnit.SECONDS );
        }
    }

    @Description( "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\"))." )
    @Procedure( name = "db.awaitIndexes", mode = READ )
    public void awaitIndexes( @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout )
    {
        graphDatabaseAPI.schema().awaitIndexesOnline( timeout, TimeUnit.SECONDS );
    }

    @Description( "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\"))." )
    @Procedure( name = "db.resampleIndex", mode = READ )
    public void resampleIndex( @Name( "index" ) String index ) throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.resampleIndex( index );
        }
    }

    @Description( "Schedule resampling of all outdated indexes." )
    @Procedure( name = "db.resampleOutdatedIndexes", mode = READ )
    public void resampleOutdatedIndexes()
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.resampleOutdatedIndexes();
        }
    }

    @Admin
    @Description(
            "Triggers an index resample and waits for it to complete, and after that clears query caches. After this " +
            "procedure has finished queries will be planned using the latest database statistics." )
    @Procedure( name = "db.prepareForReplanning", mode = READ )
    public void prepareForReplanning( @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeOutSeconds )
            throws ProcedureException
    {
        //Resample indexes
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.resampleOutdatedIndexes();
            indexProcedures.awaitIndexResampling( timeOutSeconds );
        }

        //now that index-stats are up-to-date, clear caches so that we are ready to re-plan
        graphDatabaseAPI.getDependencyResolver()
                .resolveDependency( QueryExecutionEngine.class, DependencyResolver.SelectionStrategy.FIRST )
                .clearQueryCaches();
    }

    @Procedure( name = "db.schema.nodeTypeProperties", mode = Mode.READ )
    @Description( "Show the derived property schema of the nodes in tabular form." )
    public Stream<NodePropertySchemaInfoResult> nodePropertySchema()
    {
        return new SchemaCalculator( tx ).calculateTabularResultStreamForNodes();
    }

    @Procedure( name = "db.schema.relTypeProperties", mode = Mode.READ )
    @Description( "Show the derived property schema of the relationships in tabular form." )
    public Stream<RelationshipPropertySchemaInfoResult> relationshipPropertySchema()
    {
        return new SchemaCalculator( tx ).calculateTabularResultStreamForRels();
    }

    @Description( "Visualize the schema of the data." )
    @Procedure( name = "db.schema.visualization", mode = READ )
    public Stream<SchemaProcedure.GraphResult> schemaVisualization()
    {
        return Stream.of(
                new SchemaProcedure( graphDatabaseAPI, ((GraphDatabaseFacade) graphDatabaseAPI).TEMP_TOP_LEVEL_TRANSACTION.get() ).buildSchemaGraph() );
    }

    @Description( "List all constraints in the database." )
    @Procedure( name = "db.constraints", mode = READ )
    public Stream<ConstraintResult> listConstraints()
    {

        SchemaReadCore schemaRead = tx.schemaRead().snapshot();
        TokenNameLookup tokens = new SilentTokenNameLookup( tx.tokenRead() );

        return asList( schemaRead.constraintsGetAll() )
                .stream()
                .map( constraint -> constraint.prettyPrint( tokens ) )
                .sorted()
                .map( ConstraintResult::new );
    }

    @Description( "Create a schema index with specified index provider (for example: CALL db.createIndex(\":Person(name)\", \"lucene+native-2.0\")) - " +
            "YIELD index, providerName, status" )
    @Procedure( name = "db.createIndex", mode = SCHEMA )
    public Stream<SchemaIndexInfo> createIndex(
            @Name( "index" ) String index,
            @Name( "providerName" ) String providerName )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            return indexProcedures.createIndex( index, providerName );
        }
    }

    @Description( "Create a unique property constraint with index backed by specified index provider " +
            "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
            "YIELD index, providerName, status" )
    @Procedure( name = "db.createUniquePropertyConstraint", mode = SCHEMA )
    public Stream<BuiltInProcedures.SchemaIndexInfo> createUniquePropertyConstraint(
            @Name( "index" ) String index,
            @Name( "providerName" ) String providerName )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            return indexProcedures.createUniquePropertyConstraint( index, providerName );
        }
    }

    private static long getIndexId( IndexingService indexingService, SchemaDescriptor schema )
    {
        try
        {
            return indexingService.getIndexId( schema );
        }
        catch ( IndexNotFoundKernelException e )
        {
            return NOT_EXISTING_INDEX_ID;
        }
    }

    private static List<String> propertyNames( TokenNameLookup tokens, IndexDescriptor index )
    {
        int[] propertyIds = index.schema().getPropertyIds();
        List<String> propertyNames = new ArrayList<>( propertyIds.length );
        for ( int propertyId : propertyIds )
        {
            propertyNames.add( tokens.propertyKeyGetName( propertyId ) );
        }
        return propertyNames;
    }

    private IndexProcedures indexProcedures()
    {
        return new IndexProcedures( tx, resolver.resolveDependency( IndexingService.class ) );
    }

    public static class LabelResult
    {
        public final String label;
        public final long nodeCount;

        private LabelResult( Label label, long nodeCount )
        {
            this.label = label.name();
            this.nodeCount = nodeCount;
        }
    }

    public static class PropertyKeyResult
    {
        public final String propertyKey;

        private PropertyKeyResult( String propertyKey )
        {
            this.propertyKey = propertyKey;
        }
    }

    public static class RelationshipTypeResult
    {
        public final String relationshipType;
        public final long relationshipCount;

        private RelationshipTypeResult( RelationshipType relationshipType, long relationshipCount )
        {
            this.relationshipType = relationshipType.name();
            this.relationshipCount = relationshipCount;
        }
    }

    public static class BooleanResult
    {
        public BooleanResult( Boolean success )
        {
            this.success = success;
        }

        public final Boolean success;
    }

    public static class IndexResult
    {
        public final long id;                    //1
        public final String name;                //"myIndex"
        public final String state;               //"ONLINE", "FAILED", "POPULATING"
        public final float populationPercent;    // 0.0, 100.0, 75.1
        public final String uniqueness;          //"UNIQUE", "NONUNIQUE"
        public final String type;                //"FULLTEXT", "FUSION", "BTREE"
        public final String entityType;          //"NODE", "RELATIONSHIP"
        public final List<String> labelsOrTypes; //["Label1", "Label2"], ["RelType1", "RelType2"]
        public final List<String> properties;    //["propKey", "propKey2"]
        public final String provider;            //"native-btree-1.0", "lucene+native-3.0"

        private IndexResult( long id, String name, String state, float populationPercent, String uniqueness, String type, String entityType,
                List<String> labelsOrTypes, List<String> properties, String provider )
        {
            this.id = id;
            this.name = name;
            this.state = state;
            this.populationPercent = populationPercent;
            this.uniqueness = uniqueness;
            this.type = type;
            this.entityType = entityType;
            this.labelsOrTypes = labelsOrTypes;
            this.properties = properties;
            this.provider = provider;
        }
    }

    public static class SchemaIndexInfo
    {
        public final String index;
        public final String providerName;
        public final String status;

        public SchemaIndexInfo( String index, String providerName, String status )
        {
            this.index = index;
            this.providerName = providerName;
            this.status = status;
        }
    }

    public static class ConstraintResult
    {
        public final String description;

        private ConstraintResult( String description )
        {
            this.description = description;
        }
    }

    public static class NodeResult
    {
        public NodeResult( Node node )
        {
            this.node = node;
        }

        public final Node node;
    }

    public static class WeightedNodeResult
    {
        public final Node node;
        public final double weight;

        public WeightedNodeResult( Node node, double weight )
        {
            this.node = node;
            this.weight = weight;
        }
    }

    public static class WeightedRelationshipResult
    {
        public final Relationship relationship;
        public final double weight;

        public WeightedRelationshipResult( Relationship relationship, double weight )
        {
            this.relationship = relationship;
            this.weight = weight;
        }
    }

    public static class RelationshipResult
    {
        public RelationshipResult( Relationship relationship )
        {
            this.relationship = relationship;
        }

        public final Relationship relationship;
    }

    //When we have decided on what to call different indexes
    //this should probably be moved to some more central place
    private enum IndexType
    {
        FULLTEXT,
        BTREE,
        FUSION;

        private static String getIndexTypeOf( IndexDescriptor index )
        {
            if ( index.getIndexType() == org.neo4j.internal.schema.IndexType.FULLTEXT )
            {
                return FULLTEXT.name();
            }
            else if ( index.getIndexProvider().getKey().equals( GraphDatabaseSettings.SchemaIndex.NATIVE30.providerKey() ) )
            {
                return FUSION.name();
            }
            else
            {
                return BTREE.name();
            }
        }
    }

    private enum IndexUniqueness
    {
        UNIQUE,
        NONUNIQUE;

        private static String getUniquenessOf( IndexDescriptor index )
        {
            return index.isUnique() ? UNIQUE.name() : NONUNIQUE.name();

        }
    }

    private enum IndexEntityType
    {
        NODE,
        RELATIONSHIP;

        private static String entityTypeOf( IndexDescriptor index )
        {
            if ( index.schema().entityType() == EntityType.NODE )
            {
                return NODE.name();
            }
            else
            {
                return RELATIONSHIP.name();
            }
        }
    }
}
