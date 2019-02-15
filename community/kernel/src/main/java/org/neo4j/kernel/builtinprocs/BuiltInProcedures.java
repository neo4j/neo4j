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
package org.neo4j.kernel.builtinprocs;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.fromArray;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class BuiltInProcedures
{
    private static final int NOT_EXISTING_INDEX_ID = -1;
    public static final String EXPLICIT_INDEX_DEPRECATION = "This procedure is deprecated by the schema and full-text indexes, and will be removed in 4.0.";
    public static final String DB_SCHEMA_DEPRECATION = "This procedure is deprecated by the db.schema.visualization procedure, and will be removed in 4.0.";

    @Context
    public KernelTransaction tx;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Description( "List all labels in the database and their total count." )
    @Procedure( name = "db.labels", mode = READ )
    public Stream<LabelResult> listLabels()
    {
        List<LabelResult> labelResults =
                TokenAccess.LABELS.inUse( tx ).stream().map( label ->
                {
                    int labelId = tx.tokenRead().nodeLabel( label.name() );
                    return new LabelResult( label, tx.dataRead().countsForNode( labelId ) );
                } ).collect( Collectors.toList() );
        return labelResults.stream();
    }

    @Description( "List all property keys in the database." )
    @Procedure( name = "db.propertyKeys", mode = READ )
    public Stream<PropertyKeyResult> listPropertyKeys()
    {
        List<PropertyKeyResult> propertyKeys =
                TokenAccess.PROPERTY_KEYS.inUse( tx ).stream().map( PropertyKeyResult::new ).collect( Collectors.toList() );
        return propertyKeys.stream();
    }

    @Description( "List all relationship types in the database and their total count." )
    @Procedure( name = "db.relationshipTypes", mode = READ )
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        List<RelationshipTypeResult> relationshipTypes =
                TokenAccess.RELATIONSHIP_TYPES.inUse( tx ).stream().map( type ->
                {
                    int typeId = tx.tokenRead().relationshipType( type.name() );
                    return new RelationshipTypeResult( type, tx.dataRead().countsForRelationship( Read.ANY_LABEL, typeId, Read.ANY_LABEL ) );
                } ).collect( Collectors.toList() );
        return relationshipTypes.stream();
    }

    @Description( "List all indexes in the database." )
    @Procedure( name = "db.indexes", mode = READ )
    public Stream<IndexResult> listIndexes() throws ProcedureException
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            TokenRead tokenRead = tx.tokenRead();
            TokenNameLookup tokens = new SilentTokenNameLookup( tokenRead );
            IndexingService indexingService = resolver.resolveDependency( IndexingService.class );

            SchemaRead schemaRead = tx.schemaRead();
            List<IndexReference> indexes = asList( schemaRead.indexesGetAll() );
            indexes.sort( Comparator.comparing( a -> a.userDescription( tokens ) ) );

            ArrayList<IndexResult> result = new ArrayList<>();
            for ( IndexReference index : indexes )
            {
                try
                {
                    IndexType type = IndexType.getIndexTypeOf( index );

                    SchemaDescriptor schema = index.schema();
                    LongValue indexId = longValue( getIndexId( indexingService, schema ) );
                    ListValue tokenNames = fromArray( stringArray( tokens.entityTokensGetNames( schema.entityType(),
                            schema.getEntityTokenIds() ) ) );
                    ListValue propertyNames = propertyNames( tokens, index );
                    TextValue description = stringValue( "INDEX ON " + schema.userDescription( tokens ) );
                    InternalIndexState internalIndexState = schemaRead.indexGetState( index );
                    TextValue state = stringValue( internalIndexState.toString() );
                    MapValue providerDescriptorMap = indexProviderDescriptorMap( schemaRead.index( schema ) );
                    PopulationProgress progress = schemaRead.indexGetPopulationProgress( index );
                    IndexPopulationProgress indexProgress = new IndexPopulationProgress( progress.getCompleted(), progress.getTotal() );
                    TextValue failureMessage = internalIndexState == InternalIndexState.FAILED ? stringValue( schemaRead.indexGetFailure( index ) ) : Values.EMPTY_STRING;
                    result.add( new IndexResult( indexId,
                                                 description,
                                                 stringValue( index.name() ),
                                                 tokenNames,
                                                 propertyNames,
                                                 state,
                                                 stringValue( type.typeName() ),
                                                 doubleValue( indexProgress.getCompletedPercentage() ),
                                                 providerDescriptorMap,
                                                 failureMessage ) );
                }
                catch ( IndexNotFoundKernelException e )
                {
                    throw new ProcedureException( Status.Schema.IndexNotFound, e,
                            "No index on ", index.userDescription( tokens ) );
                }
            }
            return result.stream();
        }
    }

    @Description( "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\"))." )
    @Procedure( name = "db.awaitIndex", mode = READ )
    public void awaitIndex( @Name( "index" ) String index,
            @Name( value = "timeOutSeconds", defaultValue = "300" ) LongValue timeout )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.awaitIndexByPattern( index, timeout.longValue(), TimeUnit.SECONDS );
        }
    }

    @Description( "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\"))." )
    @Procedure( name = "db.awaitIndexes", mode = READ )
    public void awaitIndexes( @Name( value = "timeOutSeconds", defaultValue = "300" ) LongValue timeout )
    {
        graphDatabaseAPI.schema().awaitIndexesOnline( timeout.longValue(), TimeUnit.SECONDS );
    }

    @Description( "Schedule resampling of an index (for example: CALL db.resampleIndex(\":Person(name)\"))." )
    @Procedure( name = "db.resampleIndex", mode = READ )
    public void resampleIndex( @Name( "index" ) TextValue index ) throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.resampleIndex( index.stringValue() );
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
    public void prepareForReplanning( @Name( value = "timeOutSeconds", defaultValue = "300" ) LongValue timeOutSeconds )
            throws ProcedureException
    {
        //Resample indexes
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.resampleOutdatedIndexes();
            indexProcedures.awaitIndexResampling( timeOutSeconds.longValue() );
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

    @Deprecated
    @Description( "Show the schema of the data." )
    @Procedure( name = "db.schema", mode = READ, deprecatedBy = DB_SCHEMA_DEPRECATION )
    public Stream<SchemaProcedure.GraphResult> schema()
    {
        return schemaVisualization();
    }

    @Description( "Visualize the schema of the data. Replaces db.schema." )
    @Procedure( name = "db.schema.visualization", mode = READ )
    public Stream<SchemaProcedure.GraphResult> schemaVisualization()
    {
        return Stream.of( new SchemaProcedure( graphDatabaseAPI, tx ).buildSchemaGraph() );
    }

    @Description( "List all constraints in the database." )
    @Procedure( name = "db.constraints", mode = READ )
    public Stream<ConstraintResult> listConstraints()
    {

        SchemaRead schemaRead = tx.schemaRead();
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
            @Name( "index" ) TextValue index,
            @Name( "providerName" ) TextValue providerName )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            return indexProcedures.createIndex( index.stringValue(), providerName.stringValue() );
        }
    }

    @Description( "Create a unique property constraint with index backed by specified index provider " +
            "(for example: CALL db.createUniquePropertyConstraint(\":Person(name)\", \"lucene+native-2.0\")) - " +
            "YIELD index, providerName, status" )
    @Procedure( name = "db.createUniquePropertyConstraint", mode = SCHEMA )
    public Stream<BuiltInProcedures.SchemaIndexInfo> createUniquePropertyConstraint(
            @Name( "index" ) TextValue index,
            @Name( "providerName" ) TextValue providerName )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            return indexProcedures.createUniquePropertyConstraint( index.stringValue(), providerName.stringValue() );
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

    private static MapValue indexProviderDescriptorMap( IndexReference indexReference )
    {
        return VirtualValues.map(
                new String[]{"key", "version"},
                new AnyValue[]{stringValue( indexReference.providerKey() ),
                        stringValue( indexReference.providerVersion() )}
        );
    }

    private static ListValue propertyNames( TokenNameLookup tokens, IndexReference index )
    {
        int[] propertyIds = index.properties();
        List<AnyValue> propertyNames = new ArrayList<>( propertyIds.length );
        for ( int propertyId : propertyIds )
        {
            propertyNames.add( stringValue( tokens.propertyKeyGetName( propertyId ) ) );
        }
        return VirtualValues.fromList( propertyNames );
    }

    private static <T> Stream<T> toStream( PrimitiveLongResourceIterator iterator, LongFunction<T> mapper )
    {
        Iterator<T> it = new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public T next()
            {
                return mapper.apply( iterator.next() );
            }
        };

        return Iterators.stream( it, Spliterator.ORDERED );
    }

    private IndexProcedures indexProcedures()
    {
        return new IndexProcedures( tx, resolver.resolveDependency( IndexingService.class ) );
    }

    public static class LabelResult
    {
        public final TextValue label;
        public final LongValue nodeCount;

        private LabelResult( Label label, long nodeCount )
        {
            this.label = stringValue(label.name());
            this.nodeCount = longValue( nodeCount );
        }
    }

    public static class PropertyKeyResult
    {
        public final TextValue propertyKey;

        private PropertyKeyResult( String propertyKey )
        {
            this.propertyKey = stringValue( propertyKey );
        }
    }

    public static class RelationshipTypeResult
    {
        public final TextValue relationshipType;
        public final LongValue relationshipCount;

        private RelationshipTypeResult( RelationshipType relationshipType, long relationshipCount )
        {
            this.relationshipType = stringValue( relationshipType.name() );
            this.relationshipCount = longValue( relationshipCount );
        }
    }

    public static class IndexResult
    {
        public final TextValue description;
        public final TextValue indexName;
        public final ListValue tokenNames;
        public final ListValue properties;
        public final TextValue state;
        public final TextValue type;
        public final DoubleValue progress;
        public final MapValue provider;
        public final LongValue id;
        public final TextValue failureMessage;

        private IndexResult( LongValue id,
                             TextValue description,
                             TextValue indexName,
                             ListValue tokenNames,
                             ListValue properties,
                             TextValue state,
                             TextValue type,
                             DoubleValue progress,
                             MapValue provider,
                             TextValue failureMessage )
        {
            this.id = id;
            this.description = description;
            this.indexName = indexName;
            this.tokenNames = tokenNames;
            this.properties = properties;
            this.state = state;
            this.type = type;
            this.progress = progress;
            this.provider = provider;
            this.failureMessage = failureMessage;
        }
    }

    public static class SchemaIndexInfo
    {
        public final TextValue index;
        public final TextValue providerName;
        public final TextValue status;

        public SchemaIndexInfo( String index, String providerName, String status )
        {
            this.index = stringValue( index );
            this.providerName = stringValue( providerName );
            this.status = stringValue( status );
        }
    }

    public static class ConstraintResult
    {
        public final TextValue description;

        private ConstraintResult( String description )
        {
            this.description = stringValue( description );
        }
    }

    //When we have decided on what to call different indexes
    //this should probably be moved to some more central place
    private enum IndexType
    {
        NODE_LABEL_PROPERTY( "node_label_property" ),
        NODE_UNIQUE_PROPERTY( "node_unique_property" ),
        REL_TYPE_PROPERTY( "relationship_type_property" ),
        NODE_FULLTEXT( "node_fulltext" ),
        RELATIONSHIP_FULLTEXT( "relationship_fulltext" );

        private final String typeName;

        IndexType( String typeName )
        {
            this.typeName = typeName;
        }

        private static IndexType getIndexTypeOf( IndexReference index )
        {
            if ( index.isFulltextIndex() )
            {
                if ( index.schema().entityType() == EntityType.NODE )
                {
                    return IndexType.NODE_FULLTEXT;
                }
                else
                {
                    return IndexType.RELATIONSHIP_FULLTEXT;
                }
            }
            else
            {
                if ( index.isUnique() )
                {
                    return IndexType.NODE_UNIQUE_PROPERTY;
                }
                else
                {
                    if ( index.schema().entityType() == EntityType.NODE )
                    {
                        return IndexType.NODE_LABEL_PROPERTY;
                    }
                    else
                    {
                        return IndexType.REL_TYPE_PROPERTY;
                    }
                }
            }
        }

        public String typeName()
        {
            return typeName;
        }
    }
}
