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

import org.eclipse.collections.api.tuple.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.helpers.collection.Iterators.stream;
import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.kernel.impl.api.TokenAccess.LABELS;
import static org.neo4j.kernel.impl.api.TokenAccess.PROPERTY_KEYS;
import static org.neo4j.kernel.impl.api.TokenAccess.RELATIONSHIP_TYPES;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class BuiltInProcedures
{
    private static final int NOT_EXISTING_INDEX_ID = -1;
    static final long LONG_FIELD_NOT_CALCULATED = -1;  // the user should not even see this because that column should be filtered away (not yielded)

    @Context
    public KernelTransaction kernelTransaction;

    @Context
    public Transaction transaction;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Context
    public ProcedureCallContext callContext;

    @SystemProcedure
    @Description( "List all labels in the database and their total count." )
    @Procedure( name = "db.labels", mode = READ )
    public Stream<LabelResult> listLabels()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        boolean shouldCount = !callContext.isCalledFromCypher() || callContext.outputFields().anyMatch( name -> name.equals( "nodeCount" ) );
        List<LabelResult> labelResults = stream( LABELS.all( kernelTransaction ) ).map( label ->
                {
                    int labelId = kernelTransaction.tokenRead().nodeLabel( label.name() );
                    long count = shouldCount ? kernelTransaction.dataRead().countsForNode( labelId ) : LONG_FIELD_NOT_CALCULATED;
                    return new LabelResult( label, count );
                } ).collect( Collectors.toList() );
        return labelResults.stream();
    }

    @SystemProcedure
    @Description( "List all property keys in the database." )
    @Procedure( name = "db.propertyKeys", mode = READ )
    public Stream<PropertyKeyResult> listPropertyKeys()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        List<PropertyKeyResult> propertyKeys = stream( PROPERTY_KEYS.all( kernelTransaction ) ).map( PropertyKeyResult::new ).collect( Collectors.toList() );
        return propertyKeys.stream();
    }

    @SystemProcedure
    @Description( "List all relationship types in the database and their total count." )
    @Procedure( name = "db.relationshipTypes", mode = READ )
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        boolean shouldCount = !callContext.isCalledFromCypher() || callContext.outputFields().anyMatch( name -> name.equals( "relationshipCount" ) );
        List<RelationshipTypeResult> relationshipTypes = stream( RELATIONSHIP_TYPES.all( kernelTransaction ) ).map( type ->
                {
                    int typeId = kernelTransaction.tokenRead().relationshipType( type.name() );
                    long count = shouldCount ? kernelTransaction.dataRead().countsForRelationship( ANY_LABEL, typeId, ANY_LABEL ) : LONG_FIELD_NOT_CALCULATED;
                    return new RelationshipTypeResult( type, count );
                } ).collect( Collectors.toList() );
        return relationshipTypes.stream();
    }

    @SystemProcedure
    @Description( "List all indexes in the database." )
    @Procedure( name = "db.indexes", mode = READ )
    public Stream<IndexResult> listIndexes()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        TokenRead tokenRead = kernelTransaction.tokenRead();
        TokenNameLookup tokenLookup = new SilentTokenNameLookup( tokenRead );
        IndexingService indexingService = resolver.resolveDependency( IndexingService.class );

        SchemaReadCore schemaRead = kernelTransaction.schemaRead().snapshot();
        List<IndexDescriptor> indexes = asList( schemaRead.indexesGetAll() );

        ArrayList<IndexResult> result = new ArrayList<>();
        for ( IndexDescriptor index : indexes )
        {
            IndexResult indexResult;
            indexResult = asIndexResult( tokenLookup, schemaRead, index );
            result.add( indexResult );
        }
        result.sort( Comparator.comparing( r -> r.name ) );
        return result.stream();
    }

    @SystemProcedure
    @Description( "Detailed description of specific index." )
    @Procedure( name = "db.indexDetails", mode = READ )
    public Stream<IndexDetailResult> indexDetails( @Name( "indexName" ) String indexName ) throws ProcedureException
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        TokenRead tokenRead = kernelTransaction.tokenRead();
        TokenNameLookup tokenLookup = new SilentTokenNameLookup( tokenRead );
        IndexingService indexingService = resolver.resolveDependency( IndexingService.class );

        SchemaReadCore schemaRead = kernelTransaction.schemaRead().snapshot();
        List<IndexDescriptor> indexes = asList( schemaRead.indexesGetAll() );
        IndexDescriptor index = null;
        for ( IndexDescriptor candidate : indexes )
        {
            if ( candidate.getName().equals( indexName ) )
            {
                index = candidate;
                break;
            }
        }
        if ( index == null )
        {
            throw new ProcedureException( Status.Schema.IndexNotFound, "Could not find index with name \"" + indexName + "\"" );
        }

        final IndexDetailResult indexDetailResult = asIndexDetails( tokenLookup, schemaRead, index );
        return Stream.of( indexDetailResult );
    }

    @SystemProcedure
    @Description( "List all statements for creating and dropping existing indexes and constraints." )
    @Procedure( name = "db.schemaStatements", mode = READ )
    public Stream<SchemaStatementResult> schemaStatements() throws ProcedureException
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        SchemaReadCore schemaRead = kernelTransaction.schemaRead().snapshot();
        final TokenRead tokenRead = kernelTransaction.tokenRead();
        Map<String,SchemaStatementResult> schemaStatements = new HashMap<>();

        // Indexes
        // If index is backing an existing constraint, it will be overwritten later.
        final Iterator<IndexDescriptor> allIndexes = schemaRead.indexesGetAll();
        while ( allIndexes.hasNext() )
        {
            final IndexDescriptor index = allIndexes.next();
            final String name = index.getName();
            final String createStatement = createStatement( tokenRead, index );
            final String dropStatement = dropStatement( index );
            schemaStatements.put( name, new SchemaStatementResult( name, "INDEX", createStatement, dropStatement ) );
        }

        // Constraints
        Iterator<ConstraintDescriptor> allConstraints = schemaRead.constraintsGetAll();
        while ( allConstraints.hasNext() )
        {
            ConstraintDescriptor constraint = allConstraints.next();
            String name = constraint.getName();
            String createStatement = createStatement( tokenRead, constraint );
            String dropStatement = dropStatement( constraint );
            schemaStatements.put( name, new SchemaStatementResult( name, "CONSTRAINT", createStatement, dropStatement ) );
        }

        return schemaStatements.values().stream();
    }

    private String createStatement( TokenRead tokenRead, ConstraintDescriptor constraint ) throws ProcedureException
    {
        try
        {
            String name = constraint.getName();
            if ( constraint.isIndexBackedConstraint() )
            {
                final String labelsOrRelTypes = labelsOrRelTypesAsStringArray( tokenRead, constraint.schema() );
                final String properties = propertiesAsStringArray( tokenRead, constraint.schema() );
                IndexBackedConstraintDescriptor indexBackedConstraint = constraint.asIndexBackedConstraint();
                IndexDescriptor backingIndex = kernelTransaction.schemaRead().indexGetForName( name );
                String providerName = backingIndex.getIndexProvider().name();
                String config = btreeConfigAsString( backingIndex );
                if ( constraint.isUniquenessConstraint() )
                {
                    return format( "CALL db.createUniquePropertyConstraint( '%s', %s, %s, '%s', %s )",
                            name, labelsOrRelTypes, properties, providerName, config );
                }
                if ( constraint.isNodeKeyConstraint() )
                {
                    return format( "CALL db.createNodeKey( '%s', %s, %s, '%s', %s )",
                            name, labelsOrRelTypes, properties, providerName, config );
                }
            }
            if ( constraint.isNodePropertyExistenceConstraint() )
            {
                // "create CONSTRAINT ON (a:A) ASSERT exists(a.p)"
                int labelId = constraint.schema().getLabelId();
                String label = tokenRead.nodeLabelName( labelId );
                int propertyId = constraint.schema().getPropertyId();
                String property = tokenRead.propertyKeyName( propertyId );
                return format( "CREATE CONSTRAINT `%s` ON (a:`%s`) ASSERT exists(a.`%s`)",
                        name, label, property );
            }
            if ( constraint.isRelationshipPropertyExistenceConstraint() )
            {
                // "create CONSTRAINT ON ()-[r:R]-() ASSERT exists(r.p)"
                int relationshipTypeId = constraint.schema().getRelTypeId();
                String relationshipType = tokenRead.relationshipTypeName( relationshipTypeId );
                int propertyId = constraint.schema().getPropertyId();
                String property = tokenRead.propertyKeyName( propertyId );
                return format( "CREATE CONSTRAINT `%s` ON ()-[a:`%s`]-() ASSERT exists(a.`%s`)",
                        name, relationshipType, property );
            }
            throw new IllegalArgumentException( "Did not recognize constraint type " + constraint );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.General.UnknownError, e, "Failed to re-create create statement." );
        }
    }

    private String dropStatement( ConstraintDescriptor constraint )
    {
        return "DROP CONSTRAINT `" + constraint.getName() + "`";
    }

    private String createStatement( TokenRead tokenRead, IndexDescriptor indexDescriptor ) throws ProcedureException
    {
        try
        {
            final String name = indexDescriptor.getName();
            final String labelsOrRelTypes = labelsOrRelTypesAsStringArray( tokenRead, indexDescriptor.schema() );
            final String properties = propertiesAsStringArray( tokenRead, indexDescriptor.schema() );
            switch ( indexDescriptor.getIndexType() )
            {
            case BTREE:
                String btreeConfig = btreeConfigAsString( indexDescriptor );
                final String providerName = indexDescriptor.getIndexProvider().name();
                return format( "CALL db.createIndex('%s', %s, %s, '%s', %s)",
                        name, labelsOrRelTypes, properties, providerName, btreeConfig );
            case FULLTEXT:
                String fulltextConfig = fulltextConfigAsString( indexDescriptor );
                switch ( indexDescriptor.schema().entityType() )
                {
                case NODE:
                    return format( "CALL db.index.fulltext.createNodeIndex('%s', %s, %s, %s)",
                            name, labelsOrRelTypes, properties, fulltextConfig );
                case RELATIONSHIP:
                    return format( "CALL db.index.fulltext.createRelationshipIndex('%s', %s, %s, %s)",
                            name, labelsOrRelTypes, properties, fulltextConfig );
                default:
                    throw new IllegalArgumentException( "Did not recognize entity type " + indexDescriptor.schema().entityType() );
                }
            default:
                throw new IllegalArgumentException( "Did not recognize index type " + indexDescriptor.getIndexType() );
            }
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.General.UnknownError, e, "Failed to re-create create statement." );
        }
    }

    private String btreeConfigAsString( IndexDescriptor indexDescriptor )
    {
        final IndexConfig indexConfig = indexDescriptor.getIndexConfig();
        StringJoiner configString = new StringJoiner( ",", "{", "}" );
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String singleConfig = "`" + entry.getOne() + "`: " + btreeConfigValueAsString( entry.getTwo() );
            configString.add( singleConfig );
        }
        return configString.toString();
    }

    private String btreeConfigValueAsString( Value configValue )
    {
        String valueString = "";
        if ( configValue instanceof DoubleArray )
        {
            final DoubleArray doubleArray = (DoubleArray) configValue;
            return Arrays.toString( doubleArray.asObjectCopy() );
        }
        if ( configValue instanceof IntValue )
        {
            final IntValue intValue = (IntValue) configValue;
            return "" + intValue.value();
        }
        if ( configValue instanceof BooleanValue )
        {
            final BooleanValue booleanValue = (BooleanValue) configValue;
            return "" + booleanValue.booleanValue();
        }
        if ( configValue instanceof StringValue )
        {
            final StringValue stringValue = (StringValue) configValue;
            return "'" + stringValue.stringValue() + "'";
        }
        throw new IllegalArgumentException( "Could not convert config value '" + configValue + "' to config string." );
    }

    private String fulltextConfigAsString( IndexDescriptor indexDescriptor )
    {
        final IndexConfig indexConfig = indexDescriptor.getIndexConfig();
        StringJoiner configString = new StringJoiner( ",", "{", "}" );
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String key = entry.getOne();
            key = key.replace( "fulltext.", "" );
            String singleConfig = "`" + key + "`: " + fulltextConfigValueAsString( entry.getTwo() );
            configString.add( singleConfig );
        }
        return configString.toString();
    }

    private String fulltextConfigValueAsString( Value configValue )
    {
        String valueString = "";
        if ( configValue instanceof BooleanValue )
        {
            final BooleanValue booleanValue = (BooleanValue) configValue;
            return "'" + booleanValue.booleanValue() + "'";
        }
        if ( configValue instanceof StringValue )
        {
            final StringValue stringValue = (StringValue) configValue;
            return "'" + stringValue.stringValue() + "'";
        }
        throw new IllegalArgumentException( "Could not convert config value '" + configValue + "' to config string." );
    }

    private String propertiesAsStringArray( TokenRead tokenRead, SchemaDescriptor schema ) throws PropertyKeyIdNotFoundKernelException
    {
        StringJoiner properties = new StringJoiner( ", ", "[", "]" );
        for ( int propertyId : schema.getPropertyIds() )
        {
            properties.add( "'" + tokenRead.propertyKeyName( propertyId ) + "'" );
        }
        return properties.toString();
    }

    private String labelsOrRelTypesAsStringArray( TokenRead tokenRead, SchemaDescriptor schema ) throws KernelException
    {
        StringJoiner labelsOrRelTypes = new StringJoiner( ", ", "[", "]" );
        for ( int entityTokenId : schema.getEntityTokenIds() )
        {
            if ( schema.entityType().equals( EntityType.NODE ) )
            {
                labelsOrRelTypes.add( "'" + tokenRead.nodeLabelName( entityTokenId ) + "'" );
            }
            else
            {
                labelsOrRelTypes.add( "'" + tokenRead.relationshipTypeName( entityTokenId ) + "'" );
            }
        }
        return labelsOrRelTypes.toString();
    }

    private String dropStatement( IndexDescriptor indexDescriptor )
    {
        return "DROP INDEX `" + indexDescriptor.getName() + "`";
    }

    private IndexResult asIndexResult( TokenNameLookup tokenLookup, SchemaReadCore schemaRead, IndexDescriptor index )
    {
        SchemaDescriptor schema = index.schema();
        long id = index.getId();
        String name = index.getName();
        IndexStatus status = getIndexStatus( schemaRead, index );
        String uniqueness = IndexUniqueness.getUniquenessOf( index );
        String type = index.getIndexType().name();
        String entityType = index.schema().entityType().name();
        List<String> labelsOrTypes = Arrays.asList( tokenLookup.entityTokensGetNames( schema.entityType(), schema.getEntityTokenIds() ) );
        List<String> properties = propertyNames( tokenLookup, index );
        String provider = index.getIndexProvider().name();

        return new IndexResult( id, name, status.state, status.populationProgress, uniqueness, type, entityType, labelsOrTypes, properties, provider );
    }

    private IndexDetailResult asIndexDetails( TokenNameLookup tokenLookup, SchemaReadCore schemaRead, IndexDescriptor index )
    {
        long id = index.getId();
        String name = index.getName();
        IndexStatus status = getIndexStatus( schemaRead, index );
        String uniqueness = IndexUniqueness.getUniquenessOf( index );
        String type = index.getIndexType().name();
        String entityType = index.schema().entityType().name();
        SchemaDescriptor schema = index.schema();
        List<String> labelsOrTypes = Arrays.asList( tokenLookup.entityTokensGetNames( schema.entityType(), schema.getEntityTokenIds() ) );
        List<String> properties = propertyNames( tokenLookup, index );
        String provider = index.getIndexProvider().name();
        Map<String,Object> indexConfig = asObjectMap( index.getIndexConfig().asMap() );

        return new IndexDetailResult( id, name, status.state, status.populationProgress, uniqueness, type, entityType, labelsOrTypes, properties, provider,
                indexConfig, status.failureMessage );
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
            status.populationProgress = 0D;
            status.failureMessage = "Index not found. It might have been concurrently dropped.";
        }
        return status;
    }

    private static Map<String,Object> asObjectMap( Map<String,Value> valueConfig )
    {
        Map<String,Object> objectConfig = new HashMap<>();
        for ( String key : valueConfig.keySet() )
        {
            objectConfig.put( key, valueConfig.get( key ).asObject() );
        }
        return objectConfig;
    }

    private static class IndexStatus
    {
        String state;
        String failureMessage;
        double populationProgress;
    }

    @SystemProcedure
    @Description( "Wait for an index to come online (for example: CALL db.awaitIndex(\"MyIndex\", 300))." )
    @Procedure( name = "db.awaitIndex", mode = READ )
    public void awaitIndex( @Name( "indexName" ) String indexName,
            @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout )
            throws ProcedureException
    {
        if ( callContext.isSystemDatabase() )
        {
            return;
        }
        IndexProcedures indexProcedures = indexProcedures();
        indexProcedures.awaitIndexByName( indexName, timeout, TimeUnit.SECONDS );
    }

    @SystemProcedure
    @Description( "Wait for all indexes to come online (for example: CALL db.awaitIndexes(300))." )
    @Procedure( name = "db.awaitIndexes", mode = READ )
    public void awaitIndexes( @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout )
    {
        if ( callContext.isSystemDatabase() )
        {
            return;
        }

        transaction.schema().awaitIndexesOnline( timeout, TimeUnit.SECONDS );
    }

    @SystemProcedure
    @Description( "Schedule resampling of an index (for example: CALL db.resampleIndex(\"MyIndex\"))." )
    @Procedure( name = "db.resampleIndex", mode = READ )
    public void resampleIndex( @Name( "indexName" ) String indexName ) throws ProcedureException
    {
        if ( callContext.isSystemDatabase() )
        {
            return;
        }

        IndexProcedures indexProcedures = indexProcedures();
        indexProcedures.resampleIndex( indexName );
    }

    @SystemProcedure
    @Description( "Schedule resampling of all outdated indexes." )
    @Procedure( name = "db.resampleOutdatedIndexes", mode = READ )
    public void resampleOutdatedIndexes()
    {
        if ( callContext.isSystemDatabase() )
        {
            return;
        }

        IndexProcedures indexProcedures = indexProcedures();
        indexProcedures.resampleOutdatedIndexes();
    }

    @Admin
    @SystemProcedure
    @Description(
            "Triggers an index resample and waits for it to complete, and after that clears query caches. After this " +
            "procedure has finished queries will be planned using the latest database statistics." )
    @Procedure( name = "db.prepareForReplanning", mode = READ )
    public void prepareForReplanning( @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeOutSeconds )
            throws ProcedureException
    {
        if ( callContext.isSystemDatabase() )
        {
            return;
        }

        //Resample indexes
        IndexProcedures indexProcedures = indexProcedures();
        indexProcedures.resampleOutdatedIndexes();
        indexProcedures.awaitIndexResampling( timeOutSeconds );

        //now that index-stats are up-to-date, clear caches so that we are ready to re-plan
        graphDatabaseAPI.getDependencyResolver()
                .resolveDependency( QueryExecutionEngine.class, DependencyResolver.SelectionStrategy.FIRST )
                .clearQueryCaches();
    }

    @SystemProcedure
    @Procedure( name = "db.schema.nodeTypeProperties", mode = Mode.READ )
    @Description( "Show the derived property schema of the nodes in tabular form." )
    public Stream<NodePropertySchemaInfoResult> nodePropertySchema()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        return new SchemaCalculator( kernelTransaction ).calculateTabularResultStreamForNodes();
    }

    @SystemProcedure
    @Procedure( name = "db.schema.relTypeProperties", mode = Mode.READ )
    @Description( "Show the derived property schema of the relationships in tabular form." )
    public Stream<RelationshipPropertySchemaInfoResult> relationshipPropertySchema()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        return new SchemaCalculator( kernelTransaction ).calculateTabularResultStreamForRels();
    }

    @SystemProcedure
    @Description( "Visualize the schema of the data." )
    @Procedure( name = "db.schema.visualization", mode = READ )
    public Stream<SchemaProcedure.GraphResult> schemaVisualization()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }
        return Stream.of( new SchemaProcedure( (InternalTransaction) transaction ).buildSchemaGraph() );
    }

    @SystemProcedure
    @Description( "List all constraints in the database." )
    @Procedure( name = "db.constraints", mode = READ )
    public Stream<ConstraintResult> listConstraints()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        SchemaReadCore schemaRead = kernelTransaction.schemaRead().snapshot();
        TokenNameLookup tokens = new SilentTokenNameLookup( kernelTransaction.tokenRead() );

        List<ConstraintResult> result = new ArrayList<>();
        final List<ConstraintDescriptor> constraintDescriptors = asList( schemaRead.constraintsGetAll() );
        for ( ConstraintDescriptor constraint : constraintDescriptors )
        {
            result.add( new ConstraintResult( constraint.getName(), constraint.prettyPrint( tokens ) ) );
        }
        result.sort( Comparator.comparing( r -> r.name ) );
        return result.stream();
    }

    @Description( "Create a named schema index with specified index provider. " +
            "The optional 'config' parameter can be used to supply settings to the index. Config settings are submitted as a map. " +
            "Note that settings keys might need to be escaped with back-ticks, " +
            "config example: {`spatial.cartesian.maxLevels`: 5, `spatial.cartesian.min`: [-45.0, -45.0]}. " +
            "Example: CALL db.createIndex(\"MyIndex\", [\"Person\"], [\"name\"], \"native-btree-1.0\", {`spatial.cartesian.maxLevels`: 5}) - " +
            "YIELD name, labels, properties, providerName, status" )
    @Procedure( name = "db.createIndex", mode = SCHEMA )
    public Stream<SchemaIndexInfo> createIndex(
            @Name( "indexName" ) String indexName,
            @Name( "labels" ) List<String> labels,
            @Name( "properties" ) List<String> properties,
            @Name( "providerName" ) String providerName,
            @Name( value = "config", defaultValue = "{}" ) Map<String,Object> config )
            throws ProcedureException
    {
        IndexProcedures indexProcedures = indexProcedures();
        final IndexProviderDescriptor indexProviderDescriptor = getIndexProviderDescriptor( providerName );
        return indexProcedures.createIndex( indexName, labels, properties, indexProviderDescriptor, config );
    }

    @Description( "Create a named unique property constraint with index backed by specified index provider. " +
            "The optional 'config' parameter can be used to supply settings to the index. Config settings are submitted as a map. " +
            "Note that settings keys might need to be escaped with back-ticks, " +
            "config example: {`spatial.cartesian.maxLevels`: 5, `spatial.cartesian.min`: [-45.0, -45.0]}. Example: " +
            "CALL db.createUniquePropertyConstraint(\"MyConstraint\", [\"Person\"], [\"name\"], \"native-btree-1.0\", {`spatial.cartesian.maxLevels`: 5}) - " +
            "YIELD name, labels, properties, providerName, status" )
    @Procedure( name = "db.createUniquePropertyConstraint", mode = SCHEMA )
    public Stream<BuiltInProcedures.SchemaIndexInfo> createUniquePropertyConstraint(
            @Name( "constraintName" ) String constraintName,
            @Name( "labels" ) List<String> labels,
            @Name( "properties" ) List<String> properties,
            @Name( "providerName" ) String providerName,
            @Name( value = "config", defaultValue = "{}" ) Map<String,Object> config )
            throws ProcedureException
    {
        IndexProcedures indexProcedures = indexProcedures();
        final IndexProviderDescriptor indexProviderDescriptor = getIndexProviderDescriptor( providerName );
        return indexProcedures.createUniquePropertyConstraint( constraintName, labels, properties, indexProviderDescriptor, config );
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
        return new IndexProcedures( kernelTransaction, resolver.resolveDependency( IndexingService.class ) );
    }

    private IndexProviderDescriptor getIndexProviderDescriptor( String providerName )
    {
        return resolver.resolveDependency( IndexingService.class ).indexProviderByName( providerName );
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
        public final double populationPercent;    // 0.0, 100.0, 75.1
        public final String uniqueness;          //"UNIQUE", "NONUNIQUE"
        public final String type;                //"FULLTEXT", "FUSION", "BTREE"
        public final String entityType;          //"NODE", "RELATIONSHIP"
        public final List<String> labelsOrTypes; //["Label1", "Label2"], ["RelType1", "RelType2"]
        public final List<String> properties;    //["propKey", "propKey2"]
        public final String provider;            //"native-btree-1.0", "lucene+native-3.0"

        private IndexResult( long id, String name, String state, double populationPercent, String uniqueness, String type, String entityType,
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

    public static class IndexDetailResult
    {
        // Copy if IndexResult
        public final long id;                    //1
        public final String name;                //"myIndex"
        public final String state;               //"ONLINE", "FAILED", "POPULATING"
        public final double populationPercent;    // 0.0, 100.0, 75.1
        public final String uniqueness;          //"UNIQUE", "NONUNIQUE"
        public final String type;                //"FULLTEXT", "FUSION", "BTREE"
        public final String entityType;          //"NODE", "RELATIONSHIP"
        public final List<String> labelsOrTypes; //["Label1", "Label2"], ["RelType1", "RelType2"]
        public final List<String> properties;    //["propKey", "propKey2"]
        public final String provider;            //"native-btree-1.0", "lucene+native-3.0"
        // Additional for IndexDetailResult
        public final Map<String,Object> indexConfig;// - map
        public final String failureMessage;
        // Maybe additional things to add
//        public final String useLucene;           //  - "True", "False"
//        public final String indexSize; // Index size on disk

        private IndexDetailResult( long id, String name, String state, double populationPercent, String uniqueness, String type, String entityType,
                List<String> labelsOrTypes, List<String> properties, String provider, Map<String,Object> indexConfig, String failureMessage )
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
            this.indexConfig = indexConfig;
            this.failureMessage = failureMessage;
        }

        private IndexDetailResult( IndexResult indexResult, Map<String,Object> indexConfig, String failureMessage )
        {
            this(
                    indexResult.id,
                    indexResult.name,
                    indexResult.state,
                    indexResult.populationPercent,
                    indexResult.uniqueness,
                    indexResult.type,
                    indexResult.entityType,
                    indexResult.labelsOrTypes,
                    indexResult.properties,
                    indexResult.provider,
                    indexConfig,
                    failureMessage );
        }
    }

    public static class SchemaStatementResult
    {
        public final String name;               // "MY INDEX", "constraint_5837f24
        public final String type;               // "INDEX", "CONSTRAINT"
        public final String createStatement;    // "CALL db.createIndex(...)"
        public final String dropStatement;      // DROP CONSTRAINT `My Constraint`

        public SchemaStatementResult( String name, String type, String createStatement, String dropStatement )
        {
            this.name = name;
            this.type = type;
            this.createStatement = createStatement;
            this.dropStatement = dropStatement;
        }
    }

    public static class SchemaIndexInfo
    {
        public final String name;
        public final List<String> labels;
        public final List<String> properties;
        public final String providerName;
        public final String status;

        public SchemaIndexInfo( String name, List<String> labels, List<String> properties, String providerName, String status )
        {
            this.name = name;
            this.labels = labels;
            this.properties = properties;
            this.providerName = providerName;
            this.status = status;
        }
    }

    public static class ConstraintResult
    {
        public final String name;
        public final String description;

        private ConstraintResult( String name, String description )
        {
            this.name = name;
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

    private enum IndexUniqueness
    {
        UNIQUE,
        NONUNIQUE;

        private static String getUniquenessOf( IndexDescriptor index )
        {
            return index.isUnique() ? UNIQUE.name() : NONUNIQUE.name();

        }
    }
}
