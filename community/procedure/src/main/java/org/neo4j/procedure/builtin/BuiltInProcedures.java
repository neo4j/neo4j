/*
 * Copyright (c) "Neo4j"
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

import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
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
import org.neo4j.storageengine.api.StoreIdProvider;

import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.helpers.collection.Iterators.stream;
import static org.neo4j.kernel.impl.api.TokenAccess.LABELS;
import static org.neo4j.kernel.impl.api.TokenAccess.PROPERTY_KEYS;
import static org.neo4j.kernel.impl.api.TokenAccess.RELATIONSHIP_TYPES;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatTime;
import static org.neo4j.storageengine.util.StoreIdDecodeUtils.decodeId;

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
    @Description( "Provides information regarding the database." )
    @Procedure( name = "db.info", mode = READ )
    public Stream<DatabaseInfo> databaseInfo() throws NoSuchAlgorithmException
    {
        var storeIdProvider = graphDatabaseAPI.getDependencyResolver().resolveDependency( StoreIdProvider.class );
        var creationTime = formatTime( storeIdProvider.getStoreId().getCreationTime(), getConfiguredTimeZone() );
        return Stream.of( new DatabaseInfo( decodeId( storeIdProvider ), graphDatabaseAPI.databaseName(), creationTime ) );
    }

    @SystemProcedure
    @Description( "List all available labels in the database." )
    @Procedure( name = "db.labels", mode = READ )
    public Stream<LabelResult> listLabels()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        AccessMode mode = kernelTransaction.securityContext().mode();
        TokenRead tokenRead = kernelTransaction.tokenRead();

        List<LabelResult> labelsInUse;
        try ( KernelTransaction.Revertable ignore = kernelTransaction.overrideWith( SecurityContext.AUTH_DISABLED ) )
        {
            // Get all labels that are in use as seen by a super user
            labelsInUse = stream( LABELS.inUse( kernelTransaction ) )
                    // filter out labels that are denied or aren't explicitly allowed
                    .filter( label -> mode.allowsTraverseNode( tokenRead.nodeLabel( label.name() ) ) )
                    .map( LabelResult::new )
                    .collect( Collectors.toList() );
        }
        return labelsInUse.stream();
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
    @Description( "List all available relationship types in the database." )
    @Procedure( name = "db.relationshipTypes", mode = READ )
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        AccessMode mode = kernelTransaction.securityContext().mode();
        TokenRead tokenRead = kernelTransaction.tokenRead();
        List<RelationshipTypeResult> relTypesInUse;
        try ( KernelTransaction.Revertable ignore = kernelTransaction.overrideWith( SecurityContext.AUTH_DISABLED ) )
        {
            // Get all relTypes that are in use as seen by a super user
            relTypesInUse = stream( RELATIONSHIP_TYPES.inUse( kernelTransaction ) )
                    // filter out relTypes that are denied or aren't explicitly allowed
                    .filter( type -> mode.allowsTraverseRelType( tokenRead.relationshipType( type.name() ) ) )
                    .map( RelationshipTypeResult::new )
                    .collect( Collectors.toList() );
        }
        return relTypesInUse.stream();
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
        indexProcedures.resampleOutdatedIndexes( timeOutSeconds );

        //now that index-stats are up-to-date, clear caches so that we are ready to re-plan
        graphDatabaseAPI.getDependencyResolver()
                .resolveDependency( QueryExecutionEngine.class )
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

    @Deprecated( since = "4.2.0", forRemoval = true )
    @SystemProcedure
    @Description( "List all constraints in the database." )
    @Procedure( name = "db.constraints", mode = READ, deprecatedBy = "SHOW CONSTRAINTS command" )
    public Stream<ConstraintResult> listConstraints()
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        SchemaReadCore schemaRead = kernelTransaction.schemaRead().snapshot();

        List<ConstraintResult> result = new ArrayList<>();
        final List<ConstraintDescriptor> constraintDescriptors = asList( schemaRead.constraintsGetAll() );
        for ( ConstraintDescriptor constraint : constraintDescriptors )
        {
            String description = ConstraintsProcedureUtil.prettyPrint( constraint, kernelTransaction.tokenRead() );
            String details = constraint.userDescription( kernelTransaction.tokenRead() );
            result.add( new ConstraintResult( constraint.getName(), description, details ) );
        }
        result.sort( Comparator.comparing( r -> r.name ) );
        return result.stream();
    }

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Create a named schema index with specified index provider and configuration (optional). " +
            "Yield: name, labels, properties, providerName, status" )
    @Procedure( name = "db.createIndex", mode = SCHEMA, deprecatedBy = "CREATE INDEX command" )
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

    @Deprecated( since = "4.2.0", forRemoval = true )
    @Description( "Create a named unique property constraint. Backing index will use specified index provider and configuration (optional). " +
            "Yield: name, labels, properties, providerName, status" )
    @Procedure( name = "db.createUniquePropertyConstraint", mode = SCHEMA, deprecatedBy = "CREATE CONSTRAINT ... IS UNIQUE command" )
    public Stream<SchemaIndexInfo> createUniquePropertyConstraint(
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

    @SystemProcedure( allowExpiredCredentials = true )
    @Procedure( name = "db.ping", mode = READ )
    @Description( "This procedure can be used by client side tooling to test whether they are correctly connected to a database. " +
                  "The procedure is available in all databases and always returns true. A faulty connection can be detected by not being able to call this " +
                  "procedure." )
    public Stream<BooleanResult> ping()
    {
        return Stream.of( new BooleanResult( Boolean.TRUE ) );
    }

    private ZoneId getConfiguredTimeZone()
    {
        Config config = resolver.resolveDependency( Config.class );
        return config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
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

        private LabelResult( Label label )
        {
            this.label = label.name();
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

    public static class DatabaseInfo
    {
        public final String id;
        public final String name;
        public final String creationDate;

        public DatabaseInfo( String id, String name, String creationDate )
        {
            this.id = id;
            this.name = name;
            this.creationDate = creationDate;
        }
    }

    public static class RelationshipTypeResult
    {
        public final String relationshipType;

        private RelationshipTypeResult( RelationshipType relationshipType )
        {
            this.relationshipType = relationshipType.name();
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
        public final String details;

        private ConstraintResult( String name, String description, String details )
        {
            this.name = name;
            this.description = description;
            this.details = details;
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
}
