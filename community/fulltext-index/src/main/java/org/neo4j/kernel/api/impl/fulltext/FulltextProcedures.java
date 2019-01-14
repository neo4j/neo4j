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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.fulltext.AnalyzerProvider;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.builtinprocs.IndexProcedures;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.util.FeatureToggles;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory.DESCRIPTOR;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.INDEX_CONFIG_ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;

/**
 * Procedures for querying the Fulltext indexes.
 */
@SuppressWarnings( "WeakerAccess" )
public class FulltextProcedures
{
    private static final long INDEX_ONLINE_QUERY_TIMEOUT_SECONDS = FeatureToggles.getInteger(
            FulltextProcedures.class, "INDEX_ONLINE_QUERY_TIMEOUT_SECONDS", 30 );

    @Context
    public KernelTransaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public DependencyResolver resolver;

    @Context
    public FulltextAdapter accessor;

    @Description( "List the available analyzers that the fulltext indexes can be configured with." )
    @Procedure( name = "db.index.fulltext.listAvailableAnalyzers", mode = READ )
    public Stream<AvailableAnalyzer> listAvailableAnalyzers()
    {
        Stream<AnalyzerProvider> stream = accessor.listAvailableAnalyzers();
        return stream.flatMap( provider ->
        {
            String description = provider.description();
            Spliterator<String> spliterator = provider.getKeys().spliterator();
            return StreamSupport.stream( spliterator, false ).map( name -> new AvailableAnalyzer( name, description ) );
        } );
    }

    @Description( "Wait for the updates from recently committed transactions to be applied to any eventually-consistent fulltext indexes." )
    @Procedure( name = "db.index.fulltext.awaitEventuallyConsistentIndexRefresh", mode = READ )
    public void awaitRefresh()
    {
        accessor.awaitRefresh();
    }

    @Description( "Similar to db.awaitIndex(index, timeout), except instead of an index pattern, the index is specified by name. " +
            "The name can be quoted by backticks, if necessary." )
    @Procedure( name = "db.index.fulltext.awaitIndex", mode = READ )
    public void awaitIndex( @Name( "index" ) String index, @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout ) throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.awaitIndexByName( index, timeout, TimeUnit.SECONDS );
        }
    }

    private IndexProcedures indexProcedures()
    {
        return new IndexProcedures( tx, resolver.resolveDependency( IndexingService.class ) );
    }

    @Description( "Create a node fulltext index for the given labels and properties. " +
                  "The optional 'config' map parameter can be used to supply settings to the index. " +
                  "Note: index specific settings are currently experimental, and might not replicated correctly in a cluster, or during backup. " +
                  "Supported settings are '" + INDEX_CONFIG_ANALYZER + "', for specifying what analyzer to use " +
                  "when indexing and querying. Use the `db.index.fulltext.listAvailableAnalyzers` procedure to see what options are available. " +
                  "And '" + INDEX_CONFIG_EVENTUALLY_CONSISTENT + "' which can be set to 'true' to make this index eventually consistent, " +
                  "such that updates from committing transactions are applied in a background thread." )
    @Procedure( name = "db.index.fulltext.createNodeIndex", mode = SCHEMA )
    public void createNodeFulltextIndex(
            @Name( "indexName" ) String name,
            @Name( "labels" ) List<String> labels,
            @Name( "propertyNames" ) List<String> properties,
            @Name( value = "config", defaultValue = "" ) Map<String,String> indexConfigurationMap )
            throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        Properties indexConfiguration = new Properties();
        indexConfiguration.putAll( indexConfigurationMap );
        SchemaDescriptor schemaDescriptor = accessor.schemaFor( EntityType.NODE, stringArray( labels ), indexConfiguration, stringArray( properties ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, DESCRIPTOR.name(), Optional.of( name ) );
    }

    private String[] stringArray( List<String> strings )
    {
        return strings.toArray( ArrayUtils.EMPTY_STRING_ARRAY );
    }

    @Description( "Create a relationship fulltext index for the given relationship types and properties. " +
                  "The optional 'config' map parameter can be used to supply settings to the index. " +
                  "Note: index specific settings are currently experimental, and might not replicated correctly in a cluster, or during backup. " +
                  "Supported settings are '" + INDEX_CONFIG_ANALYZER + "', for specifying what analyzer to use " +
                  "when indexing and querying. Use the `db.index.fulltext.listAvailableAnalyzers` procedure to see what options are available. " +
                  "And '" + INDEX_CONFIG_EVENTUALLY_CONSISTENT + "' which can be set to 'true' to make this index eventually consistent, " +
                  "such that updates from committing transactions are applied in a background thread." )
    @Procedure( name = "db.index.fulltext.createRelationshipIndex", mode = SCHEMA )
    public void createRelationshipFulltextIndex(
            @Name( "indexName" ) String name,
            @Name( "relationshipTypes" ) List<String> relTypes,
            @Name( "propertyNames" ) List<String> properties,
            @Name( value = "config", defaultValue = "" ) Map<String,String> config )
            throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        Properties settings = new Properties();
        settings.putAll( config );
        SchemaDescriptor schemaDescriptor = accessor.schemaFor( EntityType.RELATIONSHIP, stringArray( relTypes ), settings, stringArray( properties ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, DESCRIPTOR.name(), Optional.of( name ) );
    }

    @Description( "Drop the specified index." )
    @Procedure( name = "db.index.fulltext.drop", mode = SCHEMA )
    public void drop( @Name( "indexName" ) String name ) throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        IndexReference indexReference = getValidIndexReference( name );
        tx.schemaWrite().indexDrop( indexReference );
    }

    @Description( "Query the given fulltext index. Returns the matching nodes and their lucene query score, ordered by score." )
    @Procedure( name = "db.index.fulltext.queryNodes", mode = READ )
    public Stream<NodeOutput> queryFulltextForNodes( @Name( "indexName" ) String name, @Name( "queryString" ) String query )
            throws ParseException, IndexNotFoundKernelException, IOException
    {
        IndexReference indexReference = getValidIndexReference( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.NODE )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for nodes." );
        }
        ScoreEntityIterator resultIterator = accessor.query( tx, name, query );
        return resultIterator.stream()
                .map( result -> NodeOutput.forExistingEntityOrNull( db, result ) )
                .filter( Objects::nonNull );
    }

    @Description( "Query the given fulltext index. Returns the matching relationships and their lucene query score, ordered by score." )
    @Procedure( name = "db.index.fulltext.queryRelationships", mode = READ )
    public Stream<RelationshipOutput> queryFulltextForRelationships( @Name( "indexName" ) String name, @Name( "queryString" ) String query )
            throws ParseException, IndexNotFoundKernelException, IOException
    {
        IndexReference indexReference = getValidIndexReference( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.RELATIONSHIP )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for relationships." );
        }
        ScoreEntityIterator resultIterator = accessor.query( tx, name, query );
        return resultIterator.stream()
                .map( result -> RelationshipOutput.forExistingEntityOrNull( db, result ) )
                .filter( Objects::nonNull );
    }

    private IndexReference getValidIndexReference( @Name( "indexName" ) String name )
    {
        IndexReference indexReference = tx.schemaRead().indexGetForName( name );
        if ( indexReference == IndexReference.NO_INDEX )
        {
            throw new IllegalArgumentException( "There is no such fulltext schema index: " + name );
        }
        return indexReference;
    }

    private void awaitOnline( IndexReference indexReference ) throws IndexNotFoundKernelException
    {
        // We do the isAdded check on the transaction state first, because indexGetState will grab a schema read-lock, which can deadlock on the write-lock
        // held by the index populator. Also, if we index was created in this transaction, then we will never see it come online in this transaction anyway.
        // Indexes don't come online until the transaction that creates them has committed.
        if ( !((KernelTransactionImplementation)tx).txState().indexDiffSetsBySchema( indexReference.schema() ).isAdded( (IndexDescriptor) indexReference ) )
        {
            // If the index was not created in this transaction, then wait for it to come online before querying.
            Schema schema = db.schema();
            IndexDefinition index = schema.getIndexByName( indexReference.name() );
            schema.awaitIndexOnline( index, INDEX_ONLINE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS );
        }
        // If the index was created in this transaction, then we skip this check entirely.
        // We will get an exception later, when we try to get an IndexReader, so this is fine.
    }

    public static final class NodeOutput
    {
        public final Node node;
        public final double score;

        protected NodeOutput( Node node, double score )
        {
            this.node = node;
            this.score = score;
        }

        public static NodeOutput forExistingEntityOrNull( GraphDatabaseService db, ScoreEntityIterator.ScoreEntry result )
        {
            try
            {
                return new NodeOutput( db.getNodeById( result.entityId() ), result.score() );
            }
            catch ( NotFoundException ignore )
            {
                // This node was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    public static final class RelationshipOutput
    {
        public final Relationship relationship;
        public final double score;

        public RelationshipOutput( Relationship relationship, double score )
        {
            this.relationship = relationship;
            this.score = score;
        }

        public static RelationshipOutput forExistingEntityOrNull( GraphDatabaseService db, ScoreEntityIterator.ScoreEntry result )
        {
            try
            {
                return new RelationshipOutput( db.getRelationshipById( result.entityId() ), result.score() );
            }
            catch ( NotFoundException ignore )
            {
                // This relationship was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    public static final class AvailableAnalyzer
    {
        public final String analyzer;
        public final String description;

        AvailableAnalyzer( String name, String description )
        {
            this.analyzer = name;
            this.description = description;
        }
    }
}
