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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.schema.IndexDescriptor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.builtin.IndexProcedures;
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
    public GraphDatabaseAPI db;

    @Context
    public DependencyResolver resolver;

    @Context
    public FulltextAdapter accessor;

    @Description( "List the available analyzers that the fulltext indexes can be configured with." )
    @Procedure( name = "db.index.fulltext.listAvailableAnalyzers", mode = READ )
    public Stream<AvailableAnalyzer> listAvailableAnalyzers()
    {
        return accessor.listAvailableAnalyzers()
                .map( p -> new AvailableAnalyzer( p.getName(), p.description() ) );
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
            @Name( value = "config", defaultValue = "{}" ) Map<String,String> indexConfigurationMap )
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
            @Name( value = "config", defaultValue = "{}" ) Map<String,String> config )
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
    public Stream<NodeOutput> queryFulltextForNodes( @Name( "indexName" ) String name, @Name( "queryString" ) String query ) throws Exception
    {
        IndexReference indexReference = getValidIndexReference( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.NODE )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for nodes." );
        }
        NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor();
        IndexReadSession indexSession = tx.dataRead().indexReadSession( indexReference );
        tx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( query ) );

        Spliterator<NodeOutput> spliterator = new SpliteratorAdaptor<NodeOutput>()
        {
            @Override
            public boolean tryAdvance( Consumer<? super NodeOutput> action )
            {
                while ( cursor.next() )
                {
                    long nodeReference = cursor.nodeReference();
                    float score = cursor.score();
                    NodeOutput nodeOutput = NodeOutput.forExistingEntityOrNull( db, nodeReference, score );
                    if ( nodeOutput != null )
                    {
                        action.accept( nodeOutput );
                        return true;
                    }
                }
                cursor.close();
                return false;
            }
        };
        Stream<NodeOutput> stream = StreamSupport.stream( spliterator, false );
        return stream.onClose( cursor::close );
    }

    @Description( "Query the given fulltext index. Returns the matching relationships and their lucene query score, ordered by score." )
    @Procedure( name = "db.index.fulltext.queryRelationships", mode = READ )
    public Stream<RelationshipOutput> queryFulltextForRelationships( @Name( "indexName" ) String name, @Name( "queryString" ) String query ) throws Exception
    {
        IndexReference indexReference = getValidIndexReference( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.RELATIONSHIP )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for relationships." );
        }
        RelationshipIndexCursor cursor = tx.cursors().allocateRelationshipIndexCursor();
        tx.dataRead().relationshipIndexSeek( indexReference, cursor, IndexQuery.fulltextSearch( query ) );

        Spliterator<RelationshipOutput> spliterator = new SpliteratorAdaptor<RelationshipOutput>()
        {
            @Override
            public boolean tryAdvance( Consumer<? super RelationshipOutput> action )
            {
                while ( cursor.next() )
                {
                    long relationshipReference = cursor.relationshipReference();
                    float score = cursor.score();
                    RelationshipOutput relationshipOutput = RelationshipOutput.forExistingEntityOrNull( db, relationshipReference, score );
                    if ( relationshipOutput != null )
                    {
                        action.accept( relationshipOutput );
                        return true;
                    }
                }
                cursor.close();
                return false;
            }
        };
        return StreamSupport.stream( spliterator, false ).onClose( cursor::close );
    }

    private IndexReference getValidIndexReference( @Name( "indexName" ) String name )
    {
        IndexReference indexReference = tx.schemaRead().indexGetForName( name );
        if ( indexReference == IndexReference.NO_INDEX || !indexReference.isFulltextIndex() )
        {
            throw new IllegalArgumentException( "There is no such fulltext schema index: " + name );
        }
        return indexReference;
    }

    private void awaitOnline( IndexReference indexReference )
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

    private abstract static class SpliteratorAdaptor<T> implements Spliterator<T>
    {
        @Override
        public Spliterator<T> trySplit()
        {
            return null;
        }

        @Override
        public long estimateSize()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.ORDERED | Spliterator.SORTED | Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE;
        }

        @Override
        public Comparator<? super T> getComparator()
        {
            // Returning 'null' here means the items are sorted by their "natural" sort order.
            return null;
        }
    }

    public static final class NodeOutput implements Comparable<NodeOutput>
    {
        public final Node node;
        public final double score;

        protected NodeOutput( Node node, float score )
        {
            this.node = node;
            this.score = score;
        }

        public static NodeOutput forExistingEntityOrNull( GraphDatabaseService db, long nodeId, float score )
        {
            try
            {
                return new NodeOutput( db.getNodeById( nodeId ), score );
            }
            catch ( NotFoundException ignore )
            {
                // This node was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }

        @Override
        public int compareTo( NodeOutput that )
        {
            return Double.compare( that.score, this.score );
        }

        @Override
        public String toString()
        {
            return "ScoredNode(" + node + ", score=" + score + ')';
        }
    }

    public static final class RelationshipOutput implements Comparable<RelationshipOutput>
    {
        public final Relationship relationship;
        public final double score;

        public RelationshipOutput( Relationship relationship, float score )
        {
            this.relationship = relationship;
            this.score = score;
        }

        public static RelationshipOutput forExistingEntityOrNull( GraphDatabaseService db, long relationshipId, float score )
        {
            try
            {
                return new RelationshipOutput( db.getRelationshipById( relationshipId ), score );
            }
            catch ( NotFoundException ignore )
            {
                // This relationship was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }

        @Override
        public int compareTo( RelationshipOutput that )
        {
            return Double.compare( that.score, this.score );
        }

        @Override
        public String toString()
        {
            return "ScoredRelationship(" + relationship + ", score=" + score + ')';
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
