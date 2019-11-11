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

import org.eclipse.collections.impl.factory.Maps;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.EntityType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.util.FeatureToggles;

import static java.lang.String.format;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.FULLTEXT_PREFIX;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.PROCEDURE_ANALYZER;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.PROCEDURE_EVENTUALLY_CONSISTENT;
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
    public Transaction transaction;

    @Context
    public GraphDatabaseAPI db;

    @Context
    public DependencyResolver resolver;

    @Context
    public FulltextAdapter accessor;

    @Context
    public ProcedureCallContext callContext;

    @SystemProcedure
    @Description( "List the available analyzers that the full-text indexes can be configured with." )
    @Procedure( name = "db.index.fulltext.listAvailableAnalyzers", mode = READ )
    public Stream<AvailableAnalyzer> listAvailableAnalyzers()
    {
        return accessor.listAvailableAnalyzers().map( AvailableAnalyzer::new );
    }

    @SystemProcedure
    @Description( "Wait for the updates from recently committed transactions to be applied to any eventually-consistent full-text indexes." )
    @Procedure( name = "db.index.fulltext.awaitEventuallyConsistentIndexRefresh", mode = READ )
    public void awaitRefresh()
    {
        if ( callContext.isSystemDatabase() )
        {
            return;
        }

        accessor.awaitRefresh();
    }

    @Description( "Create a node full-text index for the given labels and properties. " +
                  "The optional 'config' map parameter can be used to supply settings to the index. " +
                  "Supported settings are '" + PROCEDURE_ANALYZER + "', for specifying what analyzer to use " +
                  "when indexing and querying. Use the `db.index.fulltext.listAvailableAnalyzers` procedure to see what options are available. " +
                  "And '" + PROCEDURE_EVENTUALLY_CONSISTENT + "' which can be set to 'true' to make this index eventually consistent, " +
                  "such that updates from committing transactions are applied in a background thread." )
    @Procedure( name = "db.index.fulltext.createNodeIndex", mode = SCHEMA )
    public void createNodeFulltextIndex(
            @Name( "indexName" ) String name,
            @Name( "labels" ) List<String> labelNames,
            @Name( "properties" ) List<String> properties,
            @Name( value = "config", defaultValue = "{}" ) Map<String,String> config )
    {
        Label[] labels = labelNames.stream().map( Label::label ).toArray( Label[]::new );
        IndexCreator indexCreator = transaction.schema().indexFor( labels );
        createIndex( indexCreator, name, properties, config );
    }

    @Description( "Create a relationship full-text index for the given relationship types and properties. " +
                  "The optional 'config' map parameter can be used to supply settings to the index. " +
                  "Supported settings are '" + PROCEDURE_ANALYZER + "', for specifying what analyzer to use " +
                  "when indexing and querying. Use the `db.index.fulltext.listAvailableAnalyzers` procedure to see what options are available. " +
                  "And '" + PROCEDURE_EVENTUALLY_CONSISTENT + "' which can be set to 'true' to make this index eventually consistent, " +
                  "such that updates from committing transactions are applied in a background thread." )
    @Procedure( name = "db.index.fulltext.createRelationshipIndex", mode = SCHEMA )
    public void createRelationshipFulltextIndex(
            @Name( "indexName" ) String name,
            @Name( "relationshipTypes" ) List<String> relTypes,
            @Name( "properties" ) List<String> properties,
            @Name( value = "config", defaultValue = "{}" ) Map<String,String> config )
    {
        RelationshipType[] types = relTypes.stream().map( RelationshipType::withName ).toArray( RelationshipType[]::new );
        IndexCreator indexCreator = transaction.schema().indexFor( types );
        createIndex( indexCreator, name, properties, config );
    }

    private void createIndex( IndexCreator indexCreator, String name, List<String> properties, Map<String,String> config )
    {
        indexCreator = indexCreator.withName( name );
        indexCreator = indexCreator.withIndexType( FULLTEXT );

        for ( String property : properties )
        {
            indexCreator = indexCreator.on( property );
        }

        config = sanitizeFulltextConfig( config );
        if ( !config.isEmpty() )
        {
            Map<IndexSetting,Object> parsedConfig = Maps.mutable.of();

            String analyzer = config.remove( PROCEDURE_ANALYZER );
            if ( analyzer != null )
            {
                parsedConfig.put( IndexSettingImpl.FULLTEXT_ANALYZER, analyzer );
            }

            String eventuallyConsistent = config.remove( PROCEDURE_EVENTUALLY_CONSISTENT );
            if ( eventuallyConsistent != null )
            {
                parsedConfig.put( IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT, Boolean.parseBoolean( eventuallyConsistent ) );
            }

            indexCreator = indexCreator.withIndexConfiguration( parsedConfig );
        }

        indexCreator.create();
    }

    @Description( "Drop the specified index." )
    @Procedure( name = "db.index.fulltext.drop", mode = SCHEMA )
    public void drop( @Name( "indexName" ) String name )
    {
        IndexDefinition index = transaction.schema().getIndexByName( name );
        if ( index.getIndexType() != FULLTEXT )
        {
            throw new IllegalArgumentException( "The index called '" + name + "' is not a full-text index." );
        }
        index.drop();
    }

    @SystemProcedure
    @Description( "Query the given full-text index. Returns the matching nodes and their Lucene query score, ordered by score." )
    @Procedure( name = "db.index.fulltext.queryNodes", mode = READ )
    public Stream<NodeOutput> queryFulltextForNodes( @Name( "indexName" ) String name, @Name( "queryString" ) String query ) throws Exception
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        IndexDescriptor indexReference = getValidIndex( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != NODE )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for nodes." );
        }
        NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor();
        IndexReadSession indexSession = tx.dataRead().indexReadSession( indexReference );
        tx.dataRead().nodeIndexSeek( indexSession, cursor, IndexOrder.NONE, false, IndexQuery.fulltextSearch( query ) );

        Spliterator<NodeOutput> spliterator = new SpliteratorAdaptor<>()
        {
            @Override
            public boolean tryAdvance( Consumer<? super NodeOutput> action )
            {
                while ( cursor.next() )
                {
                    long nodeReference = cursor.nodeReference();
                    float score = cursor.score();
                    NodeOutput nodeOutput = NodeOutput.forExistingEntityOrNull( transaction, nodeReference, score );
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

    @SystemProcedure
    @Description( "Query the given full-text index. Returns the matching relationships and their Lucene query score, ordered by score." )
    @Procedure( name = "db.index.fulltext.queryRelationships", mode = READ )
    public Stream<RelationshipOutput> queryFulltextForRelationships( @Name( "indexName" ) String name, @Name( "queryString" ) String query ) throws Exception
    {
        if ( callContext.isSystemDatabase() )
        {
            return Stream.empty();
        }

        IndexDescriptor indexReference = getValidIndex( name );
        awaitOnline( indexReference );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != RELATIONSHIP )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for relationships." );
        }
        RelationshipIndexCursor cursor = tx.cursors().allocateRelationshipIndexCursor();
        tx.dataRead().relationshipIndexSeek( indexReference, cursor, IndexQuery.fulltextSearch( query ) );

        Spliterator<RelationshipOutput> spliterator = new SpliteratorAdaptor<>()
        {
            @Override
            public boolean tryAdvance( Consumer<? super RelationshipOutput> action )
            {
                while ( cursor.next() )
                {
                    long relationshipReference = cursor.relationshipReference();
                    float score = cursor.score();
                    RelationshipOutput relationshipOutput =
                            RelationshipOutput.forExistingEntityOrNull( transaction, relationshipReference, score );
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

    private IndexDescriptor getValidIndex( @Name( "indexName" ) String name )
    {
        IndexDescriptor indexReference = tx.schemaRead().indexGetForName( name );
        if ( indexReference == IndexDescriptor.NO_INDEX || indexReference.getIndexType() != IndexType.FULLTEXT )
        {
            throw new IllegalArgumentException( "There is no such fulltext schema index: " + name );
        }
        return indexReference;
    }

    private void awaitOnline( IndexDescriptor index )
    {
        // We do the isAdded check on the transaction state first, because indexGetState will grab a schema read-lock, which can deadlock on the write-lock
        // held by the index populator. Also, if the index was created in this transaction, then we will never see it come online in this transaction anyway.
        // Indexes don't come online until the transaction that creates them has committed.
        KernelTransactionImplementation txImpl = (KernelTransactionImplementation) this.tx;
        if ( !txImpl.hasTxStateWithChanges() || !txImpl.txState().indexDiffSetsBySchema( index.schema() ).isAdded( index ) )
        {
            // If the index was not created in this transaction, then wait for it to come online before querying.
            Schema schema = transaction.schema();
            schema.awaitIndexOnline( index.getName(), INDEX_ONLINE_QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS );
        }
        // If the index was created in this transaction, then we skip this check entirely.
        // We will get an exception later, when we try to get an IndexReader, so this is fine.
    }

    private static Map<String,String> sanitizeFulltextConfig( Map<String,String> config )
    {
        Map<String,String> cleanMap = new HashMap<>();
        for ( Map.Entry<String,String> entry : config.entrySet() )
        {
            String key = entry.getKey();
            if ( key.startsWith( FULLTEXT_PREFIX ) )
            {
                key = key.substring( FULLTEXT_PREFIX.length(), key.length() );
            }
            String value = entry.getValue();
            String duplicate = cleanMap.put( key, value );
            if ( duplicate != null )
            {
                throw new IllegalArgumentException( format( "Config setting was specified more than once, '%s'.", key ) );
            }
        }
        return cleanMap;
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

        public NodeOutput( Node node, float score )
        {
            this.node = node;
            this.score = score;
        }

        public static NodeOutput forExistingEntityOrNull( Transaction transaction, long nodeId, float score )
        {
            try
            {
                return new NodeOutput( transaction.getNodeById( nodeId ), score );
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

        public static RelationshipOutput forExistingEntityOrNull( Transaction transaction, long relationshipId, float score )
        {
            try
            {
                return new RelationshipOutput( transaction.getRelationshipById( relationshipId ), score );
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
        public final List<String> stopwords;

        AvailableAnalyzer( AnalyzerProvider provider )
        {
            this.analyzer = provider.getName();
            this.description = provider.description();
            this.stopwords = provider.stopwords();
        }
    }
}
