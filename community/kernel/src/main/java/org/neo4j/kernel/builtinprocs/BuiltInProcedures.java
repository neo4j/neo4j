/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.api.ExplicitIndexHits;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.explicitindex.ExplicitIndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.schema.index.IndexDescriptor.Type.UNIQUE;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public class BuiltInProcedures
{
    @Context
    public KernelTransaction tx;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Description( "List all labels in the database." )
    @Procedure( name = "db.labels", mode = READ )
    public Stream<LabelResult> listLabels()
    {
        Statement statement = tx.acquireStatement();
        try
        {
            // Ownership of the reference to the acquired statement is transfered to the returned iterator stream,
            // but we still want to eagerly consume the labels, so we can catch any exceptions,
            List<LabelResult> labelResults = asList( TokenAccess.LABELS.inUse( statement ).map( LabelResult::new ) );
            return labelResults.stream();
        }
        catch ( Throwable t )
        {
            statement.close();
            throw t;
        }
    }

    @Description( "List all property keys in the database." )
    @Procedure( name = "db.propertyKeys", mode = READ )
    public Stream<PropertyKeyResult> listPropertyKeys()
    {
        Statement statement = tx.acquireStatement();
        try
        {
            // Ownership of the reference to the acquired statement is transfered to the returned iterator stream,
            // but we still want to eagerly consume the labels, so we can catch any exceptions,
            List<PropertyKeyResult> propertyKeys =
                    asList( TokenAccess.PROPERTY_KEYS.inUse( statement ).map( PropertyKeyResult::new ) );
            return propertyKeys.stream();
        }
        catch ( Throwable t )
        {
            statement.close();
            throw t;
        }
    }

    @Description( "List all relationship types in the database." )
    @Procedure( name = "db.relationshipTypes", mode = READ )
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        Statement statement = tx.acquireStatement();
        try
        {
            // Ownership of the reference to the acquired statement is transfered to the returned iterator stream,
            // but we still want to eagerly consume the labels, so we can catch any exceptions,
            List<RelationshipTypeResult> relationshipTypes =
                    asList( TokenAccess.RELATIONSHIP_TYPES.inUse( statement ).map( RelationshipTypeResult::new ) );
            return relationshipTypes.stream();
        }
        catch ( Throwable t )
        {
            statement.close();
            throw t;
        }
    }

    @Description( "List all indexes in the database." )
    @Procedure( name = "db.indexes", mode = READ )
    public Stream<IndexResult> listIndexes() throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations operations = statement.readOperations();
            TokenNameLookup tokens = new StatementTokenNameLookup( operations );

            List<IndexDescriptor> indexes = asList( operations.indexesGetAll() );
            indexes.sort( Comparator.comparing( a -> a.userDescription( tokens ) ) );

            ArrayList<IndexResult> result = new ArrayList<>();
            for ( IndexDescriptor index : indexes )
            {
                try
                {
                    String type;
                    if ( index.type() == UNIQUE )
                    {
                        type = IndexType.NODE_UNIQUE_PROPERTY.typeName();
                    }
                    else
                    {
                        type = IndexType.NODE_LABEL_PROPERTY.typeName();
                    }

                    result.add( new IndexResult( "INDEX ON " + index.schema().userDescription( tokens ),
                            operations.indexGetState( index ).toString(), type ) );
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
            @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout )
            throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.awaitIndex( index, timeout, TimeUnit.SECONDS );
        }
    }

    @Description( "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\"))." )
    @Procedure( name = "db.awaitIndexes", mode = READ )
    public void awaitIndexes( @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout )
            throws ProcedureException
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

    @Description( "Show the schema of the data." )
    @Procedure( name = "db.schema", mode = READ )
    public Stream<SchemaProcedure.GraphResult> metaGraph() throws ProcedureException
    {
        return Stream.of( new SchemaProcedure( graphDatabaseAPI, tx ).buildSchemaGraph() );
    }

    @Description( "List all constraints in the database." )
    @Procedure( name = "db.constraints", mode = READ )
    public Stream<ConstraintResult> listConstraints()
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations operations = statement.readOperations();
            TokenNameLookup tokens = new StatementTokenNameLookup( operations );

            return asList( operations.constraintsGetAll() )
                    .stream()
                    .map( constraint -> constraint.prettyPrint( tokens ) )
                    .sorted()
                    .map( ConstraintResult::new );
        }
    }

    @Description( "Get node from explicit index. Replaces `START n=node:nodes(key = 'A')`" )
    @Procedure( name = "db.index.explicit.seekNodes", mode = READ )
    public Stream<NodeResult> nodeManualIndexSeek( @Name( "indexName" ) String explicitIndexName,
            @Name( "key" ) String key,
            @Name( "value" ) Object value )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.nodeExplicitIndexGet( explicitIndexName, key, value );
            return toStream( hits, id -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Node index %s not found",
                    explicitIndexName );
        }
    }

    @Description( "Search nodes from explicit index. Replaces `START n=node:nodes('key:foo*')`" )
    @Procedure( name = "db.index.explicit.searchNodes", mode = READ )
    public Stream<WeightedNodeResult> nodeManualIndexSearch( @Name( "indexName" ) String manualIndexName,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.nodeExplicitIndexQuery( manualIndexName, query );
            return toWeightedNodeResultStream( hits );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Node index %s not found",
                    manualIndexName );
        }
    }

    @Description( "Get relationship from explicit index. Replaces `START r=relationship:relIndex(key = 'A')`" )
    @Procedure( name = "db.index.explicit.seekRelationships", mode = READ )
    public Stream<RelationshipResult> relationshipManualIndexSeek( @Name( "indexName" ) String manualIndexName,
            @Name( "key" ) String key,
            @Name( "value" ) Object value )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.relationshipExplicitIndexGet( manualIndexName, key, value, -1, -1 );
            return toStream( hits, id -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    manualIndexName );
        }
    }

    @Description( "Search relationship from explicit index. Replaces `START r=relationship:relIndex('key:foo*')`" )
    @Procedure( name = "db.index.explicit.searchRelationships", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearch(
            @Name( "indexName" ) String manualIndexName,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.relationshipExplicitIndexQuery( manualIndexName, query, -1, -1 );
            return toWeightedRelationshipResultStream( hits );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    manualIndexName );
        }
    }

    @Description( "Search relationship from explicit index, starting at the node 'in'." )
    @Procedure( name = "db.index.explicit.searchRelationshipsIn", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearchWithBoundStartNode(
            @Name( "indexName" ) String indexName,
            @Name( "in" ) Node in,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.relationshipExplicitIndexQuery( indexName, query, in.getId(), -1 );
            return toWeightedRelationshipResultStream( hits );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    indexName );
        }
    }

    @Description( "Search relationship from explicit index, ending at the node 'out'." )
    @Procedure( name = "db.index.explicit.searchRelationshipsOut", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearchWithBoundEndNode(
            @Name( "indexName" ) String indexName,
            @Name( "out" ) Node out,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.relationshipExplicitIndexQuery( indexName, query, -1, out.getId() );
            return toWeightedRelationshipResultStream( hits );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    indexName );
        }
    }

    @Description( "Search relationship from explicit index, starting at the node 'in' and ending at 'out'." )
    @Procedure( name = "db.index.explicit.searchRelationshipsBetween", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearchWithBoundNodes(
            @Name( "indexName" ) String indexName,
            @Name( "in" ) Node in,
            @Name( "out" ) Node out,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.relationshipExplicitIndexQuery( indexName, query, in.getId(), out.getId() );
            return toWeightedRelationshipResultStream( hits );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    indexName );
        }
    }

    @Description( "Get node from explicit automatic index. Replaces `START n=node:node_auto_index(key = 'A')`" )
    @Procedure( name = "db.index.explicit.auto.seekNodes", mode = READ )
    public Stream<NodeResult> nodeAutoIndexSeek( @Name( "key" ) String key, @Name( "value" ) Object value )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.nodeExplicitIndexGet( "node_auto_index", key, value );
            return toStream( hits, id -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            // auto index will not exist if no nodes have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Search nodes from explicit automatic index. Replaces `START n=node:node_auto_index('key:foo*')`" )
    @Procedure( name = "db.index.explicit.auto.searchNodes", mode = READ )
    public Stream<WeightedNodeResult> nodeAutoIndexSearch( @Name( "query" ) Object query ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits = readOperations.nodeExplicitIndexQuery( "node_auto_index", query );
            return toWeightedNodeResultStream( hits );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            // auto index will not exist if no nodes have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Get relationship from explicit automatic index. Replaces `START r=relationship:relationship_auto_index(key " +
                  "= 'A')`" )
    @Procedure( name = "db.index.explicit.auto.seekRelationships", mode = READ )
    public Stream<RelationshipResult> relationshipAutoIndexSeek( @Name( "key" ) String key,
            @Name( "value" ) Object value ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits =
                    readOperations.relationshipExplicitIndexGet( "relationship_auto_index", key, value, -1, -1 );
            return toStream( hits, id -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            // auto index will not exist if no relationships have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Search relationship from explicit automatic index. Replaces `START r=relationship:relationship_auto_index" +
                  "('key:foo*')`" )
    @Procedure( name = "db.index.explicit.auto.searchRelationships", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipAutoIndexSearch( @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            ExplicitIndexHits hits =
                    readOperations.relationshipExplicitIndexQuery( "relationship_auto_index", query, -1, -1 );
            return toWeightedRelationshipResultStream( hits );
        }
        catch ( ExplicitIndexNotFoundKernelException e )
        {
            // auto index will not exist if no relationships have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Get or create a node explicit index - YIELD type,name,config" )
    @Procedure( name = "db.index.explicit.forNodes", mode = WRITE )
    public Stream<ExplicitIndexInfo> nodeManualIndex( @Name( "indexName" ) String explicitIndexName )
    {
        IndexManager mgr = graphDatabaseAPI.index();
        Index<Node> index = mgr.forNodes( explicitIndexName );
        return Stream.of( new ExplicitIndexInfo( "NODE", explicitIndexName, mgr.getConfiguration( index ) ) );
    }

    @Description( "Get or create a relationship explicit index - YIELD type,name,config" )
    @Procedure( name = "db.index.explicit.forRelationships", mode = WRITE )
    public Stream<ExplicitIndexInfo> relationshipManualIndex( @Name( "indexName" ) String explicitIndexName )
    {
        IndexManager mgr = graphDatabaseAPI.index();
        Index<Relationship> index = mgr.forRelationships( explicitIndexName );
        return Stream.of( new ExplicitIndexInfo( "RELATIONSHIP", explicitIndexName, mgr.getConfiguration( index ) ) );
    }

    @Description( "Check if a node explicit index exists" )
    @Procedure( name = "db.index.explicit.existsForNodes", mode = READ )
    public Stream<BooleanResult> nodeManualIndexExists( @Name( "indexName" ) String explicitIndexName )
    {
        return Stream.of( new BooleanResult( graphDatabaseAPI.index().existsForNodes( explicitIndexName ) ) );
    }

    @Description( "Check if a relationship explicit index exists" )
    @Procedure( "db.index.explicit.existsForRelationships" )
    public Stream<BooleanResult> relationshipManualIndexExists( @Name( "indexName" ) String explicitIndexName )
    {
        return Stream.of( new BooleanResult( graphDatabaseAPI.index().existsForRelationships( explicitIndexName ) ) );
    }

    @Description( "List all explicit indexes - YIELD type,name,config" )
    @Procedure( name = "db.index.explicit.list", mode = READ )
    public Stream<ExplicitIndexInfo> list()
    {
        IndexManager mgr = graphDatabaseAPI.index();
        List<ExplicitIndexInfo> indexInfos = new ArrayList<>( 100 );
        for ( String name : mgr.nodeIndexNames() )
        {
            Index<Node> index = mgr.forNodes( name );
            indexInfos.add( new ExplicitIndexInfo( "NODE", name, mgr.getConfiguration( index ) ) );
        }
        for ( String name : mgr.relationshipIndexNames() )
        {
            RelationshipIndex index = mgr.forRelationships( name );
            indexInfos.add( new ExplicitIndexInfo( "RELATIONSHIP", name, mgr.getConfiguration( index ) ) );
        }
        return indexInfos.stream();
    }

    @Description( "Remove a explicit index - YIELD type,name,config" )
    @Procedure( name = "db.index.explicit.drop", mode = WRITE )
    public Stream<ExplicitIndexInfo> manualIndexDrop( @Name( "indexName" ) String explicitIndexName )
    {
        IndexManager mgr = graphDatabaseAPI.index();
        List<ExplicitIndexInfo> results = new ArrayList<>( 2 );
        if ( mgr.existsForNodes( explicitIndexName ) )
        {
            Index<Node> index = mgr.forNodes( explicitIndexName );
            results.add( new ExplicitIndexInfo( "NODE", explicitIndexName, mgr.getConfiguration( index ) ) );
            index.delete();
        }
        if ( mgr.existsForRelationships( explicitIndexName ) )
        {
            RelationshipIndex index = mgr.forRelationships( explicitIndexName );
            results.add( new ExplicitIndexInfo( "RELATIONSHIP", explicitIndexName, mgr.getConfiguration( index ) ) );
            index.delete();
        }
        return results.stream();
    }

    @Description( "Add a node to a explicit index based on a specified key and value" )
    @Procedure( name = "db.index.explicit.addNode", mode = WRITE )
    public Stream<BooleanResult> nodeManualIndexAdd( @Name( "indexName" ) String explicitIndexName,
            @Name( "node" ) Node node, @Name( "key" ) String key,
            @Name( "value" ) Object value )
    {
        graphDatabaseAPI.index().forNodes( explicitIndexName ).add( node, key, value );
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( true ) );
    }

    @Description( "Add a relationship to a explicit index based on a specified key and value" )
    @Procedure( name = "db.index.explicit.addRelationship", mode = WRITE )
    public Stream<BooleanResult> relationshipManualIndexAdd( @Name( "indexName" ) String explicitIndexName,
            @Name( "relationship" ) Relationship relationship,
            @Name( "key" ) String key, @Name( "value" ) Object value )
    {
        graphDatabaseAPI.index().forRelationships( explicitIndexName ).add( relationship, key, value );
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( true ) );
    }

    @Description( "Remove a node from a explicit index with an optional key" )
    @Procedure( name = "db.index.explicit.removeNode", mode = WRITE )
    public Stream<BooleanResult> nodeManualIndexRemove( @Name( "indexName" ) String explicitIndexName,
            @Name( "node" ) Node node, @Name( "key" ) String key )
    {
        if ( key.equals( Name.DEFAULT_VALUE ) )
        {
            graphDatabaseAPI.index().forNodes( explicitIndexName ).remove( node );
        }
        else
        {
            graphDatabaseAPI.index().forNodes( explicitIndexName ).remove( node, key );
        }
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( true ) );
    }

    @Description( "Remove a relationship from a explicit index with an optional key" )
    @Procedure( name = "db.index.explicit.removeRelationship", mode = WRITE )
    public Stream<BooleanResult> relationshipManualIndexRemove( @Name( "indexName" ) String explicitIndexName,
            @Name( "relationship" ) Relationship relationship,
            @Name( "key" ) String key )
    {
        if ( key.equals( Name.DEFAULT_VALUE ) )
        {
            graphDatabaseAPI.index().forRelationships( explicitIndexName ).remove( relationship );
        }
        else
        {
            graphDatabaseAPI.index().forRelationships( explicitIndexName ).remove( relationship, key );
        }
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( true ) );
    }

    private <T> Stream<T> toStream( PrimitiveLongResourceIterator iterator, Function<Long,T> mapper )
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

        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
    }

    private Stream<WeightedNodeResult> toWeightedNodeResultStream( ExplicitIndexHits iterator )
    {
        Iterator<WeightedNodeResult> it = new Iterator<WeightedNodeResult>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public WeightedNodeResult next()
            {
                return new WeightedNodeResult( graphDatabaseAPI.getNodeById( iterator.next() ), iterator.currentScore() );
            }
        };

        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
    }

    private Stream<WeightedRelationshipResult> toWeightedRelationshipResultStream( ExplicitIndexHits iterator )
    {
        Iterator<WeightedRelationshipResult> it = new Iterator<WeightedRelationshipResult>()
        {
            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public WeightedRelationshipResult next()
            {
                return new WeightedRelationshipResult( graphDatabaseAPI.getRelationshipById( iterator.next() ), iterator.currentScore() );
            }
        };

        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
    }

    private IndexProcedures indexProcedures()
    {
        return new IndexProcedures( tx, resolver.resolveDependency( IndexingService.class ) );
    }

    @SuppressWarnings( "unused" )
    public class LabelResult
    {
        public final String label;

        private LabelResult( Label label )
        {
            this.label = label.name();
        }
    }

    @SuppressWarnings( "unused" )
    public class PropertyKeyResult
    {
        public final String propertyKey;

        private PropertyKeyResult( String propertyKey )
        {
            this.propertyKey = propertyKey;
        }
    }

    @SuppressWarnings( "unused" )
    public class RelationshipTypeResult
    {
        public final String relationshipType;

        private RelationshipTypeResult( RelationshipType relationshipType )
        {
            this.relationshipType = relationshipType.name();
        }
    }

    @SuppressWarnings( "unused" )
    public class BooleanResult
    {
        public BooleanResult( Boolean success )
        {
            this.success = success;
        }

        public final Boolean success;
    }

    @SuppressWarnings( "unused" )
    public class IndexResult
    {
        public final String description;
        public final String state;
        public final String type;

        private IndexResult( String description, String state, String type )
        {
            this.description = description;
            this.state = state;
            this.type = type;
        }
    }

    public static class ExplicitIndexInfo
    {
        public final String type;
        public final String name;
        public final Map<String,String> config;

        public ExplicitIndexInfo( String type, String name, Map<String,String> config )
        {
            this.type = type;
            this.name = name;
            this.config = config;
        }
    }

    @SuppressWarnings( "unused" )
    public class ConstraintResult
    {
        public final String description;

        private ConstraintResult( String description )
        {
            this.description = description;
        }
    }

    @SuppressWarnings( "unused" )
    public class NodeResult
    {
        public NodeResult( Node node )
        {
            this.node = node;
        }

        public final Node node;
    }

    @SuppressWarnings( "unused" )
    public class WeightedNodeResult
    {
        public final Node node;
        public final double weight;

        public WeightedNodeResult( Node node, double weight )
        {
            this.node = node;
            this.weight = weight;
        }
    }

    @SuppressWarnings( "unused" )
    public class WeightedRelationshipResult
    {
        public final Relationship relationship;
        public final double weight;

        public WeightedRelationshipResult( Relationship relationship, double weight )
        {
            this.relationship = relationship;
            this.weight = weight;
        }
    }

    @SuppressWarnings( "unused" )
    public class RelationshipResult
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
        NODE_LABEL_PROPERTY( "node_label_property" ),
        NODE_UNIQUE_PROPERTY( "node_unique_property" );

        private final String typeName;

        IndexType( String typeName )
        {
            this.typeName = typeName;
        }

        public String typeName()
        {
            return typeName;
        }
    }
}
