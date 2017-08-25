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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.LegacyIndexHits;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.legacyindex.LegacyIndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.*;

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
        try ( Statement statement = tx.acquireStatement() )
        {
            return TokenAccess.LABELS.inUse( statement ).map( LabelResult::new ).stream();
        }
    }

    @Description( "List all property keys in the database." )
    @Procedure( name = "db.propertyKeys", mode = READ )
    public Stream<PropertyKeyResult> listPropertyKeys()
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            return TokenAccess.PROPERTY_KEYS.inUse( statement ).map( PropertyKeyResult::new ).stream();
        }
    }

    @Description( "List all relationship types in the database." )
    @Procedure( name = "db.relationshipTypes", mode = READ )
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            return TokenAccess.RELATIONSHIP_TYPES.inUse( statement ).map( RelationshipTypeResult::new ).stream();
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

                    result.add( new IndexResult( "INDEX ON " + index.schema().userDescription( tokens ), operations.indexGetState( index ).toString(), type ) );
                }
                catch ( IndexNotFoundKernelException e )
                {
                    throw new ProcedureException( Status.Schema.IndexNotFound, e, "No index on ", index.userDescription( tokens ) );
                }
            }
            return result.stream();
        }
    }

    @Description( "Wait for an index to come online (for example: CALL db.awaitIndex(\":Person(name)\"))." )
    @Procedure( name = "db.awaitIndex", mode = READ )
    public void awaitIndex( @Name( "index" ) String index, @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout ) throws ProcedureException
    {
        try ( IndexProcedures indexProcedures = indexProcedures() )
        {
            indexProcedures.awaitIndex( index, timeout, TimeUnit.SECONDS );
        }
    }

    @Description( "Wait for all indexes to come online (for example: CALL db.awaitIndexes(\"500\"))." )
    @Procedure( name = "db.awaitIndexes", mode = READ )
    public void awaitIndexes( @Name( value = "timeOutSeconds", defaultValue = "300" ) long timeout ) throws ProcedureException
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

            return asList( operations.constraintsGetAll() ).stream().map( ( constraint ) -> constraint.prettyPrint( tokens ) ).sorted().map(
                    ConstraintResult::new );
        }
    }

    @Description( "Get node from manual index. Replaces `START n=node:nodes(key = 'A')`" )
    @Procedure( name = "db.nodeManualIndexSeek", mode = READ )
    public Stream<NodeResult> nodeManualIndexSeek( @Name( "indexName" ) String legacyIndexName, @Name( "key" ) String key, @Name( "value" ) Object value )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.nodeLegacyIndexGet( legacyIndexName, key, value );
            return toStream( hits, ( id ) -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Node index %s not found", legacyIndexName );
        }
    }

    @Description( "Search nodes from manual index. Replaces `START n=node:nodes('key:foo*')`" )
    @Procedure( name = "db.nodeManualIndexSearch", mode = READ )
    public Stream<NodeResult> nodeManualIndexSearch( @Name( "indexName" ) String manualIndexName, @Name( "query" ) Object query ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.nodeLegacyIndexQuery( manualIndexName, query );
            return toStream( hits, ( id ) -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Node index %s not found", manualIndexName );
        }
    }

    @Description( "Get relationship from manual index. Replaces `START r=relationship:relIndex(key = 'A')`" )
    @Procedure( name = "db.relationshipManualIndexSeek", mode = READ )
    public Stream<RelationshipResult> relationshipManualIndexSeek( @Name( "indexName" ) String manualIndexName, @Name( "key" ) String key,
            @Name( "value" ) Object value ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.relationshipLegacyIndexGet( manualIndexName, key, value, -1, -1 );
            return toStream( hits, ( id ) -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found", manualIndexName );
        }
    }

    @Description( "Search relationship from manual index. Replaces `START r=relationship:relIndex('key:foo*')`" )
    @Procedure( name = "db.relationshipManualIndexSearch", mode = READ )
    public Stream<RelationshipResult> relationshipManualIndexSearch( @Name( "indexName" ) String manualIndexName, @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.relationshipLegacyIndexQuery( manualIndexName, query, -1, -1 );
            return toStream( hits, ( id ) -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found", manualIndexName );
        }
    }

    @Description( "Get node from automatic index. Replaces `START n=node:node_auto_index(key = 'A')`" )
    @Procedure( name = "db.nodeAutoIndexSeek", mode = READ )
    public Stream<NodeResult> nodeAutoIndexSeek( @Name( "key" ) String key, @Name( "value" ) Object value ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.nodeLegacyIndexGet( "node_auto_index", key, value );
            return toStream( hits, ( id ) -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            // auto index will not exist if no nodes have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Search nodes from automatic index. Replaces `START n=node:node_auto_index('key:foo*')`" )
    @Procedure( name = "db.nodeAutoIndexSearch", mode = READ )
    public Stream<NodeResult> nodeAutoIndexSearch( @Name( "query" ) Object query ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.nodeLegacyIndexQuery( "node_auto_index", query );
            return toStream( hits, ( id ) -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            // auto index will not exist if no nodes have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Get relationship from automatic index. Replaces `START r=relationship:relationship_auto_index(key = 'A')`" )
    @Procedure( name = "db.relationshipAutoIndexSeek", mode = READ )
    public Stream<RelationshipResult> relationshipAutoIndexSeek( @Name( "key" ) String key, @Name( "value" ) Object value ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.relationshipLegacyIndexGet( "relationship_auto_index", key, value, -1, -1 );
            return toStream( hits, ( id ) -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            // auto index will not exist if no relationships have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Search relationship from automatic index. Replaces `START r=relationship:relationship_auto_index('key:foo*')`" )
    @Procedure( name = "db.relationshipAutoIndexSearch", mode = READ )
    public Stream<RelationshipResult> relationshipAutoIndexSearch( @Name( "query" ) Object query ) throws ProcedureException
    {
        try ( Statement statement = tx.acquireStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            LegacyIndexHits hits = readOperations.relationshipLegacyIndexQuery( "relationship_auto_index", query, -1, -1 );
            return toStream( hits, ( id ) -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( LegacyIndexNotFoundKernelException e )
        {
            // auto index will not exist if no relationships have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Check if a node manual index exists" )
    @Procedure( name = "db.nodeManualIndexExists", mode = READ )
    public Stream<BooleanResult> nodeManualIndexExists( @Name( "indexName" ) String legacyIndexName )
    {
        BooleanResult result = new BooleanResult( graphDatabaseAPI.index().existsForNodes( legacyIndexName ) );
        ArrayList<BooleanResult> arrayResult = new ArrayList<>();
        arrayResult.add( result );
        return arrayResult.stream();
    }

    @Description( "Check if a relationship manual index exists" )
    @Procedure( "db.relationshipManualIndexExists" )
    public Stream<BooleanResult> relationshipManualIndexExists( @Name( "indexName" ) String legacyIndexName )
    {
        BooleanResult result = new BooleanResult( graphDatabaseAPI.index().existsForRelationships( legacyIndexName ) );
        ArrayList<BooleanResult> arrayResult = new ArrayList<>();
        arrayResult.add( result );
        return arrayResult.stream();
    }

    @Description( "Removes an manual index" )
    @Procedure( name = "db.manualIndexDrop", mode = WRITE )
    public Stream<BooleanResult> manualIndexDrop( @Name( "indexName" ) String legacyIndexName )
    {
        IndexManager mgr = graphDatabaseAPI.index();
        List<BooleanResult> results = new ArrayList<>( 2 );
        if ( mgr.existsForNodes( legacyIndexName ) )
        {
            Index<Node> index = mgr.forNodes( legacyIndexName );
            results.add( new BooleanResult( true ) );
            index.delete();
        }
        if ( mgr.existsForRelationships( legacyIndexName ) )
        {
            RelationshipIndex index = mgr.forRelationships( legacyIndexName );
            results.add( new BooleanResult( true ) );
            index.delete();
        }
        if ( results.isEmpty() )
        {
            results.add( new BooleanResult( false ) );
        }
        return results.stream();
    }

    @Description( "Add a node to a manual index" )
    @Procedure( name = "db.nodeManualIndexAdd", mode = WRITE )
    public Stream<BooleanResult> nodeManualIndexAdd( @Name( "indexName" ) String legacyIndexName, @Name( "node" ) Node node, @Name( "key" ) String key,
            @Name( "value" ) Object value )
    {
        graphDatabaseAPI.index().forNodes( legacyIndexName ).add( node, key, value );
        List<BooleanResult> results = new ArrayList<>( 1 );
        results.add( new BooleanResult( true ) );
        return results.stream();
    }

    @Description( "Add a relationship to a manual index" )
    @Procedure( name = "db.relationshipManualIndexAdd", mode = WRITE )
    public Stream<BooleanResult> relationshipManualIndexAdd( @Name( "indexName" ) String legacyIndexName, @Name( "relationship" ) Relationship relationship,
            @Name( "key" ) String key, @Name( "value" ) Object value )
    {
        graphDatabaseAPI.index().forRelationships( legacyIndexName ).add( relationship, key, value );
        List<BooleanResult> results = new ArrayList<>( 1 );
        results.add( new BooleanResult( true ) );
        return results.stream();
    }

    @Description( "Remove a node for a manual index" )
    @Procedure( name = "db.nodeManualIndexRemove", mode = WRITE )
    public Stream<BooleanResult> nodeManualIndexRemove( @Name( "indexName" ) String legacyIndexName, @Name( "node" ) Node node, @Name( "key" ) String key )
    {
        graphDatabaseAPI.index().forNodes( legacyIndexName ).remove( node, key );
        List<BooleanResult> results = new ArrayList<>( 1 );
        results.add( new BooleanResult( true ) );
        return results.stream();
    }

    @Description( "Remove a relationship for a manual index" )
    @Procedure( name = "db.relationshipManualIndexRemove", mode = WRITE )
    public Stream<BooleanResult> relationshipManualIndexRemove( @Name( "indexName" ) String legacyIndexName, @Name( "relationship" ) Relationship relationship,
            @Name( "key" ) String key )
    {
        graphDatabaseAPI.index().forRelationships( legacyIndexName ).remove( relationship, key );
        List<BooleanResult> results = new ArrayList<>( 1 );
        results.add( new BooleanResult( true ) );
        return results.stream();
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
