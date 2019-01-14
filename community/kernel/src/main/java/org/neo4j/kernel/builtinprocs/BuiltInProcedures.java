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
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
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
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;
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
        List<LabelResult> labelResults = asList( TokenAccess.LABELS.inUse( tx ).map( LabelResult::new ) );
        return labelResults.stream();
    }

    @Description( "List all property keys in the database." )
    @Procedure( name = "db.propertyKeys", mode = READ )
    public Stream<PropertyKeyResult> listPropertyKeys()
    {
        List<PropertyKeyResult> propertyKeys =
                asList( TokenAccess.PROPERTY_KEYS.inUse( tx ).map( PropertyKeyResult::new ) );
        return propertyKeys.stream();
    }

    @Description( "List all relationship types in the database." )
    @Procedure( name = "db.relationshipTypes", mode = READ )
    public Stream<RelationshipTypeResult> listRelationshipTypes()
    {
        List<RelationshipTypeResult> relationshipTypes =
                asList( TokenAccess.RELATIONSHIP_TYPES.inUse( tx ).map( RelationshipTypeResult::new ) );
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

            SchemaRead schemaRead = tx.schemaRead();
            List<IndexReference> indexes = asList( schemaRead.indexesGetAll() );
            indexes.sort( Comparator.comparing( a -> a.userDescription( tokens ) ) );

            ArrayList<IndexResult> result = new ArrayList<>();
            for ( IndexReference index : indexes )
            {
                try
                {
                    String type;
                    if ( index.isUnique() )
                    {
                        type = IndexType.NODE_UNIQUE_PROPERTY.typeName();
                    }
                    else
                    {
                        type = IndexType.NODE_LABEL_PROPERTY.typeName();
                    }

                    String label = tokenRead.nodeLabelName( index.label() );
                    List<String> propertyNames = propertyNames( tokens, index );
                    InternalIndexState internalIndexState = schemaRead.indexGetState( index );
                    String failureMessage = "";
                    if ( internalIndexState == InternalIndexState.FAILED )
                    {
                        failureMessage = schemaRead.indexGetFailure( index );
                    }
                    String description = "INDEX ON " + SchemaDescriptorFactory.forLabel( index.label(), index.properties() ).userDescription( tokens );
                    Map<String,String> provider = indexProviderDescriptorMap( schemaRead.index( index.label(), index.properties() ) );
                    result.add( new IndexResult( description, label, propertyNames, internalIndexState.toString(), type, provider, failureMessage ) );
                }
                catch ( IndexNotFoundKernelException e )
                {
                    throw new ProcedureException( Status.Schema.IndexNotFound, e,
                            "No index on ", index.userDescription( tokens ) );
                }
                catch ( LabelNotFoundKernelException e )
                {
                    throw new ProcedureException( Status.General.InvalidArguments, e,
                            "Label not found " );
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

    @Procedure( name = "db.schema.nodeTypeProperties", mode = READ )
    @Description( "Show the derived property schema of the nodes in tabular form." )
    public Stream<NodePropertySchemaInfoResult> nodePropertySchema()
    {
        return new SchemaCalculator( tx ).calculateTabularResultStreamForNodes();
    }

    @Procedure( name = "db.schema.relTypeProperties", mode = READ )
    @Description( "Show the derived property schema of the relationships in tabular form." )
    public Stream<RelationshipPropertySchemaInfoResult> relationshipPropertySchema()
    {
        return new SchemaCalculator( tx ).calculateTabularResultStreamForRels();
    }

    @Description( "Show the schema of the data." )
    @Procedure( name = "db.schema", mode = READ )
    public Stream<SchemaProcedure.GraphResult> metaGraph()
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

    @Description( "Get node from explicit index. Replaces `START n=node:nodes(key = 'A')`" )
    @Procedure( name = "db.index.explicit.seekNodes", mode = READ )
    public Stream<NodeResult> nodeManualIndexSeek( @Name( "indexName" ) String explicitIndexName,
            @Name( "key" ) String key,
            @Name( "value" ) Object value )
            throws ProcedureException
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            NodeExplicitIndexCursor cursor = tx.cursors().allocateNodeExplicitIndexCursor();
            tx.indexRead().nodeExplicitIndexLookup( cursor, explicitIndexName, key, value );

            return toStream( cursor, id -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Node index %s not found",
                    explicitIndexName );
        }

    }

    @Description( "Search nodes in explicit index. Replaces `START n=node:nodes('key:foo*')`" )
    @Procedure( name = "db.index.explicit.searchNodes", mode = READ )
    public Stream<WeightedNodeResult> nodeManualIndexSearch( @Name( "indexName" ) String manualIndexName,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            NodeExplicitIndexCursor cursor = tx.cursors().allocateNodeExplicitIndexCursor();
            tx.indexRead().nodeExplicitIndexQuery( cursor, manualIndexName, query );
            return toWeightedNodeResultStream( cursor );
        }
        catch ( KernelException e )
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
        try ( Statement ignore = tx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = tx.cursors().allocateRelationshipExplicitIndexCursor();
            tx.indexRead().relationshipExplicitIndexLookup( cursor, manualIndexName, key, value,
                    -1, -1 );
            return toStream( cursor, id -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    manualIndexName );
        }
    }

    @Description( "Search relationship in explicit index. Replaces `START r=relationship:relIndex('key:foo*')`" )
    @Procedure( name = "db.index.explicit.searchRelationships", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearch(
            @Name( "indexName" ) String manualIndexName,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = tx.cursors().allocateRelationshipExplicitIndexCursor();
            tx.indexRead().relationshipExplicitIndexQuery( cursor, manualIndexName, query, -1, -1 );
            return toWeightedRelationshipResultStream( cursor );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    manualIndexName );
        }
    }

    @Description( "Search relationship in explicit index, starting at the node 'in'." )
    @Procedure( name = "db.index.explicit.searchRelationshipsIn", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearchWithBoundStartNode(
            @Name( "indexName" ) String indexName,
            @Name( "in" ) Node in,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = tx.cursors().allocateRelationshipExplicitIndexCursor();
            tx.indexRead().relationshipExplicitIndexQuery( cursor, indexName, query, in.getId(), -1 );

            return toWeightedRelationshipResultStream( cursor );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    indexName );
        }
    }

    @Description( "Search relationship in explicit index, ending at the node 'out'." )
    @Procedure( name = "db.index.explicit.searchRelationshipsOut", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearchWithBoundEndNode(
            @Name( "indexName" ) String indexName,
            @Name( "out" ) Node out,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = tx.cursors().allocateRelationshipExplicitIndexCursor();
            tx.indexRead().relationshipExplicitIndexQuery( cursor, indexName, query, -1, out.getId() );
            return toWeightedRelationshipResultStream( cursor );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    indexName );
        }
    }

    @Description( "Search relationship in explicit index, starting at the node 'in' and ending at 'out'." )
    @Procedure( name = "db.index.explicit.searchRelationshipsBetween", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipManualIndexSearchWithBoundNodes(
            @Name( "indexName" ) String indexName,
            @Name( "in" ) Node in,
            @Name( "out" ) Node out,
            @Name( "query" ) Object query )
            throws ProcedureException
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = tx.cursors().allocateRelationshipExplicitIndexCursor();
            tx.indexRead().relationshipExplicitIndexQuery( cursor, indexName, query, in.getId(), out.getId() );

            return toWeightedRelationshipResultStream( cursor );
        }
        catch ( KernelException e )
        {
            throw new ProcedureException( Status.LegacyIndex.LegacyIndexNotFound, "Relationship index %s not found",
                    indexName );
        }
    }

    @Description( "Get node from explicit automatic index. Replaces `START n=node:node_auto_index(key = 'A')`" )
    @Procedure( name = "db.index.explicit.auto.seekNodes", mode = READ )
    public Stream<NodeResult> nodeAutoIndexSeek( @Name( "key" ) String key, @Name( "value" ) Object value )
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            NodeExplicitIndexCursor cursor = tx.cursors().allocateNodeExplicitIndexCursor();
            tx.indexRead().nodeExplicitIndexLookup( cursor, "node_auto_index", key, value );
            return toStream( cursor, id -> new NodeResult( graphDatabaseAPI.getNodeById( id ) ) );
        }
        catch ( KernelException e )
        {
            // auto index will not exist if no nodes have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Search nodes in explicit automatic index. Replaces `START n=node:node_auto_index('key:foo*')`" )
    @Procedure( name = "db.index.explicit.auto.searchNodes", mode = READ )
    public Stream<WeightedNodeResult> nodeAutoIndexSearch( @Name( "query" ) Object query )
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            NodeExplicitIndexCursor cursor = tx.cursors().allocateNodeExplicitIndexCursor();
            tx.indexRead().nodeExplicitIndexQuery( cursor, "node_auto_index", query );

            return toWeightedNodeResultStream( cursor );
        }
        catch ( KernelException e )
        {
            // auto index will not exist if no nodes have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Get relationship from explicit automatic index. Replaces `START r=relationship:relationship_auto_index(key " +
                  "= 'A')`" )
    @Procedure( name = "db.index.explicit.auto.seekRelationships", mode = READ )
    public Stream<RelationshipResult> relationshipAutoIndexSeek( @Name( "key" ) String key,
            @Name( "value" ) Object value )
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = tx.cursors().allocateRelationshipExplicitIndexCursor();
            tx.indexRead()
                    .relationshipExplicitIndexLookup( cursor, "relationship_auto_index", key,  value, -1, -1 );
            return toStream( cursor, id -> new RelationshipResult( graphDatabaseAPI.getRelationshipById( id ) ) );
        }
        catch ( KernelException e )
        {
            // auto index will not exist if no relationships have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description(
            "Search relationship in explicit automatic index. Replaces `START r=relationship:relationship_auto_index" +
            "('key:foo*')`" )
    @Procedure( name = "db.index.explicit.auto.searchRelationships", mode = READ )
    public Stream<WeightedRelationshipResult> relationshipAutoIndexSearch( @Name( "query" ) Object query )
    {
        try ( Statement ignore = tx.acquireStatement() )
        {
            RelationshipExplicitIndexCursor cursor = tx.cursors().allocateRelationshipExplicitIndexCursor();
            tx.indexRead().relationshipExplicitIndexQuery( cursor, "relationship_auto_index", query, -1, -1 );
            return toWeightedRelationshipResultStream( cursor );
        }
        catch ( KernelException e )
        {
            // auto index will not exist if no relationships have been added that match the auto-index rules
            return Stream.empty();
        }
    }

    @Description( "Get or create a node explicit index - YIELD type,name,config" )
    @Procedure( name = "db.index.explicit.forNodes", mode = WRITE )
    public Stream<ExplicitIndexInfo> nodeManualIndex( @Name( "indexName" ) String explicitIndexName,
            @Name( value = "config", defaultValue = "" ) Map<String,String> config )
    {
        IndexManager mgr = graphDatabaseAPI.index();
        Index<Node> index;
        if ( config.isEmpty() )
        {
            index = mgr.forNodes( explicitIndexName );
        }
        else
        {
            index = mgr.forNodes( explicitIndexName, config );
        }
        return Stream.of( new ExplicitIndexInfo( "NODE", explicitIndexName, mgr.getConfiguration( index ) ) );
    }

    @Description( "Get or create a relationship explicit index - YIELD type,name,config" )
    @Procedure( name = "db.index.explicit.forRelationships", mode = WRITE )
    public Stream<ExplicitIndexInfo> relationshipManualIndex( @Name( "indexName" ) String explicitIndexName,
            @Name( value = "config", defaultValue = "" ) Map<String,String> config )
    {
        IndexManager mgr = graphDatabaseAPI.index();
        Index<Relationship> index;
        if ( config.isEmpty() )
        {
            index = mgr.forRelationships( explicitIndexName );
        }
        else
        {
            index = mgr.forRelationships( explicitIndexName, config );
        }
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

    @Description( "Remove an explicit index - YIELD type,name,config" )
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

    @Description( "Add a node to an explicit index based on a specified key and value" )
    @Procedure( name = "db.index.explicit.addNode", mode = WRITE )
    public Stream<BooleanResult> nodeManualIndexAdd( @Name( "indexName" ) String explicitIndexName,
            @Name( "node" ) Node node, @Name( "key" ) String key,
            @Name( "value" ) Object value )
    {
        graphDatabaseAPI.index().forNodes( explicitIndexName ).add( node, key, value );
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( Boolean.TRUE ) );
    }

    @Description( "Add a relationship to an explicit index based on a specified key and value" )
    @Procedure( name = "db.index.explicit.addRelationship", mode = WRITE )
    public Stream<BooleanResult> relationshipManualIndexAdd( @Name( "indexName" ) String explicitIndexName,
            @Name( "relationship" ) Relationship relationship,
            @Name( "key" ) String key, @Name( "value" ) Object value )
    {
        graphDatabaseAPI.index().forRelationships( explicitIndexName ).add( relationship, key, value );
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( Boolean.TRUE ) );
    }

    private static final String DEFAULT_KEY = " <[9895b15e-8693-4a21-a58b-4b7b87e09b8e]> ";

    @Description( "Remove a node from an explicit index with an optional key" )
    @Procedure( name = "db.index.explicit.removeNode", mode = WRITE )
    public Stream<BooleanResult> nodeManualIndexRemove( @Name( "indexName" ) String explicitIndexName,
            @Name( "node" ) Node node, @Name( value = "key", defaultValue = DEFAULT_KEY ) String key )
    {
        if ( key.equals( DEFAULT_KEY ) )
        {
            graphDatabaseAPI.index().forNodes( explicitIndexName ).remove( node );
        }
        else
        {
            graphDatabaseAPI.index().forNodes( explicitIndexName ).remove( node, key );
        }
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( Boolean.TRUE ) );
    }

    @Description( "Remove a relationship from an explicit index with an optional key" )
    @Procedure( name = "db.index.explicit.removeRelationship", mode = WRITE )
    public Stream<BooleanResult> relationshipManualIndexRemove( @Name( "indexName" ) String explicitIndexName,
            @Name( "relationship" ) Relationship relationship,
            @Name( value = "key", defaultValue = DEFAULT_KEY ) String key )
    {
        if ( key.equals( DEFAULT_KEY ) )
        {
            graphDatabaseAPI.index().forRelationships( explicitIndexName ).remove( relationship );
        }
        else
        {
            graphDatabaseAPI.index().forRelationships( explicitIndexName ).remove( relationship, key );
        }
        // Failures will be expressed as exceptions before the return
        return Stream.of( new BooleanResult( Boolean.TRUE ) );
    }

    private Map<String,String> indexProviderDescriptorMap( CapableIndexReference indexReference )
    {
        return MapUtil.stringMap(
                "key", indexReference.providerKey(),
                "version", indexReference.providerVersion() );
    }

    private List<String> propertyNames( TokenNameLookup tokens, IndexReference index )
    {
        int[] propertyIds = index.properties();
        List<String> propertyNames = new ArrayList<>( propertyIds.length );
        for ( int propertyId : propertyIds )
        {
            propertyNames.add( tokens.propertyKeyGetName( propertyId ) );
        }
        return propertyNames;
    }

    private <T> Stream<T> toStream( NodeExplicitIndexCursor cursor, LongFunction<T> mapper )
    {
        PrefetchingResourceIterator<T> it = new PrefetchingResourceIterator<T>()
        {
            @Override
            protected T fetchNextOrNull()
            {
                if ( cursor.next() )
                {
                    return mapper.apply( cursor.nodeReference() );
                }
                else
                {
                    close();
                    return null;
                }
            }

            @Override
            public void close()
            {
                cursor.close();
            }
        };

        Stream<T> stream =
                StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
        return stream.onClose( cursor::close );
    }

    private <T> Stream<T> toStream( RelationshipExplicitIndexCursor cursor, LongFunction<T> mapper )
    {
        PrefetchingResourceIterator<T> it = new PrefetchingResourceIterator<T>()
        {
            @Override
            protected T fetchNextOrNull()
            {
                if ( cursor.next() )
                {
                    return mapper.apply( cursor.relationshipReference() );
                }
                else
                {
                    close();
                    return null;
                }
            }

            @Override
            public void close()
            {
                cursor.close();
            }
        };

        Stream<T> stream =
                StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
        return stream.onClose( cursor::close );
    }

    private <T> Stream<T> toStream( PrimitiveLongResourceIterator iterator, LongFunction<T> mapper )
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

        Stream<T> stream =
                StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
        return stream.onClose( () ->
        {
            if ( iterator != null )
            {
                iterator.close();
            }
        } );
    }

    private Stream<WeightedNodeResult> toWeightedNodeResultStream( NodeExplicitIndexCursor cursor )
    {
        Iterator<WeightedNodeResult> it = new PrefetchingResourceIterator<WeightedNodeResult>()
        {
            @Override
            public void close()
            {
                cursor.close();
            }

            @Override
            protected WeightedNodeResult fetchNextOrNull()
            {
                if ( cursor.next() )
                {
                    return new WeightedNodeResult( graphDatabaseAPI.getNodeById( cursor.nodeReference() ),
                            cursor.score() );
                }
                else
                {
                    close();
                    return null;
                }
            }
        };

        Stream<WeightedNodeResult> stream =
                StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
        return stream.onClose( cursor::close );
    }

    private Stream<WeightedRelationshipResult> toWeightedRelationshipResultStream(
            RelationshipExplicitIndexCursor cursor )
    {
        Iterator<WeightedRelationshipResult> it = new PrefetchingResourceIterator<WeightedRelationshipResult>()
        {
            @Override
            public void close()
            {
                cursor.close();
            }

            @Override
            protected WeightedRelationshipResult fetchNextOrNull()
            {
                if ( cursor.next() )
                {
                    return new WeightedRelationshipResult(
                            graphDatabaseAPI.getRelationshipById( cursor.relationshipReference() ), cursor.score() );
                }
                else
                {
                    close();
                    return null;
                }
            }
        };

        Stream<WeightedRelationshipResult> stream =
                StreamSupport.stream( Spliterators.spliteratorUnknownSize( it, Spliterator.ORDERED ), false );
        return stream.onClose( cursor::close );
    }

    private IndexProcedures indexProcedures()
    {
        return new IndexProcedures( tx, resolver.resolveDependency( IndexingService.class ) );
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

    public static class IndexResult
    {
        public final String description;
        public final String label;
        public final List<String> properties;
        public final String state;
        public final String type;
        public final Map<String,String> provider;
        public final String failureMessage;

        public IndexResult( String description, String label, List<String> properties, String state, String type,
                Map<String,String> provider, String failureMessage )
        {
            this.description = description;
            this.label = label;
            this.properties = properties;
            this.state = state;
            this.type = type;
            this.provider = provider;
            this.failureMessage = failureMessage;
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
