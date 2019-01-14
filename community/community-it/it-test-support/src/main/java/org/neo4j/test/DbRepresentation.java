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
package org.neo4j.test;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

public class DbRepresentation
{
    private final Map<Long,NodeRep> nodes = new TreeMap<>();
    private final Set<IndexDefinition> schemaIndexes = new HashSet<>();
    private final Set<ConstraintDefinition> constraints = new HashSet<>();
    private long highestNodeId;
    private long highestRelationshipId;

    public static DbRepresentation of( GraphDatabaseService db )
    {
        return of( db, true );
    }

    public static DbRepresentation of( GraphDatabaseService db, boolean includeIndexes )
    {
        int retryCount = 5;
        while ( true )
        {
            try ( Transaction ignore = db.beginTx() )
            {
                DbRepresentation result = new DbRepresentation();
                for ( Node node : db.getAllNodes() )
                {
                    NodeRep nodeRep = new NodeRep( db, node, includeIndexes );
                    result.nodes.put( node.getId(), nodeRep );
                    result.highestNodeId = Math.max( node.getId(), result.highestNodeId );
                    result.highestRelationshipId =
                            Math.max( nodeRep.highestRelationshipId, result.highestRelationshipId );

                }
                for ( IndexDefinition indexDefinition : db.schema().getIndexes() )
                {
                    result.schemaIndexes.add( indexDefinition );
                }
                for ( ConstraintDefinition constraintDefinition : db.schema().getConstraints() )
                {
                    result.constraints.add( constraintDefinition );
                }
                return result;
            }
            catch ( TransactionFailureException e )
            {
                if ( retryCount-- < 0 )
                {
                    throw e;
                }
            }
        }
    }

    public static DbRepresentation of( File storeDir )
    {
        return of( storeDir, true, Config.defaults() );
    }

    public static DbRepresentation of( File storeDir, Config config )
    {
        return of( storeDir, true, config );
    }

    public static DbRepresentation of( File storeDir, boolean includeIndexes, Config config )
    {
        GraphDatabaseBuilder builder = new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
        builder.setConfig( config.getRaw() );

        GraphDatabaseService db = builder.newGraphDatabase();
        try
        {
            return of( db, includeIndexes );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        return compareWith( (DbRepresentation) obj ).isEmpty();
    }

    // Accessed from HA-robustness, needs to be public
    @SuppressWarnings( "WeakerAccess" )
    public Collection<String> compareWith( DbRepresentation other )
    {
        Collection<String> diffList = new ArrayList<>();
        DiffReport diff = new CollectionDiffReport( diffList );
        nodeDiff( other, diff );
        indexDiff( other, diff );
        constraintDiff( other, diff );
        return diffList;
    }

    private void constraintDiff( DbRepresentation other, DiffReport diff )
    {
        for ( ConstraintDefinition constraint : constraints )
        {
            if ( !other.constraints.contains( constraint ) )
            {
                diff.add( "I have constraint " + constraint + " which other doesn't" );
            }
        }
        for ( ConstraintDefinition otherConstraint : other.constraints )
        {
            if ( !constraints.contains( otherConstraint ) )
            {
                diff.add( "Other has constraint " + otherConstraint + " which I don't" );
            }
        }
    }

    private void indexDiff( DbRepresentation other, DiffReport diff )
    {
        for ( IndexDefinition schemaIndex : schemaIndexes )
        {
            if ( !other.schemaIndexes.contains( schemaIndex ) )
            {
                diff.add( "I have schema index " + schemaIndex + " which other doesn't" );
            }
        }
        for ( IndexDefinition otherSchemaIndex : other.schemaIndexes )
        {
            if ( !schemaIndexes.contains( otherSchemaIndex ) )
            {
                diff.add( "Other has schema index " + otherSchemaIndex + " which I don't" );
            }
        }
    }

    private void nodeDiff( DbRepresentation other, DiffReport diff )
    {
        for ( NodeRep node : nodes.values() )
        {
            NodeRep otherNode = other.nodes.get( node.id );
            if ( otherNode == null )
            {
                diff.add( "I have node " + node.id + " which other doesn't" );
                continue;
            }
            node.compareWith( otherNode, diff );
        }

        for ( Long id : other.nodes.keySet() )
        {
            if ( !nodes.containsKey( id ) )
            {
                diff.add( "Other has node " + id + " which I don't" );
            }
        }
    }

    @Override
    public int hashCode()
    {
        return nodes.hashCode();
    }

    @Override
    public String toString()
    {
        return nodes.toString();
    }

    private static class NodeRep
    {
        private final PropertiesRep properties;
        private final Map<Long,PropertiesRep> outRelationships = new HashMap<>();
        private final long highestRelationshipId;
        private final long id;
        private final Map<String,Map<String,Serializable>> index;

        NodeRep( GraphDatabaseService db, Node node, boolean includeIndexes )
        {
            id = node.getId();
            properties = new PropertiesRep( node, node.getId() );
            long highestRel = 0;
            for ( Relationship rel : node.getRelationships( Direction.OUTGOING ) )
            {
                outRelationships.put( rel.getId(), new PropertiesRep( rel, rel.getId() ) );
                highestRel = Math.max( highestRel, rel.getId() );
            }
            this.highestRelationshipId = highestRel;
            this.index = includeIndexes ? checkIndex( db ) : null;
        }

        private Map<String,Map<String,Serializable>> checkIndex( GraphDatabaseService db )
        {
            Map<String,Map<String,Serializable>> result = new HashMap<>();
            for ( String indexName : db.index().nodeIndexNames() )
            {
                Map<String,Serializable> thisIndex = new HashMap<>();
                Index<Node> tempIndex = db.index().forNodes( indexName );
                for ( Map.Entry<String,Serializable> property : properties.props.entrySet() )
                {
                    try ( IndexHits<Node> content = tempIndex.get( property.getKey(), property.getValue() ) )
                    {
                        if ( content.hasNext() )
                        {
                            for ( Node hit : content )
                            {
                                if ( hit.getId() == id )
                                {
                                    thisIndex.put( property.getKey(), property.getValue() );
                                    break;
                                }
                            }
                        }
                    }
                }
                result.put( indexName, thisIndex );
            }
            return result;
        }

        /*
         * Yes, this is not the best way to do it - hash map does a deep equals. However,
         * if things go wrong, this way give the ability to check where the inequality
         * happened. If you feel strongly about this, feel free to change.
         * Admittedly, the implementation could use some cleanup.
         */
        private void compareIndex( NodeRep other, DiffReport diff )
        {
            if ( other.index == index )
            {
                return;
            }
            Collection<String> allIndexes = new HashSet<>();
            allIndexes.addAll( index.keySet() );
            allIndexes.addAll( other.index.keySet() );
            for ( String indexName : allIndexes )
            {
                if ( !index.containsKey( indexName ) )
                {
                    diff.add( this + " isn't indexed in " + indexName + " for mine" );
                    continue;
                }
                if ( !other.index.containsKey( indexName ) )
                {
                    diff.add( this + " isn't indexed in " + indexName + " for other" );
                    continue;
                }

                Map<String,Serializable> thisIndex = index.get( indexName );
                Map<String,Serializable> otherIndex = other.index.get( indexName );

                if ( thisIndex.size() != otherIndex.size() )
                {
                    diff.add( "other index had a different mapping count than me for node " + this + " mine:" +
                            thisIndex + ", other:" + otherIndex );
                    continue;
                }

                for ( Map.Entry<String,Serializable> indexEntry : thisIndex.entrySet() )
                {
                    if ( !indexEntry.getValue().equals(
                            otherIndex.get( indexEntry.getKey() ) ) )
                    {
                        diff.add( "other index had a different value indexed for " + indexEntry.getKey() + "=" +
                                indexEntry.getValue() + ", namely " + otherIndex.get( indexEntry.getKey() ) +
                                " for " + this );
                    }
                }
            }
        }

        void compareWith( NodeRep other, DiffReport diff )
        {
            if ( other.id != id )
            {
                diff.add( "Id differs mine:" + id + ", other:" + other.id );
            }
            properties.compareWith( other.properties, diff );
            if ( index != null && other.index != null )
            {
                compareIndex( other, diff );
            }
            compareRelationships( other, diff );
        }

        private void compareRelationships( NodeRep other, DiffReport diff )
        {
            for ( PropertiesRep rel : outRelationships.values() )
            {
                PropertiesRep otherRel = other.outRelationships.get( rel.entityId );
                if ( otherRel == null )
                {
                    diff.add( "I have relationship " + rel.entityId + " which other don't" );
                    continue;
                }
                rel.compareWith( otherRel, diff );
            }

            for ( Long id : other.outRelationships.keySet() )
            {
                if ( !outRelationships.containsKey( id ) )
                {
                    diff.add( "Other has relationship " + id + " which I don't" );
                }
            }
        }

        @Override
        public int hashCode()
        {
            int result = 7;
            result += properties.hashCode() * 7;
            result += outRelationships.hashCode() * 13;
            result += id * 17;
            if ( index != null )
            {
                result += index.hashCode() * 19;
            }
            return result;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            NodeRep nodeRep = (NodeRep) o;
            return id == nodeRep.id && Objects.equals( properties, nodeRep.properties ) &&
                    Objects.equals( outRelationships, nodeRep.outRelationships ) &&
                    Objects.equals( index, nodeRep.index );
        }

        @Override
        public String toString()
        {
            return "<id: " + id + " props: " + properties + ", rels: " + outRelationships + ", index: " + index + ">";
        }
    }

    private static class PropertiesRep
    {
        private final Map<String,Serializable> props = new HashMap<>();
        private final String entityToString;
        private final long entityId;

        PropertiesRep( PropertyContainer entity, long id )
        {
            this.entityId = id;
            this.entityToString = entity.toString();
            for ( String key : entity.getPropertyKeys() )
            {
                Serializable value = (Serializable) entity.getProperty( key, null );
                // We do this because the node may have changed since we did getPropertyKeys()
                if ( value != null )
                {
                    if ( value.getClass().isArray() )
                    {
                        props.put( key, new ArrayList<>( Arrays.asList( IoPrimitiveUtils.asArray( value ) ) ) );
                    }
                    else
                    {
                        props.put( key, value );
                    }
                }
            }
        }

        protected void compareWith( PropertiesRep other, DiffReport diff )
        {
            boolean equals = props.equals( other.props );
            if ( !equals )
            {
                diff.add( "Properties diff for " + entityToString + " mine:" + props + ", other:" + other.props );
            }
        }

        @Override
        public int hashCode()
        {
            return props.hashCode();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            PropertiesRep that = (PropertiesRep) o;
            return Objects.equals( props, that.props );
        }

        @Override
        public String toString()
        {
            return props.toString();
        }
    }

    private interface DiffReport
    {
        void add( String report );
    }

    private static class CollectionDiffReport implements DiffReport
    {
        private final Collection<String> collection;

        CollectionDiffReport( Collection<String> collection )
        {
            this.collection = collection;
        }

        @Override
        public void add( String report )
        {
            collection.add( report );
        }
    }
}
