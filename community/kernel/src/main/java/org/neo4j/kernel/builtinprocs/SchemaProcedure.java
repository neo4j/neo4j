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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.coreapi.schema.PropertyNameUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class SchemaProcedure
{

    private final GraphDatabaseAPI graphDatabaseAPI;
    private final KernelTransaction kernelTransaction;

    public SchemaProcedure( final GraphDatabaseAPI graphDatabaseAPI, final KernelTransaction kernelTransaction )
    {
        this.graphDatabaseAPI = graphDatabaseAPI;
        this.kernelTransaction = kernelTransaction;
    }

    public GraphResult buildSchemaGraph()
    {
        final Map<String,VirtualNodeHack> nodes = new HashMap<>();
        final Map<String,Set<VirtualRelationshipHack>> relationships = new HashMap<>();

        try ( Statement statement = kernelTransaction.acquireStatement() )
        {
            Read dataRead = kernelTransaction.dataRead();
            TokenRead tokenRead = kernelTransaction.tokenRead();
            TokenNameLookup tokenNameLookup = new SilentTokenNameLookup( tokenRead );
            SchemaRead schemaRead = kernelTransaction.schemaRead();
            try ( Transaction transaction = graphDatabaseAPI.beginTx() )
            {
                // add all labelsInDatabase
                try ( ResourceIterator<Label> labelsInDatabase = graphDatabaseAPI.getAllLabelsInUse().iterator() )
                {
                    while ( labelsInDatabase.hasNext() )
                    {
                        Label label = labelsInDatabase.next();
                        int labelId = tokenRead.nodeLabel( label.name() );
                        Map<String,Object> properties = new HashMap<>();

                        Iterator<IndexReference> indexReferences = schemaRead.indexesGetForLabel( labelId );
                        ArrayList<String> indexes = new ArrayList<>();
                        while ( indexReferences.hasNext() )
                        {
                            IndexReference index = indexReferences.next();
                            if ( !index.isUnique() )
                            {
                                String[] propertyNames = PropertyNameUtils.getPropertyKeys(
                                        tokenNameLookup, index.properties() );
                                indexes.add( String.join( ",", propertyNames ) );
                            }
                        }
                        properties.put( "indexes", indexes );

                        Iterator<ConstraintDescriptor> nodePropertyConstraintIterator =
                                schemaRead.constraintsGetForLabel( labelId );
                        ArrayList<String> constraints = new ArrayList<>();
                        while ( nodePropertyConstraintIterator.hasNext() )
                        {
                            ConstraintDescriptor constraint = nodePropertyConstraintIterator.next();
                            constraints.add( constraint.prettyPrint( tokenNameLookup ) );
                        }
                        properties.put( "constraints", constraints );

                        getOrCreateLabel( label.name(), properties, nodes );
                    }
                }

                //add all relationships

                try ( ResourceIterator<RelationshipType> relationshipTypeIterator =
                        graphDatabaseAPI.getAllRelationshipTypesInUse().iterator() )
                {
                    while ( relationshipTypeIterator.hasNext() )
                    {
                        RelationshipType relationshipType = relationshipTypeIterator.next();
                        String relationshipTypeGetName = relationshipType.name();
                        int relId = tokenRead.relationshipType( relationshipTypeGetName );
                        try ( ResourceIterator<Label> labelsInUse = graphDatabaseAPI.getAllLabelsInUse().iterator() )
                        {
                            List<VirtualNodeHack> startNodes = new LinkedList<>();
                            List<VirtualNodeHack> endNodes = new LinkedList<>();

                            while ( labelsInUse.hasNext() )
                            {
                                Label labelToken = labelsInUse.next();
                                String labelName = labelToken.name();
                                Map<String,Object> properties = new HashMap<>();
                                VirtualNodeHack node = getOrCreateLabel( labelName, properties, nodes );
                                int labelId = tokenRead.nodeLabel( labelName );

                                if ( dataRead.countsForRelationship( labelId, relId, Read.ANY_LABEL ) > 0 )
                                {
                                    startNodes.add( node );
                                }
                                if ( dataRead.countsForRelationship( Read.ANY_LABEL, relId, labelId ) > 0 )
                                {
                                    endNodes.add( node );
                                }
                            }

                            for ( VirtualNodeHack startNode : startNodes )
                            {
                                for ( VirtualNodeHack endNode : endNodes )
                                {
                                            addRelationship( startNode, endNode, relationshipTypeGetName, relationships );
                                }
                            }
                        }
                    }
                }
                transaction.success();
                return getGraphResult( nodes, relationships );
            }
        }
    }

    public static class GraphResult
    {
        public final List<Node> nodes;
        public final List<Relationship> relationships;

        public GraphResult( List<Node> nodes, List<Relationship> relationships )
        {
            this.nodes = nodes;
            this.relationships = relationships;
        }
    }

    private VirtualNodeHack getOrCreateLabel( String label, Map<String,Object> properties,
            final Map<String,VirtualNodeHack> nodeMap )
    {
        if ( nodeMap.containsKey( label ) )
        {
            return nodeMap.get( label );
        }
        VirtualNodeHack node = new VirtualNodeHack( label, properties );
        nodeMap.put( label, node );
        return node;
    }

    private void addRelationship( VirtualNodeHack startNode, VirtualNodeHack endNode, String relType,
            final Map<String,Set<VirtualRelationshipHack>> relationshipMap )
    {
        Set<VirtualRelationshipHack> relationshipsForType;
        if ( !relationshipMap.containsKey( relType ) )
        {
            relationshipsForType = new HashSet<>();
            relationshipMap.put( relType, relationshipsForType );
        }
        else
        {
            relationshipsForType = relationshipMap.get( relType );
        }
        VirtualRelationshipHack relationship = new VirtualRelationshipHack( startNode, endNode, relType );
        if ( !relationshipsForType.contains( relationship ) )
        {
            relationshipsForType.add( relationship );
        }
    }

    private GraphResult getGraphResult( final Map<String,VirtualNodeHack> nodeMap,
            final Map<String,Set<VirtualRelationshipHack>> relationshipMap )
    {
        List<Relationship> relationships = new LinkedList<>();
        for ( Set<VirtualRelationshipHack> relationship : relationshipMap.values() )
        {
            relationships.addAll( relationship );
        }

        GraphResult graphResult;
        graphResult = new GraphResult( new ArrayList<>( nodeMap.values() ), relationships );

        return graphResult;
    }

    private static class VirtualRelationshipHack implements Relationship
    {

        private static AtomicLong MIN_ID = new AtomicLong( -1 );

        private final long id;
        private final Node startNode;
        private final Node endNode;
        private final RelationshipType relationshipType;

        VirtualRelationshipHack( final VirtualNodeHack startNode, final VirtualNodeHack endNode, final String type )
        {
            this.id = MIN_ID.getAndDecrement();
            this.startNode = startNode;
            this.endNode = endNode;
            relationshipType = () -> type;
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public Node getStartNode()
        {
            return startNode;
        }

        @Override
        public Node getEndNode()
        {
            return endNode;
        }

        @Override
        public RelationshipType getType()
        {
            return relationshipType;
        }

        @Override
        public Map<String,Object> getAllProperties()
        {
            return new HashMap<>();
        }

        @Override
        public void delete()
        {

        }

        @Override
        public Node getOtherNode( Node node )
        {
            return null;
        }

        @Override
        public Node[] getNodes()
        {
            return new Node[0];
        }

        @Override
        public boolean isType( RelationshipType type )
        {
            return false;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return null;
        }

        @Override
        public boolean hasProperty( String key )
        {
            return false;
        }

        @Override
        public Object getProperty( String key )
        {
            return null;
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            return null;
        }

        @Override
        public void setProperty( String key, Object value )
        {

        }

        @Override
        public Object removeProperty( String key )
        {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            return null;
        }

        @Override
        public Map<String,Object> getProperties( String... keys )
        {
            return null;
        }

        @Override
        public String toString()
        {
            return String.format( "VirtualRelationshipHack[%s]", id );
        }
    }

    private static class VirtualNodeHack implements Node
    {

        private final HashMap<String,Object> propertyMap = new HashMap<>();

        private static AtomicLong MIN_ID = new AtomicLong( -1 );
        private final long id;
        private final Label label;

        VirtualNodeHack( final String label, Map<String,Object> properties )
        {
            this.id = MIN_ID.getAndDecrement();
            this.label = Label.label( label );
            propertyMap.putAll( properties );
            propertyMap.put( "name", label );
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public Map<String,Object> getAllProperties()
        {
            return propertyMap;
        }

        @Override
        public Iterable<Label> getLabels()
        {
            return Collections.singletonList( label );
        }

        @Override
        public void delete()
        {

        }

        @Override
        public Iterable<Relationship> getRelationships()
        {
            return null;
        }

        @Override
        public boolean hasRelationship()
        {
            return false;
        }

        @Override
        public Iterable<Relationship> getRelationships( RelationshipType... types )
        {
            return null;
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
        {
            return null;
        }

        @Override
        public Iterable<Relationship> getRelationships( RelationshipType type, Direction direction )
        {
            return null;
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction direction )
        {
            return null;
        }

        @Override
        public boolean hasRelationship( RelationshipType... types )
        {
            return false;
        }

        @Override
        public boolean hasRelationship( Direction direction, RelationshipType... types )
        {
            return false;
        }

        @Override
        public boolean hasRelationship( RelationshipType type, Direction direction )
        {
            return false;
        }

        @Override
        public boolean hasRelationship( Direction direction )
        {
            return false;
        }

        @Override
        public Relationship getSingleRelationship( RelationshipType type, Direction dir )
        {
            return null;
        }

        @Override
        public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
        {
            return null;
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes()
        {
            return null;
        }

        @Override
        public int getDegree()
        {
            return 0;
        }

        @Override
        public int getDegree( RelationshipType type )
        {
            return 0;
        }

        @Override
        public int getDegree( RelationshipType type, Direction direction )
        {
            return 0;
        }

        @Override
        public int getDegree( Direction direction )
        {
            return 0;
        }

        @Override
        public void addLabel( Label label )
        {

        }

        @Override
        public void removeLabel( Label label )
        {

        }

        @Override
        public boolean hasLabel( Label label )
        {
            return false;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return null;
        }

        @Override
        public boolean hasProperty( String key )
        {
            return false;
        }

        @Override
        public Object getProperty( String key )
        {
            return null;
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            return null;
        }

        @Override
        public void setProperty( String key, Object value )
        {

        }

        @Override
        public Object removeProperty( String key )
        {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            return null;
        }

        @Override
        public Map<String,Object> getProperties( String... keys )
        {
            return null;
        }

        @Override
        public String toString()
        {
            return String.format( "VirtualNodeHack[%s]", id );
        }
    }
}
