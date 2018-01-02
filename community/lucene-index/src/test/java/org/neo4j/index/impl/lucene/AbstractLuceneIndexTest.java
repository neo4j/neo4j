/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.index.impl.lucene;

import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.test.TestGraphDatabaseFactory;

public abstract class AbstractLuceneIndexTest
{
    protected static GraphDatabaseService graphDb;
    protected Transaction tx;
    public final @Rule TestName testname = new TestName();

    @BeforeClass
    public static void setUpStuff()
    {
        graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterClass
    public static void tearDownStuff()
    {
        graphDb.shutdown();
    }
    
    @After
    public void commitTx()
    {
        finishTx( true );
    }
    
    public void rollbackTx()
    {
        finishTx( false );
    }

    public void finishTx( boolean success )
    {
        if ( tx != null )
        {
            if ( success )
            {
                tx.success();
            }
            tx.close();
            tx = null;
        }
    }

    @Before
    public void beginTx()
    {
        if ( tx == null )
        {
            tx = graphDb.beginTx();
        }
    }

    void restartTx()
    {
        commitTx();
        beginTx();
    }

    protected static abstract interface EntityCreator<T extends PropertyContainer>
    {
        T create( Object... properties );
        
        void delete( T entity );
    }

    private static final RelationshipType TEST_TYPE =
            DynamicRelationshipType.withName( "TEST_TYPE" );
    protected static final EntityCreator<Node> NODE_CREATOR = new EntityCreator<Node>()
    {
        public Node create( Object... properties )
        {
            Node node = graphDb.createNode();
            setProperties( node, properties );
            return node;
        }
        
        public void delete( Node entity )
        {
            entity.delete();
        }
    };
    protected static final EntityCreator<Relationship> RELATIONSHIP_CREATOR =
            new EntityCreator<Relationship>()
            {
                public Relationship create( Object... properties )
                {
                    Relationship rel = graphDb.createNode().createRelationshipTo( graphDb.createNode(), TEST_TYPE );
                    setProperties( rel, properties );
                    return rel;
                }
                
                public void delete( Relationship entity )
                {
                    entity.delete();
                }
            };

    static class FastRelationshipCreator implements EntityCreator<Relationship>
    {
        private Node node, otherNode;

        public Relationship create( Object... properties )
        {
            if ( node == null )
            {
                node = graphDb.createNode();
                otherNode = graphDb.createNode();
            }
            Relationship rel = node.createRelationshipTo( otherNode, TEST_TYPE );
            setProperties( rel, properties );
            return rel;
        }
        
        public void delete( Relationship entity )
        {
            entity.delete();
        }
    }
    
    private static void setProperties( PropertyContainer entity, Object... properties )
    {
        for ( Map.Entry<String, Object> entry : MapUtil.map( properties ).entrySet() )
        {
            entity.setProperty( entry.getKey(), entry.getValue() );
        }
    }
    
    protected Index<Node> nodeIndex( Map<String, String> config )
    {
        return nodeIndex( currentIndexName(), config );
    }
    
    protected Index<Node> nodeIndex( String name, Map<String, String> config )
    {
        return graphDb.index().forNodes( name, config );
    }
    
    protected RelationshipIndex relationshipIndex( Map<String, String> config )
    {
        return relationshipIndex( currentIndexName(), config );
    }
    
    protected RelationshipIndex relationshipIndex( String name, Map<String, String> config )
    {
        return graphDb.index().forRelationships( name, config );
    }
    
    protected String currentIndexName()
    {
        return testname.getMethodName();
    }
}
