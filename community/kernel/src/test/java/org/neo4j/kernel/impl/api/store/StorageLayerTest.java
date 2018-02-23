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
package org.neo4j.kernel.impl.api.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreReadLayer;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.graphdb.Label.label;

/**
 * Base class for disk layer tests, which test read-access to committed data.
 */
public abstract class StorageLayerTest
{
    @SuppressWarnings( "deprecation" )
    protected GraphDatabaseAPI db;
    protected final Label label1 = label( "FirstLabel" );
    protected final Label label2 = label( "SecondLabel" );
    protected final RelationshipType relType1 = RelationshipType.withName( "type1" );
    protected final RelationshipType relType2 = RelationshipType.withName( "type2" );
    protected final String propertyKey = "name";
    protected final String otherPropertyKey = "age";
    protected KernelStatement state;
    protected StoreReadLayer disk;

    @SuppressWarnings( "deprecation" )
    @BeforeEach
    public void before()
    {
        db = (GraphDatabaseAPI) createGraphDatabase();
        DependencyResolver resolver = db.getDependencyResolver();
        this.disk = resolver.resolveDependency( StorageEngine.class ).storeReadLayer();
        this.state = new KernelStatement( null, null, disk.newStatement(), new Procedures(), new CanWrite(),
                LockTracer.NONE, null, new ClockContext(), EmptyVersionContextSupplier.EMPTY );
    }

    protected GraphDatabaseService createGraphDatabase()
    {
        return new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterEach
    public void after()
    {
        db.shutdown();
    }

    protected static Node createLabeledNode( GraphDatabaseService db, Map<String,Object> properties, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String,Object> property : properties.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            tx.success();
            return node;
        }
    }

    protected int labelId( Label label )
    {
        try ( Transaction ignored = db.beginTx();
                Statement statement = statement() )
        {
            return statement.readOperations().labelGetForName( label.name() );
        }
    }

    protected int relationshipTypeId( RelationshipType type )
    {
        try ( Transaction ignored = db.beginTx();
                Statement statement = statement() )
        {
            return statement.readOperations().relationshipTypeGetForName( type.name() );
        }
    }

    protected int propertyKeyId( String propertyKey )
    {
        try ( Transaction ignored = db.beginTx();
                Statement statement = statement() )
        {
            return statement.readOperations().propertyKeyGetForName( propertyKey );
        }
    }

    protected Statement statement()
    {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        return dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ).get();
    }
}
