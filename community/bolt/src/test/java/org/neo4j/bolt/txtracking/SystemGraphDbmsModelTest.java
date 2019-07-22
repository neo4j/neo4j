/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.DELETED;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.OFFLINE;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.ONLINE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ImpermanentDbmsExtension
class SystemGraphDbmsModelTest
{
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private GraphDatabaseService db;
    private TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private Collection<DatabaseId> updatedDatabases = new ArrayList<>();
    private SystemGraphDbmsModel dbmsModel;

    @BeforeEach
    void before()
    {
        dbmsModel = new SystemGraphDbmsModel( databaseIdRepository );
        dbmsModel.setSystemDatabase( db );

        managementService.registerTransactionEventListener( DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<>()
        {
            @Override
            public void afterCommit( TransactionData txData, Object state, GraphDatabaseService databaseService )
            {
                updatedDatabases.addAll( dbmsModel.updatedDatabases( txData ) );
            }
        } );
    }

    @Test
    void shouldDetectUpdatedDatabases()
    {
        // when
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( "A", true );
            makeDatabaseNode( "B", true );
            makeDatabaseNode( "C", false );
            tx.success();
        }

        // then
        assertThat( updatedDatabases, containsInAnyOrder( databaseIds( "A", "B", "C" ) ) );

        // given
        updatedDatabases.clear();

        // when
        try ( var tx = db.beginTx() )
        {
            makeDeletedDatabaseNode( "D" );
            makeDeletedDatabaseNode( "E" );
            makeDeletedDatabaseNode( "F" );
            tx.success();
        }

        // then
        assertThat( updatedDatabases, containsInAnyOrder( databaseIds( "D", "E", "F" ) ) );
    }

    private DatabaseId[] databaseIds( String... databaseNames )
    {
        return Stream.of( databaseNames ).map( name -> databaseIdRepository.get( name ) ).toArray( DatabaseId[]::new );
    }

    @Test
    void shouldReturnDatabaseStates()
    {
        // when
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( "A", true );
            makeDatabaseNode( "B", false );
            makeDeletedDatabaseNode( "C" );
            tx.success();
        }

        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( "D", true );
            makeDeletedDatabaseNode( "E" );
            makeDeletedDatabaseNode( "F" );
            tx.success();
        }

        // then
        var expected = new HashMap<DatabaseId,SystemGraphDbmsModel.DatabaseState>();

        expected.put( databaseIdRepository.get( "A" ), ONLINE );
        expected.put( databaseIdRepository.get( "B" ), OFFLINE );
        expected.put( databaseIdRepository.get( "C" ), DELETED );
        expected.put( databaseIdRepository.get( "D" ), ONLINE );
        expected.put( databaseIdRepository.get( "E" ), DELETED );
        expected.put( databaseIdRepository.get( "F" ), DELETED );

        assertEquals( expected, dbmsModel.getDatabaseStates() );
    }

    private void makeDatabaseNode( String databaseName, boolean online )
    {
        Node node = db.createNode( SystemGraphDbmsModel.DATABASE_LABEL );
        node.setProperty( "name", databaseName );
        node.setProperty( "status", online ? "online" : "offline" );
    }

    private void makeDeletedDatabaseNode( String databaseName )
    {
        Node node = db.createNode( SystemGraphDbmsModel.DELETED_DATABASE_LABEL );
        node.setProperty( "name", databaseName );
    }
}
