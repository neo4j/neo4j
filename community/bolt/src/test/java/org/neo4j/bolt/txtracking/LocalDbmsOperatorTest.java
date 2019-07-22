/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalDbmsOperatorTest
{
    private DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private OperatorConnector connector = mock( OperatorConnector.class );
    private LocalDbmsOperator operator = new LocalDbmsOperator( databaseIdRepository );

    private String databaseName = "my.db";

    @BeforeEach
    void setup()
    {
        when( connector.trigger( true ) ).thenReturn( Reconciliation.EMPTY );
        operator.connect( connector );
    }

    @Test
    void shouldBeAbleToDropDatabase()
    {
        operator.dropDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger( true );

        assertEquals( DROPPED, operator.desired().get( databaseIdRepository.get( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStartDatabase()
    {
        operator.startDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger( true );

        assertEquals( STARTED, operator.desired().get( databaseIdRepository.get( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStopDatabase()
    {
        operator.stopDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger( true );

        assertEquals( STOPPED, operator.desired().get( databaseIdRepository.get( databaseName ) ) );
    }
}
