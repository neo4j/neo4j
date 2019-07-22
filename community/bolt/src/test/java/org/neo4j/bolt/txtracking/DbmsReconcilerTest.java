/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.MultiDatabaseManager;

import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DbmsReconcilerTest
{
    private MultiDatabaseManager<?> databaseManager = mock( MultiDatabaseManager.class );

//    @Test
//    void shouldNotStartInitiallyStarted() throws Exception
//    {
//        // given
//        DatabaseId databaseA = new DatabaseId( "A" );
//        DatabaseId databaseB = new DatabaseId( "B" );
//
//        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
//        initialStates.put( databaseA, STARTED );
//        initialStates.put( databaseB, STARTED );
//
//        DbmsReconciler reconciler = new DbmsReconciler( databaseManager, initialStates,
//                Config.defaults(), NullLogProvider.getInstance(), new CallingThreadJobScheduler() );
//
//        TestDbmsOperator operatorA = new TestDbmsOperator( Map.of( databaseA, STARTED ) );
//        TestDbmsOperator operatorB = new TestDbmsOperator( Map.of( databaseB, STARTED ) );
//
//        List<DbmsOperator> operators = List.of( operatorA, operatorB );
//
//        // when
//        reconciler.reconcile( operators, false );
//
//        // then
//        verifyNoMoreInteractions( databaseManager );
//    }
//
//    @Test
//    void shouldStopInitiallyStarted() throws Exception
//    {
//        // given
//        DatabaseId databaseA = new DatabaseId( "A" );
//        DatabaseId databaseB = new DatabaseId( "B" );
//
//        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
//        initialStates.put( databaseA, STARTED );
//        initialStates.put( databaseB, STARTED );
//
//        DbmsReconciler reconciler = new DbmsReconciler( databaseManager, initialStates,
//                Config.defaults(), NullLogProvider.getInstance(), new CallingThreadJobScheduler() );
//
//        TestDbmsOperator operatorA = new TestDbmsOperator( Map.of( databaseA, STOPPED ) );
//        TestDbmsOperator operatorB = new TestDbmsOperator( Map.of( databaseB, STOPPED ) );
//
//        List<DbmsOperator> operators = List.of( operatorA, operatorB );
//
//        // when
//        reconciler.reconcile( operators, false );
//
//        // then
//        verify( databaseManager ).stopDatabase( databaseA );
//        verify( databaseManager ).stopDatabase( databaseB );
//    }
//
//    @Test
//    void shouldNotTouchOperatorUnknownDatabases() throws Exception
//    {
//        // given
//        DatabaseId databaseA = new DatabaseId( "A" );
//        DatabaseId databaseB = new DatabaseId( "B" );
//
//        Map<DatabaseId,OperatorState> initialStates = new HashMap<>();
//        initialStates.put( databaseA, STARTED );
//        initialStates.put( databaseB, STOPPED );
//
//        DbmsReconciler reconciler = new DbmsReconciler( databaseManager, initialStates,
//               Config.defaults(), NullLogProvider.getInstance(), new CallingThreadJobScheduler() );
//
//        TestDbmsOperator operatorA = new TestDbmsOperator( emptyMap() );
//        TestDbmsOperator operatorB = new TestDbmsOperator( emptyMap() );
//
//        List<DbmsOperator> operators = List.of( operatorA, operatorB );
//
//        // when
//        reconciler.reconcile( operators, false );
//
//        // then
//        verifyNoMoreInteractions( databaseManager );
//    }
}
