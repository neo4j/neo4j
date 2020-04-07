/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.testing;

import org.assertj.core.api.Condition;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.impl.AbstractBoltStateMachine;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.runtime.ReadyState;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.messaging.BoltResponseMessage.FAILURE;
import static org.neo4j.bolt.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.bolt.messaging.BoltResponseMessage.SUCCESS;
import static org.neo4j.bolt.runtime.statemachine.impl.BoltV4MachineRoom.newMachine;
import static org.neo4j.values.storable.Values.stringValue;

public class BoltConditions
{
    private BoltConditions()
    {
    }

    public static Condition<RecordedBoltResponse> succeeded()
    {
        return new Condition<>( response -> response.message() == SUCCESS, String.valueOf( SUCCESS ) );

    }

    public static Condition<RecordedBoltResponse> succeededWithoutMetadata( final String key )
    {
        return new Condition<>( response -> response.message() == SUCCESS && !response.hasMetadata( key ), "SUCCESS " + format( " without metadata %s", key ) );
    }

    public static Condition<RecordedBoltResponse> succeededWithMetadata( final String key, final String value )
    {
        return succeededWithMetadata( key, stringValue( value ) );
    }

    public static Condition<RecordedBoltResponse> succeededWithMetadata( final String key, final AnyValue value )
    {
        return new Condition<>( response -> response.message() == SUCCESS && response.hasMetadata( key ) && response.metadata( key ).equals( value ),
                "SUCCESS " + format( " with metadata %s = %s", key, value ) );
    }

    public static Condition<RecordedBoltResponse> containsNoRecord()
    {
        return new Condition<>( response -> response.records().size() == 0, " without record" );
    }

    public static Condition<RecordedBoltResponse> containsRecord( final Object... values )
    {
        return new Condition<>( response ->
        {
            var anyValues = Arrays.stream( values ).map( ValueUtils::of ).toArray( AnyValue[]::new );
            List<AnyValue[]> records = response.records();
            return records.size() > 0 && Arrays.equals( records.get( 0 ), anyValues );
        }, format( " with record %s", values ) );
    }

    public static Condition<RecordedBoltResponse> succeededWithRecord( final Object... values )
    {
        return new Condition<>( response ->
        {
            var anyValues = Arrays.stream( values ).map( ValueUtils::of ).toArray( AnyValue[]::new );
            List<AnyValue[]> records = response.records();
            return response.message() == SUCCESS &&
                   Arrays.equals( records.get( 0 ), anyValues );
        }, "SUCCESS " + format( " with record %s", values ) );
    }

    public static Condition<RecordedBoltResponse> succeededWithMetadata( final String key, final Pattern pattern )
    {
        return new Condition<>( response -> response.message() == SUCCESS && response.hasMetadata( key ) &&
                pattern.matcher( ((TextValue) response.metadata( key )).stringValue() ).matches(),
                "SUCCESS " + format( " with metadata %s ~ %s", key, pattern ) );
    }

    public static Condition<RecordedBoltResponse> wasIgnored()
    {
        return new Condition<>( response -> response.message() == IGNORED, String.valueOf( IGNORED ) );
    }

    public static Condition<RecordedBoltResponse> failedWithStatus( Status status )
    {
        return new Condition<>( response ->
                response.message() == FAILURE && response.hasMetadata( "code" ) &&
                response.metadata( "code" ).equals( stringValue( status.code().serialize() ) ),
                format( " failure with status code %s", status.code().serialize() ) );
    }

    public static Condition<BoltStateMachine> hasTransaction()
    {
        return new Condition<>( stateMachine ->
        {
            var machine = (AbstractBoltStateMachine) stateMachine;
            var statementProcessor = machine.statementProcessor();
            return statementProcessor != null && statementProcessor.hasTransaction();
        }, "State machine has transaction." );
    }

    public static Condition<BoltStateMachine> hasNoTransaction()
    {
        return new Condition<>( stateMachine ->
        {
            var machine = (AbstractBoltStateMachine) stateMachine;
            var statementProcessor = machine.statementProcessor();
            return statementProcessor == StatementProcessor.EMPTY || !statementProcessor.hasTransaction();
        }, "State machine has no transaction." );
    }

    public static Condition<BoltStateMachine> inState( Class<? extends BoltStateMachineState> stateClass )
    {
        return new Condition<>( stateMachine ->
        {
            var machine = (AbstractBoltStateMachine) stateMachine;
            if ( stateClass == null )
            {
                return machine.state() == null;
            }
            return stateClass.isInstance( machine.state() );
        }, "in state " + (stateClass == null ? "null" : stateClass.getName()) );
    }

    public static Condition<BoltStateMachine> isClosed()
    {
        return new Condition<>( BoltStateMachine::isClosed, "is closed" );
    }

    public static Condition<BoltStateMachine> canReset()
    {
        return new Condition<>( stateMachine ->
        {
            final BoltResponseRecorder recorder = new BoltResponseRecorder();
            try
            {
                stateMachine.interrupt();
                stateMachine.process( ResetMessage.INSTANCE, recorder );
                return recorder.responseCount() == 1 && inState( ReadyState.class ).matches( stateMachine );
            }
            catch ( BoltConnectionFatality boltConnectionFatality )
            {
                return false;
            }
        }, "Can reset state machine" );
    }

    public static void verifyKillsConnection( ThrowingAction<BoltConnectionFatality> action )
    {
        try
        {
            action.apply();
            fail( "should have killed the connection" );
        }
        catch ( BoltConnectionFatality fatality )
        {
            // expected
        }
    }

    public static void verifyOneResponse( ThrowingBiConsumer<BoltStateMachine,BoltResponseRecorder,BoltConnectionFatality> transition ) throws Exception
    {
        BoltStateMachine machine = newMachine();
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        try
        {
            transition.accept( machine, recorder );
        }
        catch ( BoltConnectionFatality connectionFatality )
        {
            // acceptable for invalid transitions
        }
        assertEquals( 1, recorder.responseCount() );
    }
}
