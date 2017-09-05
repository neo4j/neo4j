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
package org.neo4j.bolt.testing;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.StatementProcessor;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingBiConsumer;
import org.neo4j.helpers.ValueUtils;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.FAILURE;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;
import static org.neo4j.bolt.v1.runtime.BoltStateMachine.State.READY;
import static org.neo4j.bolt.v1.runtime.MachineRoom.newMachine;
import static org.neo4j.values.storable.Values.stringValue;

public class BoltMatchers
{
    private BoltMatchers()
    {
    }

    public static Matcher<RecordedBoltResponse> succeeded()
    {
        return new BaseMatcher<RecordedBoltResponse>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final RecordedBoltResponse response = (RecordedBoltResponse) item;
                return response.message() == SUCCESS;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( SUCCESS );
            }
        };
    }

    public static Matcher<RecordedBoltResponse> succeededWithMetadata( final String key, final AnyValue value )
    {
        return new BaseMatcher<RecordedBoltResponse>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final RecordedBoltResponse response = (RecordedBoltResponse) item;
                return response.message() == SUCCESS &&
                       response.hasMetadata( key ) &&
                       response.metadata( key ).equals( value );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( SUCCESS )
                        .appendText( format( " with metadata %s = %s", key, value.toString() ) );
            }
        };
    }

    public static Matcher<RecordedBoltResponse> succeededWithRecord( final Object... values )
    {
        return new BaseMatcher<RecordedBoltResponse>()
        {
            private AnyValue[] anyValues = Arrays.stream( values ).map( ValueUtils::of ).toArray( AnyValue[]::new );

            @Override
            public boolean matches( final Object item )
            {

                final RecordedBoltResponse response = (RecordedBoltResponse) item;
                QueryResult.Record[] records = response.records();
                return response.message() == SUCCESS &&
                       Arrays.equals( records[0].fields(), anyValues );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( SUCCESS ).appendText( format( " with record %s", values ) );
            }
        };
    }

    public static Matcher<RecordedBoltResponse> succeededWithMetadata( final String key, final Pattern pattern )
    {
        return new BaseMatcher<RecordedBoltResponse>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final RecordedBoltResponse response = (RecordedBoltResponse) item;
                return response.message() == SUCCESS &&
                       response.hasMetadata( key ) &&
                       pattern.matcher( ((TextValue) response.metadata( key )).stringValue() ).matches();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( SUCCESS )
                        .appendText( format( " with metadata %s ~ %s", key, pattern.toString() ) );
            }
        };
    }

    public static Matcher<RecordedBoltResponse> wasIgnored()
    {
        return new BaseMatcher<RecordedBoltResponse>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final RecordedBoltResponse response = (RecordedBoltResponse) item;
                return response.message() == IGNORED;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( IGNORED );
            }
        };
    }

    public static Matcher<RecordedBoltResponse> failedWithStatus( Status status )
    {
        return new BaseMatcher<RecordedBoltResponse>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final RecordedBoltResponse response = (RecordedBoltResponse) item;
                return response.message() == FAILURE &&
                       response.hasMetadata( "code" ) &&
                       response.metadata( "code" ).equals( stringValue( status.code().serialize() ) );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendValue( FAILURE )
                        .appendText( format( " with status code %s", status.code().serialize() ) );
            }
        };
    }

    public static Matcher<BoltStateMachine> hasTransaction()
    {
        return new BaseMatcher<BoltStateMachine>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final BoltStateMachine machine = (BoltStateMachine) item;
                final StatementProcessor statementProcessor = machine.statementProcessor();
                return statementProcessor != null && statementProcessor.hasTransaction();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "no transaction" );
            }
        };
    }

    public static Matcher<BoltStateMachine> hasNoTransaction()
    {
        return new BaseMatcher<BoltStateMachine>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final BoltStateMachine machine = (BoltStateMachine) item;
                final StatementProcessor statementProcessor = machine.statementProcessor();
                return statementProcessor == null || !statementProcessor.hasTransaction();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "no transaction" );
            }
        };
    }

    public static Matcher<BoltStateMachine> inState( final BoltStateMachine.State state )
    {
        return new BaseMatcher<BoltStateMachine>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final BoltStateMachine machine = (BoltStateMachine) item;
                return machine.state() == state;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "can reset" );
            }
        };
    }

    public static Matcher<BoltStateMachine> isClosed()
    {
        return new BaseMatcher<BoltStateMachine>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final BoltStateMachine machine = (BoltStateMachine) item;
                return machine.isClosed();
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "can reset" );
            }
        };
    }

    public static Matcher<BoltStateMachine> canReset()
    {
        return new BaseMatcher<BoltStateMachine>()
        {
            @Override
            public boolean matches( final Object item )
            {
                final BoltStateMachine machine = (BoltStateMachine) item;
                final BoltResponseRecorder recorder = new BoltResponseRecorder();
                try
                {
                    machine.reset( recorder );
                    return recorder.responseCount() == 1 && machine.state() == READY;
                }
                catch ( BoltConnectionFatality boltConnectionFatality )
                {
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "can reset" );
            }
        };
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

    public static void verifyOneResponse( BoltStateMachine.State initialState,
            ThrowingBiConsumer<BoltStateMachine,BoltResponseRecorder,BoltConnectionFatality> transition )
            throws AuthenticationException, BoltConnectionFatality
    {
        BoltStateMachine machine = newMachine( initialState );
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
