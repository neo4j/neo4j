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
package org.neo4j.bolt.v1.runtime;

import java.util.Map;

import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.values.storable.Values;

public class BoltAuthenticationHelper
{
    public static boolean processAuthentication( String userAgent, Map<String,Object> authToken, StateMachineContext context ) throws BoltConnectionFatality
    {
        try
        {
            BoltStateMachineSPI boltSpi = context.boltSpi();

            AuthenticationResult authResult = boltSpi.authenticate( authToken );
            String username = authResult.getLoginContext().subject().username();
            context.authenticatedAsUser( username, userAgent );

            StatementProcessor statementProcessor = new TransactionStateMachine( boltSpi.transactionSpi(), authResult, context.clock() );
            context.connectionState().setStatementProcessor( statementProcessor );

            if ( authResult.credentialsExpired() )
            {
                context.connectionState().onMetadata( "credentials_expired", Values.TRUE );
            }
            context.connectionState().onMetadata( "server", Values.stringValue( boltSpi.version() ) );
            boltSpi.udcRegisterClient( userAgent );

            return true;
        }
        catch ( Throwable t )
        {
            context.handleFailure( t, true );
            return false;
        }
    }
}
