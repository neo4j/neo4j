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
package org.neo4j.bolt.v1.runtime;

import java.util.Map;

import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.kernel.internal.Version;
import org.neo4j.logging.internal.LogService;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

public class BoltStateMachineV1SPI implements BoltStateMachineSPI
{
    public static final String BOLT_SERVER_VERSION_PREFIX = "Neo4j/";
    private final UsageData usageData;
    private final ErrorReporter errorReporter;
    private final Authentication authentication;
    private final String version;
    private final TransactionStateMachineSPI transactionSpi;

    public BoltStateMachineV1SPI( UsageData usageData, LogService logging,
            Authentication authentication, TransactionStateMachineSPI transactionStateMachineSPI )
    {
        this.usageData = usageData;
        this.errorReporter = new ErrorReporter( logging );
        this.authentication = authentication;
        this.transactionSpi = transactionStateMachineSPI;
        this.version = BOLT_SERVER_VERSION_PREFIX + Version.getNeo4jVersion();
    }

    @Override
    public TransactionStateMachineSPI transactionSpi()
    {
        return transactionSpi;
    }

    @Override
    public void reportError( Neo4jError err )
    {
        errorReporter.report( err );
    }

    @Override
    public AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException
    {
        return authentication.authenticate( authToken );
    }

    @Override
    public void udcRegisterClient( String clientName )
    {
        usageData.get( UsageDataKeys.clientNames ).add( clientName );
    }

    @Override
    public String version()
    {
        return version;
    }
}
