/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import org.neo4j.backup.log.InconsistencyLoggingTransactionInterceptorProvider;
import org.neo4j.backup.log.VerifyingTransactionInterceptorProvider;

enum VerificationLevel
{
    NONE( null, null ),
    VERIFYING( VerifyingTransactionInterceptorProvider.NAME, "true" ),
    LOGGING( InconsistencyLoggingTransactionInterceptorProvider.NAME, "diff" ),
    FULL_WITH_LOGGING( InconsistencyLoggingTransactionInterceptorProvider.NAME, "full" );

    final String interceptorName;
    final String configValue;

    private VerificationLevel( String name, String value )
    {
        this.interceptorName = name;
        this.configValue = value;
    }

    static VerificationLevel valueOf( boolean verification )
    {
        return verification ? VERIFYING : NONE;
    }
}
