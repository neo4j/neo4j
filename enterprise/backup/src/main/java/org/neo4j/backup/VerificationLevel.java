/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.backup.log.InconsistencyLoggingTransactionInterceptorProvider;
import org.neo4j.backup.log.VerifyingTransactionInterceptorProvider;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.ConfigParam;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

import static org.neo4j.consistency.checking.incremental.intercept.InconsistencyLoggingTransactionInterceptorProvider.CheckerMode.DIFF;
import static org.neo4j.consistency.checking.incremental.intercept.InconsistencyLoggingTransactionInterceptorProvider.CheckerMode.FULL;

enum VerificationLevel implements ConfigParam
{
    NONE( null, null )
    {
        @Override
        public void configure( Map<String, String> config )
        {
            // do nothing
        }
    },
    VERIFYING( VerifyingTransactionInterceptorProvider.NAME, "true" ),
    LOGGING( InconsistencyLoggingTransactionInterceptorProvider.NAME, DIFF.name() ),
    FULL_WITH_LOGGING( InconsistencyLoggingTransactionInterceptorProvider.NAME, FULL.name() );

    private final String interceptorName;
    private final String configValue;

    private VerificationLevel( String name, String value )
    {
        this.interceptorName = name;
        this.configValue = value;
    }

    static VerificationLevel valueOf( boolean verification )
    {
        return verification ? VERIFYING : NONE;
    }

    @Override
    public void configure( Map<String, String> config )
    {
        configure( config, configValue );
    }

    void configureWithDiffLog( Map<String, String> config, String targetFile )
    {
        configure( config, configValue + ";log=" + targetFile );
    }

    private void configure( Map<String, String> config, String value )
    {
        config.put( GraphDatabaseSettings.intercept_deserialized_transactions.name(), "true" );
        config.put( TransactionInterceptorProvider.class.getSimpleName() + "." + interceptorName, value );
    }
}
