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
package org.neo4j.dbms.diagnostics.jmx;

import java.util.Map;

import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class ReportsBean extends ManagementBeanProvider
{
    public ReportsBean()
    {
        super( Reports.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
    {
        return new ReportsImpl( management, false );
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        return new ReportsImpl( management, true );
    }

    private static class ReportsImpl extends Neo4jMBean implements Reports
    {
        private final GraphDatabaseAPI graphDatabaseAPI;

        ReportsImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            graphDatabaseAPI = management.getKernelData().graphDatabase();
        }

        @Override
        public String listTransactions()
        {
            String res;
            try ( Transaction tx = graphDatabaseAPI.beginTx() )
            {
                res = graphDatabaseAPI.execute( "CALL dbms.listTransactions()" ).resultAsString();

                tx.success();
            }
            catch ( QueryExecutionException e )
            {
                res = "dbms.listTransactions() is not available";
            }
            return res;
        }

        @Override
        public String getEnvironmentVariables()
        {
            StringBuilder sb = new StringBuilder();
            for ( Map.Entry<String,String> env : System.getenv().entrySet() )
            {
                sb.append( env.getKey() ).append( '=' ).append( env.getValue() ).append( '\n' );
            }
            return sb.toString();
        }
    }
}
