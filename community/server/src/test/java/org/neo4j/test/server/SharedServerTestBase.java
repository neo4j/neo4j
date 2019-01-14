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
package org.neo4j.test.server;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.util.concurrent.Callable;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.test.rule.SuppressOutput;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class SharedServerTestBase
{
    protected static NeoServer server()
    {
        return server;
    }

    private static NeoServer server;

    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    @BeforeClass
    public static void allocateServer() throws Throwable
    {
        System.setProperty( "org.neo4j.useInsecureCertificateGeneration", "true" );
        suppressAll().call( (Callable<Void>) () ->
        {
            ServerHolder.setServerBuilderProperty( GraphDatabaseSettings.cypher_hints_error.name(), "true" );
            ServerHolder.setServerBuilderProperty( GraphDatabaseSettings.transaction_timeout.name(), "300s" );
            ServerHolder.setServerBuilderProperty( ServerSettings.transaction_idle_timeout.name(), "300s" );
            server = ServerHolder.allocate();
            ServerHelper.cleanTheDatabase( server );
            return null;
        } );
    }

    @AfterClass
    public static void releaseServer() throws Exception
    {
        try
        {
            suppressAll().call( (Callable<Void>) () ->
            {
                ServerHolder.release( server );
                return null;
            } );
        }
        finally
        {
            server = null;
        }
    }
}
