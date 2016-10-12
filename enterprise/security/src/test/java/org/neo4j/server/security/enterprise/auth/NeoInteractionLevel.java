/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public interface NeoInteractionLevel<S>
{
    EnterpriseUserManager getLocalUserManager() throws Exception;

    GraphDatabaseFacade getLocalGraph();

    FileSystemAbstraction fileSystem();

    InternalTransaction beginLocalTransactionAsUser( S subject, KernelTransaction.Type txType ) throws Throwable;

    /*
     * The returned String is empty if the query executed as expected, and contains an error msg otherwise
     */
    String executeQuery( S subject, String call, Map<String,Object> params,
            Consumer<ResourceIterator<Map<String, Object>>> resultConsumer );

    S login( String username, String password ) throws Exception;

    void logout( S subject ) throws Exception;

    void updateAuthToken( S subject, String username, String password );

    String nameOf( S subject );

    void tearDown() throws Throwable;

    static String tempPath(String prefix, String suffix ) throws IOException
    {
        Path path = Files.createTempFile( prefix, suffix );
        Files.delete( path );
        return path.toString();
    }

    void assertAuthenticated( S subject );

    void assertPasswordChangeRequired( S subject ) throws Exception;

    void assertInitFailed( S subject );

    void assertSessionKilled( S subject );

    String getConnectionDetails();
}
