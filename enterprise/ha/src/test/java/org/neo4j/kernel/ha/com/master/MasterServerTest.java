/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.master;

import org.junit.Test;
import org.mockito.Mockito;


import org.neo4j.com.RequestContext;
import org.neo4j.com.Server;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.LogProvider;

import static org.mockito.Mockito.*;


public class MasterServerTest
{
    @Test
    public void shouldCleanExistentLockSessionOnFinishOffChannel() throws Exception
    {
        Master master = mock( Master.class );
        ConversationManager conversationManager = mock( ConversationManager.class );
        MasterServer masterServer = new MasterServer( master, mock( LogProvider.class ),
                mock(Server.Configuration.class ), mock( TxChecksumVerifier.class ),
                mock( ByteCounterMonitor.class ), mock( RequestMonitor.class ), conversationManager );
        RequestContext requestContext = new RequestContext( 1l, 1, 1, 0, 0l );

        masterServer.stopConversation( requestContext );

        Mockito.verify( conversationManager ).stop( requestContext );
    }
}
