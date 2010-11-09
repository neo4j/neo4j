/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.webadmin.backup.BackupManager;
import org.neo4j.server.webadmin.domain.BackupFailedException;
import org.neo4j.server.webadmin.rest.BackupService;
import org.quartz.SchedulerException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

@Ignore
public class ScheduleBackupFunctionalTest {

    @BeforeClass
    public static void startWebServer() throws IOException, SchedulerException, BackupFailedException {
        ServerTestUtils.nukeServer();
        ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        BackupManager.INSTANCE.start();
    }

    @AfterClass
    public static void stopWebServer() throws Exception {
        BackupManager.INSTANCE.stop();
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldGet200ForProperRequest() {
        Client client = Client.create();
        WebResource createResource = client.resource(NeoServer.server().webadminUri() + BackupService.ROOT_PATH + BackupService.JOBS_PATH);

        String properJSON = "{" + "\"name\":\"Daily\"," + "\"cronExpression\":\"0 0 0 ? 0 0\"," + "\"autoFoundation\":true," + "\"backupPath\":\"backup1\"}";

        ClientResponse scheduleResponse = createResource.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(properJSON).put(
                ClientResponse.class);

        assertEquals(200, scheduleResponse.getStatus());
    }

    @Test
    public void shouldGet400WhenSendingMalformedJSON() {
        Client client = Client.create();
        WebResource createResource = client.resource(NeoServer.server().webadminUri() + BackupService.ROOT_PATH + BackupService.JOBS_PATH);

        String badJSON = "this:::isNot::JSON}";

        ClientResponse scheduleResponse = createResource.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(badJSON).put(
                ClientResponse.class);

        System.out.println(scheduleResponse);
        assertEquals(400, scheduleResponse.getStatus());
    }
}
