/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.windows.test;

import com.sun.jersey.api.client.ClientHandlerException;
import org.junit.Test;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

public class TestInstaller {
    @Test
    public void testInstallAndUninstall() throws Throwable {
        Result install = run("msiexec /i target\\neo4j-community-setup-1.6-SNAPSHOT.msi /quiet INSTALL_DIR=\"C:\\det är dåligt. mycket mycket dåligt. gör inte såhär.\"");
        install.checkResults();
		checkDataRest();
		
        Result uninstall = run("msiexec /x target\\neo4j-community-setup-1.6-SNAPSHOT.msi /quiet");
        uninstall.checkResults();
		try {
			checkDataRest();
			fail("Server is still listening to port 7474 even after uninstall");
		} catch (ClientHandlerException e) {
		}
	}
	
	private void checkDataRest() throws Exception {
		JaxRsResponse r = RestRequest.req().get("http://localhost:7474/db/data/");
		assertThat(r.getStatus(), equalTo(200));
    } 

    private Result run(String cmd) throws IOException, InterruptedException {
        System.out.println(cmd);
        StringBuilder builder = new StringBuilder();
        Process installer = Runtime.getRuntime().exec(cmd);
        BufferedReader bri = new BufferedReader(new InputStreamReader(installer.getInputStream()));
        BufferedReader bre = new BufferedReader(new InputStreamReader(installer.getErrorStream()));
        String line;
        while ((line = bri.readLine()) != null) {
            builder.append(line);
            builder.append("\r\n");
        }
        bri.close();
        while ((line = bre.readLine()) != null) {
            builder.append(line);
            builder.append("\r\n");
        }
        bre.close();
        installer.waitFor();

        return new Result(installer.exitValue(), builder.toString(), cmd);
    }

    private class Result {
        private final int result;
        private final String message;

        private Result(int result, String message, String cmd) {
            this.result = result;
            String userDir = System.getProperty("user.dir");

            this.message = "Path: " + userDir + "\r\n" +
                    "Cmd: " + cmd + "\r\n" +
                    message;
        }

        public int getResult() {
            return result;
        }

        public String getMessage() {
            return message;
        }

        public void checkResults() {
            assertEquals(message, getResult(), 0);
        }
    }
}