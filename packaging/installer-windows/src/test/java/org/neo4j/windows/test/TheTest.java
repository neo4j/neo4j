/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

public class TheTest {
    @Test
    public void Test() throws Throwable {
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