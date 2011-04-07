/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.steps;

import cuke4duke.annotation.After;
import cuke4duke.annotation.I18n.EN.Given;
import cuke4duke.spring.StepDefinitions;

@StepDefinitions
public class ServerControlSteps
{
    private final ServerIntegrationTestFacade serverFacade;
    
    public ServerControlSteps(ServerIntegrationTestFacade serverFacade) {
        this.serverFacade = serverFacade;
    }
    
    @Given("^I have a neo4j server running$")
    public void iHaveANeo4jServerRunning() throws Exception {
        serverFacade.ensureServerIsRunning();
    }
    
    @After
    public void cleanup(){
        serverFacade.cleanup();
    }
}
