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
