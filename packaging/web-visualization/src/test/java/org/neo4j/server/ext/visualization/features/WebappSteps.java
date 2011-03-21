package org.neo4j.server.ext.visualization.features;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import cuke4duke.annotation.I18n.EN.*;
import cuke4duke.annotation.Pending;
import cuke4duke.spring.StepDefinitions;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.lang.reflect.InvocationTargetException;


/**
 * Steps to fulfill the webapp features.
 */
@StepDefinitions
public class WebappSteps
{
    private final WebDriver d;

    public WebappSteps( WebDriverFacade facade ) throws InvocationTargetException, InstantiationException, IllegalAccessException
    {
        d = facade.getWebDriver();
    }

    @Given("^a local web server hosting the visualization component$")
    public void aLocalWebServerHostingTheVisualizationComponent()
    {
        d.get( "http://localhost:8080" );
    }

    @When("^I look at the neo4j\\-visualization page$")
    public void iLookAtTheNeo4jVisualizationPage()
    {
        d.get( "http://localhost:8080/neo4j-visualization" );
    }

    @Then("^the page should show me a pretty graph$")
    public void thePageShouldShowMeAPrettyGraph()
    {
        assertThat( d.getPageSource(), containsString( "pretty graph" ) );
    }

}

