package org.neo4j.server.ext.visualization.features;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import cuke4duke.annotation.I18n.EN.*;
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

    public WebappSteps(WebDriverFacade facade) throws InvocationTargetException, InstantiationException, IllegalAccessException {
        d = facade.getWebDriver();
    }

    @Given("I am on the Google search page")
    public void visit() {
        d.get("http://google.com/");
    }

    @When("^I search for \"([^\"]*)\"$")
    public void search(String query) {
        WebElement searchField = d.findElement( By.name( "q" ));
        searchField.sendKeys(query);
        // WebDriver will find the containing form for us from the searchField element
        searchField.submit();
    }
}

