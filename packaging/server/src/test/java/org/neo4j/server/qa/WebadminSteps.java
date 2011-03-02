/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

package org.neo4j.server.qa;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.lang.reflect.InvocationTargetException;

import org.openqa.selenium.WebDriver;

import cuke4duke.annotation.I18n.EN.Then;
import cuke4duke.annotation.I18n.EN.When;
import cuke4duke.spring.StepDefinitions;

@StepDefinitions
public class WebadminSteps
{ 
    private final WebDriver d;

    public WebadminSteps( WebDriverFacade facade ) throws InvocationTargetException, InstantiationException, IllegalAccessException
    {
        d = facade.getWebDriver();
    }
    
    @When("^I look at the root page with a web browser$")
    public void iLookAtTheRootPageWithAWebBrowser()
    {
        d.get( "http://localhost:8080/" );
    }
    
    @When("^I look at the data browser page$")
    public void iLookAtTheNeo4jVisualizationPage()
    {
        d.get( "http://localhost:8080/webadmin/#/data" );
    }
    
    @Then("^I should be re-directed to the web administration page$")
    public void iShouldBeRedirectedToWebadmin() 
    {
        assertThat( d.getCurrentUrl(), is( "http://localhost:8080/webadmin/" ));
    }
    
}
