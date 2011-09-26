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
package org.neo4j.server.webadmin.steps;

import org.neo4j.server.webdriver.ElementReference;
import org.neo4j.server.webdriver.WebDriverFacade;
import org.neo4j.server.webdriver.WebadminWebdriverLibrary;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;

import cuke4duke.annotation.I18n.EN.Then;
import cuke4duke.annotation.I18n.EN.When;
import cuke4duke.spring.StepDefinitions;

@StepDefinitions
public class WebadminSteps
{ 
    private final WebDriver d;
    private final WebadminWebdriverLibrary wl;
    
    public WebadminSteps( WebDriverFacade facade, WebadminWebdriverLibrary wl ) throws Exception
    {
        d = facade.getWebDriver();
        this.wl = wl;
    }
    
    @When("^I type \"(.+)\" into the element found by the xpath (.+)$")
    public void iTypeIntoElementByXpath(String toType, String xPath) {
        ElementReference el = wl.getElement( By.xpath( xPath ) );
        wl.clearInput( el );
        el.sendKeys( toType );
    }
    
    @When("^I look at the root server page with a web browser$")
    public void iLookAtTheRootPageWithAWebBrowser()
    {
        wl.goToServerRoot();
    }
    
    @When("^I look at webadmin in a web browser$")
    public void iLookAtWebadminWithAWebBrowser() throws Exception
    {
        wl.goToWebadminStartPage();
    }
    
    @When("^I click on the (.+) tab in webadmin$")
    public void iClickOnTheXTab(String tabName) throws Exception {
        wl.clickOnTab( tabName );
    }
    
    @When("^I click on \"(.+)\" in webadmin$")
    public void iClickOnX(String text) throws Exception {
        wl.clickOn( text );
    }
    
    @When("^I click on the \"(.+)\" button in webadmin$")
    public void iClickOnXButton(String text) throws Exception {
        wl.clickOnButton( text );
    }
    
    @When("^I click on the button found by the xpath (.+)$")
    public void iClickOnButtonByXpath(String xpath) throws Exception {
        wl.clickOnByXpath(xpath);
    }
    
    @When("^I click on the \"(.+)\" link in webadmin$")
    public void iClickOnXLink(String text) throws Exception {
        wl.clickOnLink( text );
    }
    
    @When("^I look at ([^ ]+) with a web browser$")
    public void iLookAtUrlWithAWebBrowser(String url)
    {
        d.get( url );
    }
    
    @When("^I hit return in the element found by the xpath (.+)$")
    public void iHitEnterInTheElementFoundByXpath(String xpath) {
        d.findElement( By.xpath( xpath ) ).sendKeys( Keys.RETURN );
    }
    
    @When("^I click yes on all upcoming confirmation boxes$")
    public void iClickYesonAllConfirmationBoxes() {
        wl.confirmAll();
    }
    
    
    @Then("^The browser should be re-directed to (.+)$")
    public void iShouldBeRedirectedTo(String redirectUrl) throws Exception
    {
        wl.waitForUrlToBe( redirectUrl );
    }
    
    @Then("^An element should appear that can be found by the xpath (.+)$")
    public void anElementShouldAppearByXpath(String xpath) throws Exception
    {
        wl.waitForElementToAppear( By.xpath( xpath ) );
    }
    
    @Then("^A single element should appear that can be found by the xpath (.+)$")
    public void aSingleElementShouldAppearByXpath(String xpath) throws Exception
    {
        wl.waitForSingleElementToAppear( By.xpath( xpath ) );
    }

    @Then("^The element found by xpath (.+) should disappear$")
    public void elementShouldDisappearByXpath(String xpath) throws Exception
    {
        wl.waitForElementToDisappear( By.xpath( xpath ) );
    }
}
