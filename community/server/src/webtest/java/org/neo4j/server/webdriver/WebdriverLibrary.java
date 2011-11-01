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
package org.neo4j.server.webdriver;

import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;
import static org.neo4j.server.webdriver.BrowserTitleIs.browserTitleIs;
import static org.neo4j.server.webdriver.BrowserUrlIs.browserUrlIs;
import static org.neo4j.server.webdriver.ElementVisible.elementVisible;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import org.hamcrest.Matcher;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class WebdriverLibrary
{
    protected final WebDriver d;
    
    public WebdriverLibrary(WebDriverFacade df) throws InvocationTargetException, IllegalAccessException, InstantiationException {
        d = df.getWebDriver();
    }
    
    public WebDriver getWebDriver() {
        return d;
    }
    
    public void clickOn(String contains) {
        clickOnByXpath("//*[contains(.,'"+contains+"')]");
    }
    
    public void clickOn(By by) {
    	getElement( by ).click();
    }
    
    public void clickOnButton(String text) {
    	clickOnByXpath("//div[contains(.,'"+text+"') and contains(@class, 'button')]");
    }
    
    public void clickOnLink(String text) {
    	clickOnByXpath( "//a[contains(.,'"+text+"')]");
    }

    public void clickOnByXpath( String xpath )
    {
        clickOn( By.xpath( xpath ) );
    }
    
    public void waitForUrlToBe(String url) {
        waitUntil( browserUrlIs( url ), "Url did not change to expected value within a reasonable time." );
    }
    
    public void waitForTitleToBe(String title) {
        waitUntil( browserTitleIs( title ), "Title did not change to expected value within a reasonable time." );
    }
    
    public void waitForElementToAppear(By by) {
        waitUntil( elementVisible( by ), "Element ("+by.toString()+") did not appear within a reasonable time." );
    }
    
    public void waitForElementToDisappear(By by) {
        waitUntil( not( elementVisible( by )), "Element did not disappear within a reasonable time." );
    }
    
    public void clearInput(ElementReference el) {
        int len = el.getValue().length();
        
        CharSequence[] backspaces = new CharSequence[len];
        Arrays.fill(backspaces, Keys.BACK_SPACE);
        
        el.sendKeys( backspaces );
    }

    public void waitUntil(Matcher<WebDriver> matcher, String errorMessage) {
        waitUntil( matcher, errorMessage, 10000);
    }
    
    public void waitUntil(Matcher<WebDriver> matcher, String errorMessage, long timeout) {
        try {
            Condition<WebDriver> cond = new WebdriverCondition<WebDriver>( getWebDriver(), matcher, d);
            cond.waitUntilFulfilled(timeout, errorMessage);
        } catch( TimeoutException e) {
            fail(errorMessage);
        }
    }
    
    public ElementReference getElement(By by) {
        return new ElementReference(this, by);
    }
    
    public WebElement getWebElement(By by) {
        waitForElementToAppear(by);
        return d.findElement( by );
    }
    
    public void refresh() {
        d.get( d.getCurrentUrl() );
    }
}
