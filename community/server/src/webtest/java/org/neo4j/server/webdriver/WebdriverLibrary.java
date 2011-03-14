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
package org.neo4j.server.webdriver;

import static org.junit.Assert.fail;
import static org.neo4j.server.webdriver.BrowserTitleIs.browserTitleIs;
import static org.neo4j.server.webdriver.BrowserUrlIs.browserUrlIs;
import static org.neo4j.server.webdriver.ElementVisible.elementVisible;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import org.hamcrest.Matcher;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;

public class WebdriverLibrary
{
    protected final WebDriver d;
    
    public WebdriverLibrary(WebDriverFacade df) throws InvocationTargetException, IllegalAccessException, InstantiationException {
        this.d = df.getWebDriver();
    }
    
    public void clickOnButton(String text) {
        d.findElement( By.xpath( "//button[contains(.,'"+text+"')]") ).click();
    }
    
    public void clickOnLink(String text) {
        d.findElement( By.xpath( "//a[contains(.,'"+text+"')]") ).click();
    }
    
    public void waitForUrlToBe(String url) throws Exception {
        waitUntil( browserUrlIs( url ), "Url did not change within a reasonable time." );
    }
    
    public void waitForTitleToBe(String title) throws Exception {
        waitUntil( browserTitleIs( title ), "Title did not change within a reasonable time." );
    }
    
    public void waitForElementToAppear(By by) throws Exception {
        waitUntil( elementVisible( by ), "Element did not appear within a reasonable time." );
    }
    
    public void waitUntil(Matcher<WebDriver> cond, String errorMessage) throws Exception {
        waitUntil( cond, errorMessage, 10000);
    }
    
    public void clearInput(ElementReference el) {
        int len = el.getValue().length();
        while(len-- >= 0) {
            el.sendKeys( Keys.BACK_SPACE );
        }
    }
    
    public void waitUntil(Matcher<WebDriver> matcher, String errorMessage, long timeout) throws Exception {
        timeout = new Date().getTime() + timeout;
        while((new Date().getTime()) < timeout) {
          Thread.sleep( 50 );
          if (matcher.matches( d )) {
              return;
          }
        }
        
        fail(errorMessage);
    }
    
    public ElementReference getElement(By by) {
        return new ElementReference(d, by);
    }
    
    public void refresh() {
        d.get( d.getCurrentUrl() );
    }
}
