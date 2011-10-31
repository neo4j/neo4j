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

import java.lang.reflect.InvocationTargetException;

import org.openqa.selenium.By;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;

public class WebadminWebdriverLibrary extends WebdriverLibrary
{
    
    private static final String USE_DEV_HTML_FILE_KEY = "testWithDevHtmlFile";
    private String serverUrl;
    private final ElementReference dataBrowserSearchField;
    private final ElementReference dataBrowserItemSubtitle;
    private final ElementReference dataBrowserSearchButton;
    
    public WebadminWebdriverLibrary(WebDriverFacade wf, String serverUrl) throws InvocationTargetException, IllegalAccessException, InstantiationException {
        super(wf);
        
        setServerUrl( serverUrl );
        
        dataBrowserItemSubtitle = new ElementReference(this, By.xpath( "//div[@id='data-area']//div[@class='title']//p[@class='small']" ));
        dataBrowserSearchField = new ElementReference(this, By.id( "data-console" ));
        dataBrowserSearchButton = new ElementReference(this, By.id( "data-execute-console" ));
    }
    
    public void setServerUrl(String url) {
        serverUrl = url;
    }
    
    public void goToServerRoot() {
        d.get( serverUrl );
    }
    
    public void goToWebadminStartPage() {
        if(isUsingDevDotHTML()) {
            d.get( serverUrl + "webadmin/dev.html" );
        } else {
            goToServerRoot();
        }
        waitForTitleToBe( "Neo4j Monitoring and Management Tool" );
    }

    public void clickOnTab(String tab) {
        getElement( By.xpath( "//ul[@id='mainmenu']//a[contains(.,'"+tab+"')]") ).click();
    }
    
    public void searchForInDataBrowser(CharSequence ... keysToSend) {
        clearInput( dataBrowserSearchField );
        dataBrowserSearchField.sendKeys( keysToSend );
        dataBrowserSearchButton.click();
    }
    
    public String getCurrentDatabrowserItemSubtitle() {
        return dataBrowserItemSubtitle.getText();
    }
    
    public long createNodeInDataBrowser() {
        goToWebadminStartPage();
        clickOnTab( "Data browser" );
        String prevItemHeadline = getCurrentDatabrowserItemSubtitle();
        
        clickOnButton( "Node" );
        
        dataBrowserItemSubtitle.waitForTextToChangeFrom( prevItemHeadline );
        
        return extractEntityIdFromLastSegmentOfUrl(getCurrentDatabrowserItemSubtitle());
    }
    
    public long createRelationshipInDataBrowser() {
        createNodeInDataBrowser();
        String prevItemHeadline = getCurrentDatabrowserItemSubtitle();
        
        clickOnButton( "Relationship" );
        getElement( By.id( "create-relationship-to" ) ).sendKeys( "0" );
        clickOnButton( "Create" );
        
        dataBrowserItemSubtitle.waitForTextToChangeFrom( prevItemHeadline );

        return extractEntityIdFromLastSegmentOfUrl(getCurrentDatabrowserItemSubtitle());
    }
    
    public ElementReference getDataBrowserItemHeadline() {
        return dataBrowserItemSubtitle;
    }

    public void waitForSingleElementToAppear( By xpath )
    {
        waitForElementToAppear( xpath );
        int numElems = d.findElements( xpath ).size();
        if( numElems != 1) {
            throw new ConditionFailedException("Expected single element, got " + numElems + " :(." , null);
        }
    }
    
    private boolean isUsingDevDotHTML()
    {
        return System.getProperty( USE_DEV_HTML_FILE_KEY, "false" ).equals("true");
    }

    public void confirmAll()
    {
        executeScript( "window.confirm=function(){return true;}", "" );
    }
    
    public Object executeScript(String script, Object ... args) {
        if(d instanceof FirefoxDriver) {
            FirefoxDriver fd = (FirefoxDriver)d;
            return fd.executeScript( script, args);
        } else if (d instanceof ChromeDriver) {
            ChromeDriver cd = (ChromeDriver)d;
            return cd.executeScript( script, args );
        } else if(d instanceof InternetExplorerDriver) {
            InternetExplorerDriver id = (InternetExplorerDriver)d;
            return id.executeScript( script, args );
        } else {
            throw new RuntimeException("Arbitrary script execution is only available for chrome, IE and firefox.");
        }
    }

    public void writeTo(By by, CharSequence ... toWrite) {
        ElementReference el = getElement( by );
        clearInput( el );
        el.sendKeys( toWrite );
    }
    
    private long extractEntityIdFromLastSegmentOfUrl(String url) {
        return Long.valueOf(url.substring(url.lastIndexOf("/") + 1,url.length()));
    }
    
}
