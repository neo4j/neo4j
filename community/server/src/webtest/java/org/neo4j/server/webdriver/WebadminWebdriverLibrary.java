/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.openqa.selenium.JavascriptExecutor;

public class WebadminWebdriverLibrary extends WebdriverLibrary
{
    
    private static final String USE_DEV_HTML_FILE_KEY = "testWithDevHtmlFile";
    private static final String AVOID_REDIRECT_AND_GO_STRAIGHT_TO_WEB_ADMIN_HOMEPAGE = "avoidRedirectAndGoStraightToWebAdminHomepage";

    private String serverUrl;
    private final ElementReference dataBrowserSearchField;
    private final ElementReference dataBrowserItemSubtitle;
    private final ElementReference dataBrowserSearchButton;
	private final ElementReference dataBrowserTitle;

    public WebadminWebdriverLibrary(WebDriverFacade wf, String serverUrl) throws InvocationTargetException, IllegalAccessException, InstantiationException {
        super(wf);
        
        setServerUrl( serverUrl );

        dataBrowserTitle        = new ElementReference(this, By.xpath( "//div[@id='data-area']//h3" ));
        dataBrowserItemSubtitle = new ElementReference(this, By.xpath( "//div[@id='data-area']//div[@class='title']//p[@class='small']" ));
        dataBrowserSearchField  = new ElementReference(this, By.xpath( "//div[@id='data-console']//textarea" ));
        
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
        } else if(avoidRedirectAndGoStraightToWebAdminHomepage()) {
            d.get( serverUrl + "webadmin/" );
        } else {
            goToServerRoot();
        }
        waitForTitleToBe( "Neo4j Monitoring and Management Tool" );
    }

    public void clickOnTab( String tabName )
    {
        ElementReference tab = getElement( By.xpath( "//*[@id='mainmenu']//a[contains(.,'" + tabName + "')]" ) );
        new Condition<ElementReference>( new ElementClickable(), tab ).waitUntilFulfilled();
    }

    public void searchForInDataBrowser(String query) {
    	dataBrowserSearchField.waitUntilVisible();
    	executeScript("document.dataBrowserEditor.setValue(\""+query+"\")");
        dataBrowserSearchButton.click();
    }
    
    public long createNodeInDataBrowser() {
        goToWebadminStartPage();
        clickOnTab( "Data browser" );
        
        clickOnButton( "Node" );
        
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
    
    public String getCurrentDatabrowserItemSubtitle() {
        return getDataBrowserItemSubtitle().getText();
    }
    
    public ElementReference getDataBrowserItemSubtitle() 
    {
        return dataBrowserItemSubtitle;
    }
    
    public ElementReference getDataBrowserItemTitle() 
    {
        return dataBrowserTitle;
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

    private boolean avoidRedirectAndGoStraightToWebAdminHomepage()
    {
        return System.getProperty( AVOID_REDIRECT_AND_GO_STRAIGHT_TO_WEB_ADMIN_HOMEPAGE, "false" ).equals("true");
    }

    public void confirmAll()
    {
        executeScript( "window.confirm=function(){return true;}", "" );
    }
    
    public Object executeScript(String script, Object ... args) {
        if(d instanceof JavascriptExecutor ) {
            JavascriptExecutor javascriptExecutor = (JavascriptExecutor) d;
            return javascriptExecutor.executeScript( script, args);
        } else {
            throw new RuntimeException("Arbitrary script execution is only available for WebDrivers that implement " +
                    "the JavascriptExecutor interface.");
        }
    }

    public void writeTo(By by, CharSequence ... toWrite) {
        ElementReference el = getElement( by );
        el.click();
        el.clear();
        el.sendKeys( toWrite );
    }
    
    private long extractEntityIdFromLastSegmentOfUrl(String url) {
        return Long.valueOf(url.substring(url.lastIndexOf("/") + 1,url.length()));
    }

}
