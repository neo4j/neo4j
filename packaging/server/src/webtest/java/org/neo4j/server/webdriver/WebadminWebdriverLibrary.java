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

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class WebadminWebdriverLibrary extends WebdriverLibrary
{
    
    private String serverUrl;
    private final ElementReference dataBrowserSearchField;
    
    public WebadminWebdriverLibrary(WebDriver d, String serverUrl) {
        super(d);
        
        setServerUrl( serverUrl );
        dataBrowserSearchField = new ElementReference(d, By.id( "data-console" ));
    }
    
    public void setServerUrl(String url) {
        serverUrl = url;
    }
    
    public void goToServerRoot() {
        d.get( serverUrl );
    }
    
    public void goToWebadminStartPage() throws Exception {
        goToServerRoot();
        waitForTitleToBe( "Neo4j Monitoring and Management Tool" );
    }
    
    public void clickOnTab(String tab) {
        getElement( By.xpath( "//ul[@id='mainmenu']//a[contains(.,'"+tab+"')]") ).click();
    }
    
    public void searchForInDataBrowser(String query) {
        dataBrowserSearchField.sendKeys( query );
    }
    
}
