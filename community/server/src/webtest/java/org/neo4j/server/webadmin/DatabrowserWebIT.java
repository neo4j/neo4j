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
package org.neo4j.server.webadmin;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.neo4j.server.webdriver.ElementReference;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;

public class DatabrowserWebIT extends AbstractWebadminTest {

    //
    // NODE MANAGEMENT
    //
    
    @Test
    public void referenceNodeIsDefaultViewTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Data browser");
        wl.getDataBrowserItemHeadline().waitForTextToChangeTo(".+/db/data/node/0");
    }

    @Test
    public void canFindNodeByNodePredicatedIdTest() {
        long nodeId = wl.createNodeInDataBrowser();
        wl.searchForInDataBrowser("node:" + nodeId);
        wl.getDataBrowserItemHeadline().waitForTextToChangeTo(".+/db/data/node/" + nodeId);
    }
        
    @Test
    public void canFindNodeByIdTest() {
        wl.createNodeInDataBrowser();
        wl.searchForInDataBrowser("0");
        wl.getDataBrowserItemHeadline().waitForTextToChangeTo(".+/db/data/node/0");   
    }

    @Test
    public void canCreateNodeTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Data browser");
        wl.clickOnButton("Node");
        wl.getDataBrowserItemHeadline().waitForTextToChangeFrom(".+/db/data/node/0");
        wl.getDataBrowserItemHeadline().waitForTextToChangeTo(".+/db/data/node/[0-9]+");   
    }

        
    @Test
    public void canSetNodePropertyTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Data browser");
        wl.clickOnButton("Add property");
        
        wl.waitForElementToAppear(By.xpath("//li[1]/ul/li//input[@class='property-key']"));
        
        wl.writeTo(By.xpath("//li[1]/ul/li//input[@class='property-key']"), "mykey");
        wl.writeTo(By.xpath("//li[1]/ul/li//input[@class='property-value']"), "12", Keys.RETURN);
        
        wl.getElement(By.xpath("//div[@class='data-save-properties button']")).waitForTextToChangeTo("Saved");
        
        wl.searchForInDataBrowser(wl.getCurrentDatabrowserItemSubtitle());
        
        propertyShouldHaveValue("mykey","12");
    }
    
    
    //
    // RELATIONSHIP MANAGEMENT
    //
    
    @Test
    public void canCreateRelationshipTest() {
        wl.createNodeInDataBrowser();
        
        wl.clickOnButton("Relationship");
        wl.writeTo(By.xpath("//input[@id='create-relationship-to']"), "0");
        wl.clickOnButton("Create");
       
        wl.getDataBrowserItemHeadline().waitForTextToChangeTo(".+/db/data/relationship/[0-9]+");
    }

    @Test
    public void canFindRelationshipByIdTest() {
        long relId = wl.createRelationshipInDataBrowser();
        
        wl.goToWebadminStartPage();
        wl.clickOnTab("Data browser");
        
        wl.searchForInDataBrowser("rel:" + relId);
        
        wl.getDataBrowserItemHeadline().waitForTextToChangeTo(".+/db/data/relationship/" + relId);
    }

    @Test
    public void canSetRelationshipPropertiesTest() {
        canCreateRelationshipTest();
        
        wl.clickOnButton("Add property");
        
        wl.waitForElementToAppear(By.xpath("//li[1]/ul/li//input[@class='property-key']"));
        
        wl.writeTo(By.xpath("//li[1]/ul/li//input[@class='property-key']"), "mykey");
        wl.writeTo(By.xpath("//li[1]/ul/li//input[@class='property-value']"), "12", Keys.RETURN);
        
        wl.getElement(By.xpath("//div[@class='data-save-properties button']")).waitForTextToChangeTo("Saved");
        
        wl.searchForInDataBrowser(wl.getCurrentDatabrowserItemSubtitle());
        
        propertyShouldHaveValue("mykey","12");
        
    }
    
    private void propertyShouldHaveValue(String expectedKey, String expectedValue) {
        ElementReference el = wl.getElement( By.xpath( "//input[@value='"+expectedKey+"']/../../..//input[@class='property-value']"  ) );
        assertThat(el.getValue(), is(expectedValue));
    }
    
    
}
