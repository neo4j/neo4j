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

import org.junit.Test;
import org.openqa.selenium.By;

public class IndexManagerWebIT extends AbstractWebadminTest {

    @Test
    public void createNodeIndexTest() 
    {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Index manager");
        wl.writeTo(By.id( "create-node-index-name" ), "mynodeindex");
        wl.clickOn(By.xpath("//button[@class='create-node-index button']"));
        wl.waitForSingleElementToAppear(By.xpath("//*[@id='node-indexes']//td[contains(.,'mynodeindex')]"));
    }
    
    @Test
    public void createRelationshipIndexTest() 
    {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Index manager");
        wl.writeTo(By.id( "create-rel-index-name" ), "myrelindex");
        wl.clickOn(By.xpath("//button[@class='create-rel-index button']"));
        wl.waitForSingleElementToAppear(By.xpath("//*[@id='rel-indexes']//td[contains(.,'myrelindex')]"));
    }
    
    @Test
    public void removeNodeIndexTest() 
    {
        createNodeIndexTest();
        wl.confirmAll();
        wl.clickOn(By.xpath("//*[@id='node-indexes']//tr[contains(.,'mynodeindex')]//button[contains(.,'Delete')]"));
        wl.waitForElementToDisappear(By.xpath("//*[@id='node-indexes']//td[contains(.,'mynodeindex')]"));
    }
    
    @Test
    public void removeRelationshipIndexTest() 
    {
        createRelationshipIndexTest();
        wl.confirmAll();
        wl.clickOn(By.xpath("//*[@id='rel-indexes']//tr[contains(.,'myrelindex')]//button[contains(.,'Delete')]"));
        wl.waitForElementToDisappear(By.xpath("//*[@id='rel-indexes']//td[contains(.,'myrelindex')]"));
    }   
    
}
