package org.neo4j.server.webadmin;

import org.junit.Test;
import org.openqa.selenium.By;

public class IndexManagerTest extends AbstractWebadminTest {

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
