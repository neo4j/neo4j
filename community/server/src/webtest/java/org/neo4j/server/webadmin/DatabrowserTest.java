package org.neo4j.server.webadmin;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.neo4j.server.webdriver.ElementReference;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;

public class DatabrowserTest extends AbstractWebadminTest {

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
