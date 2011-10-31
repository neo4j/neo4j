package org.neo4j.server.webadmin;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;

public class ConsoleTest extends AbstractWebadminTest {

    //
    // CYPHER
    //

    @Test
    public void hasCypherConsoleTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("Cypher");
        wl.waitForElementToAppear(By
                .xpath("//ul/li[contains(.,'cypher')]"));
    }

    @Test
    public void remembersCypherStateWhenSwitchingTabsTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("Cypher");
        wl.clickOnTab("Data browser");
        wl.clickOnTab("Console");
        wl.waitForElementToAppear(By
                .xpath("//p[contains(.,'Cypher query language')]"));
    }

    @Test
    @Ignore
    public void cypherHasMultilineInput() {
        
        // Broken due to http://code.google.com/p/selenium/issues/detail?id=1723
        
        hasCypherConsoleTest();
        
        wl.writeTo(By.id("console-input"), "start a=node(1)", Keys.RETURN);
        wl.writeTo(By.id("console-input"), "return a", Keys.RETURN);
        wl.writeTo(By.id("console-input"), Keys.RETURN);

        wl.waitForElementToAppear(By.xpath("//li[contains(.,'Node[0]')]"));
    }

    //
    // GREMLIN
    //

    @Test
    public void hasGremlinConsoleTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.waitForElementToAppear(By
                .xpath("//li[contains(.,'gremlin')]"));
    }

    @Test
    @Ignore
    public void canAccessDatabaseViaGremlinConsoleTest() {
        
        // Broken due to http://code.google.com/p/selenium/issues/detail?id=1723
        
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        
        wl.writeTo(By.id("console-input"), "g.v(0)", Keys.RETURN);

        wl.waitForElementToAppear(By.xpath("//li[contains(.,'v[0]')]"));
    }

    //
    // HTTP CONSOLE
    //

    @Test
    public void hasHttpConsoleTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.waitForElementToAppear(By.xpath("//p[contains(.,'HTTP Console')]"));
    }

    @Test
    public void canAccessServerViaHttpConsoleTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "GET /db/data/", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'200')]"));
    }

    @Test
    public void httpConsoleShows404ProperlyTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "GET /asd/ads/", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'404')]"));
    }

    @Test
    public void httpConsoleShowsSyntaxErrorsTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "blus 12 blah blah", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'Invalid')]"));
    }

    @Test
    public void httpConsoleShowsJSONErrorsTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.clickOnLink("HTTP");

        wl.writeTo(By.id("console-input"), "POST / {blah}", Keys.RETURN);
        
        wl.waitForElementToAppear(By.xpath("//li[contains(.,'Invalid')]"));
    }

}
