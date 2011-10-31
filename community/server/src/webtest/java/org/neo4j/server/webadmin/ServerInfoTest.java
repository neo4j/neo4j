package org.neo4j.server.webadmin;

import org.junit.Test;
import org.openqa.selenium.By;

public class ServerInfoTest extends AbstractWebadminTest {

    @Test
    public void canShowJMXValuesTest() {
        wl.goToWebadminStartPage();
        wl.clickOnTab("Server info");
        wl.clickOnLink("Primitive count");
        
        wl.waitForElementToAppear(By.xpath("//h2[contains(.,'Primitive count')]"));
    }
}
