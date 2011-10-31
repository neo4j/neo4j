package org.neo4j.server.webadmin;

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;

public class LoadingSpinnerTest extends AbstractWebadminTest {

    @Test
    @Ignore
    public void showsLoadingSpinnerTest() {
        
        // Broken due to http://code.google.com/p/selenium/issues/detail?id=1723
        
        wl.goToWebadminStartPage();
        wl.clickOnTab("Console");
        wl.writeTo(By.id("console-input"), "Thread.sleep(3000);", Keys.RETURN);
        wl.waitForElementToAppear(By.xpath("//div[@class='loading-spinner']"));
    }
    
}
