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

import org.junit.Ignore;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;

public class LoadingSpinnerWebIT extends AbstractWebadminTest {

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
