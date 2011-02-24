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
package org.neo4j.server.webadmin.webtest;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

/**
 * This is used rather than a direct
 * (RenderedWebElement) reference, because our UI renders a lot, and such
 * references often become stale. This allows code that encounters
 * StaleElementReferenceException to simply reload the element.
 * 
 * This class also provides a subset of the RenderedWebElement API, which
 * abstracts away StaleElementReferenceException.
 */
public class ElementReference {

	protected By selector;
	protected WebDriver webDriver;
	protected boolean matchLast;
	
	public ElementReference(WebDriver webDriver, By selector) {
        this(webDriver, selector, false);
    }
	
	public ElementReference(WebDriver webDriver, By selector, boolean matchLast) {
		this.webDriver = webDriver;
		this.selector = selector;
		this.matchLast = matchLast;
	}
	
	/**
	 * Attempts to fetch this element. If no element can be found, 
	 * fetching the element will be retried until one is found, or
	 * 10 seconds have passed.
	 * 
	 * @return
	 */
	public RenderedWebElement getElement() {
		return getElement(selector);
    }
	
	/**
	 * Attempts to fetch an element. If no element can be found, 
	 * fetching will be retried until one is found, or
	 * 10 seconds have passed.
	 * 
	 * @return
	 */
	public RenderedWebElement getElement(By selector) {
		long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            try {
                if(matchLast) {
                    List<WebElement> els = webDriver.findElements(selector);
                    if(els.size() > 0) {
                        return (RenderedWebElement) els.get(els.size()-1);
                    }
                } else {
                    return (RenderedWebElement) webDriver.findElement(selector);
                }
            } catch (NoSuchElementException ex) {
                try {
					Thread.sleep(13);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
            }
        }

        throw new NoSuchElementException("Unable to locate element: " + selector.toString());
    }
	
	public RenderedWebElement findElement(By by) {
		try {
    		return (RenderedWebElement)this.getElement().findElement(by);
    	} catch (StaleElementReferenceException e) {
    		return this.findElement(by);
        }
	}
	
	public List<WebElement> findElements(By by) {
		try {
    		return this.getElement().findElements(by);
    	} catch (StaleElementReferenceException e) {
    		return this.findElements(by);
        }
	}

	public String getAttribute( String attributeName ) {
		try {
    		return this.getElement().getAttribute(attributeName);
    	} catch (StaleElementReferenceException e) {
    		return this.getAttribute(attributeName);
        }
	}
	
	public void click() {
		try {
			this.getElement().click();
		} catch (StaleElementReferenceException e) {
			this.click();
	    }
	}
	
	public String getValueOfCssProperty(String cssProperty) {
		try {
    		return this.getElement().getValueOfCssProperty(cssProperty);
    	} catch (StaleElementReferenceException e) {
    		return this.getValueOfCssProperty(cssProperty);
        }
	}
	
	public void sendKeys(CharSequence ... keysToSend) {
		try {
    		this.getElement().sendKeys(keysToSend);
    	} catch (StaleElementReferenceException e) {
    		this.sendKeys(keysToSend);
        }
	}
	
	public String getValue() {
		try {
    		return this.getElement().getValue();
    	} catch (StaleElementReferenceException e) {
    		return this.getValue();
        }
	}
	
	public String getText() {
		try {
    		return this.getElement().getText();
    	} catch (StaleElementReferenceException e) {
    		return this.getText();
        }
	}
	
	public void clear() {
		try {
			this.getElement().clear();
    	} catch (StaleElementReferenceException e) {
    		this.clear();
        }
	}
	
	public void waitUntilVisible() {
        long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            try {
                if ( ! this.getValueOfCssProperty("display").equals("none")) {
                    return;
                }
            } catch(NoSuchElementException e) {
                // Empty
            }
            try{
            	Thread.sleep(13);
            } catch(Exception e) {
            	throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Element did not become visible within a reasonable time.");
    }
    
    public void waitUntilNotVisible() {
        long end = System.currentTimeMillis() + 10000;
        try{
            while (System.currentTimeMillis() < end) {
                if ( this.getElement().getValueOfCssProperty("display").equals("none")) {
                    return;
                }
                try{
                    Thread.sleep(13);
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } catch(StaleElementReferenceException e) { 
            return;
        }  catch(NoSuchElementException e) {
            return;
        }

        throw new RuntimeException("Element did disappear within a reasonable time. ");
    }

	public void waitForAttributeToBe(String attributeName, String expectedValue) {
        long end = System.currentTimeMillis() + 10000;
        String attr;
        while (System.currentTimeMillis() < end) {
            try {
                attr = this.getAttribute(attributeName);
                
                if ( (attr == null && expectedValue == null) || (attr != null && attr.equals(expectedValue))) {
                    return;
                }
            } catch(NoSuchElementException e) {
                // Empty
            }
            try{
            	Thread.sleep(13);
            } catch(Exception e) {
            	throw new RuntimeException(e);
            }
            
        }

        throw new RuntimeException("Element did not become visible within a reasonable time.");
    }

	public void waitForAttributeToChangeFrom(String attributeName, String currentValue) {
        long end = System.currentTimeMillis() + 10000;
        String attr;
        while (System.currentTimeMillis() < end) {
            try{
                attr = this.getAttribute(attributeName);
                if ( (attr == null && currentValue != null) || ( attr != null && !attr.equals(currentValue))) {
                    return;
                }
            } catch(NoSuchElementException e) {
                // Empty
            }
            try{
            	Thread.sleep(13);
            } catch(Exception e) {
            	throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Element attribute did not change within a reasonable time.");
    }
	
	public void waitForTextToChangeFrom(String currentValue) {
        long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            if ( !this.getText().equals(currentValue)) {
                return;
            }
            try{
            	Thread.sleep(13);
            } catch(Exception e) {
            	throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Element attribute did not change within a reasonable time.");
    }
    
    public void waitForTextToChangeTo(String newValue) {
        long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            if ( this.getText().equals(newValue)) {
                return;
            }
            try{
                Thread.sleep(13);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        throw new RuntimeException("Element attribute did not change within a reasonable time.");
    }

}
