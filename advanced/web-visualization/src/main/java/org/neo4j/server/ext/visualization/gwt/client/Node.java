package org.neo4j.server.ext.visualization.gwt.client;

import java.util.HashSet;
import java.util.Set;

import com.google.gwt.dom.client.Style;
import com.google.gwt.dom.client.Style.Position;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.event.dom.client.MouseDownEvent;
import com.google.gwt.event.dom.client.MouseDownHandler;
import com.google.gwt.event.dom.client.MouseMoveEvent;
import com.google.gwt.event.dom.client.MouseMoveHandler;
import com.google.gwt.event.dom.client.MouseUpEvent;
import com.google.gwt.event.dom.client.MouseUpHandler;
import com.google.gwt.user.client.DOM;
import com.google.gwt.user.client.Element;
import com.google.gwt.user.client.Random;
import com.google.gwt.user.client.Window;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.UIObject;

class Node {
    private static final int FONT_SIZE = 16;
    private static final int NODE_SIZE = FONT_SIZE * 3;

    final VGraphComponent parent;
    final HTML widget;
    boolean dragging;
    boolean explored;
    int dragStartX;
    int dragStartY;
    private final Set<Relationship> outgoing = new HashSet<Relationship>();
    private final Set<Relationship> incoming = new HashSet<Relationship>();
    protected boolean mouseDown;

    Node(VGraphComponent parent, String label, double x, double y) {
        this.parent = parent;
        widget = new HTML("<div style='text-align:center'>" + label + "</div>");
        parent.add(widget);

        Style style = widget.getElement().getStyle();
        style.setPosition(Position.ABSOLUTE);
        style.setLeft(x, Unit.PX);
        style.setTop(y, Unit.PX);
        style.setWidth(NODE_SIZE, Unit.PX);
        style.setHeight(NODE_SIZE, Unit.PX);
        style.setBackgroundColor("lightblue");
        style.setFontSize(FONT_SIZE, Unit.PX);

        addHandlers();
        update();
    }

    private void addHandlers() {
        widget.addDomHandler(new MouseDownHandler() {

            public void onMouseDown(MouseDownEvent event) {
                mouseDown = true;

                DOM.setCapture(widget.getElement());
                dragStartX = event.getX();
                dragStartY = event.getY();
                event.preventDefault();
            }

        }, MouseDownEvent.getType());

        widget.addDomHandler(new MouseMoveHandler() {

            public void onMouseMove(MouseMoveEvent event) {
                if (mouseDown) {
                    dragging = true;
                    Element element = widget.getElement();
                    int newX = event.getX() + element.getOffsetLeft()
                            - dragStartX;
                    int newY = event.getY() + element.getOffsetTop()
                            - dragStartY;
                    move(newX, newY);
                    int clientX = event.getClientX();
                    int clientY = event.getClientY();
                    if (clientX < 0 || clientY < 0
                            || clientX > Window.getClientWidth()
                            || clientY > Window.getClientHeight()) {
                        dragging = false;
                    }
                }
                event.preventDefault();
            }
        }, MouseMoveEvent.getType());

        widget.addDomHandler(new MouseUpHandler() {

            public void onMouseUp(MouseUpEvent event) {
                if (!dragging && !explored) {
                    explored = true;
                    Style style = widget.getElement().getStyle();
                    style.setBorderColor("black");
                    style.setBorderStyle(Style.BorderStyle.SOLID);
                    style.setBorderWidth(2, Unit.PX);
                    do {
                        parent.addNeighborTo(Node.this);
                    } while (Random.nextBoolean());
                }
                dragging = false;
                mouseDown = false;
                DOM.releaseCapture(widget.getElement());
                event.preventDefault();
            }
        }, MouseUpEvent.getType());
    }

    void addIncoming(Relationship edge) {
        incoming.add(edge);
    }

    void addOutgoing(Relationship edge) {
        outgoing.add(edge);
    }

    double getCenterX() {
        return getCenterX(widget);
    }

    double getCenterY() {
        return getCenterY(widget);
    }

    void move(int newX, int newY) {
        Style style = widget.getElement().getStyle();
        style.setLeft(
                limit(0, newX,
                        parent.getOffsetWidth() - widget.getOffsetWidth()),
                Unit.PX);
        style.setTop(
                limit(0, newY,
                        parent.getOffsetHeight() - widget.getOffsetHeight()),
                Unit.PX);
        updateRelationships();
    }

    void update() {
        Element element = widget.getElement();
        move(element.getOffsetLeft(), element.getOffsetTop());
    }

    void updateRelationships() {
        update(outgoing);
        update(incoming);
    }

    private static double getCenterX(Element element) {
        return element.getOffsetLeft() + element.getOffsetWidth() / 2.0;
    }

    private static double getCenterX(UIObject widget) {
        return getCenterX(widget.getElement());
    }

    private static double getCenterY(Element element) {
        return element.getOffsetTop() + element.getOffsetHeight() / 2.0;
    }

    private static double getCenterY(UIObject widget) {
        return getCenterY(widget.getElement());
    }

    private static int limit(int min, int value, int max) {
        return Math.min(Math.max(min, value), max);
    }

    private static void update(Set<Relationship> edges) {
        if (edges != null) {
            for (Relationship edge : edges) {
                edge.update();
            }
        }
    }
}
