package org.neo4j.server.ext.visualization.gwt.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vaadin.gwtgraphics.client.DrawingArea;
import org.vaadin.gwtgraphics.client.Line;

import com.google.gwt.dom.client.Style;
import com.google.gwt.dom.client.Style.Position;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Element;
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.UIObject;
import com.google.gwt.user.client.ui.Widget;
import com.vaadin.terminal.gwt.client.ApplicationConnection;
import com.vaadin.terminal.gwt.client.Paintable;
import com.vaadin.terminal.gwt.client.UIDL;

public class VGraphComponent extends Composite implements Paintable,
        ClickHandler {

    /** Set the CSS class name to allow styling. */
    public static final String CLASSNAME = "v-mycomponent";

    private static final int NODE_SIZE = 30;

    /** The client side widget identifier */
    protected String paintableId;

    /** Reference to the server connection object. */
    ApplicationConnection client;

    private Panel root;
    private DrawingArea canvas;
    private Set<Widget> nodes = new HashSet<Widget>();
    private Map<Widget, Set<Line>> linesFrom = new HashMap<Widget, Set<Line>>();
    private Map<Widget, Set<Line>> linesTo = new HashMap<Widget, Set<Line>>();

    /**
     * The constructor should first call super() to initialize the component and
     * then handle any initialization relevant to Vaadin.
     */
    public VGraphComponent() {
        root = new AbsolutePanel();
        initWidget(root);

        canvas = new DrawingArea(400, 400);
        RootPanel.getBodyElement().getStyle().setBackgroundColor("green");
        Style canvasStyle = canvas.getElement().getStyle();
        canvasStyle.setPosition(Position.ABSOLUTE);
        canvasStyle.setBackgroundColor("white");
        root.add(canvas);
        root.getElement().getStyle().setPosition(Position.ABSOLUTE);

        canvas.addClickHandler(this);

        // This method call of the Paintable interface sets the component
        // style name in DOM tree
        setStyleName(CLASSNAME);
    }

    private Widget createRandomNode() {
        return createNode(Math.random() * canvas.getHeight(), Math.random()
                * canvas.getWidth());
    }

    private Widget createNode(double x, double y) {
        HTML node = new HTML();
        Style nodeStyle = node.getElement().getStyle();
        nodeStyle.setPosition(Position.ABSOLUTE);
        nodeStyle.setLeft(x, Unit.PX);
        nodeStyle.setTop(y, Unit.PX);
        nodeStyle.setWidth(NODE_SIZE, Unit.PX);
        nodeStyle.setHeight(NODE_SIZE, Unit.PX);
        nodeStyle.setBackgroundColor("blue");
        root.add(node);
        nodes.add(node);
        new NodeHandler(node, this);
        return node;
    }

    static int getCenterX(UIObject node) {
        return getCenterX(node.getElement());
    }

    static int getCenterY(UIObject node) {
        return getCenterY(node.getElement());
    }

    static int getCenterX(Element element) {
        return element.getOffsetLeft() + element.getOffsetWidth() / 2;
    }

    static int getCenterY(Element element) {
        return element.getOffsetTop() + element.getOffsetHeight() / 2;
    }

    public void onClick(ClickEvent event) {
        System.out.println("Click " + event.getNativeButton());
        if (event.isShiftKeyDown()) {
            createNode(event.getRelativeX(canvas.getElement()),
                    event.getRelativeY(canvas.getElement()));
            event.preventDefault();
        }
    }

    /**
     * Called whenever an update is received from the server
     */
    public void updateFromUIDL(UIDL uidl, ApplicationConnection client) {
        // This call should be made first.
        // It handles sizes, captions, tooltips, etc. automatically.
        if (client.updateComponent(this, uidl, true)) {
            // If client.updateComponent returns true there has been no changes
            // and we
            // do not need to update anything.
            return;
        }

        // Save reference to server connection object to be able to send
        // user interaction later
        this.client = client;

        // Save the client side identifier (paintable id) for the widget
        paintableId = uidl.getId();

        Widget node1 = createRandomNode();
        Widget node2 = createRandomNode();

        createLine(node1, node2);
    }

    private Line createLine(Widget node1, Widget node2) {
        Line line = new Line(getCenterX(node1), getCenterY(node1),
                getCenterX(node2), getCenterY(node2));

        canvas.add(line);
        mapRelationship(linesFrom, node1, line);
        mapRelationship(linesTo, node2, line);

        return line;
    }

    private void mapRelationship(Map<Widget, Set<Line>> map, Widget node,
            Line line) {
        Set<Line> lines = map.get(node);
        if (lines == null) {
            lines = new HashSet<Line>();
            map.put(node, lines);
        }
        lines.add(line);
    }

    void updateLinesFor(Widget node) {
        int centerX = getCenterX(node);
        int centerY = getCenterY(node);
        Set<Line> outgoing = linesFrom.get(node);
        if (outgoing != null) {
            for (Line line : outgoing) {
                line.setX1(centerX);
                line.setY1(centerY);
            }
        }
        Set<Line> incoming = linesTo.get(node);
        if (incoming != null) {
            for (Line line : incoming) {
                line.setX2(centerX);
                line.setY2(centerY);
            }
        }
    }
}
