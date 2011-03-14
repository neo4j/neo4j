package org.neo4j.server.ext.visualization.gwt.client;

import org.vaadin.gwtgraphics.client.Line;

import com.google.gwt.dom.client.Style;
import com.google.gwt.dom.client.Style.Position;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.user.client.ui.HTML;

/**
 * A directional, labeled edge/line between 2 nodes.
 * 
 * @author Marlon Richert @ Vaadin
 */
class Relationship {

    private static final int ARROWHEAD_LENGTH = 10;
    private static final int ARROWHEAD_WIDTH = ARROWHEAD_LENGTH / 2;
    private Line edge;
    private HTML label;
    private Node from;
    private Node to;
    private Line arrowheadLeft;
    private Line arrowheadRight;

    Relationship(VGraphComponent parent, Node node1, Node node2, String type) {
        this.from = node1;
        this.to = node2;
        addEdge(parent);
        addArrowhead(parent);
        addLabel(parent, type);
    }

    private void addArrowhead(VGraphComponent parent) {
        arrowheadLeft = new Line(0, 0, 0, 0);
        parent.add(arrowheadLeft);
        arrowheadRight = new Line(0, 0, 0, 0);
        parent.add(arrowheadRight);
        updateArrowhead();
    }

    private void addEdge(VGraphComponent parent) {
        edge = new Line((int) Math.round(from.getCenterX()),
                (int) Math.round(from.getCenterY()), (int) Math.round(to
                        .getCenterX()), (int) Math.round(to.getCenterY()));
        parent.add(edge);
    }

    private void addLabel(VGraphComponent parent, String type) {
        label = new HTML("<div style='text-align:center'>" + type + "</div>");
        parent.add(label);

        Style style = label.getElement().getStyle();
        style.setPosition(Position.ABSOLUTE);
        style.setBackgroundColor("white");
        style.setFontSize(10, Unit.PX);
        updateLabel();
    }

    void update() {
        updateEdge();
        updateLabel();
        updateArrowhead();
    }

    private void updateArrowhead() {
        double fromX = from.getCenterX();
        double fromY = from.getCenterY();
        double toX = to.getCenterX();
        double toY = to.getCenterY();
        double originX = getArrowheadOrigin(fromX, toX);
        double originY = getArrowheadOrigin(fromY, toY);
        double angle = Math.atan2(toY - fromY, toX - fromX);

        double leftX = originX
                + rotateX(-ARROWHEAD_LENGTH, -ARROWHEAD_WIDTH, angle);
        double leftY = originY
                + rotateY(-ARROWHEAD_LENGTH, -ARROWHEAD_WIDTH, angle);
        updateLine(arrowheadLeft, originX, originY, leftX, leftY);

        double rightX = originX
                + rotateX(-ARROWHEAD_LENGTH, ARROWHEAD_WIDTH, angle);
        double rightY = originY
                + rotateY(-ARROWHEAD_LENGTH, ARROWHEAD_WIDTH, angle);
        updateLine(arrowheadRight, originX, originY, rightX, rightY);
    }

    private void updateEdge() {
        updateLine(edge, from.getCenterX(), from.getCenterY(), to.getCenterX(),
                to.getCenterY());
    }

    private Style updateLabel() {
        Style style = label.getElement().getStyle();

        double x = getLabelCenter(from.getCenterX(), to.getCenterX())
                - label.getOffsetWidth() / 2;
        style.setLeft(x, Unit.PX);
        double y = getLabelCenter(from.getCenterY(), to.getCenterY())
                - label.getOffsetHeight() / 2;
        style.setTop(y, Unit.PX);
        return style;
    }

    private void updateLine(Line line, double x1, double y1, double x2,
            double y2) {
        updateLine(line, (int) Math.round(x1), (int) Math.round(y1),
                (int) Math.round(x2), (int) Math.round(y2));
    }

    private void updateLine(Line line, int x1, int y1, int x2, int y2) {
        line.setX1(x1);
        line.setY1(y1);
        line.setX2(x2);
        line.setY2(y2);
    }

    private static double getArrowheadOrigin(double nearest, double farthest) {
        return .1 * nearest + .9 * farthest;
    }

    private static double getLabelCenter(double nearest, double farthest) {
        return .33 * nearest + .67 * farthest;
    }

    private static double rotateX(double x, double y, double angle) {
        return x * Math.cos(angle) - y * Math.sin(angle);
    }

    private static double rotateY(double x, double y, double angle) {
        return x * Math.sin(angle) + y * Math.cos(angle);
    }
}
