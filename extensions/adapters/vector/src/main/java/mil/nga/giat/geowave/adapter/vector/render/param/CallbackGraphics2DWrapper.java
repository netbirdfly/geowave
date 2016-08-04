package mil.nga.giat.geowave.adapter.vector.render.param;

import java.awt.Color;
import java.awt.Composite;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.GraphicsConfiguration;
import java.awt.Image;
import java.awt.Paint;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.RenderingHints.Key;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.Transparency;
import java.awt.font.FontRenderContext;
import java.awt.font.GlyphVector;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.awt.image.BufferedImageOp;
import java.awt.image.ColorModel;
import java.awt.image.ImageObserver;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.awt.image.renderable.RenderableImage;
import java.text.AttributedCharacterIterator;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.render.EventingWritableRaster;
import mil.nga.giat.geowave.adapter.vector.render.WritableRasterCallback;

public class CallbackGraphics2DWrapper extends
		Graphics2D
{
	private final Graphics2D parent;
	private final Rectangle screenSize;
	private BufferedImage image;

	private Graphics2D delayedBackBufferDelegate;
	private WritableRasterCallback rasterCallback;

	public CallbackGraphics2DWrapper(
			final Graphics2D master,
			final Rectangle screenSize ) {
		this(
				master,
				screenSize,
				null);
	}

	public CallbackGraphics2DWrapper(
			final Graphics2D master,
			final Rectangle screenSize,
			final WritableRasterCallback rasterCallback ) {
		parent = master;
		this.screenSize = screenSize;
		this.rasterCallback = rasterCallback;
	}

	public synchronized void setCallback(
			final WritableRasterCallback rasterCallback ) {
		this.rasterCallback = rasterCallback;

		if (delayedBackBufferDelegate != null) {
			final ColorModel cm = image.getColorModel();
			final boolean isAlphaPremultiplied = cm.isAlphaPremultiplied();
			final WritableRaster raster = new EventingWritableRaster(
					image.getRaster(),
					this.rasterCallback);
			image = new BufferedImage(
					cm,
					raster,
					isAlphaPremultiplied,
					null);
			this.rasterCallback.setImage(
					image);
			delayedBackBufferDelegate = image.createGraphics();
			delayedBackBufferDelegate.setRenderingHints(
					parent.getRenderingHints());
		}
	}

	/**
	 * In practice this method must be called before use
	 */
	public synchronized void init() {
		if (delayedBackBufferDelegate == null) {
			image = parent.getDeviceConfiguration().createCompatibleImage(
					screenSize.width,
					screenSize.height,
					Transparency.TRANSLUCENT);
			if (rasterCallback != null) {
				final ColorModel cm = image.getColorModel();
				final boolean isAlphaPremultiplied = cm.isAlphaPremultiplied();
				final WritableRaster raster = new EventingWritableRaster(
						image.getRaster(),
						rasterCallback);
				image = new BufferedImage(
						cm,
						raster,
						isAlphaPremultiplied,
						null);

				rasterCallback.setImage(
						image);
			}
			delayedBackBufferDelegate = image.createGraphics();
			delayedBackBufferDelegate.setRenderingHints(
					parent.getRenderingHints());
		}
	}

	public synchronized BufferedImage getImage() {
		return image;
	}

	@Override
	public int hashCode() {
		if (delayedBackBufferDelegate != null) {
			return delayedBackBufferDelegate.hashCode();
		}
		return parent.hashCode();
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (delayedBackBufferDelegate != null) {
			return delayedBackBufferDelegate.equals(
					obj);
		}
		return parent.equals(
				obj);
	}

	@Override
	public Graphics create() {
		return delayedBackBufferDelegate.create();
	}

	@Override
	public Graphics create(
			final int x,
			final int y,
			final int width,
			final int height ) {
		return delayedBackBufferDelegate.create(
				x,
				y,
				width,
				height);
	}

	@Override
	public Color getColor() {
		return delayedBackBufferDelegate.getColor();
	}

	@Override
	public void setColor(
			final Color c ) {
		delayedBackBufferDelegate.setColor(
				c);
	}

	@Override
	public void setPaintMode() {
		delayedBackBufferDelegate.setPaintMode();
	}

	@Override
	public void setXORMode(
			final Color c1 ) {
		delayedBackBufferDelegate.setXORMode(
				c1);
	}

	@Override
	public Font getFont() {
		return delayedBackBufferDelegate.getFont();
	}

	@Override
	public void setFont(
			final Font font ) {
		delayedBackBufferDelegate.setFont(
				font);
	}

	@Override
	public FontMetrics getFontMetrics() {
		return delayedBackBufferDelegate.getFontMetrics();
	}

	@Override
	public FontMetrics getFontMetrics(
			final Font f ) {
		return delayedBackBufferDelegate.getFontMetrics(
				f);
	}

	@Override
	public Rectangle getClipBounds() {
		return delayedBackBufferDelegate.getClipBounds();
	}

	@Override
	public void clipRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delayedBackBufferDelegate.clipRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void setClip(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delayedBackBufferDelegate.setClip(
				x,
				y,
				width,
				height);
	}

	@Override
	public Shape getClip() {
		return delayedBackBufferDelegate.getClip();
	}

	@Override
	public void setClip(
			final Shape clip ) {
		delayedBackBufferDelegate.setClip(
				clip);
	}

	@Override
	public void copyArea(
			final int x,
			final int y,
			final int width,
			final int height,
			final int dx,
			final int dy ) {
		delayedBackBufferDelegate.copyArea(
				x,
				y,
				width,
				height,
				dx,
				dy);
	}

	@Override
	public void drawLine(
			final int x1,
			final int y1,
			final int x2,
			final int y2 ) {
		delayedBackBufferDelegate.drawLine(
				x1,
				y1,
				x2,
				y2);
	}

	@Override
	public void fillRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delayedBackBufferDelegate.fillRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void drawRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delayedBackBufferDelegate.drawRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void draw3DRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final boolean raised ) {
		delayedBackBufferDelegate.draw3DRect(
				x,
				y,
				width,
				height,
				raised);
	}

	@Override
	public void clearRect(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delayedBackBufferDelegate.clearRect(
				x,
				y,
				width,
				height);
	}

	@Override
	public void drawRoundRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final int arcWidth,
			final int arcHeight ) {
		delayedBackBufferDelegate.drawRoundRect(
				x,
				y,
				width,
				height,
				arcWidth,
				arcHeight);
	}

	@Override
	public void fill3DRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final boolean raised ) {
		delayedBackBufferDelegate.fill3DRect(
				x,
				y,
				width,
				height,
				raised);
	}

	@Override
	public void fillRoundRect(
			final int x,
			final int y,
			final int width,
			final int height,
			final int arcWidth,
			final int arcHeight ) {
		delayedBackBufferDelegate.fillRoundRect(
				x,
				y,
				width,
				height,
				arcWidth,
				arcHeight);
	}

	@Override
	public void draw(
			final Shape s ) {
		delayedBackBufferDelegate.draw(
				s);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final AffineTransform xform,
			final ImageObserver obs ) {
		return delayedBackBufferDelegate.drawImage(
				img,
				xform,
				obs);
	}

	@Override
	public void drawImage(
			final BufferedImage img,
			final BufferedImageOp op,
			final int x,
			final int y ) {
		delayedBackBufferDelegate.drawImage(
				img,
				op,
				x,
				y);
	}

	@Override
	public void drawOval(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delayedBackBufferDelegate.drawOval(
				x,
				y,
				width,
				height);
	}

	@Override
	public void drawRenderedImage(
			final RenderedImage img,
			final AffineTransform xform ) {
		delayedBackBufferDelegate.drawRenderedImage(
				img,
				xform);
	}

	@Override
	public void fillOval(
			final int x,
			final int y,
			final int width,
			final int height ) {
		delayedBackBufferDelegate.fillOval(
				x,
				y,
				width,
				height);
	}

	@Override
	public void drawArc(
			final int x,
			final int y,
			final int width,
			final int height,
			final int startAngle,
			final int arcAngle ) {
		delayedBackBufferDelegate.drawArc(
				x,
				y,
				width,
				height,
				startAngle,
				arcAngle);
	}

	@Override
	public void drawRenderableImage(
			final RenderableImage img,
			final AffineTransform xform ) {
		delayedBackBufferDelegate.drawRenderableImage(
				img,
				xform);
	}

	@Override
	public void drawString(
			final String str,
			final int x,
			final int y ) {
		delayedBackBufferDelegate.drawString(
				str,
				x,
				y);
	}

	@Override
	public void fillArc(
			final int x,
			final int y,
			final int width,
			final int height,
			final int startAngle,
			final int arcAngle ) {
		delayedBackBufferDelegate.fillArc(
				x,
				y,
				width,
				height,
				startAngle,
				arcAngle);
	}

	@Override
	public void drawString(
			final String str,
			final float x,
			final float y ) {
		delayedBackBufferDelegate.drawString(
				str,
				x,
				y);
	}

	@Override
	public void drawPolyline(
			final int[] xPoints,
			final int[] yPoints,
			final int nPoints ) {
		delayedBackBufferDelegate.drawPolyline(
				xPoints,
				yPoints,
				nPoints);
	}

	@Override
	public void drawString(
			final AttributedCharacterIterator iterator,
			final int x,
			final int y ) {
		delayedBackBufferDelegate.drawString(
				iterator,
				x,
				y);
	}

	@Override
	public void drawPolygon(
			final int[] xPoints,
			final int[] yPoints,
			final int nPoints ) {
		delayedBackBufferDelegate.drawPolygon(
				xPoints,
				yPoints,
				nPoints);
	}

	@Override
	public void drawString(
			final AttributedCharacterIterator iterator,
			final float x,
			final float y ) {
		delayedBackBufferDelegate.drawString(
				iterator,
				x,
				y);
	}

	@Override
	public void drawPolygon(
			final Polygon p ) {
		delayedBackBufferDelegate.drawPolygon(
				p);
	}

	@Override
	public void fillPolygon(
			final int[] xPoints,
			final int[] yPoints,
			final int nPoints ) {
		delayedBackBufferDelegate.fillPolygon(
				xPoints,
				yPoints,
				nPoints);
	}

	@Override
	public void drawGlyphVector(
			final GlyphVector g,
			final float x,
			final float y ) {
		delayedBackBufferDelegate.drawGlyphVector(
				g,
				x,
				y);
	}

	@Override
	public void fillPolygon(
			final Polygon p ) {
		delayedBackBufferDelegate.fillPolygon(
				p);
	}

	@Override
	public void fill(
			final Shape s ) {
		delayedBackBufferDelegate.fill(
				s);
	}

	@Override
	public boolean hit(
			final Rectangle rect,
			final Shape s,
			final boolean onStroke ) {
		return delayedBackBufferDelegate.hit(
				rect,
				s,
				onStroke);
	}

	@Override
	public void drawChars(
			final char[] data,
			final int offset,
			final int length,
			final int x,
			final int y ) {
		delayedBackBufferDelegate.drawChars(
				data,
				offset,
				length,
				x,
				y);
	}

	@Override
	public GraphicsConfiguration getDeviceConfiguration() {
		return delayedBackBufferDelegate.getDeviceConfiguration();
	}

	@Override
	public void setComposite(
			final Composite comp ) {
		delayedBackBufferDelegate.setComposite(
				comp);
	}

	@Override
	public void drawBytes(
			final byte[] data,
			final int offset,
			final int length,
			final int x,
			final int y ) {
		delayedBackBufferDelegate.drawBytes(
				data,
				offset,
				length,
				x,
				y);
	}

	@Override
	public void setPaint(
			final Paint paint ) {
		delayedBackBufferDelegate.setPaint(
				paint);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final ImageObserver observer ) {
		return delayedBackBufferDelegate.drawImage(
				img,
				x,
				y,
				observer);
	}

	@Override
	public void setStroke(
			final Stroke s ) {
		delayedBackBufferDelegate.setStroke(
				s);
	}

	@Override
	public void setRenderingHint(
			final Key hintKey,
			final Object hintValue ) {
		delayedBackBufferDelegate.setRenderingHint(
				hintKey,
				hintValue);
	}

	@Override
	public Object getRenderingHint(
			final Key hintKey ) {
		return delayedBackBufferDelegate.getRenderingHint(
				hintKey);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final int width,
			final int height,
			final ImageObserver observer ) {
		return delayedBackBufferDelegate.drawImage(
				img,
				x,
				y,
				width,
				height,
				observer);
	}

	@Override
	public void setRenderingHints(
			final Map<?, ?> hints ) {
		delayedBackBufferDelegate.setRenderingHints(
				hints);
	}

	@Override
	public void addRenderingHints(
			final Map<?, ?> hints ) {
		delayedBackBufferDelegate.addRenderingHints(
				hints);
	}

	@Override
	public RenderingHints getRenderingHints() {
		return delayedBackBufferDelegate.getRenderingHints();
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final Color bgcolor,
			final ImageObserver observer ) {
		return delayedBackBufferDelegate.drawImage(
				img,
				x,
				y,
				bgcolor,
				observer);
	}

	@Override
	public void translate(
			final int x,
			final int y ) {
		delayedBackBufferDelegate.translate(
				x,
				y);
	}

	@Override
	public void translate(
			final double tx,
			final double ty ) {
		delayedBackBufferDelegate.translate(
				tx,
				ty);
	}

	@Override
	public void rotate(
			final double theta ) {
		delayedBackBufferDelegate.rotate(
				theta);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int x,
			final int y,
			final int width,
			final int height,
			final Color bgcolor,
			final ImageObserver observer ) {
		return delayedBackBufferDelegate.drawImage(
				img,
				x,
				y,
				width,
				height,
				bgcolor,
				observer);
	}

	@Override
	public void rotate(
			final double theta,
			final double x,
			final double y ) {
		delayedBackBufferDelegate.rotate(
				theta,
				x,
				y);
	}

	@Override
	public void scale(
			final double sx,
			final double sy ) {
		delayedBackBufferDelegate.scale(
				sx,
				sy);
	}

	@Override
	public void shear(
			final double shx,
			final double shy ) {
		delayedBackBufferDelegate.shear(
				shx,
				shy);
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int dx1,
			final int dy1,
			final int dx2,
			final int dy2,
			final int sx1,
			final int sy1,
			final int sx2,
			final int sy2,
			final ImageObserver observer ) {
		return delayedBackBufferDelegate.drawImage(
				img,
				dx1,
				dy1,
				dx2,
				dy2,
				sx1,
				sy1,
				sx2,
				sy2,
				observer);
	}

	@Override
	public void transform(
			final AffineTransform Tx ) {
		delayedBackBufferDelegate.transform(
				Tx);
	}

	@Override
	public void setTransform(
			final AffineTransform Tx ) {
		delayedBackBufferDelegate.setTransform(
				Tx);
	}

	@Override
	public AffineTransform getTransform() {
		return delayedBackBufferDelegate.getTransform();
	}

	@Override
	public boolean drawImage(
			final Image img,
			final int dx1,
			final int dy1,
			final int dx2,
			final int dy2,
			final int sx1,
			final int sy1,
			final int sx2,
			final int sy2,
			final Color bgcolor,
			final ImageObserver observer ) {
		return delayedBackBufferDelegate.drawImage(
				img,
				dx1,
				dy1,
				dx2,
				dy2,
				sx1,
				sy1,
				sx2,
				sy2,
				bgcolor,
				observer);
	}

	@Override
	public Paint getPaint() {
		return delayedBackBufferDelegate.getPaint();
	}

	@Override
	public Composite getComposite() {
		return delayedBackBufferDelegate.getComposite();
	}

	@Override
	public void setBackground(
			final Color color ) {
		delayedBackBufferDelegate.setBackground(
				color);
	}

	@Override
	public Color getBackground() {
		return delayedBackBufferDelegate.getBackground();
	}

	@Override
	public Stroke getStroke() {
		return delayedBackBufferDelegate.getStroke();
	}

	@Override
	public void clip(
			final Shape s ) {
		delayedBackBufferDelegate.clip(
				s);
	}

	@Override
	public FontRenderContext getFontRenderContext() {
		return delayedBackBufferDelegate.getFontRenderContext();
	}

	@Override
	public void dispose() {
		if (delayedBackBufferDelegate != null) {
			delayedBackBufferDelegate.dispose();
		}
	}

	@Override
	public void finalize() {
		delayedBackBufferDelegate.finalize();
	}

	@Override
	public String toString() {
		return delayedBackBufferDelegate.toString();
	}

	@Override
	public Rectangle getClipRect() {
		return delayedBackBufferDelegate.getClipRect();
	}

	@Override
	public boolean hitClip(
			final int x,
			final int y,
			final int width,
			final int height ) {
		return delayedBackBufferDelegate.hitClip(
				x,
				y,
				width,
				height);
	}

	@Override
	public Rectangle getClipBounds(
			final Rectangle r ) {
		return delayedBackBufferDelegate.getClipBounds(
				r);
	}
}
