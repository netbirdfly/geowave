package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.Color;
import java.awt.image.IndexColorModel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import javax.xml.transform.TransformerException;

import org.geoserver.wms.WMS;
import org.geoserver.wms.map.RenderedImageMapOutputFormat;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.styling.NamedLayer;
import org.geotools.styling.Rule;
import org.geotools.styling.SLDParser;
import org.geotools.styling.SLDTransformer;
import org.geotools.styling.Style;
import org.geotools.styling.StyledLayer;
import org.geotools.styling.StyledLayerDescriptor;

import mil.nga.giat.geowave.adapter.vector.render.param.ServerFeatureStyle;
import mil.nga.giat.geowave.core.index.Persistable;

public class DistributedRenderOptions implements
		Persistable
{
	// it doesn't make sense to grab this from the context of the geoserver
	// settings, although it is unclear whether in distributed rendering this
	// should be enabled or disabled by default
	private final static boolean USE_GLOBAL_RENDER_POOL = true;
	
	private String antialias;
	private boolean continuousMapWrapping;
	private boolean advancedProjectionHandlingEnabled;
	private int mapWidth;
	private int mapHeight;
	private int buffer;
	private double angle;
	private IndexColorModel palette;
	private boolean transparent;
	private boolean isMetatile;
	private Color bgColor;
	private int maxRenderTime;
	private boolean kmlPlacemark;
	private int maxFilters;
	private boolean optimizeLineWidth;
	private ReferencedEnvelope envelope;
	private boolean renderScaleMethodAccurate;
	private int interpolationOrdinal;

	private Style style;

	public boolean isOptimizeLineWidth() {
		return optimizeLineWidth;
	}

	public void setOptimizeLineWidth(
			boolean optimizeLineWidth ) {
		this.optimizeLineWidth = optimizeLineWidth;
	}

	public static boolean isUseGlobalRenderPool() {
		return USE_GLOBAL_RENDER_POOL;
	}

	public void setMapHeight(
			int mapHeight ) {
		this.mapHeight = mapHeight;
	}

	public Style getStyle() {
		return style;
	}

	public void setStyle(
			Style style ) {
		this.style = style;
	}

	public int getInterpolationOrdinal() {
		return interpolationOrdinal;
	}

	public void setInterpolationOrdinal(
			int interpolationOrdinal ) {
		this.interpolationOrdinal = interpolationOrdinal;
	}

	public int getMaxRenderTime() {
		return maxRenderTime;
	}

	public void setMaxRenderTime(
			int maxRenderTime ) {
		this.maxRenderTime = maxRenderTime;
	}


	public boolean isRenderScaleMethodAccurate() {
		return renderScaleMethodAccurate;
	}

	public void setRenderScaleMethodAccurate(
			boolean renderScaleMethodAccurate ) {
		this.renderScaleMethodAccurate = renderScaleMethodAccurate;
	}

	public int getBuffer() {
		return buffer;
	}

	public void setBuffer(
			int buffer ) {
		this.buffer = buffer;
	}

	public void setPalette(
			IndexColorModel palette ) {
		this.palette = palette;
	}

	public String getAntialias() {
		return antialias;
	}

	public void setAntialias(
			String antialias ) {
		this.antialias = antialias;
	}

	public boolean isContinuousMapWrapping() {
		return continuousMapWrapping;
	}

	public void setContinuousMapWrapping(
			boolean continuousMapWrapping ) {
		this.continuousMapWrapping = continuousMapWrapping;
	}

	public boolean isAdvancedProjectionHandlingEnabled() {
		return advancedProjectionHandlingEnabled;
	}

	public void setAdvancedProjectionHandlingEnabled(
			boolean advancedProjectionHandlingEnabled ) {
		this.advancedProjectionHandlingEnabled = advancedProjectionHandlingEnabled;
	}

	public boolean isKmlPlacemark() {
		return kmlPlacemark;
	}

	public void setKmlPlacemark(
			boolean kmlPlacemark ) {
		this.kmlPlacemark = kmlPlacemark;
	}

	public boolean isTransparent() {
		return transparent;
	}

	public void setTransparent(
			boolean transparent ) {
		this.transparent = transparent;
	}

	public boolean isMetatile() {
		return isMetatile;
	}

	public void setMetatile(
			boolean isMetatile ) {
		this.isMetatile = isMetatile;
	}

	public Color getBgColor() {
		return bgColor;
	}

	public void setBgColor(
			Color bgColor ) {
		this.bgColor = bgColor;
	}

	public int getMapWidth() {
		return mapWidth;
	}

	public void setMapWidth(
			int mapWidth ) {
		this.mapWidth = mapWidth;
	}

	public int getMapHeight() {
		return mapHeight;
	}

	public void setPaintHeight(
			int mapHeight ) {
		this.mapHeight = mapHeight;
	}

	public double getAngle() {
		return angle;
	}

	public void setAngle(
			double angle ) {
		this.angle = angle;
	}

	public int getMaxFilters() {
		return maxFilters;
	}

	public void setMaxFilters(
			int maxFilters ) {
		this.maxFilters = maxFilters;
	}

	public ReferencedEnvelope getEnvelope() {
		return envelope;
	}

	public void setEnvelope(
			ReferencedEnvelope envelope ) {
		this.envelope = envelope;
	}
	
	public IndexColorModel getPalette() {
		return palette;
	}

	@Override
	public byte[] toBinary() {
		SLDTransformer transformer = new SLDTransformer();

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try {
			transformer.transform(
					new Style[] {
						style
					},
					baos);
		}
		catch (final TransformerException e) {
			LOGGER.warn(
					"Unable to create SLD from style",
					e);
		}
		byte[] styleBinary = baos.toByteArray();
		return null;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		final SLDParser parser = new SLDParser(
				CommonFactoryFinder.getStyleFactory(
						null),
				new ByteArrayInputStream(
						rulesBinary));
		Style[] styles = parser.readDOM();
	}

}
