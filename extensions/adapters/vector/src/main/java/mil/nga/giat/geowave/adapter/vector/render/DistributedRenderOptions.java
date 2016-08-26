package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.Color;
import java.awt.image.IndexColorModel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.List;

import javax.media.jai.Interpolation;
import javax.media.jai.InterpolationNearest;
import javax.xml.transform.TransformerException;

import org.geoserver.wms.DefaultWebMapService;
import org.geoserver.wms.GetMapRequest;
import org.geoserver.wms.WMS;
import org.geoserver.wms.WMSMapContent;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.Layer;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.SLDParser;
import org.geotools.styling.SLDTransformer;
import org.geotools.styling.Style;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.Persistable;

public class DistributedRenderOptions implements
		Persistable
{
	private static final Logger LOGGER = LoggerFactory.getLogger(
			DistributedRenderOptions.class);
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
	private int maxErrors;
	private boolean kmlPlacemark;
	private int maxFilters;
	private boolean optimizeLineWidth;
	private ReferencedEnvelope envelope;
	private boolean renderScaleMethodAccurate;
	private int wmsIterpolationOrdinal;
	private List<Integer> interpolationOrdinals;

	private Style style;

	protected DistributedRenderOptions() {}

	public DistributedRenderOptions(
			final WMS wms,
			final WMSMapContent mapContent,
			final Layer layer ) {
		optimizeLineWidth = DefaultWebMapService.isLineWidthOptimizationEnabled();
		maxFilters = DefaultWebMapService.getMaxFilterRules();

		transparent = mapContent.isTransparent();
		buffer = mapContent.getBuffer();
		angle = mapContent.getAngle();
		mapWidth = mapContent.getMapWidth();
		mapHeight = mapContent.getMapHeight();
		bgColor = mapContent.getBgColor();
		palette = mapContent.getPalette();
		renderScaleMethodAccurate = StreamingRenderer.SCALE_ACCURATE.equals(
				mapContent.getRendererScaleMethod());
		wmsIterpolationOrdinal = wms.getInterpolation().ordinal();
		maxErrors = wms.getMaxRenderingErrors();
		style = layer.getStyle();

		final GetMapRequest request = mapContent.getRequest();
		final Object timeoutOption = request.getFormatOptions().get(
				"timeout");
		int localMaxRenderTime = 0;
		if (timeoutOption != null) {
			try {
				// local render time is in millis, while WMS max render time is
				// in seconds
				localMaxRenderTime = Integer.parseInt(
						timeoutOption.toString()) / 1000;
			}
			catch (final NumberFormatException e) {
				LOGGER.warn(
						"Could not parse format_option \"timeout\": " + timeoutOption,
						e);
			}
		}
		maxRenderTime = getMaxRenderTime(
				localMaxRenderTime,
				wms);
		isMetatile = request.isTiled() && (request.getTilesOrigin() != null);
		final Object antialiasObj = request.getFormatOptions().get(
				"antialias");
		if (antialiasObj != null) {
			antialias = antialiasObj.toString();
		}

		if (request.getFormatOptions().get(
				"kmplacemark") != null) {
			kmlPlacemark = ((Boolean) request.getFormatOptions().get(
					"kmplacemark")).booleanValue();
		}
		// turn on advanced projection handling
		advancedProjectionHandlingEnabled = wms.isAdvancedProjectionHandlingEnabled();
		final Object advancedProjectionObj = request.getFormatOptions().get(
				WMS.ADVANCED_PROJECTION_KEY);
		if ((advancedProjectionObj != null) && "false".equalsIgnoreCase(
				advancedProjectionObj.toString())) {
			advancedProjectionHandlingEnabled = false;
			continuousMapWrapping = false;
		}
		final Object mapWrappingObj = request.getFormatOptions().get(
				WMS.ADVANCED_PROJECTION_KEY);
		if ((mapWrappingObj != null) && "false".equalsIgnoreCase(
				mapWrappingObj.toString())) {
			continuousMapWrapping = false;
		}
		final List<Interpolation> interpolations = request.getInterpolations();
		if ((interpolations == null) || interpolations.isEmpty()) {
			interpolationOrdinals = Collections.emptyList();
		}
		else {
			interpolationOrdinals = Lists.transform(
					interpolations,
					new Function<Interpolation, Integer>() {

						@Override
						public Integer apply(
								final Interpolation input ) {
							if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							else if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							else if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							else if (input instanceof InterpolationNearest) {
								return Interpolation.INTERP_NEAREST;
							}
							return Interpolation.INTERP_NEAREST;
						}
					});
		}
	}

	public int getMaxRenderTime(
			final int localMaxRenderTime,
			final WMS wms ) {
		int wmsMaxRenderTime = wms.getMaxRenderingTime();

		if (wmsMaxRenderTime == 0) {
			wmsMaxRenderTime = localMaxRenderTime;
		}
		else if (localMaxRenderTime != 0) {
			maxRenderTime = Math.min(
					maxRenderTime,
					localMaxRenderTime);
		}
		return maxRenderTime;
	}

	public boolean isOptimizeLineWidth() {
		return optimizeLineWidth;
	}

	public int getMaxErrors() {
		return maxErrors;
	}

	public void setMaxErrors(
			final int maxErrors ) {
		this.maxErrors = maxErrors;
	}

	public void setOptimizeLineWidth(
			final boolean optimizeLineWidth ) {
		this.optimizeLineWidth = optimizeLineWidth;
	}

	public List<Integer> getInterpolationOrdinals() {
		return interpolationOrdinals;
	}

	public List<Interpolation> getInterpolations() {
		if ((interpolationOrdinals != null) && !interpolationOrdinals.isEmpty()) {
			return Lists.transform(
					interpolationOrdinals,
					new Function<Integer, Interpolation>() {

						@Override
						public Interpolation apply(
								final Integer input ) {
							return Interpolation.getInstance(
									input);
						}
					});
		}
		return Collections.emptyList();
	}

	public void setInterpolationOrdinals(
			final List<Integer> interpolationOrdinals ) {
		this.interpolationOrdinals = interpolationOrdinals;
	}

	public static boolean isUseGlobalRenderPool() {
		return USE_GLOBAL_RENDER_POOL;
	}

	public Style getStyle() {
		return style;
	}

	public void setStyle(
			final Style style ) {
		this.style = style;
	}

	public int getWmsInterpolationOrdinal() {
		return wmsIterpolationOrdinal;
	}

	public void setWmsInterpolationOrdinal(
			final int wmsIterpolationOrdinal ) {
		this.wmsIterpolationOrdinal = wmsIterpolationOrdinal;
	}

	public int getMaxRenderTime() {
		return maxRenderTime;
	}

	public void setMaxRenderTime(
			final int maxRenderTime ) {
		this.maxRenderTime = maxRenderTime;
	}

	public boolean isRenderScaleMethodAccurate() {
		return renderScaleMethodAccurate;
	}

	public void setRenderScaleMethodAccurate(
			final boolean renderScaleMethodAccurate ) {
		this.renderScaleMethodAccurate = renderScaleMethodAccurate;
	}

	public int getBuffer() {
		return buffer;
	}

	public void setBuffer(
			final int buffer ) {
		this.buffer = buffer;
	}

	public void setPalette(
			final IndexColorModel palette ) {
		this.palette = palette;
	}

	public String getAntialias() {
		return antialias;
	}

	public void setAntialias(
			final String antialias ) {
		this.antialias = antialias;
	}

	public boolean isContinuousMapWrapping() {
		return continuousMapWrapping;
	}

	public void setContinuousMapWrapping(
			final boolean continuousMapWrapping ) {
		this.continuousMapWrapping = continuousMapWrapping;
	}

	public boolean isAdvancedProjectionHandlingEnabled() {
		return advancedProjectionHandlingEnabled;
	}

	public void setAdvancedProjectionHandlingEnabled(
			final boolean advancedProjectionHandlingEnabled ) {
		this.advancedProjectionHandlingEnabled = advancedProjectionHandlingEnabled;
	}

	public boolean isKmlPlacemark() {
		return kmlPlacemark;
	}

	public void setKmlPlacemark(
			final boolean kmlPlacemark ) {
		this.kmlPlacemark = kmlPlacemark;
	}

	public boolean isTransparent() {
		return transparent;
	}

	public void setTransparent(
			final boolean transparent ) {
		this.transparent = transparent;
	}

	public boolean isMetatile() {
		return isMetatile;
	}

	public void setMetatile(
			final boolean isMetatile ) {
		this.isMetatile = isMetatile;
	}

	public Color getBgColor() {
		return bgColor;
	}

	public void setBgColor(
			final Color bgColor ) {
		this.bgColor = bgColor;
	}

	public int getMapWidth() {
		return mapWidth;
	}

	public void setMapWidth(
			final int mapWidth ) {
		this.mapWidth = mapWidth;
	}

	public int getMapHeight() {
		return mapHeight;
	}

	public void setMapHeight(
			final int mapHeight ) {
		this.mapHeight = mapHeight;
	}

	public double getAngle() {
		return angle;
	}

	public void setAngle(
			final double angle ) {
		this.angle = angle;
	}

	public int getMaxFilters() {
		return maxFilters;
	}

	public void setMaxFilters(
			final int maxFilters ) {
		this.maxFilters = maxFilters;
	}

	public ReferencedEnvelope getEnvelope() {
		return envelope;
	}

	public void setEnvelope(
			final ReferencedEnvelope envelope ) {
		this.envelope = envelope;
	}

	public IndexColorModel getPalette() {
		return palette;
	}

	@Override
	public byte[] toBinary() {
		final SLDTransformer transformer = new SLDTransformer();

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
		final byte[] styleBinary = baos.toByteArray();
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final SLDParser parser = new SLDParser(
				CommonFactoryFinder.getStyleFactory(
						null),
				new ByteArrayInputStream(
						rulesBinary));
		final Style[] styles = parser.readDOM();
	}

}
