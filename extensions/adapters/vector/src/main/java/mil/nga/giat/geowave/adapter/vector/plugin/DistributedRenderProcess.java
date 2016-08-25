package mil.nga.giat.geowave.adapter.vector.plugin;

import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.IndexColorModel;
import java.io.ByteArrayOutputStream;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.factory.Hints;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geotools.resources.coverage.FeatureUtilities;
import org.geotools.styling.SLDParser;
import org.geotools.styling.SLDTransformer;
import org.geotools.styling.Style;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.coverage.grid.GridGeometry;

import mil.nga.giat.geowave.adapter.vector.render.DistributedRenderOptions;

/**
 * This class can be used as a GeoTools Render Transform ('nga:Decimation')
 * within an SLD on any layer that uses the GeoWave Data Store. An example SLD
 * is provided (example-slds/DecimatePoints.sld). The pixel-size allows you to
 * skip more than a single pixel. For example, a pixel size of 3 would skip an
 * estimated 3x3 pixel cell in GeoWave's row IDs. Note that rows are only
 * skipped when a feature successfully passes filters.
 * 
 */
@DescribeProcess(title = "DecimateToPixelResolution", description = "This process will enable GeoWave to decimate WMS rendering down to pixel resolution to not oversample data.  This will efficiently render overlapping geometry that would otherwise be hidden but it assume an opaque style and does not take transparency into account.")
public class DistributedRenderProcess
{
	public static final Hints.Key STYLE = new Hints.Key(
			Style.class);
	public static final Hints.Key OPTIONS = new Hints.Key(
			DistributedRenderOptions.class);

	@DescribeResult(name = "result", description = "This is just a pass-through, the key is to provide enough information within invertQuery to perform a map to screen transform")
	public GridCoverage2D execute(
			@DescribeParameter(name = "data", description = "Feature collection containing the data")
			final SimpleFeatureCollection features,
			@DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output")
			final ReferencedEnvelope argOutputEnv,
			@DescribeParameter(name = "outputWidth", description = "Width of the output raster")
			final Integer argOutputWidth,
			@DescribeParameter(name = "outputHeight", description = "Height of the output raster")
			final Integer argOutputHeight )
			throws ProcessException {
		// vector-to-raster render transform that takes a set of features that
		// wrap graphics2D objects and converts them to a GridCoverage2D
		FeatureUtilities.wrapGridCoverage(
				coverage);
		final ColorModel model = masterImage.getImage().getColorModel();
		BufferedImage master;
		if (model instanceof IndexColorModel) {
			master = new BufferedImage(
					masterImage.getImage().getWidth(),
					masterImage.getImage().getHeight(),
					masterImage.getImage().getType(),
					(IndexColorModel) model);
		}
		else {
			master = new BufferedImage(
					masterImage.getImage().getWidth(),
					masterImage.getImage().getHeight(),
					masterImage.getImage().getType());
		}
		Graphics2D graphics = master.createGraphics();
		graphics.setComposite(
				AlphaComposite.getInstance(
						AlphaComposite.SRC_OVER));

		for (final BufferedImage ftsGraphics : mergeGraphics) {
			// we may have not found anything to paint, in that case the
			// delegate
			// has not been initialized
			if (ftsGraphics != null) {
				graphics.drawImage(
						ftsGraphics,
						0,
						0,
						null);
			}
		}
		return features;
	}

	public Query invertQuery(
			@DescribeParameter(name = "outputBBOX", description = "Georeferenced bounding box of the output")
			final ReferencedEnvelope argOutputEnv,
			@DescribeParameter(name = "outputWidth", description = "Width of the output raster")
			final Integer argOutputWidth,
			@DescribeParameter(name = "outputHeight", description = "Height of the output raster")
			final Integer argOutputHeight,
			final Query targetQuery,
			final GridGeometry targetGridGeometry )
			throws ProcessException {
		// add to the query hints
		Style style = (Style) targetQuery.getHints().get(
				STYLE);
		if (style != null) {
			DistributedRenderOptions options = new DistributedRenderOptions();
			options.setStyle(
					style);
			targetQuery.getHints().put(
					OPTIONS,
					options);
		}
		return targetQuery;
	}
}
