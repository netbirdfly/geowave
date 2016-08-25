package mil.nga.giat.geowave.adapter.vector.render;

import org.geoserver.wms.GetMapCallbackAdapter;
import org.geoserver.wms.WMSMapContent;
import org.geotools.map.Layer;

import mil.nga.giat.geowave.adapter.vector.plugin.DistributedRenderProcess;

/**
 * The purpose of this callback is completely to get the layer Style accessible
 * from the query, in particular making the style available to either the
 * FeatureReader or to a RenderingTransformation
 *
 */
public class DistributedRenderCallback extends
		GetMapCallbackAdapter
{

	public DistributedRenderCallback() {}

	@Override
	public WMSMapContent beforeRender(
			final WMSMapContent mapContent ) {
		// add the Style to the Query Hints so that they can be used for
		// distributed rendering
		for (final Layer layer : mapContent.layers()) {
			layer.getQuery().getHints().put(
					DistributedRenderProcess.STYLE,
					layer.getStyle());
		}
		return mapContent;
	}
}