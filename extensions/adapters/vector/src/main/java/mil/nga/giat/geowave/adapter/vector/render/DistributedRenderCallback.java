package mil.nga.giat.geowave.adapter.vector.render;

import org.geoserver.wms.GetMapCallbackAdapter;
import org.geoserver.wms.WMS;
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
	private WMS wms;
	public DistributedRenderCallback(WMS wms) {
		this.wms = wms;
	}

	@Override
	public WMSMapContent beforeRender(
			final WMSMapContent mapContent ) {
		// add the Style to the Query Hints so that they can be used for
		// distributed rendering
		for (final Layer layer : mapContent.layers()) {
			//TODO, only stuff the aggregation in the query hints if the render transform is distributed rendering
//			if (layer.getStyle() != null && layer.getStyle().getFeatureTypeStyles()[0].getTransformation().accept(visitor, extraData))
			layer.getQuery().getHints().put(
					DistributedRenderProcess.OPTIONS,
					layer.getStyle());
		}
		return mapContent;
	}
}