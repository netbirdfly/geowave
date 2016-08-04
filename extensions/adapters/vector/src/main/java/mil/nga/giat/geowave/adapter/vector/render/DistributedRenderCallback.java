package mil.nga.giat.geowave.adapter.vector.render;

import org.geoserver.wms.GetMapCallbackAdapter;
import org.geoserver.wms.GetMapRequest;
import org.geoserver.wms.WMSMapContent;
import org.geotools.filter.function.EnvFunction;

public class DistributedRenderCallback extends
		GetMapCallbackAdapter
{

	public DistributedRenderCallback() {}

	@Override
	public GetMapRequest initRequest(
			GetMapRequest request ) {
		return request;
	}

	@Override
	public void initMapContent(
			WMSMapContent mapContent ) {}

	@Override
	public WMSMapContent beforeRender(
			WMSMapContent mapContent ) {
		// put a mapping of layer type names to style within the environment so
		// that they can be used by a DistributedRender render transform
		EnvFunction.setLocalValue(
				"style",
				mapContent.getRequest().getLayers().get(
						0).getStyle());
		return mapContent;
	}
}
