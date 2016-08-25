package mil.nga.giat.geowave.adapter.vector.render;

import org.geotools.feature.FeatureIterator;
import org.geotools.feature.collection.BaseFeatureCollection;
import org.geotools.feature.collection.DelegateSimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.aol.cyclops.data.async.Queue;

public class AsyncQueueFeatureCollection extends
		BaseFeatureCollection<SimpleFeatureType, SimpleFeature>
{
	private final Queue<SimpleFeature> asyncQueue;

	public AsyncQueueFeatureCollection(
			final Queue<SimpleFeature> asyncQueue ) {
		this.asyncQueue = asyncQueue;
	}

	@Override
	public FeatureIterator<SimpleFeature> features() {
		return new DelegateSimpleFeatureIterator(
				asyncQueue.stream().iterator());
	}
}
