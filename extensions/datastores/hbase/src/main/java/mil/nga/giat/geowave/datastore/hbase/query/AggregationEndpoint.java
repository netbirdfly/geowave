package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.hbase.query.generated.AggregationProtos;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class AggregationEndpoint extends
		AggregationProtos.AggregationService implements
		Coprocessor,
		CoprocessorService
{

	private RegionCoprocessorEnvironment env;

	@Override
	public void start(
			CoprocessorEnvironment env )
			throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		}
		else {
			throw new CoprocessorException(
					"Must be loaded on a table region!");
		}
	}

	@Override
	public void stop(
			CoprocessorEnvironment env )
			throws IOException {
		// nothing to do when coprocessor is shutting down
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void aggregate(
			RpcController controller,
			AggregationProtos.AggregationRequest request,
			RpcCallback<AggregationProtos.AggregationResponse> done ) {
		AggregationProtos.AggregationResponse response = null;
		ByteString value = null;

		// Get the aggregation type
		String aggregationType = request.getType().getName();
		Aggregation aggregation = null;

		try {
			aggregation = (Aggregation) Class.forName(
					aggregationType).newInstance();

			// TODO: Handle aggregation params
		}
		catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}

		if (aggregation != null) {
			// Get the filter
			byte[] filterBytes = request.getFilter().toByteArray();
			FilterList filterList = null;

			try {
				filterList = FilterList.parseFrom(filterBytes);
			}
			catch (DeserializationException de) {
				de.printStackTrace();
			}

			try {
				Mergeable mvalue = getValue(
						aggregation,
						filterList);

				byte[] bvalue = PersistenceUtils.toBinary(mvalue);
				value = ByteString.copyFrom(bvalue);
			}
			catch (IOException ioe) {
				ResponseConverter.setControllerException(
						controller,
						ioe);
			}
		}

		response = AggregationProtos.AggregationResponse.newBuilder().setValue(
				value).build();

		done.run(response);
	}

	private Mergeable getValue(
			Aggregation aggregation,
			Filter filter )
			throws IOException {
		Scan scan = new Scan();
		scan.setMaxVersions(1);

		if (filter != null) {
			scan.setFilter(filter);
		}

		aggregation.clearResult();

		Region region = env.getRegion();

		RegionScanner scanner = region.getScanner(scan);
		region.startRegionOperation();

		List<Cell> results = new ArrayList<Cell>();
		boolean hasMore = false;

		do {
			hasMore = scanner.nextRaw(results);			
			aggregation.aggregate(results);
		}
		while (hasMore);

		scanner.close();
		region.closeRegionOperation();

		return aggregation.getResult();
	}
}
