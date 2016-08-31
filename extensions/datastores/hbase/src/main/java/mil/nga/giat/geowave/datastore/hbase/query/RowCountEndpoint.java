package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.datastore.hbase.query.generated.RowCountProtos;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class RowCountEndpoint extends
		RowCountProtos.RowCountService implements
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
	public void getRowCount(
			RpcController controller,
			RowCountProtos.CountRequest request,
			RpcCallback<RowCountProtos.CountResponse> done ) {
		RowCountProtos.CountResponse response = null;

		byte[] filterBytes = request.getFilter().toByteArray();

		try {
			final MultiRowRangeFilter multiFilter = MultiRowRangeFilter.parseFrom(filterBytes);
			System.out.println("Coprocessor: Multi-filter has " + multiFilter.getRowRanges().size() + " ranges.");

			long count = getCount(multiFilter);

			response = RowCountProtos.CountResponse.newBuilder().setCount(
					count).build();
		}
		catch (IOException ioe) {
			ResponseConverter.setControllerException(
					controller,
					ioe);
		}
		catch (DeserializationException de) {
			de.printStackTrace();
		}

		done.run(response);
	}

	/**
	 * Helper method to count rows or cells. *
	 * 
	 * @param filter
	 *            The optional filter instance.
	 * @param countCells
	 *            Hand in <code>true</code> for cell counting.
	 * @return The count as per the flags.
	 * @throws IOException
	 *             When something fails with the scan.
	 */
	private long getCount(Filter filter )
			throws IOException {
		long count = 0;
		
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		
		if (filter != null) {
			scan.setFilter(filter);
		}
		
		try (InternalScanner scanner = env.getRegion().getScanner(
				scan);) {
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMore = false;
			byte[] lastRow = null;
			do {
				hasMore = scanner.next(results);
				for (Cell cell : results) {
					if (lastRow == null || !CellUtil.matchingRow(
							cell,
							lastRow)) {
						lastRow = CellUtil.cloneRow(cell);
						count++;
					}
				}
				results.clear();
			}
			while (hasMore);
		}
		return count;
	}
}
