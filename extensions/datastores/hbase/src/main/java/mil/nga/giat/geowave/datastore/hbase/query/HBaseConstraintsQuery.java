package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.ConstraintsQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.query.generated.RowCountProtos;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;

public class HBaseConstraintsQuery extends
		HBaseFilteredIndexQuery
{
	protected final ConstraintsQuery base;

	private final static Logger LOGGER = Logger.getLogger(HBaseConstraintsQuery.class);

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final String[] authorizations ) {
		this(
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(index.getIndexStrategy()) : null,
				query != null ? query.createFilters(index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				indexMetaData,
				duplicateCounts,
				authorizations);
	}

	public HBaseConstraintsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final String[] authorizations ) {

		super(
				adapterIds,
				index,
				scanCallback,
				authorizations);

		base = new ConstraintsQuery(
				constraints,
				aggregation,
				indexMetaData,
				index,
				queryFilters,
				clientDedupeFilter,
				duplicateCounts,
				this);

		if (isAggregation()) {
			// Because aggregations are done client-side make sure to set
			// the adapter ID here
			this.adapterIds = Collections.singletonList(aggregation.getLeft().getAdapterId());
			LOGGER.setLevel(Level.DEBUG);
		}
	}

	protected boolean isAggregation() {
		return base.isAggregation();
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		return base.getRanges();
	}

	@Override
	protected List<QueryFilter> getAllFiltersList() {
		final List<QueryFilter> filters = super.getAllFiltersList();
		for (final QueryFilter distributable : base.distributableFilters) {
			if (!filters.contains(distributable)) {
				filters.add(distributable);
			}
		}
		return filters;
	}

	@Override
	protected List<Filter> getDistributableFilter() {
		return new ArrayList<Filter>();
	}

	@Override
	public CloseableIterator<Object> query(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		if (!isAggregation()) {
			return super.query(
					operations,
					adapterStore,
					limit);
		}

		//TODO: determine whether we can use the coprocessor
		return aggQuery(
				operations,
				adapterStore,
				limit);
	}

	private CloseableIterator<Object> aggQueryTest(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		try {
			if (!validateAdapters(operations)) {
				LOGGER.warn("Query contains no valid adapters.");
				return new CloseableIterator.Empty();
			}
			if (!operations.tableExists(StringUtils.stringFromBinary(index.getId().getBytes()))) {
				LOGGER.warn("Table does not exist " + StringUtils.stringFromBinary(index.getId().getBytes()));
				return new CloseableIterator.Empty();
			}
		}
		catch (final IOException ex) {
			LOGGER.warn("Unable to check if " + StringUtils.stringFromBinary(index.getId().getBytes()) + " table exists");
			return new CloseableIterator.Empty();
		}

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());

		final List<Filter> distributableFilters = getDistributableFilter();
		CloseableIterator<DataAdapter<?>> adapters = null;
		if ((fieldIds != null) && !fieldIds.isEmpty()) {
			adapters = adapterStore.getAdapters();
		}

		Scan multiScanner = getMultiScanner(
				limit,
				distributableFilters,
				adapters);

		if (authorizations != null) {
			multiScanner.setAuthorizations(new Authorizations(
					authorizations));
		}

		AggregationClient aggregationClient = new AggregationClient(
				operations.getConfig());

		try {
			LOGGER.debug("Calling aggregation client...");

			long total = aggregationClient.rowCount(
					BasicHBaseOperations.getTableName(tableName),
					new LongColumnInterpreter(),
					multiScanner);

			LOGGER.debug("Aggregation client returned " + total + " items.");

			aggregationClient.close();

			return new Wrapper(
					Iterators.singletonIterator(total));

		}
		catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		LOGGER.error("Results were empty");
		return new CloseableIterator.Empty();
	}

	private CloseableIterator<Object> aggQuery(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		// Use the row count coprocessor
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		long total = 0;

		try {
			Table table = operations.getTable(tableName);

			if (!table.getTableDescriptor().hasCoprocessor(
					RowCountEndpoint.class.getName())) {
				LOGGER.debug(tableName + " does not have coprocessor. Adding " + RowCountEndpoint.class.getName());
				
				// TODO: retrieve coprocessor jar path from config
				table.getTableDescriptor().addCoprocessor(
						RowCountEndpoint.class.getName());
			}
			else {
				LOGGER.debug(tableName + " has coprocessor " + RowCountEndpoint.class.getName());
			}

			MultiRowRangeFilter multiFilter = getMultiFilter();
			LOGGER.debug("Client: Multi-filter has " + multiFilter.getRowRanges().size() + " ranges.");

			final RowCountProtos.CountRequest.Builder requestBuilder = RowCountProtos.CountRequest.newBuilder();
			requestBuilder.setFilter(ByteString.copyFrom(multiFilter.toByteArray()));

			final RowCountProtos.CountRequest request = requestBuilder.build();

			Map<byte[], Long> results = table.coprocessorService(
					RowCountProtos.RowCountService.class,
					null,
					null,
					new Batch.Call<RowCountProtos.RowCountService, Long>() {
						public Long call(
								RowCountProtos.RowCountService counter )
								throws IOException {
							BlockingRpcCallback<RowCountProtos.CountResponse> rpcCallback = new BlockingRpcCallback<RowCountProtos.CountResponse>();
							counter.getRowCount(
									null,
									request,
									rpcCallback);
							RowCountProtos.CountResponse response = rpcCallback.get();
							return response.hasCount() ? response.getCount() : 0;
						}
					});

			for (Map.Entry<byte[], Long> entry : results.entrySet()) {
				total += entry.getValue().longValue();
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
		catch (Throwable e) {
			e.printStackTrace();
		}

		return new Wrapper(
				Iterators.singletonIterator(total));
	}

	protected MultiRowRangeFilter getMultiFilter() {
		// create the multi-row filter
		final List<RowRange> rowRanges = new ArrayList<RowRange>();

		List<ByteArrayRange> ranges = base.getAllRanges();

		if ((ranges == null) || ranges.isEmpty()) {
			rowRanges.add(new RowRange(
					HConstants.EMPTY_BYTE_ARRAY,
					true,
					HConstants.EMPTY_BYTE_ARRAY,
					false));
		}
		else {
			for (final ByteArrayRange range : ranges) {
				if (range.getStart() != null) {
					byte[] startRow = range.getStart().getBytes();
					byte[] stopRow;
					if (!range.isSingleValue()) {
						stopRow = HBaseUtils.getNextPrefix(range.getEnd().getBytes());
					}
					else {
						stopRow = HBaseUtils.getNextPrefix(range.getStart().getBytes());
					}

					RowRange rowRange = new RowRange(
							startRow,
							true,
							stopRow,
							true);

					rowRanges.add(rowRange);
				}
			}
		}

		// Create the multi-range filter
		try {
			MultiRowRangeFilter filter = new MultiRowRangeFilter(
					rowRanges);

			return filter;
		}
		catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}
}
