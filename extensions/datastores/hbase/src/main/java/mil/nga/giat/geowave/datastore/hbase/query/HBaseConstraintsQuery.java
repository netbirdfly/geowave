package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.Mergeable;
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
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.query.generated.AggregationProtos;
import mil.nga.giat.geowave.datastore.hbase.query.generated.RowCountProtos;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;

public class HBaseConstraintsQuery extends
		HBaseFilteredIndexQuery
{
	protected final ConstraintsQuery base;
	protected HBaseOptions options = null;

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

	public void setOptions(
			HBaseOptions options ) {
		this.options = options;
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

		// Aggregate without coprocessor
		if (options == null || !options.isEnableCoprocessors()) {
			final CloseableIterator<Object> it = super.query(
					operations,
					adapterStore,
					limit);

			if ((it != null) && it.hasNext()) {
				final Aggregation aggregationFunction = base.aggregation.getRight();
				synchronized (aggregationFunction) {

					aggregationFunction.clearResult();
					while (it.hasNext()) {
						final Object input = it.next();
						if (input != null) {
							aggregationFunction.aggregate(input);
						}
					}
					try {
						it.close();
					}
					catch (final IOException e) {
						LOGGER.warn(
								"Unable to close hbase scanner",
								e);
					}

					return new Wrapper(
							Iterators.singletonIterator(aggregationFunction.getResult()));
				}
			}

			return new CloseableIterator.Empty();
		}

		// If we made it this far, we're using a coprocessor for aggregation
		return aggregateWithCoprocessor(
				operations,
				adapterStore,
				limit);
	}

	private CloseableIterator<Object> aggregateWithCoprocessor(
			final BasicHBaseOperations operations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		Mergeable total = null;

		try {
			// Use the row count coprocessor
			operations.verifyCoprocessor(
					tableName,
					AggregationEndpoint.class.getName(),
					options.getCoprocessorJar());

			MultiRowRangeFilter multiFilter = getMultiFilter();
			
			final Aggregation aggregation = base.aggregation.getRight();
			total = aggregation.getResult().getClass().newInstance();

			FilterList filterList = new FilterList(
					multiFilter);
			
			AggregationProtos.AggregationType.Builder aggregationBuilder = AggregationProtos.AggregationType.newBuilder();
			aggregationBuilder.setName(aggregation.getClass().getName());

			final AggregationProtos.AggregationRequest.Builder requestBuilder = AggregationProtos.AggregationRequest.newBuilder();
			requestBuilder.setType(aggregationBuilder.build());
			
			requestBuilder.setFilter(ByteString.copyFrom(filterList.toByteArray()));

			final AggregationProtos.AggregationRequest request = requestBuilder.build();

			Table table = operations.getTable(tableName);

			Map<byte[], ByteString> results = table.coprocessorService(
					AggregationProtos.AggregationService.class,
					null,
					null,
					new Batch.Call<AggregationProtos.AggregationService, ByteString>() {
						public ByteString call(
								AggregationProtos.AggregationService counter )
								throws IOException {
							BlockingRpcCallback<AggregationProtos.AggregationResponse> rpcCallback = new BlockingRpcCallback<AggregationProtos.AggregationResponse>();
							counter.aggregate(
									null,
									request,
									rpcCallback);
							AggregationProtos.AggregationResponse response = rpcCallback.get();
							return response.hasValue() ? response.getValue() : null;
						}
					});
			

			for (Map.Entry<byte[], ByteString> entry : results.entrySet()) {
				byte[] bvalue = entry.getValue().toByteArray(); // instance of Mergeable
				Mergeable mvalue = aggregation.getResult().getClass().newInstance();
				mvalue.fromBinary(bvalue);
				
				total.merge(mvalue);
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
