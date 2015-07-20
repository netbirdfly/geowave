package mil.nga.giat.geowave.core.store.components;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.MemoryIndexStore;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class MemoryDataStore implements
		DataStore
{
	private Map<ByteArrayId, TreeSet<EntryRow>> storeData = new HashMap<ByteArrayId, TreeSet<EntryRow>>();
	private AdapterStore adapterStore;
	private IndexStore indexStore;

	public MemoryDataStore() {
		super();
		adapterStore = new MemoryAdapterStore();
		indexStore = new MemoryIndexStore();
	}

	public MemoryDataStore(
			AdapterStore adapterStore,
			IndexStore indexStore ) {
		super();
		this.adapterStore = adapterStore;
		this.indexStore = indexStore;
	}

	@Override
	public <T> IndexWriter createIndexWriter(
			final Index index ) {

		return createWriter(
				index,
				new IngestCallback<T>() {

					@Override
					public void entryIngested(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				DataStoreUtils.DEFAULT_VISIBILITY);
	}

	@Override
	public <T> List<ByteArrayId> ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			T entry ) {
		return createWriter(
				index,
				new IngestCallback<T>() {

					@Override
					public void entryIngested(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				DataStoreUtils.DEFAULT_VISIBILITY).write(
				writableAdapter,
				entry);
	}

	@Override
	public <T> void ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			Iterator<T> entryIterator ) {
		IndexWriter writer = createWriter(
				index,
				new IngestCallback<T>() {

					@Override
					public void entryIngested(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				DataStoreUtils.DEFAULT_VISIBILITY);
		while (entryIterator.hasNext()) {
			T nextEntry = entryIterator.next();
			writer.write(
					writableAdapter,
					nextEntry);
		}

	}

	@Override
	public <T> List<ByteArrayId> ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			T entry,
			VisibilityWriter<T> customFieldVisibilityWriter ) {
		return createWriter(
				index,
				new IngestCallback<T>() {

					@Override
					public void entryIngested(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				customFieldVisibilityWriter).write(
				writableAdapter,
				entry);
	}

	@Override
	public <T> void ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			Iterator<T> entryIterator,
			IngestCallback<T> ingestCallback ) {
		IndexWriter writer = createWriter(
				index,
				ingestCallback,
				DataStoreUtils.DEFAULT_VISIBILITY);
		while (entryIterator.hasNext()) {
			T nextEntry = entryIterator.next();
			writer.write(
					writableAdapter,
					nextEntry);
		}
	}

	@Override
	public <T> void ingest(
			WritableDataAdapter<T> writableAdapter,
			Index index,
			Iterator<T> entryIterator,
			IngestCallback<T> ingestCallback,
			VisibilityWriter<T> customFieldVisibilityWriter ) {
		IndexWriter writer = createWriter(
				index,
				ingestCallback,
				customFieldVisibilityWriter);
		while (entryIterator.hasNext()) {
			T nextEntry = entryIterator.next();
			writer.write(
					writableAdapter,
					nextEntry);
		}
	}

	private <T> IndexWriter createWriter(
			final Index index,
			final IngestCallback<T> ingestCallback,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		return new MyIndexWriter<T>(
				index,
				ingestCallback,
				customFieldVisibilityWriter);
	}

	private class MyIndexWriter<S> implements
			IndexWriter
	{
		final Index index;
		final IngestCallback<S> ingestCallback;
		final VisibilityWriter<S> customFieldVisibilityWriter;

		public MyIndexWriter(
				Index index,
				IngestCallback<S> ingestCallback,
				VisibilityWriter<S> customFieldVisibilityWriter ) {
			super();
			this.index = index;
			this.ingestCallback = ingestCallback;
			this.customFieldVisibilityWriter = customFieldVisibilityWriter;
		}

		@Override
		public void close()
				throws IOException {}

		@Override
		public <T> List<ByteArrayId> write(
				WritableDataAdapter<T> writableAdapter,
				T entry ) {
			final ByteArrayId dataId = writableAdapter.getDataId(entry);
			final List<EntryRow> rows = DataStoreUtils.entryToRows(
					writableAdapter,
					index,
					entry,
					(IngestCallback<T>) ingestCallback,
					(VisibilityWriter<T>) customFieldVisibilityWriter);
			for (EntryRow row : rows) {
				storeData.get(
						index.getId()).add(
						row);
			}
			return null;
		}

		@Override
		public <T> void setupAdapter(
				WritableDataAdapter<T> writableAdapter ) {}

		@Override
		public Index getIndex() {
			return index;
		}

		@Override
		public void flush() {}
	}

	@Override
	public CloseableIterator<?> query(
			Query query ) {
		return query(
				query,
				-1);
	}

	@Override
	public <T> T getEntry(
			Index index,
			ByteArrayId rowId ) {
		final Iterator<EntryRow> rowIt = storeData.get(
				index.getId()).iterator();
		while (rowIt.hasNext()) {
			EntryRow row = rowIt.next();
			if (Arrays.equals(
					row.getTableRowId().getRowId(),
					rowId.getBytes())) return (T) row.getEntry();
		}
		return null;
	}

	@Override
	public <T> T getEntry(
			Index index,
			ByteArrayId dataId,
			ByteArrayId adapterId,
			String... additionalAuthorizations ) {
		final Iterator<EntryRow> rowIt = storeData.get(
				index.getId()).iterator();
		while (rowIt.hasNext()) {
			EntryRow row = rowIt.next();
			if (Arrays.equals(
					row.getTableRowId().getDataId(),
					dataId.getBytes()) && Arrays.equals(
					row.getTableRowId().getAdapterId(),
					adapterId.getBytes()) && isAuthorized(
					row,
					additionalAuthorizations)) return (T) row.getEntry();
		}
		return null;
	}

	@Override
	public boolean deleteEntry(
			Index index,
			ByteArrayId dataId,
			ByteArrayId adapterId,
			String... authorizations ) {
		final Iterator<EntryRow> rowIt = storeData.get(
				index.getId()).iterator();
		while (rowIt.hasNext()) {
			EntryRow row = rowIt.next();
			if (Arrays.equals(
					row.getTableRowId().getDataId(),
					dataId.getBytes()) && Arrays.equals(
					row.getTableRowId().getAdapterId(),
					adapterId.getBytes()) && isAuthorized(
					row,
					authorizations)) rowIt.remove();
		}
		return false;
	}

	@Override
	public <T> CloseableIterator<T> getEntriesByPrefix(
			final Index index,
			final ByteArrayId rowPrefix,
			final String... authorizations ) {

		final Iterator<EntryRow> rowIt = storeData.get(
				index.getId()).iterator();

		return new CloseableIterator<T>() {

			EntryRow nextRow = null;

			private boolean getNext() {
				while (nextRow == null && rowIt.hasNext()) {
					final EntryRow row = rowIt.next();
					if (!Arrays.equals(
							rowPrefix.getBytes(),
							Arrays.copyOf(
									row.rowId.getRowId(),
									rowPrefix.getBytes().length))) continue;
					nextRow = row;
					break;
				}
				return nextRow != null;
			}

			@Override
			public boolean hasNext() {
				return getNext();
			}

			@Override
			public T next() {
				final EntryRow currentRow = nextRow;
				nextRow = null;
				return (T) currentRow.entry;
			}

			@Override
			public void remove() {
				rowIt.remove();
			}

			@Override
			public void close()
					throws IOException {}
		};
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Query query ) {
		return (CloseableIterator<T>) query(
				Arrays.asList(adapter.getAdapterId()),
				query,
				-1);
	}

	@Override
	public <T> CloseableIterator<T> query(
			Index index,
			Query query ) {
		return query(
				index,
				query,
				-1);
	}

	@Override
	public <T> CloseableIterator<T> query(
			Index index,
			Query query,
			QueryOptions queryOptions ) {
		return query(
				index,
				query,
				-1);
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			Query query ) {
		return query(
				adapter,
				index,
				query,
				-1,
				new ScanCallback<T>() {

					@Override
					public void entryScanned(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				"");
	}

	@Override
	public CloseableIterator<?> query(
			List<ByteArrayId> adapterIds,
			Query query ) {
		return query(
				adapterIds,
				query,
				-1);
	}

	@Override
	public CloseableIterator<?> query(
			Query query,
			int limit ) {
		final List<ByteArrayId> adapterIds = new ArrayList<ByteArrayId>();
		try (CloseableIterator<DataAdapter<?>> adapterIt = adapterStore.getAdapters()) {
			while (adapterIt.hasNext()) {
				adapterIds.add(adapterIt.next().getAdapterId());
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return query(
				adapterIds,
				query,
				limit);
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Query query,
			int limit ) {
		return (CloseableIterator<T>) query(
				Arrays.asList(adapter.getAdapterId()),
				query,
				limit);
	}

	@Override
	public <T> CloseableIterator<T> query(
			Index index,
			Query query,
			int limit ) {
		return query(
				null,
				index,
				query,
				limit,
				new ScanCallback<T>() {

					@Override
					public void entryScanned(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				"");
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			Query query,
			int limit ) {
		return query(
				adapter,
				index,
				query,
				limit,
				new ScanCallback<T>() {

					@Override
					public void entryScanned(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				"");
	}

	@Override
	public CloseableIterator<?> query(
			final List<ByteArrayId> adapterIds,
			final Query query,
			final int limit ) {
		final CloseableIterator<Index> indexIt = indexStore.getIndices();
		return new CloseableIterator<Object>() {
			Iterator<ByteArrayId> adapterIt = adapterIds.iterator();
			Index index = null;
			int count = 0;
			CloseableIterator<Object> dataIt = null;

			private boolean getNext() {
				while (dataIt == null || !dataIt.hasNext()) {
					if (index == null) {
						if (indexIt.hasNext()) {
							index = indexIt.next();
						}
						else {
							return false;
						}
					}
					if (adapterIt != null && adapterIt.hasNext()) {
						dataIt = (CloseableIterator<Object>) query(
								adapterStore.getAdapter(adapterIt.next()),
								index,
								query,
								-1,
								new ScanCallback<Object>() {

									@Override
									public void entryScanned(
											DataStoreEntryInfo entryInfo,
											Object entry ) {

									}
								},
								"");
						continue;
					}
					index = null;
					adapterIt = adapterIds.iterator();
				}

				return true;

			}

			@Override
			public boolean hasNext() {
				return (limit <= 0 || count < limit) && getNext();
			}

			@Override
			public Object next() {
				count++;
				return dataIt.next();
			}

			@Override
			public void remove() {
				dataIt.remove();

			}

			@Override
			public void close()
					throws IOException {
				indexIt.close();
			}

		};
	}

	@Override
	public <T> CloseableIterator<T> query(
			DataAdapter<T> adapter,
			Index index,
			Query query,
			int limit,
			String... authorizations ) {
		return query(
				adapter,
				index,
				query,
				limit,
				new ScanCallback<T>() {

					@Override
					public void entryScanned(
							DataStoreEntryInfo entryInfo,
							T entry ) {

					}
				},
				authorizations);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final DataAdapter<T> adapter,
			final Index index,
			final Query query,
			final Integer limit,
			final ScanCallback<?> scanCallback,
			final String... authorizations ) {
		final Iterator<EntryRow> rowIt = query.isSupported(index) ? storeData.get(
				index.getId()).iterator() : Collections.<EntryRow> emptyIterator();

		final List<QueryFilter> filters = query.createFilters(index.getIndexModel());
		return new CloseableIterator<T>() {
			int count = 0;
			EntryRow nextRow = null;

			private boolean getNext() {
				while (nextRow == null && rowIt.hasNext()) {
					final EntryRow row = rowIt.next();
					final DataAdapter<T> innerAdapter = (DataAdapter<T>) (adapter == null ? adapterStore.getAdapter(new ByteArrayId(
							row.getTableRowId().getAdapterId())) : adapter);
					final IndexedAdapterPersistenceEncoding encoding = DataStoreUtils.getEncoding(
							index.getIndexModel(),
							innerAdapter,
							row);
					for (QueryFilter filter : filters) {
						if (!filter.accept(encoding)) continue;
					}
					count++;
					nextRow = row;
					break;
				}
				return nextRow != null && (limit == null || limit <= 0 || count < limit);
			}

			@Override
			public boolean hasNext() {
				return getNext();
			}

			@Override
			public T next() {
				final EntryRow currentRow = nextRow;
				((ScanCallback<T>) scanCallback).entryScanned(
						currentRow.getInfo(),
						(T) currentRow.entry);
				nextRow = null;
				return (T) currentRow.entry;
			}

			@Override
			public void remove() {
				rowIt.remove();
			}

			@Override
			public void close()
					throws IOException {}
		};
	}

	private boolean isAuthorized(
			EntryRow row,
			String... authorizations ) {
		for (FieldInfo info : row.info.getFieldInfo()) {
			info.getVisibility())
		}
		return true;
	}
}
