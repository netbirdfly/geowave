package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.flatten.FlattenedDataSet;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadData;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.encoding.HBaseCommonIndexedPersistenceEncoding;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

/**
 * This class wraps our Distributable filters in an HBase filter so that a coprocessor can use them.
 * 
 * @author kent
 *
 */
public class HBaseDistributableFilter extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(HBaseDistributableFilter.class);

	private DistributableQueryFilter filter;
	protected CommonIndexModel model;
	private final List<ByteArrayId> commonIndexFieldIds = new ArrayList<>();

	public HBaseDistributableFilter() {}

	public boolean init(
			final byte[] filterBytes,
			final byte[] modelBytes ) {
		filter = PersistenceUtils.fromBinary(
				filterBytes,
				DistributableQueryFilter.class);

		if (filter == null) {
			final ByteBuffer buf = ByteBuffer.wrap(filterBytes);
			final int classNameLength = buf.getInt();
			final byte[] classNameBinary = new byte[classNameLength];
			final byte[] persistableBinary = new byte[filterBytes.length - classNameLength - 4];
			buf.get(classNameBinary);

			final String className = StringUtils.stringFromBinary(classNameBinary);
			System.out.println("Failed to decode filter type: " + className);
			
			final DistributableQueryFilter retVal = PersistenceUtils.classFactory(
					className,
					DistributableQueryFilter.class);
			if (retVal != null) {
				System.out.println("Created filter object; decoding content...");
				buf.get(persistableBinary);
				retVal.fromBinary(persistableBinary);
				System.out.println("Dist filter OK: type = " + retVal.getClass().getName());
			}
			else {
				System.out.println("Failed to create filter object");
			}
			
			return false;
		}

		model = PersistenceUtils.fromBinary(
				modelBytes,
				CommonIndexModel.class);

		if (model == null) {
			System.out.println("Failed to create model object");
			return false;
		}

		for (final NumericDimensionField<? extends CommonIndexValue> numericDimension : model.getDimensions()) {
			commonIndexFieldIds.add(numericDimension.getFieldId());
		}

		return true;
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell cell )
			throws IOException {
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

		final FlattenedUnreadData unreadData = aggregateFieldData(
				cell,
				commonData);

		return applyRowFilter(
				cell,
				commonData,
				unreadData);
	}

	protected ReturnCode applyRowFilter(
			final Cell cell,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		ReturnCode returnCode = ReturnCode.SKIP;

		try {
			if (applyRowFilter(getEncoding(
					cell,
					commonData,
					unreadData))) {
				returnCode = ReturnCode.INCLUDE;
			}
		}
		catch (Exception e) {

			e.printStackTrace();
		}

		return returnCode;
	}

	protected static CommonIndexedPersistenceEncoding getEncoding(
			final Cell cell,
			final PersistentDataset<CommonIndexValue> commonData,
			final FlattenedUnreadData unreadData ) {
		final GeowaveRowId rowId = new GeowaveRowId(
				CellUtil.cloneRow(cell));

		return new HBaseCommonIndexedPersistenceEncoding(
				new ByteArrayId(
						rowId.getAdapterId()),
				new ByteArrayId(
						rowId.getDataId()),
				new ByteArrayId(
						rowId.getInsertionId()),
				rowId.getNumberOfDuplicates(),
				commonData,
				unreadData);
	}

	protected boolean applyRowFilter(
			final CommonIndexedPersistenceEncoding encoding ) {
		if (filter == null) {
			System.out.println("FILTER IS NULL");
			return false;
		}

		if (model == null) {
			System.out.println("MODEL IS NULL");
			return false;
		}

		if (encoding == null) {
			System.out.println("ENCODING IS NULL");
			return false;
		}

		return filter.accept(
				model,
				encoding);
	}

	protected FlattenedUnreadData aggregateFieldData(
			final Cell cell,
			final PersistentDataset<CommonIndexValue> commonData ) {
		final ByteArrayId colQual = new ByteArrayId(
				CellUtil.cloneQualifier(cell));

		final byte[] valueBytes = CellUtil.cloneValue(cell);

		final FlattenedDataSet dataSet = DataStoreUtils.decomposeFlattenedFields(
				colQual.getBytes(),
				valueBytes,
				null,
				-1);

		final List<FlattenedFieldInfo> fieldInfos = dataSet.getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final int ordinal = fieldInfo.getFieldPosition();

			if (ordinal < model.getDimensions().length) {
				final ByteArrayId commonIndexFieldId = commonIndexFieldIds.get(ordinal);
				final FieldReader<? extends CommonIndexValue> reader = model.getReader(commonIndexFieldId);
				if (reader != null) {
					final CommonIndexValue fieldValue = reader.readField(fieldInfo.getValue());
					// TODO: handle visibility
					// fieldValue.setVisibility(key.getColumnVisibility().getBytes());
					commonData.addValue(new PersistentValue<CommonIndexValue>(
							commonIndexFieldId,
							fieldValue));
				}
				else {
					LOGGER.error("Could not find reader for common index field: " + commonIndexFieldId.getString());
				}
			}
		}

		return dataSet.getFieldsDeferred();
	}
}
