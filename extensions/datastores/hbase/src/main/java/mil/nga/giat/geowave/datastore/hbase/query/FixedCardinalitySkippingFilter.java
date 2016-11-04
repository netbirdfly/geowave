package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.IndexUtils;

public class FixedCardinalitySkippingFilter extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(
			FixedCardinalitySkippingFilter.class);

	private Integer bitPosition;
	private Cell nextCellHint;

	public FixedCardinalitySkippingFilter() {}

	public FixedCardinalitySkippingFilter(
			final Integer bitPosition ) {
		this.bitPosition = bitPosition;
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell cell )
			throws IOException {
		skip(cell);

		if (nextCellHint == null) {
			LOGGER.warn("SkipFilter has reached the end of the data!");
			return ReturnCode.SKIP;
		}

		return ReturnCode.SEEK_NEXT_USING_HINT;
	}

	@Override
	public Cell getNextCellHint(
			final Cell currentKV ) {
		return nextCellHint;
	}

	private void skip(
			final Cell currentCell ) {
		final byte[] row = CellUtil.cloneRow(
				currentCell);

		final byte[] nextRow = IndexUtils.getNextRowForSkip(
				row,
				bitPosition);

		nextCellHint = nextRow != null ? CellUtil.createCell(
				nextRow) : null;
	}

	public byte[] toByteArray()
			throws IOException {
		final ByteBuffer buf = ByteBuffer.allocate(
				Integer.BYTES);
		buf.putInt(
				this.bitPosition);

		return buf.array();
	}

	public static FixedCardinalitySkippingFilter parseFrom(
			final byte[] bytes )
			throws DeserializationException {
		final ByteBuffer buf = ByteBuffer.wrap(
				bytes);
		final int bitpos = buf.getInt();

		return new FixedCardinalitySkippingFilter(
				bitpos);
	}

}
