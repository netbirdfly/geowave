package mil.nga.giat.geowave.datastore.hbase.query;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;

public class FixedCardinalitySkippingFilter extends
		FilterBase
{
	private final static Logger LOGGER = Logger.getLogger(
			FixedCardinalitySkippingFilter.class);

	private Integer bitPosition;
	private Cell nextCellHint;
	
	public FixedCardinalitySkippingFilter(){}

	public FixedCardinalitySkippingFilter(final Integer bitPosition) {
		this.bitPosition = bitPosition;
	}

	@Override
	public ReturnCode filterKeyValue(
			Cell cell )
			throws IOException {
		incrementBit(cell);
		
		if (nextCellHint == null) {
			return ReturnCode.SKIP;
		}

		return ReturnCode.SEEK_NEXT_USING_HINT; // Implement getNextCellHint(Cell)
	}
	
	@Override
	public Cell getNextCellHint(final Cell currentKV) {
		return nextCellHint;
	}

	private void incrementBit( final Cell currentCell ) {
		final byte[] row = CellUtil.cloneRow(currentCell);
		final int cardinality = bitPosition + 1;
		final byte[] rowCopy = new byte[(int) Math.ceil(
				cardinality / 8.0)];
		System.arraycopy(
				row,
				0,
				rowCopy,
				0,
				rowCopy.length);
		// number of bits not used in the last byte
		int remainder = (8 - (cardinality % 8));
		if (remainder == 8) {
			remainder = 0;
		}

		final int numIncrements = (int) Math.pow(
				2,
				remainder);
		if (remainder > 0) {
			for (int i = 0; i < remainder; i++) {
				rowCopy[rowCopy.length - 1] |= (1 << (i));
			}
		}
		for (int i = 0; i < numIncrements; i++) {
			if (!ByteArrayUtils.increment(
					rowCopy)) {
				nextCellHint = null;
				return;
			}
		}
		
		nextCellHint = CellUtil.createCell(rowCopy);
	}

}
