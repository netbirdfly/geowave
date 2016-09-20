package mil.nga.giat.geowave.adapter.vector.field;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;

import org.opengis.feature.simple.SimpleFeatureType;

public class SimpleFeatureSerializationProvider
{

	public static class WholeFeatureReader implements
			FieldReader<byte[][]>
	{
		byte[] fieldId;
		List<Integer> pos;

		public WholeFeatureReader(byte[] fieldId ) {
			super();
			this.fieldId = fieldId;
			pos = BitmaskUtils.getFieldPositions(fieldId);
		}

		@Override
		public byte[][] readField(
				final byte[] fieldData ) {
			if (fieldData == null) {
				return null;
			}
			final ByteBuffer input = ByteBuffer.wrap(fieldData);
			int attrCnt = pos.size();
			byte[][] retVal = new byte[attrCnt + 1][];
			for (int i = 0; i < attrCnt; i++) {
				int byteLength;
				byteLength = input.getInt();
				if (byteLength < 0) {
					retVal[i] = null;
					continue;
				}
				byte[] fieldValue = new byte[byteLength];
				input.get(fieldValue);
				retVal[i] = fieldValue;
			}
			retVal[attrCnt] = fieldId;
			return retVal;
		}

	}

	public static class WholeFeatureWriter implements
			FieldWriter<Object, Object[]>
	{
		public WholeFeatureWriter() {
			super();

		}

		@Override
		public byte[] writeField(
				final Object[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final DataOutputStream output = new DataOutputStream(
					baos);

			try {
				for (Object attr : fieldValue) {
					if (attr == null) {
						output.writeInt(-1);

						continue;
					}
					FieldWriter writer = FieldUtils.getDefaultWriterForClass(attr.getClass());
					byte[] binary = writer.writeField(attr);
					output.writeInt(binary.length);
					output.write(binary);
				}
				output.close();
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return baos.toByteArray();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Object[] fieldValue ) {
			return new byte[] {};
		}

	}

}
