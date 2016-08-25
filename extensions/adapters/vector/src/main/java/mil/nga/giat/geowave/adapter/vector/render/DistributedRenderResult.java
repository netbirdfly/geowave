package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.AlphaComposite;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.IndexColorModel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.renderer.lite.DelayedBackbufferGraphic;
import org.geotools.renderer.lite.LiteFeatureTypeStyle;

import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.render.RenderedMaster;
import mil.nga.giat.geowave.render.RenderedStyle;
import mil.nga.giat.geowave.render.DistributedRenderResultStore.StyleImage;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class DistributedRenderResult implements
		Mergeable
{
	public static class CompositeGroupResult
	{
		// the master image essentially gets labels rendered to it
		private PersistableRenderedImage masterImage;

		private PersistableComposite composite;

		// keep each style separate so they can be composited together in the
		// original draw order
		private List<Pair<PersistableRenderedImage, PersistableComposite>> orderedStyles;

		public CompositeGroupResult(
				PersistableRenderedImage masterImage,
				PersistableComposite composite,
				List<Pair<PersistableRenderedImage, PersistableComposite>> orderedStyles ) {
			this.masterImage = masterImage;
			this.composite = composite;
			this.orderedStyles = orderedStyles;
		}

		
	}

	// geotools has a concept of composites, which we need to keep separate so
	// that they can be composited in the original draw order, by default there
	// is only a single composite
	private List<CompositeGroupResult> orderedComposites;

	protected DistributedRenderResult() {}

	public DistributedRenderResult(
			final List<CompositeGroupResult> orderedComposites ) {
		this.orderedComposites = orderedComposites;
	}

	@Override
	public byte[] toBinary() {
		final byte[] masterBinary = masterImage.toBinary();
		final List<byte[]> styleBinaries = new ArrayList<byte[]>(
				renderedStyles.size());
		int styleBinaryLength = 0;
		for (final RenderedStyle style : renderedStyles) {
			final byte[] binary = style.toBinary();
			styleBinaries.add(
					binary);
			styleBinaryLength += (binary.length + 4);
		}

		final ByteBuffer buf = ByteBuffer.allocate(
				8 + styleBinaryLength + masterBinary.length);
		buf.putInt(
				masterBinary.length);
		buf.put(
				masterBinary);
		buf.putInt(
				styleBinaries.size());
		for (final byte[] styleBinary : styleBinaries) {
			buf.putInt(
					styleBinary.length);
			buf.put(
					styleBinary);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(
				bytes);
		final int masterLength = buf.getInt();
		final byte[] masterBytes = new byte[masterLength];
		buf.get(
				masterBytes);
		masterImage = new PersistableRenderedImage();
		masterImage.fromBinary(
				masterBytes);
		final int numStyles = buf.getInt();
		renderedStyles = new ArrayList<RenderedStyle>(
				numStyles);
		for (int i = 0; i < numStyles; i++) {
			final int styleBinaryLength = buf.getInt();
			final byte[] styleBinary = new byte[styleBinaryLength];
			buf.get(
					styleBinary);
			final RenderedStyle style = new RenderedStyle();
			style.fromBinary(
					styleBinary);
			renderedStyles.add(
					style);
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if (merge instanceof DistributedRenderResult) {
			GeoWaveStoreType.ACCUMULO
		}
	}

	private final static class StyleImage
	{
		private final Map<String, BufferedImage> images = new TreeMap<String, BufferedImage>();

		private void addRenderedStyle(
				final String resultId,
				final RenderedStyle style ) {
			images.put(
					resultId,
					style.getImage());
		}
	}

	private final static class DrawRenderResultStore
	{
		private final Map<String, BufferedImage> masterImagesToMerge = new TreeMap<String, BufferedImage>();
		private final Map<String, StyleImage> styleImagesToMerge = new HashMap<String, StyleImage>();

		public void addResult(
				final String resultId,
				final RenderedMaster result ) {
			if (result.getImage() != null) {
				masterImagesToMerge.put(
						resultId,
						result.getImage());
			}
			for (final RenderedStyle s : result.getRenderedStyles()) {
				StyleImage img = styleImagesToMerge.get(
						s.getStyleId());
				if (img == null) {
					img = new StyleImage();
					styleImagesToMerge.put(
							s.getStyleId(),
							img);
				}
				img.addRenderedStyle(
						resultId,
						s);
			}
		}

		public BufferedImage[] getDrawOrderResults(
				final String[] styleIdDrawOrder ) {
			final List<BufferedImage> orderedImages = new ArrayList<>();
			// first do the styles in order
			for (final String styleId : styleIdDrawOrder) {
				final StyleImage styleImage = styleImagesToMerge.get(
						styleId);
				if (styleImage != null) {
					// add the images in order of the result key
					orderedImages.addAll(
							styleImage.images.values());
				}
			}
			// then the master (which are typically just labels), in order of
			// result
			// key
			orderedImages.addAll(
					masterImagesToMerge.values());
			return orderedImages.toArray(
					new BufferedImage[] {});
		}
	}
}
