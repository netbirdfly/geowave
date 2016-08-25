package mil.nga.giat.geowave.adapter.vector.render;

import java.awt.AlphaComposite;
import java.awt.Composite;

import org.geotools.renderer.composite.BlendComposite;
import org.geotools.renderer.composite.BlendComposite.BlendingMode;

import mil.nga.giat.geowave.core.index.Persistable;

public class PersistableComposite implements
		Persistable
{
	private boolean isBlend = true;
	private int blendModeOrAlphaRule = 0;
	private float alpha = 1f;

	public PersistableComposite(
			final Composite composite ) {
		if (composite instanceof BlendComposite) {
			isBlend = true;
			blendModeOrAlphaRule = ((BlendComposite) composite).getBlend().ordinal();
			alpha = ((BlendComposite) composite).getAlpha();
		}
		else if (composite instanceof AlphaComposite) {
			isBlend = false;
			blendModeOrAlphaRule = ((AlphaComposite) composite).getRule();
			alpha = ((AlphaComposite) composite).getAlpha();
		}
	}

	public Composite getComposite(){
		if(isBlend){
			return BlendComposite.getInstance(BlendingMode.values()[blendModeOrAlphaRule], alpha);
		}
		else{
			return AlphaComposite.getInstance(blendModeOrAlphaRule, alpha);
		}
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		// TODO Auto-generated method stub

	}

}
