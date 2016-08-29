package mil.nga.giat.geowave.examples.hbase.app;

import java.util.concurrent.TimeUnit;

import mil.nga.giat.geowave.datastore.hbase.query.RowCountEndpoint;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GeoWaveDemoApp
{

	public static void main(
			final String[] args )
			throws Exception {
		Logger.getRootLogger().setLevel(
				Level.WARN);

		final boolean interactive = (System.getProperty("interactive") != null) ? Boolean.parseBoolean(System.getProperty("interactive")) : true;

		final HBaseTestingUtility utility = new HBaseTestingUtility();

		// set minicluster config for coprocessor
		final Configuration conf = utility.getConfiguration();
		conf.setStrings(
				CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
				RowCountEndpoint.class.getName());

		// start it up
		utility.startMiniCluster();

		System.out.println("starting up HBase ...");
		Thread.sleep(3000);

		if (interactive) {
			System.out.println("hit Enter to shutdown ..");
			System.in.read();
			System.out.println("Shutting down!");
			utility.shutdownMiniCluster();
		}
		else {
			Runtime.getRuntime().addShutdownHook(
					new Thread() {
						@Override
						public void run() {
							try {
								utility.shutdownMiniCluster();
							}
							catch (final Exception e) {
								System.out.println("Error shutting down hbase.");
							}
							System.out.println("Shutting down!");
						}
					});

			while (true) {
				Thread.sleep(TimeUnit.MILLISECONDS.convert(
						Long.MAX_VALUE,
						TimeUnit.DAYS));
			}
		}
	}
}
