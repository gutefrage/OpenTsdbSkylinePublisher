package net.gutefrage.tsdb;

import com.stumbleupon.async.Deferred;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.tsd.RTPublisher;
import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Proof of Concept
 *
 * This OpenTSDB Plugin publishes data to a skyline UDP server
 *
 * make sure that you have 2 new settings in your opentsdb.conf:
 * tsd.plugin.skyline.host = Your skyline host
 * tsd.plugin.skyline.port = Your skyline port
 * 
 */
public class SkylinePublisher extends RTPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(SkylinePublisher.class);
    private DatagramSocket udpSocket;
    private MessagePack msgpack = new MessagePack();
    private InetAddress skylineIa;
    private int skylinePort;

    public void initialize(final TSDB tsdb) {
        LOG.info("init SkylinePublisher");

        skylinePort = tsdb.getConfig().getInt("tsd.plugin.skyline.port");

        try {
            skylineIa = InetAddress.getByName(tsdb.getConfig().getString("tsd.plugin.skyline.host"));
        } catch (UnknownHostException e) {
            LOG.error("UnknownHostException in SkylinePublisher initialize");
        }

        try {
            udpSocket = new DatagramSocket();
        } catch (SocketException e) {
            LOG.error("SocketException in SkylinePublisher initialize");
        }
    }

    public Deferred<Object> shutdown() {
        return new Deferred<Object>();
    }

    public String version() {
        return "2.0.1";
    }

    public void collectStats(final StatsCollector collector) {
    }

    public Deferred<Object> publishDataPoint(final String metric,
            final long timestamp, final long value, final Map<String, String> tags,
            final byte[] tsuid) {

        sendSocket(makeMetricName(metric, tags), timestamp, value);

        return new Deferred<Object>();
    }

    public Deferred<Object> publishDataPoint(final String metric,
            final long timestamp, final double value, final Map<String, String> tags,
            final byte[] tsuid) {

        sendSocket(makeMetricName(metric, tags), timestamp, value);

        return new Deferred<Object>();
    }

    /*
     * a skyline metric name will be in the format of
     * <metric>.<tag1_key>_<tag1_value>.<tag2_key>_<tag2_value>
     */
    private String makeMetricName(String metric, Map<String, String> tags) {

        String metricName = metric;

        SortedSet<String> keys = new TreeSet<String>(tags.keySet());
        for (String key : keys) {
            metricName = metricName.concat("." + key + "_" + tags.get(key));
        }

        return metricName;
    }

    //Sends the data to the skyline server
    private void sendSocket(String skylineMetricName, final long timestamp, final double value) {
        try {

            // Build the neeeded structure for skyline/msgpack
            // [ metricName, [timestamp, value]]
            List metricList = new ArrayList();
            List datapointList = new ArrayList();

            datapointList.add(timestamp);
            datapointList.add(value);

            metricList.add(skylineMetricName);
            metricList.add(datapointList);

            byte[] data = msgpack.write(metricList);
            DatagramPacket packet = new DatagramPacket(data, data.length, skylineIa, skylinePort);

            //send it!
            udpSocket.send(packet);
        } catch (IOException e) {
            LOG.error("IOException in SkylinePublisher send");
        }

    }
}
