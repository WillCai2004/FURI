/*
 * Copyright 2010 Aalto University, ComNet
 * Released under GPLv3. See LICENSE.txt for details.
 */
package routing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import core.Connection;
import core.DTNHost;
import core.Message;
import core.MessageListener;
import core.Settings;
import core.SimClock;
import core.SimError;

/**
 * PRoPHET router with local, per-neighbor observation logging.
 */
public class InstrumentedProphetRouter extends ProphetRouter {
	/** Settings namespace ({@value}). */
	public static final String INSTRUMENTED_NS = "InstrumentedProphetRouter";
	/** Window size in seconds -setting id ({@value}). */
	public static final String WINDOW_SIZE_S = "windowSize";
	/** Log directory -setting id ({@value}). */
	public static final String LOG_DIR_S = "logDir";

	private static final String NORMAL_PREFIX = "M";
	private static final String FLOOD_PREFIX = "F";

	private final Map<DTNHost, NeighborStats> neighborStats;
	private final Map<DTNHost, Double> activeContacts;

	private double windowSize;
	private double windowStart;
	private String logDir;
	private File logFile;

	private long bufferBytesSum;
	private long bufferBytesMax;
	private long bufferSamples;
	private long dropBufferNormal;
	private long dropBufferFlood;

	public InstrumentedProphetRouter(Settings s) {
		super(s);
		this.neighborStats = new HashMap<DTNHost, NeighborStats>();
		this.activeContacts = new HashMap<DTNHost, Double>();

		Settings instrumentedSettings = new Settings(INSTRUMENTED_NS);
        Settings is = new Settings(INSTRUMENTED_NS);
        this.windowSize = is.contains(WINDOW_SIZE_S) ? is.getDouble(WINDOW_SIZE_S) : 300.0;
        this.logDir = is.contains(LOG_DIR_S) ? is.getSetting(LOG_DIR_S) : "logs";

	}

	protected InstrumentedProphetRouter(InstrumentedProphetRouter r) {
		super(r);
		this.neighborStats = new HashMap<DTNHost, NeighborStats>();
		this.activeContacts = new HashMap<DTNHost, Double>();
		this.windowSize = r.windowSize;
		this.logDir = r.logDir;
	}

	@Override
	public void init(DTNHost host, List<MessageListener> mListeners) {
		super.init(host, mListeners);
		this.windowStart = SimClock.getTime();
		this.bufferBytesSum = 0;
		this.bufferBytesMax = 0;
		this.bufferSamples = 0;
		this.dropBufferNormal = 0;
		this.dropBufferFlood = 0;

		File logDirectory = new File(logDir);
		if (!logDirectory.exists() && !logDirectory.mkdirs()) {
			throw new SimError("Could not create log directory " + logDir);
		}
		this.logFile = new File(logDirectory,
				"node_" + host.getAddress() + ".csv");
		writeHeaderIfNeeded();
	}

	@Override
	public void changedConnection(Connection con) {
		super.changedConnection(con);

		DTNHost other = con.getOtherNode(getHost());
		if (con.isUp()) {
			NeighborStats stats = getStats(other);
			stats.contacts++;
			activeContacts.put(other, SimClock.getTime());
		} else {
			Double started = activeContacts.remove(other);
			if (started != null) {
				NeighborStats stats = getStats(other);
				stats.contactTime += SimClock.getTime() - started;
			}
		}
	}

	@Override
	protected int startTransfer(Message m, Connection con) {
		DTNHost other = con.getOtherNode(getHost());
		NeighborStats stats = getStats(other);
		if (isFloodMessage(m)) {
			stats.txOfferFlood++;
		} else if (isNormalMessage(m)) {
			stats.txOfferNormal++;
		}
		return super.startTransfer(m, con);
	}

	@Override
	protected void transferDone(Connection con) {
		Message m = con.getMessage();
		if (m != null) {
			NeighborStats stats = getStats(con.getOtherNode(getHost()));
			if (isFloodMessage(m)) {
				stats.txOkFlood++;
			} else if (isNormalMessage(m)) {
				stats.txOkNormal++;
			}
		}
	}

	@Override
	protected void transferAborted(Connection con) {
		Message m = con.getMessage();
		if (m != null) {
			NeighborStats stats = getStats(con.getOtherNode(getHost()));
			if (isFloodMessage(m)) {
				stats.txAbortFlood++;
			} else if (isNormalMessage(m)) {
				stats.txAbortNormal++;
			}
		}
	}

	@Override
	public Message messageTransferred(String id, DTNHost from) {
		Message m = super.messageTransferred(id, from);
		NeighborStats stats = getStats(from);
		if (isFloodMessage(m)) {
			stats.rxFlood++;
		} else if (isNormalMessage(m)) {
			stats.rxNormal++;
		}
		return m;
	}

	@Override
	public void deleteMessage(String id, boolean drop) {
		Message removed = getMessage(id);
		super.deleteMessage(id, drop);
		if (drop && removed != null) {
			if (isFloodMessage(removed)) {
				dropBufferFlood++;
			} else if (isNormalMessage(removed)) {
				dropBufferNormal++;
			}
		}
	}

	@Override
	public void update() {
		super.update();
		recordBufferSample();
		flushWindowsIfNeeded();
	}

	@Override
	public MessageRouter replicate() {
		return new InstrumentedProphetRouter(this);
	}

	private NeighborStats getStats(DTNHost host) {
		NeighborStats stats = neighborStats.get(host);
		if (stats == null) {
			stats = new NeighborStats();
			neighborStats.put(host, stats);
		}
		return stats;
	}

	private void recordBufferSample() {
		long occupancy = getBufferOccupancy();
		bufferBytesSum += occupancy;
		bufferSamples++;
		if (occupancy > bufferBytesMax) {
			bufferBytesMax = occupancy;
		}
	}

	private long getBufferOccupancy() {
		if (getBufferSize() == Integer.MAX_VALUE) {
			long occupancy = 0;
			for (Message m : getMessageCollection()) {
				occupancy += m.getSize();
			}
			return occupancy;
		}
		return getBufferSize() - getFreeBufferSize();
	}

	private void flushWindowsIfNeeded() {
		double now = SimClock.getTime();
		while (now >= windowStart + windowSize) {
			double windowEnd = windowStart + windowSize;
			recordOngoingContactTime(windowEnd);
			writeWindow(windowStart, windowEnd);
			resetWindow(windowEnd);
		}
	}

	private void recordOngoingContactTime(double windowEnd) {
		for (Map.Entry<DTNHost, Double> entry : activeContacts.entrySet()) {
			NeighborStats stats = getStats(entry.getKey());
			stats.contactTime += windowEnd - entry.getValue();
			entry.setValue(windowEnd);
		}
	}

	private void writeWindow(double windowStartTime, double windowEndTime) {
		double bufferAvg = bufferSamples > 0
				? (double) bufferBytesSum / bufferSamples
				: 0.0;

		try (BufferedWriter writer = new BufferedWriter(
				new FileWriter(logFile, true))) {
			for (Map.Entry<DTNHost, NeighborStats> entry :
					neighborStats.entrySet()) {
				NeighborStats stats = entry.getValue();
				writer.write(String.format(
						"%d,%d,%.0f,%.0f,%d,%.0f,%d,%d,%d,%d,%d,%d,%d,%d,%.2f,%d,%d,%d%n",
						getHost().getAddress(),
						entry.getKey().getAddress(),
						windowStartTime,
						windowEndTime,
						stats.contacts,
						stats.contactTime,
						stats.txOfferNormal,
						stats.txOkNormal,
						stats.txAbortNormal,
						stats.rxNormal,
						stats.txOfferFlood,
						stats.txOkFlood,
						stats.txAbortFlood,
						stats.rxFlood,
						bufferAvg,
						bufferBytesMax,
						dropBufferNormal,
						dropBufferFlood));
			}
		} catch (IOException e) {
			throw new SimError("Could not write log file " + logFile, e);
		}
	}

	private void resetWindow(double nextWindowStart) {
		neighborStats.clear();
		windowStart = nextWindowStart;
		bufferBytesSum = 0;
		bufferBytesMax = 0;
		bufferSamples = 0;
		dropBufferNormal = 0;
		dropBufferFlood = 0;
	}

	private void writeHeaderIfNeeded() {
		if (logFile.exists() && logFile.length() > 0) {
			return;
		}
		try (BufferedWriter writer = new BufferedWriter(
				new FileWriter(logFile, true))) {
			writer.write("observer,neighbor,window_start,window_end,");
			writer.write("contacts,contact_time,");
			writer.write("tx_offer_M,tx_ok_M,tx_abort_M,rx_M,");
			writer.write("tx_offer_F,tx_ok_F,tx_abort_F,rx_F,");
			writer.write("buf_bytes_avg,buf_bytes_max,");
			writer.write("drop_buf_M,drop_buf_F");
			writer.newLine();
		} catch (IOException e) {
			throw new SimError("Could not write header to log file " + logFile,
					e);
		}
	}

	private boolean isFloodMessage(Message m) {
		return m != null && m.getId().startsWith(FLOOD_PREFIX);
	}

	private boolean isNormalMessage(Message m) {
		return m != null && m.getId().startsWith(NORMAL_PREFIX);
	}

	private static class NeighborStats {
		private long contacts;
		private double contactTime;
		private long txOfferNormal;
		private long txOkNormal;
		private long txAbortNormal;
		private long rxNormal;
		private long txOfferFlood;
		private long txOkFlood;
		private long txAbortFlood;
		private long rxFlood;
	}
}