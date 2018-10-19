package com.zendesk.maxwell.producer;
/* respresents a list of inflight messages -- stuff being sent over the
   network, that may complete in any order.  Allows for only bumping
   the binlog position upon completion of the oldest outstanding item.

   Assumes .addInflight(position) will be call monotonically.
   */

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

public class InflightMessageList {

	class InflightTXMessage {
		public final Position position;
		public boolean isComplete;
		public final long sendTimeMS;
		public final long eventTimeMS;

		InflightTXMessage(Position p, long eventTimeMS) {
			this.position = p;
			this.isComplete = false;
			this.sendTimeMS = System.currentTimeMillis();
			this.eventTimeMS = eventTimeMS;
		}

		long timeSinceSendMS() {
			return System.currentTimeMillis() - sendTimeMS;
		}
	}

	private static final long INIT_CAPACITY = 1000;
	private static final double COMPLETE_PERCENTAGE_THRESHOLD = 0.9;

	private final LinkedHashMap<Integer, InflightTXMessage> txMessages;
	private final LinkedHashSet<Integer> nonTXMessages;
	private final MaxwellContext context;
	private final long capacity;
	private final long nonTXCapacity;
	private final long producerAckTimeoutMS;
	private final double completePercentageThreshold;
	private volatile boolean isFullTX;
	private volatile boolean isFullNonTX;

	public InflightMessageList(MaxwellContext context) {
		this(context, INIT_CAPACITY, COMPLETE_PERCENTAGE_THRESHOLD);
	}

	public InflightMessageList(MaxwellContext context, long capacity, double completePercentageThreshold) {
		this.context = context;
		this.producerAckTimeoutMS = context.getConfig().producerAckTimeout;
		this.completePercentageThreshold = completePercentageThreshold;
		this.txMessages = new LinkedHashMap<>();
		this.nonTXMessages = new LinkedHashSet<>();
		this.capacity = capacity;
		this.nonTXCapacity = capacity * 100;
	}

	public void addTXMessage(int rowId, Position p, long eventTimestampMillis) throws InterruptedException {
		synchronized (this.txMessages) {
			while (isFullTX) {
				this.txMessages.wait();
			}

			InflightTXMessage m = new InflightTXMessage(p, eventTimestampMillis);
			this.txMessages.put(rowId, m);

			if (txMessages.size() >= capacity) {
				isFullTX = true;
			}
		}
	}

	public void addNonTXMessage(int rowId) throws InterruptedException {
		synchronized (this.nonTXMessages) {
			while (isFullNonTX) {
				this.nonTXMessages.wait();
			}

			this.nonTXMessages.add(rowId);

			if (nonTXMessages.size() >= nonTXCapacity) {
				isFullNonTX = true;
			}
		}
	}

	/* returns the position that stuff is complete up to, or null if there were no changes */
	public InflightTXMessage completeTXMessage(int rowId) {
		synchronized (this.txMessages) {
			InflightTXMessage m = this.txMessages.get(rowId);
			assert(m != null);

			m.isComplete = true;

			InflightTXMessage completeUntil = null;
			Iterator<InflightTXMessage> iterator = iterator();

			while ( iterator.hasNext() ) {
				InflightTXMessage msg = iterator.next();
				if ( !msg.isComplete ) {
					break;
				}

				completeUntil = msg;
				iterator.remove();
			}

			if (isFullTX && txMessages.size() < capacity) {
				isFullTX = false;
				this.txMessages.notify();
			}

			// If the head is stuck for the length of time (configurable) and majority of the messages have completed,
			// we assume the head will unlikely get acknowledged, hence terminate Maxwell.
			// This gatekeeper is the last resort since if anything goes wrong,
			// producer should have raised exceptions earlier than this point when all below conditions are met.
			if (producerAckTimeoutMS > 0 && isFullTX) {
				Iterator<InflightTXMessage> it = iterator();
				if (it.hasNext() && it.next().timeSinceSendMS() > producerAckTimeoutMS && completePercentage() >= completePercentageThreshold) {
					context.terminate(new IllegalStateException(
							"Did not receive acknowledgement for the head of the inflight message list for " + producerAckTimeoutMS + " ms"));
				}
			}

			return completeUntil;
		}
	}

	public void completeNonTXMessage(int rowId) {
		synchronized (this.nonTXMessages) {
			nonTXMessages.remove(rowId);
			if (isFullNonTX && nonTXMessages.size() < nonTXCapacity) {
				isFullNonTX = false;
				this.nonTXMessages.notify();
			}

			return;
		}
	}

	public int size() {
		return txMessages.size() + nonTXMessages.size();
	}

	private double completePercentage() {
		long completed = txMessages.values().stream().filter(m -> m.isComplete).count();
		return completed / ((double) txMessages.size());
	}

	private Iterator<InflightTXMessage> iterator() {
		return this.txMessages.values().iterator();
	}
}
