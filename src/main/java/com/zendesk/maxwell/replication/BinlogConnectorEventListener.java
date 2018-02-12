package com.zendesk.maxwell.replication;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashMap;

class BinlogConnectorEventListener implements BinaryLogClient.EventListener {
	private static final Logger LOGGER = LoggerFactory.getLogger(BinlogConnectorEventListener.class);

	private final BlockingQueue<BinlogConnectorEvent> queue;
	private final Timer queueTimer;
	protected final AtomicBoolean mustStop = new AtomicBoolean(false);
	private final BinaryLogClient client;
	private long replicationLag;
	private String gtid;
	private MaxwellFilter filter;
	protected final String maxwellSchemaDatabaseName;
	private final HashMap<Long, String[]> tableCache = new HashMap<>();

	public BinlogConnectorEventListener(
		BinaryLogClient client,
		BlockingQueue<BinlogConnectorEvent> q,
		MaxwellFilter f,
		String maxwellSchemaDatabaseName,
		Metrics metrics) {
		this.client = client;
		this.queue = q;
		this.filter = f;
		this.maxwellSchemaDatabaseName = maxwellSchemaDatabaseName;
		this.queueTimer =  metrics.getRegistry().timer(metrics.metricName("replication", "queue", "time"));

		final BinlogConnectorEventListener self = this;
		metrics.register(metrics.metricName("replication", "lag"), (Gauge<Long>) () -> self.replicationLag);
	}

	public void stop() {
		mustStop.set(true);
	}

	@Override
	public void onEvent(Event event) {
		long eventSeenAt = 0;
		boolean trackMetrics = false;

		if (event.getHeader().getEventType() == EventType.GTID) {
			gtid = ((GtidEventData)event.getData()).getGtid();
		}

		BinlogConnectorEvent ep = new BinlogConnectorEvent(event, client.getBinlogFilename(), client.getGtidSet(), gtid);

		switch (ep.getType()) {
			case WRITE_ROWS:
			case EXT_WRITE_ROWS:
			case UPDATE_ROWS:
			case EXT_UPDATE_ROWS:
			case DELETE_ROWS:
			case EXT_DELETE_ROWS:
				String[] table = tableCache.get(ep.getTableID());
				if ( table != null && shouldOutputEvent(table[0], table[1], filter) ) {
					break;
				}
				return;
			case TABLE_MAP:
				TableMapEventData data = ep.tableMapData();
				String[] tbl = { data.getDatabase(), data.getTable() };
				tableCache.put(data.getTableId(), tbl);
				break;
			case ROTATE:
				tableCache.clear();
				break;
			default:
				break;
		}

		if (ep.isCommitEvent()) {
			trackMetrics = true;
			eventSeenAt = System.currentTimeMillis();
			replicationLag = eventSeenAt - event.getHeader().getTimestamp();
		}

		while (mustStop.get() != true) {
			try {
				if ( queue.offer(ep, 100, TimeUnit.MILLISECONDS ) ) {
					break;
				}
			} catch (InterruptedException e) {
				return;
			}
		}

		if (trackMetrics) {
			queueTimer.update(System.currentTimeMillis() - eventSeenAt, TimeUnit.MILLISECONDS);
		}
	}

	protected boolean shouldOutputEvent(String database, String table, MaxwellFilter filter) {
		Boolean isSystemWhitelisted = this.maxwellSchemaDatabaseName.equals(database)
			&& "bootstrap".equals(table);

		if ( MaxwellFilter.isSystemBlacklisted(database, table) )
			return false;
		else if ( isSystemWhitelisted )
			return true;
		else
			return MaxwellFilter.matches(filter, database, table);
	}
}
