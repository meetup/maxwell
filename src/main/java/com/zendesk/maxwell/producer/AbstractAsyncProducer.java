package com.zendesk.maxwell.producer;

import com.codahale.metrics.Gauge;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;

import java.util.concurrent.TimeUnit;

public abstract class AbstractAsyncProducer extends AbstractProducer {

	public class CallbackCompleter {
		private InflightMessageList inflightMessages;
		private final MaxwellContext context;
		private final int rowId;
		private final boolean isTXCommit;

		public CallbackCompleter(InflightMessageList inflightMessages, int rowId, boolean isTXCommit, MaxwellContext context) {
			this.inflightMessages = inflightMessages;
			this.context = context;
			this.rowId = rowId;
			this.isTXCommit = isTXCommit;
		}

		public void markCompleted() {
			if(isTXCommit) {
				InflightMessageList.InflightTXMessage message = inflightMessages.completeTXMessage(rowId);

				if (message != null) {
					context.setPosition(message.position);
					metricsTimer.update(message.timeSinceSendMS(), TimeUnit.MILLISECONDS);
				}
			} else {
				inflightMessages.completeNonTXMessage(rowId);
			}
		}
	}

	private InflightMessageList inflightMessages;

	public AbstractAsyncProducer(MaxwellContext context) {
		super(context);

		this.inflightMessages = new InflightMessageList(context);

		Metrics metrics = context.getMetrics();
		String gaugeName = metrics.metricName("inflightmessages", "count");
		metrics.register(gaugeName, (Gauge<Long>) () -> (long) inflightMessages.size());
	}

	public abstract void sendAsync(RowMap r, CallbackCompleter cc) throws Exception;

	@Override
	public final void push(RowMap r) throws Exception {
		if(r.isTXCommit()) {
			Position position = r.getPosition();

			inflightMessages.addTXMessage(r.getRowId(), position);

			// Rows that do not get sent to a target will be automatically marked as complete.
			// We will attempt to commit a checkpoint up to the current row.
			if(!r.shouldOutput(outputConfig)) {
				InflightMessageList.InflightTXMessage completed = inflightMessages.completeTXMessage(r.getRowId());
				if(completed != null) {
					context.setPosition(completed.position);
				}
				return;
			}
		} else {
			inflightMessages.addNonTXMessage(r.getRowId());
		}

		CallbackCompleter cc = new CallbackCompleter(inflightMessages, r.getRowId(), r.isTXCommit(), context);

		sendAsync(r, cc);
	}
}
