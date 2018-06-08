package com.zendesk.maxwell.producer;

import java.util.HashMap;

import com.zendesk.maxwell.row.RowMap;

public class MaxwellKinesisStreamMapper {
	private final HashMap<String, String> streamMap = new HashMap<>();
	private String defaultKinesisStream;

	public MaxwellKinesisStreamMapper(String kinesisMultiStreams) {
		for (String stream : kinesisMultiStreams.split(",")) {
			String[] map = stream.split(":");
			if (this.defaultKinesisStream == null)
				this.defaultKinesisStream = map[0];
			streamMap.put(map[1], map[0]);
		}
	}

	public String getStream(RowMap r) {
		String key = r.getDatabase() + "." + r.getTable();
		if ( streamMap.containsKey(key) )
			return streamMap.get(key);

		return this.defaultKinesisStream;
	}
}
