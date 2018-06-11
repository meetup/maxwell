package com.zendesk.maxwell.producer;

import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zendesk.maxwell.row.RowMap;

public class MaxwellKinesisStreamMapper {
	static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKinesisStreamMapper.class);
	private final HashMap<String, String> streamMap = new HashMap<>();
	private String defaultKinesisStream;

	public MaxwellKinesisStreamMapper(String kinesisMultiStreams) {
		for (String stream : kinesisMultiStreams.split(",")) {
			String[] map = stream.split(":");
			if ( map == null || map.length != 2 || map[0].isEmpty() || map[1].isEmpty() ) {
				LOGGER.error("stream mapping not in correct format, skipping: " + stream);
				continue;
			}
			if (this.defaultKinesisStream == null) {
				this.defaultKinesisStream = map[0];
				LOGGER.info("Setting default kinesis stream to " + this.defaultKinesisStream);
			}
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
