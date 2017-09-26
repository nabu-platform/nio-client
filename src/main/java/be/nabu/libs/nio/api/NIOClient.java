package be.nabu.libs.nio.api;

import java.io.IOException;
import java.util.concurrent.Future;

public interface NIOClient extends NIOServer {
	public Future<Pipeline> connect(String host, Integer port) throws IOException;
}
