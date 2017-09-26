package be.nabu.libs.nio.api;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface NIOConnector {
	public SocketChannel connect(NIOClient client, String host, Integer port) throws IOException;
	public void tunnel(NIOClient client, String host, Integer port, Pipeline pipeline) throws IOException;
}
