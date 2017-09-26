package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import be.nabu.libs.nio.api.NIOClient;
import be.nabu.libs.nio.api.NIOConnector;
import be.nabu.libs.nio.api.Pipeline;

public class NIODirectConnector implements NIOConnector {

	@Override
	public SocketChannel connect(NIOClient client, String host, Integer port) throws IOException {
		InetSocketAddress serverAddress = new InetSocketAddress(host, port);
		SocketChannel channel = SocketChannel.open();
	    channel.configureBlocking(false);
	    channel.connect(serverAddress);
	    return channel;
	}

	@Override
	public void tunnel(NIOClient client, String host, Integer port, Pipeline pipeline) {
		// do nothing
	}
}
